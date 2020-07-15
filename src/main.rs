#![deny(clippy::all)]
#![deny(clippy::nursery)]

use anyhow::{bail, Result};
use clap::{load_yaml, App, ArgMatches};
use log::{error, info, trace};
use notify::RawEvent;
use prometheus::IntGauge;
use signal_hook::iterator::Signals;
use std::{
    sync::{mpsc, Arc},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    sync::mpsc::{channel, Sender},
    task,
    time::delay_for,
};

mod fib_retry;
mod prom;
mod s3;
mod watcher;
mod web;

use crate::prom::*;

#[tokio::main]
async fn main() {
    // initialise logging.  set log level with environment var RUST_LOG={trace,debug,info,warn,error}
    pretty_env_logger::init();

    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let dir = matches
        .value_of("directory")
        .expect("Watch directory not supplied");

    let wp = std::path::PathBuf::from(dir)
        .canonicalize()
        .expect("cannot canonicalize the watch directory.");
    info!("Watching directory: {}", wp.display());

    let (default_bucket, map) =
        get_directory_to_bucket_map(&matches).expect("default bucket not supplied.");
    let uploader = Arc::new(s3::S3Uploader::new(wp.clone(), default_bucket, map));

    initialise_prometheus_metrics();
    // Start the webserver.  We'll await it a bit later.
    let (mut tx_web, mut rx_web) = channel::<()>(1);
    let web_future = task::spawn(async move {
        web::serve(&mut rx_web).await;
    });

    // Create a channel to receive the IO events.
    let (tx_io, rx_io) = mpsc::channel();

    // special processing for process signals, so we don't abort in the middle of an upload
    let tx = tx_io.clone();
    let signal_future = task::spawn(async move { wait_signal(&mut tx_web, tx).await });

    watcher::clear_current_files(uploader.clone()).await;

    // if we are not running as a daemon, then stop right here.
    if !matches.is_present("daemon") {
        return;
    }

    info!("Starting daemon.");
    // Wait for one  branch to complete.
    // For the web server this is when a sigint/sighup results in wait_signal sending a message to the receive channel.
    // For the directory watcher, this will be when the signal handler sends it an abort error.
    // For the signal handler, this is only after a sigterm or sighup have been received.
    tokio::select! {
        _ = web_future => {
            info!("Exiting: Web server shutdown.")
        }
        _ = watcher::watch_dir(uploader.clone(), rx_io, tx_io) => {
            info!("Exiting: Watcher Abort.")
        }
        _ = signal_future => {
            info!("Exiting: Singal.")
        }
    };
}

// set all the prometheus metrics so that on first scrape we have zero values
fn initialise_prometheus_metrics() {
    IN_FLIGHT_UPLOADS.set(0);
    FAILED_UPLOADS.reset();
    FAILED_DELETES.reset();
    COMPLETED_UPLOADS.reset();
    FAILED_ATTEMPTS.reset();
    BYTES_TRANSFERRED.reset();
}

// returns a tuple of default bucket to write to, and a Vector of subdirectory to alternate bucket.
fn get_directory_to_bucket_map(m: &ArgMatches) -> Result<(String, Vec<(String, String)>)> {
    let full_map = m
        .values_of("map")
        .expect("map values don't exist")
        .map(|kv| {
            let vec: Vec<&str> = kv.split('=').collect();
            (vec[0].to_string(), vec[1].to_string())
        });

    // get the default bucket to write to.
    // the default bucket is the directory `.`  this equates to args of:
    // `-m .=somebucket`, and must be passed from the command line.
    let default_bucket = full_map
        .clone()
        .filter(|(k, _)| *k == ".")
        .map(|(_, v)| v)
        .collect();

    // if the default bucket is not found, this is an error.
    if default_bucket == "" {
        bail!("No default bucket provided");
    }

    // the directory -> bucket map is everything that does not include the default dir -> bucket mapping.
    let bucket_map = full_map.filter(|(k, _)| *k != ".").collect();

    Ok((default_bucket, bucket_map))
}

// wait for a SIGINT or SIGHUP and then ensure everything shuts down gracefully.
async fn wait_signal(tx_web: &mut Sender<()>, tx_watcher: mpsc::Sender<RawEvent>) -> Result<()> {
    let signals = Signals::new(&[signal_hook::SIGINT, signal_hook::SIGHUP])?;
    for signal in &signals {
        match signal {
            signal_hook::SIGINT | signal_hook::SIGHUP => {
                info!("Signal Received.");

                // wait on there being no in flight requests.
                wait_for_completion_of_in_flight_uploads(
                    Duration::from_secs(1),
                    Duration::from_secs(3),
                    Duration::from_secs(30),
                    &IN_FLIGHT_UPLOADS,
                )
                .await?;

                // shutdown the watcher
                tx_watcher.send(RawEvent {
                    path: None,
                    op: Err(notify::Error::Generic("Abort".to_string())),
                    cookie: None,
                })?;

                // shut down the web server
                let _ = tx_web.send(()).await;
                break;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[derive(Error, Debug, PartialEq)]
enum WaitError {
    #[error("exceeded graceful shutdown period: '{:?}'", .0)]
    Timeout(Duration),
}

// ? Not sure how I can still use start_graceful_time after assigning it to current_quiet.
// ? Is this because of the Copy trait?
//
// @freq: how often to check the metric (sleep between checks)
// @quiet_for: how long the metric must be 0 before we can consider that there are no further uploads to be made
// @timeout: how long is too long to wait.
// @metric: prometheus metric representing in flight uploads
async fn wait_for_completion_of_in_flight_uploads(
    freq: Duration,
    quiet_for: Duration,
    timeout: Duration,
    metric: &IntGauge,
) -> Result<Duration, WaitError> {
    // set our starting time.
    let start_graceful_time = Instant::now();
    let mut current_quiet = start_graceful_time;

    // quit if the most recent quite time exeeds the requested quiet period.
    while current_quiet.elapsed() < quiet_for {
        // * return a timeout error if we have waited past the requested timeout.
        exceeded_timeout(&start_graceful_time, &timeout)?;

        // we are still trying to load files if the passed in metric is greater than 0
        while metric.get() > 0 {
            // * return a timeout error if we have waited past the requested timeout.
            exceeded_timeout(&start_graceful_time, &timeout)?;

            trace!("awaiting in flight requests to complete.");
            delay_for(freq).await;
            current_quiet = Instant::now();
        }
        trace!("awaiting graceperiod.");
        delay_for(freq).await;
    }
    Ok(start_graceful_time.elapsed())
}

// checks if the time elapsed since the instant was created exceeds a defined duration
// ? Is this the correct way to name this method?
// ? Alternatives:
// ?   * has_exceeded_timeout
// ?   * error_if_timeout_exceeded
// ? The only reason this makes sense is because of the question mark operator.
// ? It looks like it's asking a question, so reads nicely.
fn exceeded_timeout(s: &Instant, max: &Duration) -> Result<(), WaitError> {
    if s.elapsed() > *max {
        error!("timeout exceeded while shutting down");
        return Err(WaitError::Timeout(*max));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{self, register_int_gauge, IntGauge};

    #[tokio::test]
    async fn get_map_returns_empty_vector_if_only_default_is_defined() {
        // * get_directory_to_bucket_map should return the default bucket, and an empty vector if
        // * only the default was specified.

        let yaml = load_yaml!("cli.yaml");
        let matches = App::from_yaml(yaml).get_matches_from(vec![
            "uploader",
            "/tmp/test",
            "-m",
            ".=test_bucket",
        ]);
        let (db, map) = get_directory_to_bucket_map(&matches).unwrap();
        assert_eq!(map.len(), 0);
        assert_eq!(db, "test_bucket");
    }

    #[tokio::test]
    async fn get_map_returns_non_empty_vector() {
        // * get_directory_to_bucket_map should return the default bucket, and a vector if
        // * subdirectories are specified.

        let yaml = load_yaml!("cli.yaml");
        let matches = App::from_yaml(yaml).get_matches_from(vec![
            "uploader",
            "/tmp/test",
            "-m",
            ".=test_bucket",
            "-m",
            "sub=jedi",
        ]);
        let (db, map) = get_directory_to_bucket_map(&matches).unwrap();
        assert_eq!(db, "test_bucket");
        assert_eq!(map.len(), 1);
        assert_eq!(map[0], ("sub".to_string(), "jedi".to_string()));
    }

    #[test]
    fn get_map_returns_error_if_no_default_bucket_provided() {
        // * get_directory_to_bucket_map should bail if no default bucket provided

        let yaml = load_yaml!("cli.yaml");
        let matches =
            App::from_yaml(yaml).get_matches_from(vec!["uploader", "/tmp/test", "-m", "sub=jedi"]);
        let err = get_directory_to_bucket_map(&matches).unwrap_err(); // should panic here if not an error
        assert_eq!(err.to_string(), "No default bucket provided");
    }

    #[tokio::test]
    async fn wait_times_out() {
        // * wait_for_completion_of_in_flight_uploads should error if the timeout period is exceeded.

        let test_metric_timeout: IntGauge =
            register_int_gauge!("test_metric_timeout", "Number of in flight uploads.").unwrap();
        test_metric_timeout.set(1);

        let actual = wait_for_completion_of_in_flight_uploads(
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(50),
            &test_metric_timeout,
        )
        .await;

        assert_eq!(
            actual.unwrap_err(),
            WaitError::Timeout(Duration::from_millis(50))
        )
    }

    #[tokio::test]
    async fn wait_returns_after_in_flight_finishes() {
        // * wait_for_completion_of_in_flight_uploads should return after all in flight uploads are complete.

        let test_metric_returns: IntGauge =
            register_int_gauge!("test_metric_returns", "Number of in flight uploads.").unwrap();

        test_metric_returns.set(1);

        let future = wait_for_completion_of_in_flight_uploads(
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(50),
            &test_metric_returns,
        );
        delay_for(Duration::from_millis(10)).await;
        test_metric_returns.set(0);

        let actual = future.await; // ! should this be on a timer?

        actual.unwrap(); // this will error unless the call was successful.
    }

    #[tokio::test]
    async fn wait_ensures_quiet_period() {
        // * wait_for_completion_of_in_flight_uploads should not return before the quiet period

        let test_metric_quiet: IntGauge =
            register_int_gauge!("test_metric_quiet", "Number of in flight uploads.").unwrap();
        test_metric_quiet.set(0);

        let future = wait_for_completion_of_in_flight_uploads(
            Duration::from_millis(5),
            Duration::from_millis(30),
            Duration::from_millis(200),
            &test_metric_quiet,
        );

        let actual = future.await;

        assert!(actual.unwrap() >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn wait_ensures_quiet_period_and_await_in_flight() {
        // * wait_for_completion_of_in_flight_uploads should wait for in flight requests to finish, and then
        // * ensure the quiet period is observed.

        let test_metric_no_timeout: IntGauge =
            register_int_gauge!("test_metric_no_timeout", "Number of in flight uploads.").unwrap();
        test_metric_no_timeout.set(1);

        let metric = test_metric_no_timeout.clone();
        let future = task::spawn(async move {
            wait_for_completion_of_in_flight_uploads(
                Duration::from_millis(20),
                Duration::from_millis(300),
                Duration::from_millis(20000),
                &metric,
            )
            .await
        });
        task::yield_now().await;
        delay_for(Duration::from_millis(300)).await;
        test_metric_no_timeout.set(0);
        delay_for(Duration::from_millis(200)).await;
        test_metric_no_timeout.set(1);
        delay_for(Duration::from_millis(100)).await;
        test_metric_no_timeout.set(0);

        let actual = future.await;

        dbg!(&actual);
        assert!(actual.unwrap().unwrap() >= Duration::from_millis(900));
    }
}
