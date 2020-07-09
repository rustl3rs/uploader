use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::info;
use prometheus::{self, Encoder, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// metrics endpoint: displays text output in Prometheus compatible form.
async fn metrics(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = prometheus::gather();

    // Encode them to send.
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("failed to encode Prometheus metrics.");

    let output = String::from_utf8(buffer).expect("failed to convert buff to string.");

    // respond with the metrics.
    Ok(Response::new(output.into()))
}

// create a webserver and serve incoming requests unti @signal is sent.
pub async fn serve(signal: &mut Receiver<()>) {
    info!("Starting metrics server");

    // ! make both the bind interface and port configurable.
    // We'll bind to 127.0.0.1:3000
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // A `Service` is needed for every connection, so this
    // creates one from our `metrics` function.
    let make_svc = make_service_fn(|_conn| async {
        // service_fn converts our function into a `Service`
        Ok::<_, Infallible>(service_fn(metrics))
    });

    let server = Server::bind(&addr).serve(make_svc);

    let graceful = server.with_graceful_shutdown(async {
        signal.recv().await;
    });

    // Run this server for... forever!  Well, at least until we receive a signal to shutdown.
    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
}
