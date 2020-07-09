use log::{debug, info, warn};
use notify::{op, raw_watcher, RawEvent, RecursiveMode, Watcher};
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};
use tokio::task;
use walkdir::{DirEntry, WalkDir};

use crate::prom::*;
use crate::s3;

pub async fn watch_dir(
    uploader: Arc<dyn s3::RetryMove>,
    receiver: Receiver<RawEvent>,
    transmitter: Sender<RawEvent>,
) {
    info!("Watching");

    // Create a watcher object, delivering raw events.
    // The notification back-end is selected based on the platform.
    let mut watcher = raw_watcher(transmitter).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher
        .watch(uploader.get_base_directory(), RecursiveMode::Recursive)
        .unwrap();

    loop {
        match receiver.recv() {
            Ok(RawEvent {
                path: Some(path),
                op: Ok(op),
                cookie,
            }) => {
                if op.contains(op::WRITE) && !op.contains(op::REMOVE) {
                    // WRITES are the operations we care about.
                    info!("{:?} {:?} ({:?})", op, path, cookie);
                    let u = uploader.clone();
                    task::spawn(async move {
                        IN_FLIGHT_UPLOADS.inc();
                        let pb = path.canonicalize().unwrap();
                        u.retry_move(pb).await;
                        IN_FLIGHT_UPLOADS.dec();
                    });
                } else {
                    debug!("Ignoring event: {:?} {:?} ({:?})", op, path, cookie);
                }
            }
            Ok(RawEvent {
                path: None,
                op: Err(err),
                cookie: None,
            }) => match err {
                notify::Error::Generic(e) => {
                    if e == "Abort" {
                        return;
                    }
                }
                _ => warn!("{}", err),
            },
            Ok(event) => {
                debug!("broken event: {:?}", event);
            }
            Err(e) => {
                warn!("watch error: {:?}", e);
            }
        }
    }
}

// https://rust-lang-nursery.github.io/rust-cookbook/file/dir.html
// transfer existing files.
// Recursively, for every file found the base directory, transfer it.
// Do this as much in parallel as is possible.
pub async fn clear_current_files(uploader: Arc<dyn s3::RetryMove>) {
    let local = task::LocalSet::new();

    let wd = uploader.get_base_directory();

    WalkDir::new(wd)
        .into_iter()
        .filter_entry(|e| is_not_hidden(e)) // no hidden files
        .filter_map(|v| v.ok())
        .filter(|e| !e.file_type().is_dir()) // no directories
        .for_each(|entry| {
            let canonicalized_path = entry
                .into_path()
                .canonicalize()
                .expect("cannot canonicalize path");
            debug!("queue {:?}", &canonicalized_path);
            // spawn a new task to move the file to s3.
            let u = uploader.clone();
            local.spawn_local(async move {
                IN_FLIGHT_UPLOADS.inc();
                u.retry_move(canonicalized_path).await;
                IN_FLIGHT_UPLOADS.dec();
            });
        });

    // wait for all the tasks that we just spawned to finish.
    local.await;
}

// https://rust-lang-nursery.github.io/rust-cookbook/file/dir.html
/// Allows the filtering out of hidden files (those beginning with `.`)
fn is_not_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| entry.depth() == 0 || !s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use notify::Op;
    use s3::RetryMove;
    use std::{
        fs::File,
        path::PathBuf,
        sync::{mpsc::channel, Mutex},
    };
    use tempfile::tempdir;

    struct MockUploader {
        base_dir: String,
        paths: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl RetryMove for MockUploader {
        fn get_base_directory(&self) -> &String {
            &self.base_dir
        }

        async fn retry_move(&self, pathbuf: PathBuf) {
            info!("retry move mock: {:?}", &pathbuf);
            self.paths
                .lock()
                .as_mut()
                .unwrap()
                .push(pathbuf.to_str().unwrap().to_owned());
        }
    }

    #[tokio::test]
    async fn clear_does_not_find_hidden_files() {
        let dir = tempdir().expect("failed to create temp directory");
        let file_path = dir.path().join(".hidden_file");
        let _ = File::create(file_path).expect("failed to create hidden file");
        let mock = MockUploader {
            base_dir: dir.path().to_str().unwrap().to_owned(),
            paths: Mutex::new(Vec::<String>::new()),
        };
        let u = Arc::new(mock);

        clear_current_files(u.clone()).await;

        assert_eq!(u.paths.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn clear_does_find_existing_files() {
        let dir = tempdir().expect("failed to create temp directory");
        let file_path = dir.path().join("existing_file");
        let _ = File::create(&file_path).expect("failed to create file");
        let mock = MockUploader {
            base_dir: dir.path().to_str().unwrap().to_owned(),
            paths: Mutex::new(Vec::<String>::new()),
        };
        let u = Arc::new(mock);

        clear_current_files(u.clone()).await;

        assert_eq!(u.paths.lock().unwrap().len(), 1);
        assert_eq!(
            u.paths.lock().unwrap()[0],
            file_path
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        );
    }

    #[tokio::test]
    async fn watch_moves_file_on_write() {
        let (tx, rx) = channel();
        let dir = tempdir().expect("failed to create temp directory");
        let file_path = dir.path().join("existing_file");
        let _ = File::create(&file_path).expect("failed to create file");

        let mock = MockUploader {
            base_dir: dir.path().to_str().unwrap().to_string(),
            paths: Mutex::new(Vec::<String>::new()),
        };
        let u = Arc::new(mock);

        tx.send(RawEvent {
            path: Some(file_path),
            op: Ok(Op::WRITE),
            cookie: Some(0),
        })
        .unwrap();

        tx.send(RawEvent {
            path: None,
            op: Err(notify::Error::Generic("Abort".to_string())),
            cookie: None,
        })
        .unwrap();

        watch_dir(u.clone(), rx, tx).await;
        task::yield_now().await;

        assert_eq!(u.paths.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn watch_ignores_deletes() {
        let (tx, rx) = channel();
        let dir = tempdir().expect("failed to create temp directory");
        let file_path = dir.path().join("file_that_gets_deleted");

        let mock = MockUploader {
            base_dir: dir.path().to_str().unwrap().to_string(),
            paths: Mutex::new(Vec::<String>::new()),
        };
        let u = Arc::new(mock);

        tx.send(RawEvent {
            path: Some(file_path),
            op: Ok(Op::REMOVE),
            cookie: Some(0),
        })
        .unwrap();

        tx.send(RawEvent {
            path: None,
            op: Err(notify::Error::Generic("Abort".to_string())),
            cookie: None,
        })
        .unwrap();

        watch_dir(u.clone(), rx, tx).await;
        task::yield_now().await;

        assert_eq!(u.paths.lock().unwrap().len(), 0);
    }
}
