use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use futures_retry::FutureRetry;
use log::{debug, error, trace};
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::{fs::File, path::PathBuf, time::Duration};
use thiserror::Error;
use tokio_util::codec;

use crate::prom::*;

/// S3Uploader data.
#[derive(Debug, Clone)]
pub struct S3Uploader {
    base_directory: String,
    default_bucket: String,
    bucket_map: Vec<(String, String)>,
}

// ! should this be in a separate file/module?
/// RetryMove, can be implemented for any uploader.
/// here we implement it for S3, but that does not preclude it from
/// being implemented for Azure Storage, or the GCP equivalent, or
/// indeed, any other cloud storage.
#[async_trait]
pub trait RetryMove: Send + Sync {
    fn get_base_directory(&self) -> &String;
    async fn retry_move(&self, pathbuf: PathBuf);
}

impl S3Uploader {
    // create a new S3Uploader.
    // @pathbuf: the base directory that is being watch.
    // @default_bucket: the default bucket to transfer files to.
    // @map: Vector of string/string that represents a directory -> bucket mapping for subdirectories
    //       of the base directory.
    // ? so why if I'm asking for a mutable map, so I not have to pass in with the `mut` keyword?
    pub fn new(pathbuf: PathBuf, default_bucket: String, mut map: Vec<(String, String)>) -> Self {
        let base = pathbuf
            .to_str()
            .expect("conversion of pathbut to string failed")
            .to_owned();

        // * needs sorting - reverse alphanumeric ---- because:
        // * consider a situation where a sub.sub directory was given as needing to visit a different bucket
        // * this should be possible, and handled gracefully.
        //
        // * e.g. args like `-m .=default_bucket -m android=android_bucket -m elf=elf_bucket -m elf/half_elf=halfelf_bucket`
        // *      we would want the file elf/half_elf/test.txt to go to s3://halfelf_bucket.
        // *      comparisons would need to be done in the order of elf/half_elf -> elf since the pattern matching strips the
        // *      subdir from the path in order to understand whether it fits.
        //
        // * The standard usage would be to watch a directory and have multiple sub directories off that
        // * that point to independant buckets, with subdirs of those becoming the prefix along with the filename.
        // let mut m2 = map.clone();
        map.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            base_directory: base,
            default_bucket,
            bucket_map: map,
        }
    }
}

#[allow(clippy::empty_line_after_outer_attr)]
#[async_trait]
impl RetryMove for S3Uploader {
    // retieve the base directory, the one being watched.
    // ! can probably be removed if I just pass the directory straight to the 2 functions that use it.
    fn get_base_directory(&self) -> &String {
        &self.base_directory
    }

    // attempts to move the specified file to S3
    // failures are retried according to the FibonacciBackoff retry strategy.
    // in this case, that is determined as 10 retries with a max delay of 6 seconds
    async fn retry_move(&self, pathbuf: PathBuf) {
        use crate::fib_retry;
        use fib_retry::FibonacciBackoff;
        use futures_retry::{ErrorHandler, RetryPolicy};

        // ! can I extract this to it's own module and add error_handler to S3Uploader struct?
        // ! should I?  certainly makes the function shorter.
        impl ErrorHandler<anyhow::Error> for fib_retry::FibonacciBackoff {
            type OutError = anyhow::Error;

            fn handle(
                &mut self,
                current_attempt: usize,
                e: anyhow::Error,
            ) -> RetryPolicy<anyhow::Error> {
                if Some(current_attempt as u64) > self.max_retries {
                    return RetryPolicy::ForwardError(e);
                }

                FAILED_ATTEMPTS.inc();

                match e.downcast_ref::<S3Error>() {
                    Some(S3Error::DeleteFile(_)) | Some(S3Error::PutObject(_)) => match self.next()
                    {
                        Some(duration) => RetryPolicy::WaitRetry(duration),
                        None => RetryPolicy::ForwardError(e),
                    },
                    None => RetryPolicy::ForwardError(e),
                }
            }
        }

        let error_handler = FibonacciBackoff::from_millis(100)
            .max_retries(10)
            .max_delay(Duration::from_secs(6));

        // need to pass &pathbuf here because if we handed ownership over to the function, we wouldn't be able to retry.
        // and I wouldn't be able to delete it either. :)
        let pb = &pathbuf;
        let retval = FutureRetry::new(move || copy_to_s3(pb, self), error_handler.clone()).await;
        match retval {
            Ok(_) => {}
            Err(_) => {
                FAILED_UPLOADS.inc();
                error!(
                    "[{:?}] Exceeded maximum retry ({:?}) for copy",
                    &pathbuf, error_handler.max_retries
                );
                return;
            }
        }

        // need to pass &pathbuf here because if we handed ownership over to the function, we wouldn't be able to retry.
        let retval = FutureRetry::new(|| delete_local(&pathbuf), error_handler.clone()).await;
        match retval {
            Ok(_) => {}
            Err(_) => {
                FAILED_DELETES.inc();
                error!(
                    "[{:?}] Exceeded maximum retry ({:?}) for delete",
                    &pathbuf, error_handler.max_retries
                );
            } // TODO: there should be a return here.  Write a test RED/GREEN/REFACTOR.
        }

        COMPLETED_UPLOADS.inc();
    }
}

// delete a local file at @pathbuf.
async fn delete_local(pathbuf: &PathBuf) -> Result<()> {
    // File transfered.  Now delete it locally.
    match std::fs::remove_file(pathbuf) {
        Ok(_) => {
            debug!("Successfully deleted file: {:?}", pathbuf);
            Ok(())
        }
        Err(_) => {
            error!("Failed to delete file: {:?}", pathbuf);
            let path = pathbuf
                .to_str()
                .expect("unable to convert pathbuf to string")
                .to_owned();
            bail!(S3Error::DeleteFile(path))
        }
    }
}

// copy a file @pathbuf to the designated bucket and prefix.
async fn copy_to_s3(pathbuf: &PathBuf, uploader: &S3Uploader) -> Result<()> {
    trace!("copy_to_s3: {:?}", pathbuf);

    let file = File::open(pathbuf.as_path())?;
    let metadata = file.metadata()?;
    let (bucket, prefix) = get_bucket_and_prefix(pathbuf, uploader);

    let tokio_file = tokio::fs::File::from_std(file);
    let s3_client = S3Client::new(Region::EuWest2);
    let start_upload_time = std::time::Instant::now();
    let result = s3_client
        .put_object(PutObjectRequest {
            bucket: bucket.to_string(),
            key: prefix,
            server_side_encryption: Some("AES256".to_string()),
            content_length: Some(metadata.len() as i64),
            body: Some(rusoto_core::ByteStream::new(
                codec::FramedRead::new(tokio_file, codec::BytesCodec::new())
                    .map_ok(|bytes| bytes.freeze()),
            )),
            ..Default::default()
        })
        .await;
    let end_upload_time = std::time::Instant::now();

    match result {
        Ok(_) => {
            // * if everything is good, we just want to set a couple of Prometheus metrics
            BYTES_TRANSFERRED.inc_by(metadata.len() as i64);

            // get upload duration in milliseconds
            let d = end_upload_time
                .duration_since(start_upload_time)
                .as_millis();
            BYTES_PER_MILLISECOND.observe(metadata.len() as f64 / d as f64);

            Ok(())
        }
        Err(_) => {
            // * if it's not good, return an error. this will be picked up by the retrier, and
            // * appropriate action taken.
            let path = pathbuf
                .as_path()
                .to_str()
                .expect("unable to convert path to string")
                .to_owned();
            bail!(S3Error::PutObject(path))
        }
    }
}

// errors that this module can produce and handle.
#[derive(Error, Debug)]
enum S3Error {
    // failed to transfer the file to S3
    #[error("failed to push file to s3: '{}'", .0)]
    PutObject(String),

    // failed to delete the local file
    #[error("failed to delete local copy of file: '{}'", .0)]
    DeleteFile(String),
}

/// get_bucket_and_prefix returns a tuple of the bucket and prefix to load a specific
/// file to, given a known uploader..
/// pathbuf should already be canonicalized by the time it hits here.
fn get_bucket_and_prefix<'a>(pathbuf: &PathBuf, uploader: &'a S3Uploader) -> (&'a str, String) {
    let pb = pathbuf.strip_prefix(&uploader.base_directory).unwrap();
    for (base, bucket) in uploader.bucket_map.iter() {
        if pb.starts_with(base) {
            let pbs = pb
                .strip_prefix(base)
                .expect("unable to strip the prefix")
                .to_string_lossy()
                .to_string();
            return (bucket, pbs);
        }
    }
    let pb = pb.to_string_lossy().to_string();
    (&uploader.default_bucket, pb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_bucket_and_prefix_get_default() {
        // * get_bucket_and_prefix returns the default bucket if no alternative buckets available.

        let map = Vec::<(String, String)>::new();
        let base_path = PathBuf::from("/tmp/uploader/");
        let file_path = PathBuf::from("/tmp/uploader/subdir/jedi/mindtrick.txt");
        let uploader = S3Uploader::new(base_path, "test_bucket".to_string(), map);

        let (bucket, prefix) = get_bucket_and_prefix(&file_path, &uploader);

        assert_eq!("test_bucket".to_string(), bucket);
        assert_eq!("subdir/jedi/mindtrick.txt".to_string(), prefix);
    }

    #[test]
    fn get_bucket_and_prefix_gets_correct_bucket_for_sub_directory() {
        // * get_bucket_and_prefix returns the correct bucket if provided alternatives for subdirectories.

        let mut map = Vec::<(String, String)>::new();
        map.push(("subdir".to_string(), "different_bucket".to_string()));
        let base_path = PathBuf::from("/tmp/uploader");
        let file_path = PathBuf::from("/tmp/uploader/subdir/jedi/mindtrick.txt");
        let uploader = S3Uploader::new(base_path, "test_bucket".to_string(), map);

        let (bucket, prefix) = get_bucket_and_prefix(&file_path, &uploader);

        assert_eq!("different_bucket".to_string(), bucket);
        assert_eq!("jedi/mindtrick.txt".to_string(), prefix);
    }

    #[test]
    fn get_bucket_and_prefix_gets_correct_bucket_for_overlapped_sub_directory() {
        // * get_bucket_and_prefix returns the correct bucket if there are overlapping sub directories supplied.

        let mut map = Vec::<(String, String)>::new();
        map.push(("subdir/jedi".to_string(), "other_bucket".to_string()));
        map.push(("subdir".to_string(), "different_bucket".to_string()));
        let base_path = PathBuf::from("/tmp/uploader");
        let file_path = PathBuf::from("/tmp/uploader/subdir/jedi/mindtrick.txt");
        let uploader = S3Uploader::new(base_path, "test_bucket".to_string(), map);

        let (bucket, prefix) = get_bucket_and_prefix(&file_path, &uploader);

        assert_eq!("other_bucket".to_string(), bucket);
        assert_eq!("mindtrick.txt".to_string(), prefix);
    }
}
