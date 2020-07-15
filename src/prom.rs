use lazy_static::lazy_static;
use prometheus::{
    self, register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter,
    IntGauge,
};

lazy_static! {
    pub static ref IN_FLIGHT_UPLOADS: IntGauge =
        register_int_gauge!("uploader_in_flight", "Number of in flight uploads.").expect("IN_FLIGHT_UPLOADS could not be registerd.");

    pub static ref COMPLETED_UPLOADS: IntCounter = register_int_counter!(
        "uploader_completed_uploads",
        "Number of uploads successfully transferred."
    )
    .expect("COMPLETED_UPLOADS could not be registerd.");

    pub static ref FAILED_UPLOADS: IntCounter = register_int_counter!(
        "uploader_failed_send",
        "Number of uploads that were unable to be copied.  Could indicate incorrect remote permissions."
    )
    .expect("FAILED_UPLOADS could not be registerd.");

    pub static ref FAILED_ATTEMPTS: IntCounter = register_int_counter!(
        "uploader_failed_attempts",
        "Number of upload or delete attempts that were made.  Indicates transient trouble with permissions or locks."
    )
    .expect("FAILED_ATTEMPTS could not be registerd.");

    pub static ref FAILED_DELETES: IntCounter = register_int_counter!(
        "uploader_failed_local_delete",
        "Number of uploads that were unable to successfully delete the local copy of the file.  Could indicate incorrect local permissions."
    )
    .expect("FAILED_DELETES could not be registerd.");

    pub static ref BYTES_PER_MILLISECOND: Histogram = register_histogram!(
        "uploader_bytes_per_millisecond",
        "Number of bytes transferred per millisecond."
    )
    .expect("BYTES_PER_MILLISECOND could not be registerd.");

    pub static ref BYTES_TRANSFERRED: IntCounter = register_int_counter!(
        "uploader_total_bytes_transferred",
        "Total number of bytes transferred."
    )
    .expect("BYTES_TRANSFERRED could not be registerd.");
}
