//! Object storage module
//!
//! Provides support for S3-compatible object storage.
//! Can use native implementation or integrate with s5cmd for maximum performance.

mod s3;
mod s5cmd;

pub use s3::*;
pub use s5cmd::*;
