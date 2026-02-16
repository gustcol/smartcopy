//! Incremental sync and delta transfer module
//!
//! Provides intelligent synchronization with:
//! - Metadata-based change detection
//! - Manifest tracking for efficient re-sync
//! - Delta/chunked transfer for large files
//! - Resume interrupted transfers

mod incremental;
mod delta;
mod manifest;
mod resume;

#[cfg(feature = "parquet_manifest")]
pub mod parquet_manifest;

pub use incremental::*;
pub use delta::*;
pub use manifest::*;
pub use resume::{ResumeManager, TransferState, FileTransferState, TransferStatus, FileStatus, ResumeResult, ResumableWriter};
