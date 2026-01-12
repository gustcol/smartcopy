//! # SmartCopy - High-Performance File Copy for HPC
//!
//! SmartCopy is a blazingly fast, intelligent file copy utility designed for
//! High-Performance Computing (HPC) environments. Built in Rust for maximum
//! performance, memory safety, and zero-cost abstractions.
//!
//! ## Features
//!
//! - **Multi-threaded Parallel Copying**: Utilizing work-stealing thread pools
//! - **Intelligent Resource Detection**: Auto-detect CPU, memory, storage type
//! - **Smart File Ordering**: Smallest files first for quick wins
//! - **Integrity Verification**: XXHash3 (ultra-fast), BLAKE3, SHA-256
//! - **Incremental Sync**: Only copy changed files
//! - **Delta Transfer**: Block-level change detection for large files
//! - **Network Support**: SSH/SFTP and direct TCP modes
//! - **LZ4 Compression**: Ultra-fast on-the-fly compression
//! - **System Tuning**: Recommendations for optimal performance
//!
//! ## Quick Start
//!
//! ```no_run
//! use smartcopy::core::{CopyEngine, simple_copy};
//! use smartcopy::config::CopyConfig;
//! use std::path::Path;
//!
//! // Simple copy
//! let result = simple_copy(
//!     Path::new("/source"),
//!     Path::new("/destination")
//! ).unwrap();
//!
//! println!("Copied {} files ({} bytes)", result.files_copied, result.bytes_copied);
//! ```
//!
//! ## Advanced Usage
//!
//! ```no_run
//! use smartcopy::config::{CopyConfig, HashAlgorithm, OrderingStrategy};
//! use smartcopy::core::CopyEngine;
//! use smartcopy::progress::ProgressReporter;
//! use std::path::PathBuf;
//!
//! let config = CopyConfig {
//!     source: PathBuf::from("/source"),
//!     destination: PathBuf::from("/destination"),
//!     threads: 8,
//!     verify: Some(HashAlgorithm::XXHash3),
//!     incremental: true,
//!     ordering: OrderingStrategy::SmallestFirst,
//!     ..Default::default()
//! };
//!
//! let progress = ProgressReporter::new();
//! let engine = CopyEngine::new(config).with_progress(progress);
//!
//! let result = engine.execute().unwrap();
//! result.print_summary();
//! ```
//!
//! ## System Analysis
//!
//! ```no_run
//! use smartcopy::system::{SystemInfo, TuningAnalyzer};
//! use smartcopy::config::WorkloadType;
//!
//! let system_info = SystemInfo::collect();
//! system_info.print_summary();
//!
//! let analyzer = TuningAnalyzer::new(system_info, WorkloadType::Mixed);
//! analyzer.print_recommendations();
//! ```
//!
//! ## Delta Transfer for Large Files
//!
//! ```no_run
//! use smartcopy::sync::{ChunkedCopier, FileSignature, FileDelta};
//! use std::path::Path;
//!
//! let copier = ChunkedCopier::new(1024 * 1024, 4); // 1MB chunks, 4 workers
//!
//! // Parallel chunked copy
//! let result = copier.copy_parallel(
//!     Path::new("/source/large_file.bin"),
//!     Path::new("/dest/large_file.bin")
//! ).unwrap();
//!
//! println!("Copied {} in {:?}", result.bytes_copied, result.duration);
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod api;
pub mod config;
pub mod core;
pub mod crypto;
pub mod error;
pub mod fs;
pub mod hash;
pub mod network;
pub mod progress;
pub mod storage;
pub mod sync;
pub mod system;

// Re-export commonly used types
pub use config::{CopyConfig, HashAlgorithm, OrderingStrategy};
pub use core::{CopyEngine, CopyResult};
pub use error::{Result, SmartCopyError};
pub use progress::ProgressReporter;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Prelude module for convenient imports
pub mod prelude {
    //! Convenient re-exports for common usage
    //!
    //! ```no_run
    //! use smartcopy::prelude::*;
    //! ```

    pub use crate::config::{CopyConfig, HashAlgorithm, OrderingStrategy};
    pub use crate::core::{simple_copy, CopyEngine, CopyResult};
    pub use crate::crypto::{EncryptionKey, FileEncryptor, EncryptionAlgorithm, EncryptionConfig};
    pub use crate::error::{Result, SmartCopyError};
    pub use crate::fs::{FileEntry, Scanner, ScanConfig, SparseCopier, BandwidthSchedule, WindowsAcl};
    pub use crate::hash::{hash_file, verify_files_match, HashResult};
    pub use crate::progress::ProgressReporter;
    pub use crate::storage::{S3Client, S3Config, S5cmdClient};
    pub use crate::sync::{ChunkedCopier, IncrementalSync, SyncManifest, ResumeManager};
    pub use crate::system::{SystemInfo, TuningAnalyzer, JobScheduler, JobInfo};
}
