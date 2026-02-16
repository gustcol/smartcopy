//! File system operations module
//!
//! Provides high-performance file scanning, metadata handling,
//! and optimized I/O operations for the copy engine.

mod scanner;
mod operations;
pub mod uring;
pub mod throttle;
pub mod compress;
pub mod sparse;
pub mod scheduler;
pub mod acl;
pub mod patricia;

pub use scanner::*;
pub use operations::*;
pub use uring::{check_io_uring_support, IoUringStatus, IoUringCopier};
pub use throttle::BandwidthLimiter;
pub use compress::{Lz4Compressor, CompressionStats};
pub use sparse::{SparseCopier, SparseInfo, SparseCopyResult, is_sparse};
pub use scheduler::{BandwidthSchedule, ScheduleRule, ScheduledLimiter, ScheduleStatus};
pub use acl::{WindowsAcl, SecurityInfo, Acl, AclEntry, AccessMask};
