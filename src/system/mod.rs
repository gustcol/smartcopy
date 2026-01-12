//! System analysis and resource detection module
//!
//! Provides functionality to detect system resources, analyze capabilities,
//! and generate tuning recommendations for optimal performance.

mod resources;
mod tuning;
pub mod numa;
pub mod hpc;

pub use resources::*;
pub use tuning::*;
pub use numa::{NumaTopology, NumaNode, ThreadAffinity};
pub use hpc::{SchedulerType, JobInfo, JobConfig, JobScheduler, JobStatus};
