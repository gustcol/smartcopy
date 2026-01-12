//! Core copy engine module
//!
//! Provides the main copy orchestration, task scheduling,
//! and parallel worker pool for high-performance file transfers.

mod copier;
mod scheduler;

pub use copier::*;
pub use scheduler::*;
