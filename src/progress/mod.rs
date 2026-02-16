//! Progress reporting module
//!
//! Provides real-time progress visualization for copy operations
//! with support for multiple bars, ETA calculation, and throughput display.

mod reporter;

#[cfg(feature = "tui")]
pub mod tui;

pub use reporter::*;
