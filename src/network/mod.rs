//! Network transfer module
//!
//! Provides remote file transfer capabilities:
//! - SSH/SFTP for secure transfers
//! - Direct TCP for maximum LAN throughput
//! - QUIC for modern, high-performance transfers
//! - SSH Agent for rsync-like delta sync
//! - Parallel streams for high bandwidth utilization
//!
//! ## Transport Comparison
//!
//! | Transport | Security | Speed | Use Case |
//! |-----------|----------|-------|----------|
//! | SSH/SFTP | TLS via SSH | Moderate | Remote servers |
//! | SSH+Agent | TLS via SSH | Fast | Delta sync |
//! | TCP Direct | None | Very Fast | Trusted LAN |
//! | QUIC | TLS 1.3 | Very Fast | Modern networks |
//!
//! ## Quick Start
//!
//! ```bash
//! # SSH with tuning
//! smartcopy /local user@remote:/path --ssh-control-master --ssh-cipher aes128-gcm
//!
//! # SSH with agent (delta sync)
//! smartcopy /local user@remote:/path --agent --delta
//!
//! # QUIC transfer
//! smartcopy /local remote:9877:/path --quic
//!
//! # TCP direct (LAN)
//! smartcopy /local remote:9876:/path --tcp-direct
//! ```

mod ssh;
mod tcp;
mod ssh_tuning;
mod agent;
mod quic;
mod parallel_sync;

pub use ssh::*;
pub use tcp::*;
pub use ssh_tuning::*;
pub use agent::*;
pub use quic::*;
pub use parallel_sync::*;
