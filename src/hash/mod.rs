//! Hash computation and integrity verification module
//!
//! Provides ultra-fast hashing using XXHash3, BLAKE3, and SHA-256
//! with streaming support for single-pass copy-and-hash operations.

mod integrity;

pub use integrity::*;
