//! Bandwidth throttling for controlled transfer rates
//!
//! Implements rate limiting using the Governor crate for smooth,
//! token-bucket based bandwidth control.

use governor::{Quota, RateLimiter, clock::DefaultClock, state::{InMemoryState, NotKeyed}};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// Bandwidth limiter for controlling transfer rates
pub struct BandwidthLimiter {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    bytes_per_token: usize,
}

impl BandwidthLimiter {
    /// Create a new bandwidth limiter
    ///
    /// # Arguments
    /// * `bytes_per_second` - Maximum transfer rate in bytes per second
    ///
    /// # Example
    /// ```
    /// use smartcopy::fs::throttle::BandwidthLimiter;
    /// let limiter = BandwidthLimiter::new(100 * 1024 * 1024); // 100 MB/s
    /// ```
    pub fn new(bytes_per_second: u64) -> Self {
        // Use 1KB chunks as tokens for smoother throttling
        const BYTES_PER_TOKEN: usize = 1024;

        let tokens_per_second = (bytes_per_second as usize / BYTES_PER_TOKEN).max(1);

        // Cap at u32::MAX to prevent overflow when casting
        let capped_tokens = tokens_per_second.min(u32::MAX as usize) as u32;
        let quota = Quota::per_second(NonZeroU32::new(capped_tokens).unwrap_or(NonZeroU32::MIN));

        let limiter = RateLimiter::direct(quota);

        Self {
            limiter: Arc::new(limiter),
            bytes_per_token: BYTES_PER_TOKEN,
        }
    }

    /// Create from a human-readable rate string (e.g., "100M", "1G", "500K")
    pub fn from_rate_string(rate: &str) -> Option<Self> {
        let rate = rate.trim().to_uppercase();

        let (num_str, multiplier) = if rate.ends_with("G") || rate.ends_with("GB") {
            (rate.trim_end_matches("GB").trim_end_matches('G'), 1024 * 1024 * 1024)
        } else if rate.ends_with("M") || rate.ends_with("MB") {
            (rate.trim_end_matches("MB").trim_end_matches('M'), 1024 * 1024)
        } else if rate.ends_with("K") || rate.ends_with("KB") {
            (rate.trim_end_matches("KB").trim_end_matches('K'), 1024)
        } else {
            (rate.as_str(), 1)
        };

        let num: f64 = num_str.parse().ok()?;
        let bytes_per_second = (num * multiplier as f64) as u64;

        if bytes_per_second > 0 {
            Some(Self::new(bytes_per_second))
        } else {
            None
        }
    }

    /// Wait until we're allowed to transfer the given number of bytes
    pub async fn wait_for_capacity(&self, bytes: usize) {
        let tokens_needed = (bytes / self.bytes_per_token).max(1);

        // Request tokens - this will block until available
        for _ in 0..tokens_needed {
            self.limiter.until_ready().await;
        }
    }

    /// Wait (blocking) until we're allowed to transfer the given number of bytes
    pub fn wait_for_capacity_blocking(&self, bytes: usize) {
        let tokens_needed = (bytes / self.bytes_per_token).max(1);

        for _ in 0..tokens_needed {
            // Spin-wait for capacity
            while self.limiter.check().is_err() {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }

    /// Try to acquire capacity without blocking
    /// Returns true if capacity was available
    pub fn try_acquire(&self, bytes: usize) -> bool {
        let tokens_needed = (bytes / self.bytes_per_token).max(1);

        for _ in 0..tokens_needed {
            if self.limiter.check().is_err() {
                return false;
            }
        }
        true
    }

    /// Get a clone of this limiter for sharing across threads
    pub fn clone_limiter(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            bytes_per_token: self.bytes_per_token,
        }
    }
}

impl Clone for BandwidthLimiter {
    fn clone(&self) -> Self {
        self.clone_limiter()
    }
}

/// Throttled reader that limits read bandwidth
pub struct ThrottledReader<R> {
    inner: R,
    limiter: BandwidthLimiter,
}

impl<R: std::io::Read> ThrottledReader<R> {
    pub fn new(reader: R, limiter: BandwidthLimiter) -> Self {
        Self {
            inner: reader,
            limiter,
        }
    }
}

impl<R: std::io::Read> std::io::Read for ThrottledReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Wait for capacity before reading
        self.limiter.wait_for_capacity_blocking(buf.len());

        self.inner.read(buf)
    }
}

/// Throttled writer that limits write bandwidth
pub struct ThrottledWriter<W> {
    inner: W,
    limiter: BandwidthLimiter,
}

impl<W: std::io::Write> ThrottledWriter<W> {
    pub fn new(writer: W, limiter: BandwidthLimiter) -> Self {
        Self {
            inner: writer,
            limiter,
        }
    }
}

impl<W: std::io::Write> std::io::Write for ThrottledWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Wait for capacity before writing
        self.limiter.wait_for_capacity_blocking(buf.len());

        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_string_parsing() {
        assert!(BandwidthLimiter::from_rate_string("100M").is_some());
        assert!(BandwidthLimiter::from_rate_string("1G").is_some());
        assert!(BandwidthLimiter::from_rate_string("500KB").is_some());
        assert!(BandwidthLimiter::from_rate_string("50MB").is_some());
        assert!(BandwidthLimiter::from_rate_string("invalid").is_none());
    }

    #[test]
    fn test_limiter_creation() {
        let limiter = BandwidthLimiter::new(100 * 1024 * 1024); // 100 MB/s
        assert!(limiter.try_acquire(1024)); // Should have initial capacity
    }
}
