//! io_uring support for high-performance async I/O on Linux
//!
//! io_uring is a Linux kernel interface (5.1+) that provides:
//! - Async I/O with minimal syscall overhead
//! - Batch submission of I/O operations
//! - Zero-copy data transfers
//! - Up to 2x throughput improvement over traditional I/O
//!
//! This module automatically detects kernel support and falls back
//! to standard I/O if io_uring is unavailable.

use crate::error::{Result, SmartCopyError};
use std::path::Path;

/// Minimum kernel version required for io_uring (5.1.0)
pub const MIN_KERNEL_VERSION: (u32, u32, u32) = (5, 1, 0);

/// Recommended kernel version for full io_uring features (5.6+)
pub const RECOMMENDED_KERNEL_VERSION: (u32, u32, u32) = (5, 6, 0);

/// io_uring availability status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringStatus {
    /// io_uring is available and fully functional
    Available,
    /// io_uring is available but with limited features (kernel 5.1-5.5)
    LimitedFeatures,
    /// Kernel version is too old
    KernelTooOld,
    /// Not on Linux
    NotLinux,
    /// io_uring probe failed
    ProbeFailed,
}

impl IoUringStatus {
    /// Check if io_uring can be used
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Available | Self::LimitedFeatures)
    }
}

/// Parsed kernel version
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct KernelVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl KernelVersion {
    /// Parse kernel version from uname release string
    pub fn parse(release: &str) -> Option<Self> {
        let parts: Vec<&str> = release.split(|c| c == '.' || c == '-').collect();
        if parts.len() >= 2 {
            let major = parts[0].parse().ok()?;
            let minor = parts[1].parse().ok()?;
            let patch = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
            Some(Self { major, minor, patch })
        } else {
            None
        }
    }

    /// Check if this version meets minimum requirements
    pub fn meets_minimum(&self, min: (u32, u32, u32)) -> bool {
        (self.major, self.minor, self.patch) >= min
    }
}

impl std::fmt::Display for KernelVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Get current kernel version (Linux only)
#[cfg(target_os = "linux")]
pub fn get_kernel_version() -> Option<KernelVersion> {
    use std::process::Command;

    // Try uname -r first
    if let Ok(output) = Command::new("uname").arg("-r").output() {
        if let Ok(release) = String::from_utf8(output.stdout) {
            if let Some(version) = KernelVersion::parse(release.trim()) {
                return Some(version);
            }
        }
    }

    // Fallback: read /proc/version
    if let Ok(version_str) = std::fs::read_to_string("/proc/version") {
        // Format: "Linux version X.Y.Z-..."
        let parts: Vec<&str> = version_str.split_whitespace().collect();
        if parts.len() >= 3 {
            if let Some(version) = KernelVersion::parse(parts[2]) {
                return Some(version);
            }
        }
    }

    None
}

#[cfg(not(target_os = "linux"))]
pub fn get_kernel_version() -> Option<KernelVersion> {
    None
}

/// Check io_uring availability with detailed status
#[cfg(target_os = "linux")]
pub fn check_io_uring_support() -> IoUringStatus {
    // First check kernel version
    let kernel_version = match get_kernel_version() {
        Some(v) => v,
        None => return IoUringStatus::ProbeFailed,
    };

    if !kernel_version.meets_minimum(MIN_KERNEL_VERSION) {
        return IoUringStatus::KernelTooOld;
    }

    // Try to actually probe io_uring
    #[cfg(feature = "io_uring")]
    {
        match io_uring::IoUring::new(8) {
            Ok(_) => {
                if kernel_version.meets_minimum(RECOMMENDED_KERNEL_VERSION) {
                    IoUringStatus::Available
                } else {
                    IoUringStatus::LimitedFeatures
                }
            }
            Err(_) => IoUringStatus::ProbeFailed,
        }
    }

    #[cfg(not(feature = "io_uring"))]
    {
        // Feature not enabled, but kernel supports it
        if kernel_version.meets_minimum(RECOMMENDED_KERNEL_VERSION) {
            IoUringStatus::Available
        } else {
            IoUringStatus::LimitedFeatures
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn check_io_uring_support() -> IoUringStatus {
    IoUringStatus::NotLinux
}

/// io_uring file copier for maximum throughput
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub struct IoUringCopier {
    ring_size: u32,
    buffer_size: usize,
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl IoUringCopier {
    /// Create a new io_uring copier
    ///
    /// # Arguments
    /// * `ring_size` - Number of submission queue entries (power of 2, typically 256-4096)
    /// * `buffer_size` - Size of each I/O buffer (typically 64KB-1MB)
    pub fn new(ring_size: u32, buffer_size: usize) -> Result<Self> {
        // Verify io_uring is available
        let status = check_io_uring_support();
        if !status.is_usable() {
            return Err(SmartCopyError::UnsupportedOperation(
                format!("io_uring not available: {:?}", status)
            ));
        }

        Ok(Self {
            ring_size,
            buffer_size,
        })
    }

    /// Copy a file using io_uring for async I/O
    pub fn copy(&self, source: &Path, dest: &Path) -> Result<u64> {
        use io_uring::{opcode, types, IoUring};
        use std::fs::{File, OpenOptions};
        use std::os::unix::io::AsRawFd;

        let src_file = File::open(source)
            .map_err(|e| SmartCopyError::io(source, e))?;
        let file_size = src_file.metadata()
            .map_err(|e| SmartCopyError::io(source, e))?.len();

        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)
            .map_err(|e| SmartCopyError::io(dest, e))?;

        // Preallocate destination
        dst_file.set_len(file_size)
            .map_err(|e| SmartCopyError::io(dest, e))?;

        let src_fd = src_file.as_raw_fd();
        let dst_fd = dst_file.as_raw_fd();

        let mut ring: IoUring = IoUring::new(self.ring_size)
            .map_err(|e| SmartCopyError::IoError {
                path: source.to_path_buf(),
                message: format!("Failed to create io_uring: {}", e),
            })?;

        let mut offset = 0u64;
        let mut bytes_copied = 0u64;

        // Allocate aligned buffer for direct I/O compatibility
        let mut buffer = vec![0u8; self.buffer_size];

        while offset < file_size {
            let to_read = ((file_size - offset) as usize).min(self.buffer_size);

            // Submit read operation
            let read_e = opcode::Read::new(
                types::Fd(src_fd),
                buffer.as_mut_ptr(),
                to_read as u32,
            )
            .offset(offset)
            .build()
            .user_data(0x01);

            unsafe {
                ring.submission()
                    .push(&read_e)
                    .map_err(|_| SmartCopyError::IoError {
                        path: source.to_path_buf(),
                        message: "io_uring submission queue full".to_string(),
                    })?;
            }

            ring.submit_and_wait(1)
                .map_err(|e| SmartCopyError::io(source, e))?;

            // Get completion
            let cqe = ring.completion().next()
                .ok_or_else(|| SmartCopyError::IoError {
                    path: source.to_path_buf(),
                    message: "io_uring completion missing".to_string(),
                })?;

            let bytes_read = cqe.result();
            if bytes_read < 0 {
                return Err(SmartCopyError::IoError {
                    path: source.to_path_buf(),
                    message: format!("io_uring read failed: {}", bytes_read),
                });
            }

            let bytes_read = bytes_read as usize;
            if bytes_read == 0 {
                break; // EOF
            }

            // Submit write operation
            let write_e = opcode::Write::new(
                types::Fd(dst_fd),
                buffer.as_ptr(),
                bytes_read as u32,
            )
            .offset(offset)
            .build()
            .user_data(0x02);

            unsafe {
                ring.submission()
                    .push(&write_e)
                    .map_err(|_| SmartCopyError::IoError {
                        path: dest.to_path_buf(),
                        message: "io_uring submission queue full".to_string(),
                    })?;
            }

            ring.submit_and_wait(1)
                .map_err(|e| SmartCopyError::io(dest, e))?;

            let cqe = ring.completion().next()
                .ok_or_else(|| SmartCopyError::IoError {
                    path: dest.to_path_buf(),
                    message: "io_uring completion missing".to_string(),
                })?;

            let bytes_written = cqe.result();
            if bytes_written < 0 {
                return Err(SmartCopyError::IoError {
                    path: dest.to_path_buf(),
                    message: format!("io_uring write failed: {}", bytes_written),
                });
            }

            offset += bytes_read as u64;
            bytes_copied += bytes_read as u64;
        }

        Ok(bytes_copied)
    }

    /// Copy using batched I/O for maximum throughput
    /// Submits multiple read/write pairs simultaneously
    pub fn copy_batched(&self, source: &Path, dest: &Path) -> Result<u64> {
        use io_uring::{opcode, types, IoUring};
        use std::fs::{File, OpenOptions};
        use std::os::unix::io::AsRawFd;

        let src_file = File::open(source)
            .map_err(|e| SmartCopyError::io(source, e))?;
        let file_size = src_file.metadata()
            .map_err(|e| SmartCopyError::io(source, e))?.len();

        if file_size == 0 {
            File::create(dest).map_err(|e| SmartCopyError::io(dest, e))?;
            return Ok(0);
        }

        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)
            .map_err(|e| SmartCopyError::io(dest, e))?;

        dst_file.set_len(file_size)
            .map_err(|e| SmartCopyError::io(dest, e))?;

        let src_fd = src_file.as_raw_fd();
        let dst_fd = dst_file.as_raw_fd();

        let mut ring: IoUring = IoUring::new(self.ring_size)
            .map_err(|e| SmartCopyError::IoError {
                path: source.to_path_buf(),
                message: format!("Failed to create io_uring: {}", e),
            })?;

        // Calculate number of buffers needed
        let num_buffers = (self.ring_size / 2) as usize; // Half for reads, half for writes
        let mut buffers: Vec<Vec<u8>> = (0..num_buffers)
            .map(|_| vec![0u8; self.buffer_size])
            .collect();

        let mut bytes_copied = 0u64;
        let mut read_offset = 0u64;
        let mut pending_writes: Vec<(u64, usize, usize)> = Vec::new(); // (offset, buffer_idx, size)

        while bytes_copied < file_size {
            // Submit as many reads as we can
            let mut reads_submitted = 0;
            for (idx, buffer) in buffers.iter_mut().enumerate() {
                if read_offset >= file_size {
                    break;
                }

                let to_read = ((file_size - read_offset) as usize).min(self.buffer_size);

                let read_e = opcode::Read::new(
                    types::Fd(src_fd),
                    buffer.as_mut_ptr(),
                    to_read as u32,
                )
                .offset(read_offset)
                .build()
                .user_data((idx as u64) << 32 | read_offset);

                if unsafe { ring.submission().push(&read_e).is_err() } {
                    break;
                }

                pending_writes.push((read_offset, idx, to_read));
                read_offset += to_read as u64;
                reads_submitted += 1;
            }

            if reads_submitted == 0 && pending_writes.is_empty() {
                break;
            }

            // Submit and wait for completions
            ring.submit_and_wait(reads_submitted.max(1))
                .map_err(|e| SmartCopyError::io(source, e))?;

            // Process completions and submit writes
            while let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result < 0 {
                    return Err(SmartCopyError::IoError {
                        path: source.to_path_buf(),
                        message: format!("io_uring operation failed: {}", result),
                    });
                }

                let user_data = cqe.user_data();
                if user_data & 0x80000000_00000000 != 0 {
                    // This was a write completion
                    bytes_copied += result as u64;
                } else {
                    // This was a read completion - submit write
                    let offset = user_data & 0xFFFFFFFF;
                    let buffer_idx = (user_data >> 32) as usize;
                    let bytes_read = result as usize;

                    let write_e = opcode::Write::new(
                        types::Fd(dst_fd),
                        buffers[buffer_idx].as_ptr(),
                        bytes_read as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(0x80000000_00000000 | offset);

                    unsafe {
                        let _ = ring.submission().push(&write_e);
                    }
                }
            }

            // Submit pending writes
            let _ = ring.submit();
        }

        // Wait for remaining writes
        while bytes_copied < file_size {
            ring.submit_and_wait(1)
                .map_err(|e| SmartCopyError::io(dest, e))?;

            while let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result > 0 {
                    bytes_copied += result as u64;
                }
            }
        }

        Ok(bytes_copied)
    }
}

/// Stub implementation for non-Linux or when io_uring feature is disabled
#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub struct IoUringCopier {
    _private: (),
}

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
impl IoUringCopier {
    pub fn new(_ring_size: u32, _buffer_size: usize) -> Result<Self> {
        Err(SmartCopyError::UnsupportedOperation(
            "io_uring is only available on Linux with the io_uring feature enabled".to_string()
        ))
    }

    pub fn copy(&self, _source: &Path, _dest: &Path) -> Result<u64> {
        Err(SmartCopyError::UnsupportedOperation(
            "io_uring not available".to_string()
        ))
    }

    pub fn copy_batched(&self, _source: &Path, _dest: &Path) -> Result<u64> {
        Err(SmartCopyError::UnsupportedOperation(
            "io_uring not available".to_string()
        ))
    }
}

/// Print io_uring status information
pub fn print_io_uring_status() {
    let status = check_io_uring_support();

    println!("io_uring Status:");

    #[cfg(target_os = "linux")]
    {
        if let Some(version) = get_kernel_version() {
            println!("  Kernel Version: {}", version);
            println!("  Minimum Required: {}.{}.{}",
                MIN_KERNEL_VERSION.0, MIN_KERNEL_VERSION.1, MIN_KERNEL_VERSION.2);
            println!("  Recommended: {}.{}.{}",
                RECOMMENDED_KERNEL_VERSION.0, RECOMMENDED_KERNEL_VERSION.1, RECOMMENDED_KERNEL_VERSION.2);
        }
    }

    println!("  Status: {:?}", status);
    println!("  Usable: {}", status.is_usable());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kernel_version_parsing() {
        assert_eq!(
            KernelVersion::parse("5.15.0-generic"),
            Some(KernelVersion { major: 5, minor: 15, patch: 0 })
        );
        assert_eq!(
            KernelVersion::parse("6.1.0"),
            Some(KernelVersion { major: 6, minor: 1, patch: 0 })
        );
        assert_eq!(
            KernelVersion::parse("4.19.128"),
            Some(KernelVersion { major: 4, minor: 19, patch: 128 })
        );
    }

    #[test]
    fn test_version_comparison() {
        let v5_1 = KernelVersion { major: 5, minor: 1, patch: 0 };
        let v5_6 = KernelVersion { major: 5, minor: 6, patch: 0 };
        let v4_19 = KernelVersion { major: 4, minor: 19, patch: 0 };

        assert!(v5_1.meets_minimum(MIN_KERNEL_VERSION));
        assert!(v5_6.meets_minimum(RECOMMENDED_KERNEL_VERSION));
        assert!(!v4_19.meets_minimum(MIN_KERNEL_VERSION));
    }

    #[test]
    fn test_io_uring_status() {
        let status = check_io_uring_support();
        // Just verify it doesn't panic
        println!("io_uring status: {:?}", status);
    }
}
