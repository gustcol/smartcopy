//! Sparse file support module
//!
//! Provides efficient handling of sparse files (files with holes).
//! Sparse files contain regions of zeros that don't occupy disk space.
//! This module detects and preserves sparse regions during copy operations.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

/// Information about sparse regions in a file
#[derive(Debug, Clone)]
pub struct SparseInfo {
    /// Total file size (logical)
    pub logical_size: u64,
    /// Actual blocks allocated on disk
    pub blocks_allocated: u64,
    /// Block size used by filesystem
    pub block_size: u64,
    /// Estimated sparse ratio (0.0 = no holes, 1.0 = all holes)
    pub sparse_ratio: f64,
    /// Detected hole regions
    pub holes: Vec<HoleRegion>,
}

/// A region of zeros (hole) in a sparse file
#[derive(Debug, Clone, Copy)]
pub struct HoleRegion {
    /// Start offset of the hole
    pub offset: u64,
    /// Length of the hole in bytes
    pub length: u64,
}

/// Result of a sparse copy operation
#[derive(Debug, Clone)]
pub struct SparseCopyResult {
    /// Bytes actually written (excluding holes)
    pub bytes_written: u64,
    /// Total logical size of file
    pub logical_size: u64,
    /// Number of holes preserved
    pub holes_preserved: u64,
    /// Space saved by preserving holes
    pub space_saved: u64,
}

/// Sparse file copier with hole detection and preservation
pub struct SparseCopier {
    /// Minimum hole size to detect (smaller holes are filled with zeros)
    min_hole_size: u64,
    /// Buffer size for reading
    buffer_size: usize,
    /// Zero buffer for comparison
    zero_buffer: Vec<u8>,
}

impl SparseCopier {
    /// Create a new sparse copier
    pub fn new() -> Self {
        Self::with_config(4096, 1024 * 1024)
    }

    /// Create with custom configuration
    pub fn with_config(min_hole_size: u64, buffer_size: usize) -> Self {
        Self {
            min_hole_size,
            buffer_size,
            zero_buffer: vec![0u8; buffer_size],
        }
    }

    /// Analyze a file for sparse regions
    #[cfg(unix)]
    pub fn analyze<P: AsRef<Path>>(&self, path: P) -> io::Result<SparseInfo> {
        let metadata = std::fs::metadata(path.as_ref())?;
        let logical_size = metadata.len();
        let blocks = metadata.blocks();
        let block_size = metadata.blksize();

        // Calculate actual disk usage
        let blocks_allocated = blocks * 512; // blocks are in 512-byte units

        let sparse_ratio = if logical_size > 0 {
            1.0 - (blocks_allocated as f64 / logical_size as f64)
        } else {
            0.0
        };

        // Detect holes by scanning for zero regions
        let holes = if sparse_ratio > 0.01 {
            self.detect_holes(path)?
        } else {
            Vec::new()
        };

        Ok(SparseInfo {
            logical_size,
            blocks_allocated,
            block_size,
            sparse_ratio: sparse_ratio.max(0.0),
            holes,
        })
    }

    /// Analyze a file for sparse regions (non-Unix fallback)
    #[cfg(not(unix))]
    pub fn analyze<P: AsRef<Path>>(&self, path: P) -> io::Result<SparseInfo> {
        let metadata = std::fs::metadata(path.as_ref())?;
        let logical_size = metadata.len();

        // On non-Unix, we can't easily detect sparse files
        // Scan for zero regions instead
        let holes = self.detect_holes(path)?;
        let holes_size: u64 = holes.iter().map(|h| h.length).sum();

        let sparse_ratio = if logical_size > 0 {
            holes_size as f64 / logical_size as f64
        } else {
            0.0
        };

        Ok(SparseInfo {
            logical_size,
            blocks_allocated: logical_size - holes_size,
            block_size: 4096,
            sparse_ratio,
            holes,
        })
    }

    /// Detect holes (zero regions) in a file
    fn detect_holes<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<HoleRegion>> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let mut holes = Vec::new();

        let mut buffer = vec![0u8; self.buffer_size];
        let mut offset: u64 = 0;
        let mut in_hole = false;
        let mut hole_start: u64 = 0;

        while offset < file_size {
            let to_read = std::cmp::min(self.buffer_size as u64, file_size - offset) as usize;
            let bytes_read = file.read(&mut buffer[..to_read])?;

            if bytes_read == 0 {
                break;
            }

            if self.is_zero_buffer(&buffer[..bytes_read]) {
                if !in_hole {
                    in_hole = true;
                    hole_start = offset;
                }
            } else {
                if in_hole {
                    let hole_length = offset - hole_start;
                    if hole_length >= self.min_hole_size {
                        holes.push(HoleRegion {
                            offset: hole_start,
                            length: hole_length,
                        });
                    }
                    in_hole = false;
                }
            }

            offset += bytes_read as u64;
        }

        // Handle hole at end of file
        if in_hole {
            let hole_length = offset - hole_start;
            if hole_length >= self.min_hole_size {
                holes.push(HoleRegion {
                    offset: hole_start,
                    length: hole_length,
                });
            }
        }

        Ok(holes)
    }

    /// Check if buffer contains only zeros
    fn is_zero_buffer(&self, buffer: &[u8]) -> bool {
        // Use SIMD-friendly comparison
        buffer.iter().all(|&b| b == 0)
    }

    /// Copy a file preserving sparse regions
    pub fn copy_sparse<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
    ) -> io::Result<SparseCopyResult> {
        let sparse_info = self.analyze(&src)?;

        if sparse_info.holes.is_empty() {
            // Not sparse, use regular copy
            return self.copy_regular(&src, &dst);
        }

        self.copy_with_holes(&src, &dst, &sparse_info)
    }

    /// Copy file preserving holes
    #[cfg(unix)]
    fn copy_with_holes<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
        sparse_info: &SparseInfo,
    ) -> io::Result<SparseCopyResult> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut src_file = File::open(&src)?;
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(&dst)?;

        // Pre-allocate the file size (creates sparse file)
        dst_file.set_len(sparse_info.logical_size)?;

        let mut buffer = vec![0u8; self.buffer_size];
        let mut bytes_written: u64 = 0;
        let mut current_pos: u64 = 0;
        let mut hole_idx = 0;

        while current_pos < sparse_info.logical_size {
            // Check if we're in a hole
            if hole_idx < sparse_info.holes.len() {
                let hole = &sparse_info.holes[hole_idx];
                if current_pos >= hole.offset && current_pos < hole.offset + hole.length {
                    // Skip the hole
                    current_pos = hole.offset + hole.length;
                    hole_idx += 1;
                    src_file.seek(SeekFrom::Start(current_pos))?;
                    dst_file.seek(SeekFrom::Start(current_pos))?;
                    continue;
                }
            }

            // Calculate how much to read (until next hole or end)
            let mut to_read = self.buffer_size as u64;
            if hole_idx < sparse_info.holes.len() {
                let next_hole = &sparse_info.holes[hole_idx];
                if next_hole.offset > current_pos {
                    to_read = std::cmp::min(to_read, next_hole.offset - current_pos);
                }
            }
            to_read = std::cmp::min(to_read, sparse_info.logical_size - current_pos);

            let bytes_read = src_file.read(&mut buffer[..to_read as usize])?;
            if bytes_read == 0 {
                break;
            }

            dst_file.write_all(&buffer[..bytes_read])?;
            bytes_written += bytes_read as u64;
            current_pos += bytes_read as u64;
        }

        let holes_size: u64 = sparse_info.holes.iter().map(|h| h.length).sum();

        Ok(SparseCopyResult {
            bytes_written,
            logical_size: sparse_info.logical_size,
            holes_preserved: sparse_info.holes.len() as u64,
            space_saved: holes_size,
        })
    }

    /// Copy file preserving holes (Windows version using FSCTL_SET_SPARSE)
    #[cfg(windows)]
    fn copy_with_holes<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
        sparse_info: &SparseInfo,
    ) -> io::Result<SparseCopyResult> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use winapi::um::ioapiset::DeviceIoControl;
        use winapi::um::winioctl::FSCTL_SET_SPARSE;

        let mut src_file = File::open(&src)?;
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dst)?;

        // Mark destination as sparse file
        unsafe {
            let handle = dst_file.as_raw_handle();
            let mut bytes_returned: u32 = 0;
            DeviceIoControl(
                handle as *mut _,
                FSCTL_SET_SPARSE,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
                &mut bytes_returned,
                std::ptr::null_mut(),
            );
        }

        // Pre-allocate size
        dst_file.set_len(sparse_info.logical_size)?;

        let mut buffer = vec![0u8; self.buffer_size];
        let mut bytes_written: u64 = 0;
        let mut current_pos: u64 = 0;
        let mut hole_idx = 0;

        while current_pos < sparse_info.logical_size {
            // Check if we're in a hole
            if hole_idx < sparse_info.holes.len() {
                let hole = &sparse_info.holes[hole_idx];
                if current_pos >= hole.offset && current_pos < hole.offset + hole.length {
                    current_pos = hole.offset + hole.length;
                    hole_idx += 1;
                    src_file.seek(SeekFrom::Start(current_pos))?;
                    dst_file.seek(SeekFrom::Start(current_pos))?;
                    continue;
                }
            }

            let mut to_read = self.buffer_size as u64;
            if hole_idx < sparse_info.holes.len() {
                let next_hole = &sparse_info.holes[hole_idx];
                if next_hole.offset > current_pos {
                    to_read = std::cmp::min(to_read, next_hole.offset - current_pos);
                }
            }
            to_read = std::cmp::min(to_read, sparse_info.logical_size - current_pos);

            let bytes_read = src_file.read(&mut buffer[..to_read as usize])?;
            if bytes_read == 0 {
                break;
            }

            dst_file.write_all(&buffer[..bytes_read])?;
            bytes_written += bytes_read as u64;
            current_pos += bytes_read as u64;
        }

        let holes_size: u64 = sparse_info.holes.iter().map(|h| h.length).sum();

        Ok(SparseCopyResult {
            bytes_written,
            logical_size: sparse_info.logical_size,
            holes_preserved: sparse_info.holes.len() as u64,
            space_saved: holes_size,
        })
    }

    /// Copy file preserving holes (fallback for non-Unix/Windows)
    #[cfg(not(any(unix, windows)))]
    fn copy_with_holes<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
        _sparse_info: &SparseInfo,
    ) -> io::Result<SparseCopyResult> {
        // Fallback to regular copy
        self.copy_regular(&src, &dst)
    }

    /// Regular file copy (no sparse handling)
    fn copy_regular<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
    ) -> io::Result<SparseCopyResult> {
        let bytes_written = std::fs::copy(&src, &dst)?;
        Ok(SparseCopyResult {
            bytes_written,
            logical_size: bytes_written,
            holes_preserved: 0,
            space_saved: 0,
        })
    }
}

impl Default for SparseCopier {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a file is likely sparse
#[cfg(unix)]
pub fn is_sparse<P: AsRef<Path>>(path: P) -> io::Result<bool> {
    let metadata = std::fs::metadata(path)?;
    let logical_size = metadata.len();
    let blocks_allocated = metadata.blocks() * 512;

    // File is sparse if allocated blocks < logical size (with some tolerance)
    Ok(blocks_allocated < logical_size.saturating_sub(4096))
}

/// Check if a file is likely sparse (non-Unix fallback)
#[cfg(not(unix))]
pub fn is_sparse<P: AsRef<Path>>(_path: P) -> io::Result<bool> {
    // Can't easily detect on non-Unix without scanning
    Ok(false)
}

/// Create a sparse file with a hole at the specified region
#[cfg(unix)]
pub fn create_sparse_file<P: AsRef<Path>>(
    path: P,
    size: u64,
    holes: &[HoleRegion],
) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(path)?;

    file.set_len(size)?;

    // Write non-zero data to non-hole regions
    let mut pos: u64 = 0;
    let data = vec![0xFFu8; 4096];

    for hole in holes {
        // Write data before the hole
        while pos < hole.offset {
            let to_write = std::cmp::min(4096, (hole.offset - pos) as usize);
            file.seek(SeekFrom::Start(pos))?;
            file.write_all(&data[..to_write])?;
            pos += to_write as u64;
        }
        // Skip the hole
        pos = hole.offset + hole.length;
    }

    // Write data after last hole
    while pos < size {
        let to_write = std::cmp::min(4096, (size - pos) as usize);
        file.seek(SeekFrom::Start(pos))?;
        file.write_all(&data[..to_write])?;
        pos += to_write as u64;
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn create_sparse_file<P: AsRef<Path>>(
    path: P,
    size: u64,
    _holes: &[HoleRegion],
) -> io::Result<()> {
    let file = File::create(path)?;
    file.set_len(size)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_sparse_copier_regular_file() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("source.txt");
        let dst = dir.path().join("dest.txt");

        fs::write(&src, "Hello, World!").unwrap();

        let copier = SparseCopier::new();
        let result = copier.copy_sparse(&src, &dst).unwrap();

        assert_eq!(result.logical_size, 13);
        assert!(fs::read_to_string(&dst).unwrap() == "Hello, World!");
    }

    #[test]
    fn test_is_zero_buffer() {
        let copier = SparseCopier::new();
        assert!(copier.is_zero_buffer(&[0, 0, 0, 0]));
        assert!(!copier.is_zero_buffer(&[0, 1, 0, 0]));
    }
}
