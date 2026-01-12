//! High-performance file operations
//!
//! Provides optimized file copy operations using memory mapping,
//! zero-copy techniques, and platform-specific optimizations.

use crate::error::{IoResultExt, Result, SmartCopyError};
use crate::fs::FileEntry;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufReader, BufWriter};
use std::path::Path;



/// Copy operation statistics
#[derive(Debug, Clone, Default)]
pub struct CopyStats {
    /// Bytes copied
    pub bytes_copied: u64,
    /// Duration of the copy
    pub duration: std::time::Duration,
    /// Throughput in bytes/second
    pub throughput: f64,
    /// Method used for copy
    pub method: CopyMethod,
}

impl CopyStats {
    /// Calculate throughput from bytes and duration
    pub fn calculate_throughput(&mut self) {
        if self.duration.as_secs_f64() > 0.0 {
            self.throughput = self.bytes_copied as f64 / self.duration.as_secs_f64();
        }
    }
}

/// Copy method used
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CopyMethod {
    /// Standard buffered I/O
    #[default]
    Buffered,
    /// Memory-mapped I/O
    MemoryMapped,
    /// Zero-copy using splice/sendfile
    ZeroCopy,
    /// Chunked parallel copy
    ParallelChunks,
    /// Network-optimized (SMB multichannel, NFS parallel)
    NetworkOptimized,
}

/// Storage type detection for optimization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedStorageType {
    /// Local NVMe SSD
    NVMe,
    /// Local SSD
    SSD,
    /// Local HDD
    HDD,
    /// SMB/CIFS network share (supports multichannel)
    SMB,
    /// NFS network filesystem
    NFS,
    /// Other network filesystem (GPFS, Lustre, etc.)
    NetworkFS,
    /// Unknown storage type
    Unknown,
}

/// Options for file copy operations
#[derive(Debug, Clone)]
pub struct CopyOptions {
    /// Buffer size for buffered operations
    pub buffer_size: usize,
    /// Preserve file permissions
    pub preserve_permissions: bool,
    /// Preserve modification time
    pub preserve_mtime: bool,
    /// Use memory mapping for large files
    pub use_mmap: bool,
    /// Minimum size for memory mapping
    pub mmap_threshold: u64,
    /// Use zero-copy if available
    pub use_zero_copy: bool,
    /// Preallocate destination file
    pub preallocate: bool,
    /// Sync to disk after copy
    pub sync: bool,
    /// Use direct I/O (bypass page cache)
    pub direct_io: bool,
    /// Enable network-optimized transfers (SMB multichannel, NFS parallel)
    pub network_optimized: bool,
    /// Number of parallel streams for network transfers (SMB multichannel)
    pub network_streams: usize,
    /// Network buffer size (larger for high-latency networks)
    pub network_buffer_size: usize,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            buffer_size: 1024 * 1024, // 1MB
            preserve_permissions: true,
            preserve_mtime: true,
            use_mmap: true,
            mmap_threshold: 10 * 1024 * 1024, // 10MB
            use_zero_copy: true,
            preallocate: true,
            sync: false,
            direct_io: false,
            network_optimized: true,
            network_streams: 4, // SMB multichannel typically uses 4-8 channels
            network_buffer_size: 4 * 1024 * 1024, // 4MB for network transfers
        }
    }
}

impl CopyOptions {
    /// Create options optimized for SMB/CIFS transfers with multichannel support
    pub fn for_smb_multichannel() -> Self {
        Self {
            buffer_size: 4 * 1024 * 1024, // 4MB - matches SMB max read/write size
            preserve_permissions: true,
            preserve_mtime: true,
            use_mmap: false, // mmap not efficient over network
            mmap_threshold: u64::MAX, // disable mmap
            use_zero_copy: false, // not available for network FS
            preallocate: true,
            sync: false,
            direct_io: false,
            network_optimized: true,
            network_streams: 8, // SMB3 multichannel can use up to 8 channels
            network_buffer_size: 4 * 1024 * 1024, // 4MB optimal for SMB
        }
    }

    /// Create options optimized for NFS transfers
    pub fn for_nfs() -> Self {
        Self {
            buffer_size: 1024 * 1024, // 1MB - common NFS rsize/wsize
            preserve_permissions: true,
            preserve_mtime: true,
            use_mmap: false,
            mmap_threshold: u64::MAX,
            use_zero_copy: false,
            preallocate: true,
            sync: false,
            direct_io: false,
            network_optimized: true,
            network_streams: 4, // NFS pNFS can use parallel streams
            network_buffer_size: 1024 * 1024,
        }
    }

    /// Create options optimized for local NVMe/SSD
    pub fn for_local_ssd() -> Self {
        Self {
            buffer_size: 1024 * 1024, // 1MB
            preserve_permissions: true,
            preserve_mtime: true,
            use_mmap: true,
            mmap_threshold: 10 * 1024 * 1024, // 10MB
            use_zero_copy: true,
            preallocate: true,
            sync: false,
            direct_io: false,
            network_optimized: false,
            network_streams: 1,
            network_buffer_size: 1024 * 1024,
        }
    }
}

/// Detect storage type from filesystem path
pub fn detect_storage_type(path: &Path) -> DetectedStorageType {
    #[cfg(unix)]
    {
        use std::process::Command;

        // Try to get mount info
        if let Ok(output) = Command::new("df").arg("-T").arg(path).output() {
            if let Ok(stdout) = String::from_utf8(output.stdout) {
                let lower = stdout.to_lowercase();

                // Check filesystem type
                if lower.contains("cifs") || lower.contains("smb") {
                    return DetectedStorageType::SMB;
                }
                if lower.contains("nfs") {
                    return DetectedStorageType::NFS;
                }
                if lower.contains("gpfs") || lower.contains("lustre")
                    || lower.contains("gluster") || lower.contains("ceph")
                    || lower.contains("beegfs") {
                    return DetectedStorageType::NetworkFS;
                }
            }
        }

        // Check for NVMe device
        if let Ok(output) = Command::new("df").arg(path).output() {
            if let Ok(stdout) = String::from_utf8(output.stdout) {
                if stdout.contains("nvme") {
                    return DetectedStorageType::NVMe;
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        // On macOS, check mount output
        if let Ok(output) = std::process::Command::new("mount").output() {
            if let Ok(stdout) = String::from_utf8(output.stdout) {
                let path_str = path.to_string_lossy();
                for line in stdout.lines() {
                    if line.contains("smbfs") && line.contains(&*path_str) {
                        return DetectedStorageType::SMB;
                    }
                    if line.contains("nfs") && line.contains(&*path_str) {
                        return DetectedStorageType::NFS;
                    }
                }
            }
        }
    }

    DetectedStorageType::Unknown
}

/// Get optimized copy options based on detected storage type
pub fn get_optimized_options(source: &Path, dest: &Path) -> CopyOptions {
    let src_type = detect_storage_type(source);
    let dst_type = detect_storage_type(dest);

    // If either is SMB, use SMB multichannel optimization
    if src_type == DetectedStorageType::SMB || dst_type == DetectedStorageType::SMB {
        return CopyOptions::for_smb_multichannel();
    }

    // If either is NFS, use NFS optimization
    if src_type == DetectedStorageType::NFS || dst_type == DetectedStorageType::NFS {
        return CopyOptions::for_nfs();
    }

    // If either is other network FS, use NFS-like options
    if src_type == DetectedStorageType::NetworkFS || dst_type == DetectedStorageType::NetworkFS {
        return CopyOptions::for_nfs();
    }

    // For local NVMe/SSD, use local optimizations
    if src_type == DetectedStorageType::NVMe || dst_type == DetectedStorageType::NVMe
        || src_type == DetectedStorageType::SSD || dst_type == DetectedStorageType::SSD {
        return CopyOptions::for_local_ssd();
    }

    // Default options
    CopyOptions::default()
}

/// High-performance file copier
pub struct FileCopier {
    options: CopyOptions,
}

impl FileCopier {
    /// Create a new file copier with the given options
    pub fn new(options: CopyOptions) -> Self {
        Self { options }
    }

    /// Create with default options
    pub fn default_copier() -> Self {
        Self::new(CopyOptions::default())
    }

    /// Create with auto-detected optimized options for the given paths
    pub fn auto_optimized(source: &Path, dest: &Path) -> Self {
        Self::new(get_optimized_options(source, dest))
    }

    /// Copy a file from source to destination
    pub fn copy(&self, source: &Path, dest: &Path) -> Result<CopyStats> {
        let start = std::time::Instant::now();

        // Get source metadata
        let metadata = std::fs::metadata(source).with_path(source)?;
        let size = metadata.len();

        // Ensure parent directory exists
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).with_path(parent)?;
        }

        // Choose copy method based on file size and options
        let (bytes_copied, method) = if size == 0 {
            // Empty file - just create it
            File::create(dest).with_path(dest)?;
            (0, CopyMethod::Buffered)
        } else if self.options.use_zero_copy && self.can_use_zero_copy() {
            match self.copy_zero_copy(source, dest, size) {
                Ok(bytes) => (bytes, CopyMethod::ZeroCopy),
                Err(_) => {
                    // Fallback to buffered copy
                    let bytes = self.copy_buffered(source, dest)?;
                    (bytes, CopyMethod::Buffered)
                }
            }
        } else if self.options.use_mmap && size >= self.options.mmap_threshold {
            match self.copy_mmap(source, dest, size) {
                Ok(bytes) => (bytes, CopyMethod::MemoryMapped),
                Err(_) => {
                    // Fallback to buffered copy
                    let bytes = self.copy_buffered(source, dest)?;
                    (bytes, CopyMethod::Buffered)
                }
            }
        } else {
            let bytes = self.copy_buffered(source, dest)?;
            (bytes, CopyMethod::Buffered)
        };

        // Preserve attributes
        if self.options.preserve_permissions {
            self.copy_permissions(source, dest)?;
        }

        if self.options.preserve_mtime {
            self.copy_mtime(source, dest)?;
        }

        // Sync if requested
        if self.options.sync {
            let file = File::open(dest).with_path(dest)?;
            file.sync_all().with_path(dest)?;
        }

        let duration = start.elapsed();
        let mut stats = CopyStats {
            bytes_copied,
            duration,
            throughput: 0.0,
            method,
        };
        stats.calculate_throughput();

        Ok(stats)
    }

    /// Buffered copy - reliable fallback
    fn copy_buffered(&self, source: &Path, dest: &Path) -> Result<u64> {
        let src_file = File::open(source).with_path(source)?;
        let dst_file = File::create(dest).with_path(dest)?;

        // Preallocate if possible
        if self.options.preallocate {
            let size = src_file.metadata().with_path(source)?.len();
            if size > 0 {
                let _ = dst_file.set_len(size);
            }
        }

        let mut reader = BufReader::with_capacity(self.options.buffer_size, src_file);
        let mut writer = BufWriter::with_capacity(self.options.buffer_size, dst_file);

        let bytes_copied = std::io::copy(&mut reader, &mut writer)
            .map_err(|e| SmartCopyError::io(source, e))?;

        writer.flush().with_path(dest)?;

        Ok(bytes_copied)
    }

    /// Memory-mapped copy for large files
    fn copy_mmap(&self, source: &Path, dest: &Path, size: u64) -> Result<u64> {
        use memmap2::{Mmap, MmapMut};

        let src_file = File::open(source).with_path(source)?;
        let dst_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)
            .with_path(dest)?;

        // Set destination file size
        dst_file.set_len(size).with_path(dest)?;

        // Memory map both files
        let src_mmap = unsafe { Mmap::map(&src_file) }
            .map_err(|e| SmartCopyError::io(source, e))?;
        let mut dst_mmap = unsafe { MmapMut::map_mut(&dst_file) }
            .map_err(|e| SmartCopyError::io(dest, e))?;

        // Copy data
        dst_mmap.copy_from_slice(&src_mmap);

        // Flush memory map
        dst_mmap.flush()
            .map_err(|e| SmartCopyError::io(dest, e))?;

        Ok(size)
    }

    /// Zero-copy using splice (Linux) or copy_file_range
    #[cfg(target_os = "linux")]
    fn copy_zero_copy(&self, source: &Path, dest: &Path, size: u64) -> Result<u64> {
        use std::os::unix::io::AsRawFd;

        let src_file = File::open(source).with_path(source)?;
        let dst_file = File::create(dest).with_path(dest)?;

        // Preallocate destination
        if self.options.preallocate && size > 0 {
            let _ = dst_file.set_len(size);
        }

        let src_fd = src_file.as_raw_fd();
        let dst_fd = dst_file.as_raw_fd();

        let mut total_copied: u64 = 0;
        let mut offset_in: i64 = 0;
        let mut offset_out: i64 = 0;

        while total_copied < size {
            let to_copy = (size - total_copied).min(i64::MAX as u64) as usize;

            let copied = unsafe {
                libc::copy_file_range(
                    src_fd,
                    &mut offset_in,
                    dst_fd,
                    &mut offset_out,
                    to_copy,
                    0,
                )
            };

            if copied < 0 {
                let err = std::io::Error::last_os_error();
                return Err(SmartCopyError::io(source, err));
            }

            if copied == 0 {
                break; // EOF
            }

            total_copied += copied as u64;
        }

        Ok(total_copied)
    }

    #[cfg(not(target_os = "linux"))]
    fn copy_zero_copy(&self, source: &Path, dest: &Path, _size: u64) -> Result<u64> {
        // Fall back to buffered copy on non-Linux
        self.copy_buffered(source, dest)
    }

    /// Check if zero-copy is available
    fn can_use_zero_copy(&self) -> bool {
        cfg!(target_os = "linux")
    }

    /// Copy file permissions
    fn copy_permissions(&self, source: &Path, dest: &Path) -> Result<()> {
        let metadata = std::fs::metadata(source).with_path(source)?;
        let permissions = metadata.permissions();
        std::fs::set_permissions(dest, permissions).with_path(dest)?;
        Ok(())
    }

    /// Copy modification time
    fn copy_mtime(&self, source: &Path, dest: &Path) -> Result<()> {
        let metadata = std::fs::metadata(source).with_path(source)?;

        if let Ok(mtime) = metadata.modified() {
            let _ = filetime::set_file_mtime(dest, filetime::FileTime::from_system_time(mtime));
        }

        if let Ok(atime) = metadata.accessed() {
            let _ = filetime::set_file_atime(dest, filetime::FileTime::from_system_time(atime));
        }

        Ok(())
    }

    /// Copy extended attributes (xattr) - Unix only
    #[cfg(unix)]
    pub fn copy_xattr(&self, source: &Path, dest: &Path) -> Result<()> {
        use xattr::FileExt;

        let src_file = File::open(source).with_path(source)?;
        let dst_file = File::open(dest).with_path(dest)?;

        // List all extended attributes from source
        if let Ok(xattrs) = src_file.list_xattr() {
            for attr_name in xattrs {
                if let Ok(Some(value)) = src_file.get_xattr(&attr_name) {
                    // Set the attribute on destination
                    let _ = dst_file.set_xattr(&attr_name, &value);
                }
            }
        }

        Ok(())
    }

    #[cfg(not(unix))]
    pub fn copy_xattr(&self, _source: &Path, _dest: &Path) -> Result<()> {
        // Extended attributes not supported on this platform
        Ok(())
    }

    /// Copy using Direct I/O (O_DIRECT) - bypasses page cache for huge files
    /// This prevents cache pollution and can improve performance for sequential large file copies
    #[cfg(target_os = "linux")]
    pub fn copy_direct_io(&self, source: &Path, dest: &Path, size: u64) -> Result<u64> {
        use std::os::unix::io::AsRawFd;

        // O_DIRECT requires aligned buffers and aligned file offsets
        const ALIGNMENT: usize = 4096; // 4KB alignment for most filesystems
        const BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4MB buffer

        // Open source with O_DIRECT
        let src_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(source)
            .with_path(source)?;

        // Open destination with O_DIRECT
        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(dest)
            .with_path(dest)?;

        // Preallocate destination
        if self.options.preallocate && size > 0 {
            let _ = dst_file.set_len(size);
        }

        let src_fd = src_file.as_raw_fd();
        let dst_fd = dst_file.as_raw_fd();

        // Allocate aligned buffer
        let layout = std::alloc::Layout::from_size_align(BUFFER_SIZE, ALIGNMENT)
            .map_err(|_| SmartCopyError::IoError {
                path: source.to_path_buf(),
                message: "Failed to create aligned buffer layout".to_string(),
            })?;

        let buffer = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                return Err(SmartCopyError::IoError {
                    path: source.to_path_buf(),
                    message: "Failed to allocate aligned buffer".to_string(),
                });
            }
            std::slice::from_raw_parts_mut(ptr, BUFFER_SIZE)
        };

        let mut total_copied = 0u64;
        let mut offset = 0i64;

        loop {
            // Read with pread for thread safety
            let bytes_read = unsafe {
                libc::pread(src_fd, buffer.as_mut_ptr() as *mut libc::c_void, BUFFER_SIZE, offset)
            };

            if bytes_read < 0 {
                // Cleanup and return error
                unsafe {
                    std::alloc::dealloc(buffer.as_mut_ptr(), layout);
                }
                let err = std::io::Error::last_os_error();
                return Err(SmartCopyError::io(source, err));
            }

            if bytes_read == 0 {
                break; // EOF
            }

            // For the last chunk, we might need to handle non-aligned size
            let to_write = bytes_read as usize;

            // Write with pwrite
            let bytes_written = unsafe {
                libc::pwrite(dst_fd, buffer.as_ptr() as *const libc::c_void, to_write, offset)
            };

            if bytes_written < 0 {
                unsafe {
                    std::alloc::dealloc(buffer.as_mut_ptr(), layout);
                }
                let err = std::io::Error::last_os_error();
                return Err(SmartCopyError::io(dest, err));
            }

            offset += bytes_read;
            total_copied += bytes_read as u64;
        }

        // Cleanup aligned buffer
        unsafe {
            std::alloc::dealloc(buffer.as_mut_ptr(), layout);
        }

        // Truncate to exact size if needed (in case of alignment padding)
        if total_copied != size {
            let _ = dst_file.set_len(size);
        }

        Ok(total_copied.min(size))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn copy_direct_io(&self, source: &Path, dest: &Path, _size: u64) -> Result<u64> {
        // Direct I/O not available, fall back to buffered copy
        self.copy_buffered(source, dest)
    }

    /// Preserve file attributes (permissions and mtime)
    pub fn preserve_attributes(&self, source: &Path, dest: &Path) -> Result<()> {
        if self.options.preserve_permissions {
            self.copy_permissions(source, dest)?;
        }

        if self.options.preserve_mtime {
            self.copy_mtime(source, dest)?;
        }

        Ok(())
    }

    /// Preserve all attributes including xattr
    pub fn preserve_all_attributes(&self, source: &Path, dest: &Path) -> Result<()> {
        self.preserve_attributes(source, dest)?;
        self.copy_xattr(source, dest)?;
        Ok(())
    }

    /// Copy a file with streaming hash computation
    pub fn copy_with_hash<H: HashWriter>(
        &self,
        source: &Path,
        dest: &Path,
        hasher: &mut H,
    ) -> Result<CopyStats> {
        let start = std::time::Instant::now();

        let src_file = File::open(source).with_path(source)?;
        let dst_file = File::create(dest).with_path(dest)?;

        // Get size for preallocation
        let size = src_file.metadata().with_path(source)?.len();
        if self.options.preallocate && size > 0 {
            let _ = dst_file.set_len(size);
        }

        let mut reader = BufReader::with_capacity(self.options.buffer_size, src_file);
        let mut writer = BufWriter::with_capacity(self.options.buffer_size, dst_file);

        let mut buffer = vec![0u8; self.options.buffer_size];
        let mut bytes_copied = 0u64;

        loop {
            let bytes_read = reader.read(&mut buffer)
                .map_err(|e| SmartCopyError::io(source, e))?;

            if bytes_read == 0 {
                break;
            }

            // Update hash
            hasher.update(&buffer[..bytes_read]);

            // Write to destination
            writer.write_all(&buffer[..bytes_read])
                .map_err(|e| SmartCopyError::io(dest, e))?;

            bytes_copied += bytes_read as u64;
        }

        writer.flush().with_path(dest)?;

        // Preserve attributes
        if self.options.preserve_permissions {
            self.copy_permissions(source, dest)?;
        }

        if self.options.preserve_mtime {
            self.copy_mtime(source, dest)?;
        }

        let duration = start.elapsed();
        let mut stats = CopyStats {
            bytes_copied,
            duration,
            throughput: 0.0,
            method: CopyMethod::Buffered,
        };
        stats.calculate_throughput();

        Ok(stats)
    }
}

/// Trait for hash writers that can receive streaming data
pub trait HashWriter {
    /// Update the hash with more data
    fn update(&mut self, data: &[u8]);
}

/// Copy a file entry to a new location
pub fn copy_entry(entry: &FileEntry, dest_root: &Path, options: &CopyOptions) -> Result<CopyStats> {
    let dest_path = dest_root.join(&entry.relative_path);

    if entry.is_dir {
        std::fs::create_dir_all(&dest_path).with_path(&dest_path)?;
        return Ok(CopyStats::default());
    }

    if entry.is_symlink {
        if let Some(target) = &entry.symlink_target {
            // Remove existing symlink if any
            let _ = std::fs::remove_file(&dest_path);
            std::os::unix::fs::symlink(target, &dest_path)
                .map_err(|e| SmartCopyError::SymlinkError {
                    path: dest_path.clone(),
                    message: e.to_string(),
                })?;
        }
        return Ok(CopyStats::default());
    }

    let copier = FileCopier::new(options.clone());
    copier.copy(&entry.path, &dest_path)
}

/// Create all directories from a list of entries
pub fn create_directories(entries: &[FileEntry], dest_root: &Path) -> Result<usize> {
    let mut created = 0;

    // Sort by path length to create parents first
    let mut dirs: Vec<_> = entries.iter().filter(|e| e.is_dir).collect();
    dirs.sort_by_key(|e| e.relative_path.components().count());

    for entry in dirs {
        let dest_path = dest_root.join(&entry.relative_path);
        if !dest_path.exists() {
            std::fs::create_dir_all(&dest_path).with_path(&dest_path)?;
            created += 1;
        }
    }

    Ok(created)
}

/// Verify that a file was copied correctly by size comparison
pub fn verify_copy_size(source: &Path, dest: &Path) -> Result<bool> {
    let src_meta = std::fs::metadata(source).with_path(source)?;
    let dst_meta = std::fs::metadata(dest).with_path(dest)?;

    Ok(src_meta.len() == dst_meta.len())
}

/// Remove a file or directory
pub fn remove_path(path: &Path) -> Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path).with_path(path)?;
    } else {
        std::fs::remove_file(path).with_path(path)?;
    }
    Ok(())
}

/// Get available space at a path
pub fn available_space(path: &Path) -> Result<u64> {
    use sysinfo::Disks;

    let disks = Disks::new_with_refreshed_list();

    let path_str = path.to_string_lossy();
    let mut best_match = None;
    let mut best_len = 0;

    for disk in disks.iter() {
        let mount = disk.mount_point().to_string_lossy();
        if path_str.starts_with(mount.as_ref()) && mount.len() > best_len {
            best_match = Some(disk.available_space());
            best_len = mount.len();
        }
    }

    best_match.ok_or_else(|| SmartCopyError::NotFound(path.to_path_buf()))
}

/// Check if there's enough space for a copy operation
pub fn check_space(dest: &Path, required: u64) -> Result<()> {
    let available = available_space(dest)?;

    if available < required {
        return Err(SmartCopyError::InsufficientSpace {
            path: dest.to_path_buf(),
            required,
            available,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write;

    fn create_test_file(dir: &Path, name: &str, size: usize) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![0xABu8; size]).unwrap();
        path
    }

    #[test]
    fn test_copy_small_file() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();

        let src = create_test_file(src_dir.path(), "test.txt", 1024);
        let dst = dst_dir.path().join("test.txt");

        let copier = FileCopier::default_copier();
        let stats = copier.copy(&src, &dst).unwrap();

        assert_eq!(stats.bytes_copied, 1024);
        assert!(dst.exists());
        assert!(verify_copy_size(&src, &dst).unwrap());
    }

    #[test]
    fn test_copy_large_file_mmap() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();

        // Create a file larger than mmap threshold
        let src = create_test_file(src_dir.path(), "large.bin", 15 * 1024 * 1024);
        let dst = dst_dir.path().join("large.bin");

        let options = CopyOptions {
            mmap_threshold: 10 * 1024 * 1024,
            ..Default::default()
        };

        let copier = FileCopier::new(options);
        let stats = copier.copy(&src, &dst).unwrap();

        assert_eq!(stats.bytes_copied, 15 * 1024 * 1024);
        assert!(dst.exists());
        assert!(verify_copy_size(&src, &dst).unwrap());
    }

    #[test]
    fn test_copy_empty_file() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();

        let src = src_dir.path().join("empty.txt");
        File::create(&src).unwrap();
        let dst = dst_dir.path().join("empty.txt");

        let copier = FileCopier::default_copier();
        let stats = copier.copy(&src, &dst).unwrap();

        assert_eq!(stats.bytes_copied, 0);
        assert!(dst.exists());
    }

    #[test]
    fn test_copy_creates_parent_dirs() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();

        let src = create_test_file(src_dir.path(), "test.txt", 100);
        let dst = dst_dir.path().join("a/b/c/test.txt");

        let copier = FileCopier::default_copier();
        copier.copy(&src, &dst).unwrap();

        assert!(dst.exists());
        assert!(dst.parent().unwrap().exists());
    }

    #[test]
    fn test_available_space() {
        let dir = TempDir::new().unwrap();
        let space = available_space(dir.path());
        assert!(space.is_ok());
        assert!(space.unwrap() > 0);
    }
}
