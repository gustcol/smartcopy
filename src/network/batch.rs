//! TAR batch streaming for efficient small-file transfers
//!
//! When transferring many small files, per-file overhead (open/close/metadata)
//! dominates. This module batches small files into TAR archives streamed as
//! a single transfer unit, dramatically reducing overhead.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

/// Default batch size threshold: 64 MB
const DEFAULT_BATCH_SIZE: u64 = 64 * 1024 * 1024;

/// Default small file threshold: files under 1 MB are batched
const SMALL_FILE_THRESHOLD: u64 = 1024 * 1024;

/// Builds TAR archives from a collection of small files.
pub struct BatchBuilder {
    batch_size_limit: u64,
    small_file_threshold: u64,
}

impl BatchBuilder {
    /// Create a new BatchBuilder with default settings.
    pub fn new() -> Self {
        Self {
            batch_size_limit: DEFAULT_BATCH_SIZE,
            small_file_threshold: SMALL_FILE_THRESHOLD,
        }
    }

    /// Set the maximum batch size in bytes.
    pub fn with_batch_size(mut self, size: u64) -> Self {
        self.batch_size_limit = size;
        self
    }

    /// Set the threshold below which files are considered "small".
    pub fn with_small_file_threshold(mut self, size: u64) -> Self {
        self.small_file_threshold = size;
        self
    }

    /// Partition files into batches of small files and a list of large files.
    ///
    /// Returns `(batches, large_files)` where each batch is a Vec of paths
    /// that fit within `batch_size_limit`.
    pub fn partition_files(
        &self,
        files: &[(PathBuf, u64)],
    ) -> (Vec<Vec<(PathBuf, u64)>>, Vec<(PathBuf, u64)>) {
        let mut batches: Vec<Vec<(PathBuf, u64)>> = Vec::new();
        let mut current_batch: Vec<(PathBuf, u64)> = Vec::new();
        let mut current_size: u64 = 0;
        let mut large_files: Vec<(PathBuf, u64)> = Vec::new();

        for (path, size) in files {
            if *size >= self.small_file_threshold {
                large_files.push((path.clone(), *size));
                continue;
            }

            if current_size + size > self.batch_size_limit && !current_batch.is_empty() {
                batches.push(std::mem::take(&mut current_batch));
                current_size = 0;
            }

            current_batch.push((path.clone(), *size));
            current_size += size;
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        (batches, large_files)
    }

    /// Create a TAR archive from a batch of files, writing to the given writer.
    pub fn create_tar<W: Write>(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
        writer: W,
    ) -> io::Result<u64> {
        let mut builder = tar::Builder::new(writer);
        let mut total_bytes: u64 = 0;

        for (path, _size) in files {
            let full_path = base_dir.join(path);
            if full_path.exists() {
                builder.append_path_with_name(&full_path, path)?;
                total_bytes += _size;
            }
        }

        builder.finish()?;
        Ok(total_bytes)
    }
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracts files from a TAR archive stream.
pub struct BatchExtractor;

impl BatchExtractor {
    /// Extract a TAR archive into the given destination directory.
    pub fn extract<R: Read>(reader: R, dest_dir: &Path) -> io::Result<usize> {
        let mut archive = tar::Archive::new(reader);
        let mut count = 0;

        for entry in archive.entries()? {
            let mut entry = entry?;
            let dest_path = dest_dir.join(entry.path()?);

            // Create parent directories
            if let Some(parent) = dest_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            entry.unpack(&dest_path)?;
            count += 1;
        }

        Ok(count)
    }
}

/// Manages batch transfer operations combining partitioning, archiving,
/// and extraction into a single workflow.
pub struct BatchTransferManager {
    builder: BatchBuilder,
}

impl BatchTransferManager {
    pub fn new(batch_size_mb: u64) -> Self {
        Self {
            builder: BatchBuilder::new().with_batch_size(batch_size_mb * 1024 * 1024),
        }
    }

    /// Get a reference to the underlying BatchBuilder.
    pub fn builder(&self) -> &BatchBuilder {
        &self.builder
    }

    /// Create a batch archive from a set of files, writing to a temp file.
    /// Returns the path to the temporary TAR file.
    pub fn create_batch_file(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
    ) -> io::Result<(PathBuf, u64)> {
        let tmp = std::env::temp_dir().join(format!(
            "smartcopy-batch-{}.tar",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let file = File::create(&tmp)?;
        let bytes = self.builder.create_tar(base_dir, files, file)?;

        Ok((tmp, bytes))
    }

    /// Extract a batch TAR file into the destination.
    pub fn extract_batch_file(&self, tar_path: &Path, dest_dir: &Path) -> io::Result<usize> {
        let file = File::open(tar_path)?;
        let reader = BufReader::new(file);
        BatchExtractor::extract(reader, dest_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn test_partition_files() {
        let builder = BatchBuilder::new()
            .with_batch_size(1024 * 1024) // 1 MB batches
            .with_small_file_threshold(512 * 1024); // 512 KB threshold

        let files: Vec<(PathBuf, u64)> = (0..20)
            .map(|i| (PathBuf::from(format!("small_{}.txt", i)), 100 * 1024)) // 100 KB each
            .chain(std::iter::once((PathBuf::from("large.bin"), 2 * 1024 * 1024)))
            .collect();

        let (batches, large) = builder.partition_files(&files);

        assert_eq!(large.len(), 1);
        assert_eq!(large[0].0, PathBuf::from("large.bin"));

        let total_small: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_small, 20);
    }

    #[test]
    fn test_tar_roundtrip() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        // Create test files
        for i in 0..5 {
            let p = src_dir.path().join(format!("file_{}.txt", i));
            let mut f = File::create(&p).unwrap();
            writeln!(f, "content of file {}", i).unwrap();
        }

        let files: Vec<(PathBuf, u64)> = (0..5)
            .map(|i| {
                let name = format!("file_{}.txt", i);
                let size = src_dir.path().join(&name).metadata().unwrap().len();
                (PathBuf::from(name), size)
            })
            .collect();

        // Create TAR
        let builder = BatchBuilder::new();
        let mut tar_buf: Vec<u8> = Vec::new();
        builder
            .create_tar(src_dir.path(), &files, &mut tar_buf)
            .unwrap();

        // Extract TAR
        let count = BatchExtractor::extract(&tar_buf[..], dst_dir.path()).unwrap();
        assert_eq!(count, 5);

        // Verify content
        for i in 0..5 {
            let extracted = dst_dir.path().join(format!("file_{}.txt", i));
            assert!(extracted.exists());
            let content = std::fs::read_to_string(&extracted).unwrap();
            assert_eq!(content.trim(), format!("content of file {}", i));
        }
    }
}
