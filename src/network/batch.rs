//! TAR batch streaming for efficient small-file transfers
//!
//! When transferring many small files, per-file overhead (open/close/metadata)
//! dominates. This module batches small files into TAR archives streamed as
//! a single transfer unit, dramatically reducing overhead.
//!
//! Supports optional compression (Zstd, LZ4), persistent worker pools for
//! CPU-bound batch creation, and adaptive batch sizing that self-tunes to
//! the hardware's throughput characteristics.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default batch size threshold: 64 MB
const DEFAULT_BATCH_SIZE: u64 = 64 * 1024 * 1024;

/// Default small file threshold: files under 1 MB are batched
const SMALL_FILE_THRESHOLD: u64 = 1024 * 1024;

/// Minimum batch file count for adaptive sizing
const MIN_BATCH_FILES: usize = 10;

/// Maximum batch file count for adaptive sizing
const MAX_BATCH_FILES: usize = 50_000;

/// Default file count per batch for adaptive sizing
const DEFAULT_BATCH_FILES: usize = 1_000;

/// Target batch processing time lower bound (too fast → grow batch)
const ADAPTIVE_FAST_THRESHOLD: Duration = Duration::from_secs(1);

/// Target batch processing time upper bound (too slow → shrink batch)
const ADAPTIVE_SLOW_THRESHOLD: Duration = Duration::from_secs(5);

/// Batch archive format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFormat {
    /// Plain TAR (no compression)
    Tar,
    /// TAR with Zstd compression
    #[cfg(feature = "batch_zstd")]
    TarZstd,
    /// TAR with LZ4 compression (uses the existing lz4_flex crate)
    TarLz4,
}

impl Default for BatchFormat {
    fn default() -> Self {
        Self::Tar
    }
}

impl BatchFormat {
    /// Parse from a CLI string.
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "tar" => Self::Tar,
            #[cfg(feature = "batch_zstd")]
            "tar-zstd" | "zstd" => Self::TarZstd,
            "tar-lz4" | "lz4" => Self::TarLz4,
            _ => Self::Tar,
        }
    }
}

/// Builds TAR archives from a collection of small files.
pub struct BatchBuilder {
    batch_size_limit: u64,
    small_file_threshold: u64,
    format: BatchFormat,
}

impl BatchBuilder {
    /// Create a new BatchBuilder with default settings.
    pub fn new() -> Self {
        Self {
            batch_size_limit: DEFAULT_BATCH_SIZE,
            small_file_threshold: SMALL_FILE_THRESHOLD,
            format: BatchFormat::default(),
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

    /// Set the batch archive format.
    pub fn with_format(mut self, format: BatchFormat) -> Self {
        self.format = format;
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
    /// Applies compression according to the configured format.
    pub fn create_tar<W: Write>(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
        writer: W,
    ) -> io::Result<u64> {
        match self.format {
            BatchFormat::Tar => self.create_tar_plain(base_dir, files, writer),
            #[cfg(feature = "batch_zstd")]
            BatchFormat::TarZstd => self.create_tar_zstd(base_dir, files, writer),
            BatchFormat::TarLz4 => self.create_tar_lz4(base_dir, files, writer),
        }
    }

    fn create_tar_plain<W: Write>(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
        writer: W,
    ) -> io::Result<u64> {
        let mut builder = tar::Builder::new(writer);
        let mut total_bytes: u64 = 0;

        for (path, size) in files {
            let full_path = base_dir.join(path);
            if full_path.exists() {
                builder.append_path_with_name(&full_path, path)?;
                total_bytes += size;
            }
        }

        builder.finish()?;
        Ok(total_bytes)
    }

    #[cfg(feature = "batch_zstd")]
    fn create_tar_zstd<W: Write>(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
        writer: W,
    ) -> io::Result<u64> {
        let zstd_writer = zstd::Encoder::new(writer, 3)?
            .auto_finish();
        self.create_tar_plain(base_dir, files, zstd_writer)
    }

    fn create_tar_lz4<W: Write>(
        &self,
        base_dir: &Path,
        files: &[(PathBuf, u64)],
        writer: W,
    ) -> io::Result<u64> {
        let lz4_writer = Lz4WriteAdapter::new(writer);
        self.create_tar_plain(base_dir, files, lz4_writer)
    }
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapter to stream LZ4 compression using lz4_flex frame encoder.
struct Lz4WriteAdapter<W: Write> {
    inner: W,
    buf: Vec<u8>,
}

impl<W: Write> Lz4WriteAdapter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(64 * 1024),
        }
    }
}

impl<W: Write> Write for Lz4WriteAdapter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // Buffer data, compress in chunks
        self.buf.extend_from_slice(data);

        if self.buf.len() >= 64 * 1024 {
            let compressed = lz4_flex::compress_prepend_size(&self.buf);
            // Write length-prefixed compressed block
            self.inner.write_all(&(compressed.len() as u32).to_le_bytes())?;
            self.inner.write_all(&compressed)?;
            self.buf.clear();
        }

        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let compressed = lz4_flex::compress_prepend_size(&self.buf);
            self.inner.write_all(&(compressed.len() as u32).to_le_bytes())?;
            self.inner.write_all(&compressed)?;
            self.buf.clear();
        }
        // Write zero-length terminator
        self.inner.write_all(&0u32.to_le_bytes())?;
        self.inner.flush()
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

    /// Set the batch format for all operations.
    pub fn with_format(mut self, format: BatchFormat) -> Self {
        self.builder = self.builder.with_format(format);
        self
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
        let ext = match self.builder.format {
            BatchFormat::Tar => "tar",
            #[cfg(feature = "batch_zstd")]
            BatchFormat::TarZstd => "tar.zst",
            BatchFormat::TarLz4 => "tar.lz4",
        };

        let tmp = std::env::temp_dir().join(format!(
            "smartcopy-batch-{}.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            ext
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

/// Worker pool for parallel batch creation.
///
/// Uses persistent threads (one per CPU core) with crossbeam channels
/// for lock-free work distribution. Workers stay alive between batches,
/// avoiding thread spawn overhead.
pub struct BatchWorkerPool {
    sender: crossbeam::channel::Sender<BatchJob>,
    _workers: Vec<std::thread::JoinHandle<()>>,
    results: crossbeam::channel::Receiver<BatchResult>,
}

/// A batch job sent to workers.
struct BatchJob {
    id: u64,
    base_dir: PathBuf,
    files: Vec<(PathBuf, u64)>,
    format: BatchFormat,
}

/// Result from a completed batch job.
pub struct BatchResult {
    /// Job ID
    pub id: u64,
    /// TAR archive bytes
    pub data: Vec<u8>,
    /// Total uncompressed bytes of the input files
    pub input_bytes: u64,
    /// Processing duration
    pub duration: Duration,
}

impl BatchWorkerPool {
    /// Create a worker pool with one worker per CPU core.
    pub fn new(num_workers: usize) -> Self {
        let (job_tx, job_rx) = crossbeam::channel::bounded::<BatchJob>(num_workers * 2);
        let (result_tx, result_rx) = crossbeam::channel::unbounded::<BatchResult>();

        let mut workers = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let rx = job_rx.clone();
            let tx = result_tx.clone();

            let handle = std::thread::Builder::new()
                .name(format!("batch-worker-{}", worker_id))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        let start = Instant::now();
                        let builder = BatchBuilder::new().with_format(job.format);
                        let mut tar_buf: Vec<u8> = Vec::new();

                        let input_bytes = builder
                            .create_tar(&job.base_dir, &job.files, &mut tar_buf)
                            .unwrap_or(0);

                        let _ = tx.send(BatchResult {
                            id: job.id,
                            data: tar_buf,
                            input_bytes,
                            duration: start.elapsed(),
                        });
                    }
                })
                .expect("failed to spawn batch worker");

            workers.push(handle);
        }

        Self {
            sender: job_tx,
            _workers: workers,
            results: result_rx,
        }
    }

    /// Submit a batch job for processing. Returns immediately.
    pub fn submit(
        &self,
        id: u64,
        base_dir: PathBuf,
        files: Vec<(PathBuf, u64)>,
        format: BatchFormat,
    ) -> Result<(), crossbeam::channel::SendError<()>> {
        self.sender
            .send(BatchJob {
                id,
                base_dir,
                files,
                format,
            })
            .map_err(|_| crossbeam::channel::SendError(()))
    }

    /// Receive the next completed batch result (blocking).
    pub fn recv(&self) -> Option<BatchResult> {
        self.results.recv().ok()
    }

    /// Try to receive a result without blocking.
    pub fn try_recv(&self) -> Option<BatchResult> {
        self.results.try_recv().ok()
    }
}

/// Adaptive batch sizer that dynamically adjusts the number of files per batch
/// based on observed processing duration.
///
/// The goal is to keep each batch within a 1-5 second processing window
/// for smooth throughput and responsive progress reporting.
pub struct AdaptiveBatchSizer {
    current_batch_files: usize,
    min_files: usize,
    max_files: usize,
    /// Exponential moving average of throughput (files/sec)
    ema_throughput: f64,
    /// Smoothing factor for EMA (0.0 - 1.0)
    ema_alpha: f64,
}

impl AdaptiveBatchSizer {
    /// Create with default settings.
    pub fn new() -> Self {
        Self {
            current_batch_files: DEFAULT_BATCH_FILES,
            min_files: MIN_BATCH_FILES,
            max_files: MAX_BATCH_FILES,
            ema_throughput: 0.0,
            ema_alpha: 0.3,
        }
    }

    /// Create with custom initial batch size.
    pub fn with_initial_size(mut self, files: usize) -> Self {
        self.current_batch_files = files.clamp(self.min_files, self.max_files);
        self
    }

    /// Return the current recommended batch size (number of files).
    pub fn current_size(&self) -> usize {
        self.current_batch_files
    }

    /// Report the result of processing a batch and adjust sizing.
    ///
    /// Call this after each batch completes with the actual file count
    /// and processing duration.
    pub fn report(&mut self, files_processed: usize, duration: Duration) {
        if files_processed == 0 || duration.as_secs_f64() < 0.001 {
            return;
        }

        let throughput = files_processed as f64 / duration.as_secs_f64();

        // Update exponential moving average
        if self.ema_throughput == 0.0 {
            self.ema_throughput = throughput;
        } else {
            self.ema_throughput =
                self.ema_alpha * throughput + (1.0 - self.ema_alpha) * self.ema_throughput;
        }

        // Adjust batch size based on processing time
        if duration < ADAPTIVE_FAST_THRESHOLD {
            // Too fast → increase by 50%
            self.current_batch_files = (self.current_batch_files * 3 / 2).min(self.max_files);
        } else if duration > ADAPTIVE_SLOW_THRESHOLD {
            // Too slow → decrease by 25%
            self.current_batch_files = (self.current_batch_files * 3 / 4).max(self.min_files);
        }
        // Within the 1-5 second window: no change
    }

    /// Get the smoothed throughput estimate (files/sec).
    pub fn throughput(&self) -> f64 {
        self.ema_throughput
    }
}

impl Default for AdaptiveBatchSizer {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn test_batch_format_parsing() {
        assert_eq!(BatchFormat::from_str_loose("tar"), BatchFormat::Tar);
        assert_eq!(BatchFormat::from_str_loose("tar-lz4"), BatchFormat::TarLz4);
        assert_eq!(BatchFormat::from_str_loose("lz4"), BatchFormat::TarLz4);
        assert_eq!(BatchFormat::from_str_loose("unknown"), BatchFormat::Tar);

        #[cfg(feature = "batch_zstd")]
        {
            assert_eq!(BatchFormat::from_str_loose("tar-zstd"), BatchFormat::TarZstd);
            assert_eq!(BatchFormat::from_str_loose("zstd"), BatchFormat::TarZstd);
        }
    }

    #[test]
    fn test_adaptive_batch_sizer_grow() {
        let mut sizer = AdaptiveBatchSizer::new().with_initial_size(100);

        // Fast batch → should grow
        sizer.report(100, Duration::from_millis(500));
        assert!(sizer.current_size() > 100, "Should grow after fast batch");
        assert_eq!(sizer.current_size(), 150); // 100 * 1.5
    }

    #[test]
    fn test_adaptive_batch_sizer_shrink() {
        let mut sizer = AdaptiveBatchSizer::new().with_initial_size(10_000);

        // Slow batch → should shrink
        sizer.report(10_000, Duration::from_secs(10));
        assert!(sizer.current_size() < 10_000, "Should shrink after slow batch");
        assert_eq!(sizer.current_size(), 7_500); // 10000 * 0.75
    }

    #[test]
    fn test_adaptive_batch_sizer_stable() {
        let mut sizer = AdaptiveBatchSizer::new().with_initial_size(500);

        // Batch within 1-5 second window → no change
        sizer.report(500, Duration::from_secs(3));
        assert_eq!(sizer.current_size(), 500);
    }

    #[test]
    fn test_adaptive_batch_sizer_clamps() {
        let mut sizer = AdaptiveBatchSizer::new().with_initial_size(MIN_BATCH_FILES);

        // Even after slow batch, shouldn't go below min
        sizer.report(MIN_BATCH_FILES, Duration::from_secs(10));
        assert_eq!(sizer.current_size(), MIN_BATCH_FILES);

        let mut sizer = AdaptiveBatchSizer::new().with_initial_size(MAX_BATCH_FILES);

        // Even after fast batch, shouldn't go above max
        sizer.report(MAX_BATCH_FILES, Duration::from_millis(100));
        assert_eq!(sizer.current_size(), MAX_BATCH_FILES);
    }

    #[test]
    fn test_batch_worker_pool() {
        let src_dir = tempfile::tempdir().unwrap();

        // Create test files
        for i in 0..3 {
            let p = src_dir.path().join(format!("file_{}.txt", i));
            let mut f = File::create(&p).unwrap();
            writeln!(f, "worker pool test file {}", i).unwrap();
        }

        let files: Vec<(PathBuf, u64)> = (0..3)
            .map(|i| {
                let name = format!("file_{}.txt", i);
                let size = src_dir.path().join(&name).metadata().unwrap().len();
                (PathBuf::from(name), size)
            })
            .collect();

        let pool = BatchWorkerPool::new(2);

        pool.submit(1, src_dir.path().to_path_buf(), files, BatchFormat::Tar)
            .unwrap();

        let result = pool.recv().unwrap();
        assert_eq!(result.id, 1);
        assert!(!result.data.is_empty());
        assert!(result.input_bytes > 0);
    }
}
