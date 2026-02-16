//! Main copy engine
//!
//! Orchestrates multi-threaded file copying with intelligent scheduling,
//! progress reporting, and integrity verification.

use crate::config::{CopyConfig, HashAlgorithm, OrderingStrategy};
use crate::core::{TaskResult, TaskScheduler, TaskSuccess};
use crate::error::{Result, SmartCopyError};
use crate::fs::{create_directories, CopyOptions, FileEntry, FileCopier, FileSizeCategory, Scanner, ScanConfig, ScanResult};
use crate::hash::{HashResult, StreamingHasher};
use crate::progress::ProgressReporter;
use crate::sync::ChunkedCopier;
use rayon::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Copy operation result
#[derive(Debug)]
pub struct CopyResult {
    /// Total files copied
    pub files_copied: u64,
    /// Total bytes copied
    pub bytes_copied: u64,
    /// Total directories created
    pub dirs_created: u64,
    /// Failed operations
    pub failures: Vec<(String, String)>,
    /// Total duration
    pub duration: Duration,
    /// Average throughput in bytes/second
    pub throughput: f64,
    /// Verification results (if enabled)
    pub verification: Option<VerificationSummary>,
}

impl CopyResult {
    /// Check if the copy was completely successful
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }

    /// Print summary to console
    pub fn print_summary(&self) {
        println!("\n=== Copy Summary ===");
        println!("Files copied:    {}", self.files_copied);
        println!("Bytes copied:    {}", humansize::format_size(self.bytes_copied, humansize::BINARY));
        println!("Directories:     {}", self.dirs_created);
        println!("Duration:        {:.2?}", self.duration);
        println!("Throughput:      {}/s", humansize::format_size(self.throughput as u64, humansize::BINARY));

        if !self.failures.is_empty() {
            println!("\nFailures: {}", self.failures.len());
            for (path, error) in &self.failures {
                println!("  {} - {}", path, error);
            }
        }

        if let Some(verification) = &self.verification {
            println!("\nVerification:");
            println!("  Verified:  {}", verification.verified);
            println!("  Passed:    {}", verification.passed);
            println!("  Failed:    {}", verification.failed);
        }
    }
}

/// Verification summary
#[derive(Debug, Clone)]
pub struct VerificationSummary {
    /// Files verified
    pub verified: u64,
    /// Files that passed verification
    pub passed: u64,
    /// Files that failed verification
    pub failed: u64,
    /// Mismatched files
    pub mismatches: Vec<(String, String, String)>,
}

/// Main copy engine
pub struct CopyEngine {
    /// Configuration
    config: CopyConfig,
    /// File copier
    copier: FileCopier,
    /// Progress reporter
    progress: Option<ProgressReporter>,
    /// Cancellation flag
    cancelled: Arc<AtomicBool>,
}

impl CopyEngine {
    /// Create a new copy engine
    pub fn new(config: CopyConfig) -> Self {
        let copy_options = CopyOptions {
            buffer_size: config.buffer_size,
            preserve_permissions: config.preserve,
            preserve_mtime: config.preserve,
            use_mmap: true,
            mmap_threshold: 10 * 1024 * 1024,
            use_zero_copy: true,
            preallocate: true,
            sync: false,
            direct_io: false,
            network_optimized: true,
            network_streams: 4,
            network_buffer_size: 4 * 1024 * 1024,
        };

        Self {
            config,
            copier: FileCopier::new(copy_options),
            progress: None,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set progress reporter
    pub fn with_progress(mut self, progress: ProgressReporter) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Get cancellation flag for external control
    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// Cancel the operation
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Execute the copy operation
    pub fn execute(&self) -> Result<CopyResult> {
        let start_time = Instant::now();

        // Scan source directory
        let scan_result = self.scan_source()?;

        if scan_result.files.is_empty() && scan_result.directories.is_empty() {
            return Ok(CopyResult {
                files_copied: 0,
                bytes_copied: 0,
                dirs_created: 0,
                failures: Vec::new(),
                duration: start_time.elapsed(),
                throughput: 0.0,
                verification: None,
            });
        }

        // Initialize progress
        if let Some(progress) = &self.progress {
            progress.set_total_files(scan_result.file_count as u64);
            progress.set_total_bytes(scan_result.total_size);
        }

        // Create directory structure first
        let dirs_created = create_directories(&scan_result.directories, &self.config.destination)?;

        if self.is_cancelled() {
            return Err(SmartCopyError::Cancelled);
        }

        // Execute parallel copy
        let (files_copied, bytes_copied, failures, hashes) = self.copy_files_parallel(&scan_result)?;

        // Verify if requested
        let verification = if self.config.verify.is_some() && !hashes.is_empty() {
            Some(self.verify_copies(&hashes)?)
        } else {
            None
        };

        let duration = start_time.elapsed();
        let throughput = bytes_copied as f64 / duration.as_secs_f64();

        Ok(CopyResult {
            files_copied,
            bytes_copied,
            dirs_created: dirs_created as u64,
            failures,
            duration,
            throughput,
            verification,
        })
    }

    /// Scan source directory
    fn scan_source(&self) -> Result<ScanResult> {
        let scan_config = ScanConfig {
            follow_symlinks: self.config.follow_symlinks,
            include_hidden: self.config.include_hidden,
            max_depth: None,
            include_patterns: self.config.include_patterns.clone(),
            exclude_patterns: self.config.exclude_patterns.clone(),
            min_size: self.config.min_size,
            max_size: self.config.max_size,
            threads: self.config.threads,
        };

        let scanner = Scanner::new(scan_config)?;

        if let Some(progress) = &self.progress {
            progress.set_status("Scanning source directory...");
        }

        scanner.scan_sorted(&self.config.source, self.config.ordering)
    }

    /// Copy files in parallel using rayon
    fn copy_files_parallel(
        &self,
        scan_result: &ScanResult,
    ) -> Result<(u64, u64, Vec<(String, String)>, Vec<(String, HashResult)>)> {
        let threads = if self.config.threads == 0 {
            num_cpus::get()
        } else {
            self.config.threads
        };

        // Configure thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(|e| SmartCopyError::ThreadPoolError(e.to_string()))?;

        let dest = &self.config.destination;
        let verify_algo = self.config.verify;
        let cancelled = &self.cancelled;
        let progress = &self.progress;

        let results: Vec<_> = pool.install(|| {
            scan_result
                .files
                .par_iter()
                .filter_map(|entry| {
                    if cancelled.load(Ordering::SeqCst) {
                        return None;
                    }

                    let result = self.copy_single_file(entry, dest, verify_algo);

                    if let Some(progress) = progress {
                        progress.increment_files(1);
                        let bytes = match &result {
                            Ok((copied, _)) => *copied,
                            Err(_) => 0,
                        };
                        progress.increment_bytes(bytes);
                    }

                    Some((entry.relative_path.to_string_lossy().to_string(), result))
                })
                .collect()
        });

        // Process results
        let mut files_copied = 0u64;
        let mut bytes_copied = 0u64;
        let mut failures = Vec::new();
        let mut hashes = Vec::new();

        for (path, result) in results {
            match result {
                Ok((bytes, hash)) => {
                    files_copied += 1;
                    bytes_copied += bytes;
                    if let Some(h) = hash {
                        hashes.push((path, h));
                    }
                }
                Err(e) => {
                    if self.config.continue_on_error {
                        failures.push((path, e.to_string()));
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok((files_copied, bytes_copied, failures, hashes))
    }

    /// Copy a single file
    fn copy_single_file(
        &self,
        entry: &FileEntry,
        dest: &Path,
        verify_algo: Option<HashAlgorithm>,
    ) -> Result<(u64, Option<HashResult>)> {
        let dest_path = dest.join(&entry.relative_path);

        // Ensure parent directory exists
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        // Check if we should skip (incremental mode)
        if self.config.incremental {
            if let Ok(dest_meta) = std::fs::metadata(&dest_path) {
                let dest_mtime = dest_meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                if dest_meta.len() == entry.size && dest_mtime >= entry.modified {
                    // Skip - destination is same or newer
                    return Ok((0, None));
                }
            }
        }

        // Dry run - just report
        if self.config.dry_run {
            return Ok((entry.size, None));
        }

        // Determine file size category for optimal copy strategy
        let size_category = FileSizeCategory::from_size(entry.size);

        // For HUGE files (>=1GB), use parallel chunked copy for maximum performance
        if size_category.use_parallel_chunks() {
            // Use parallel chunked copy - each chunk is copied by a separate thread
            // Chunk size of 64MB with workers = CPU cores for optimal I/O parallelism
            let chunk_size = 64 * 1024 * 1024; // 64MB chunks
            let workers = num_cpus::get().max(4);
            let chunked_copier = ChunkedCopier::new(chunk_size, workers);

            let result = chunked_copier.copy_parallel(&entry.path, &dest_path)?;

            // Preserve attributes
            self.copier.preserve_attributes(&entry.path, &dest_path)?;

            // If verification is requested, compute a proper streaming hash of the
            // source file. We can't use per-chunk composite hashes because they won't
            // match the streaming hash that verify_copies() computes on the destination.
            // XXHash3 runs at 30+ GB/s so the extra read is negligible.
            let hash_result = if let Some(algo) = verify_algo {
                Some(crate::hash::hash_file(&entry.path, algo)?)
            } else {
                None
            };

            return Ok((result.bytes_copied, hash_result));
        }

        // Copy with or without hashing
        if let Some(algo) = verify_algo {
            let mut hasher = StreamingHasher::new(algo);
            self.copier.copy_with_hash(&entry.path, &dest_path, &mut hasher)?;
            let hash = hasher.finalize();
            Ok((entry.size, Some(hash)))
        } else {
            let stats = self.copier.copy(&entry.path, &dest_path)?;
            Ok((stats.bytes_copied, None))
        }
    }

    /// Verify copied files
    fn verify_copies(&self, hashes: &[(String, HashResult)]) -> Result<VerificationSummary> {
        if let Some(progress) = &self.progress {
            progress.set_status("Verifying copies...");
        }

        let algo = self.config.verify.unwrap_or(HashAlgorithm::XXHash3);
        let dest = &self.config.destination;

        let results: Vec<_> = hashes
            .par_iter()
            .map(|(path, expected_hash)| {
                let dest_path = dest.join(path);
                let actual = crate::hash::hash_file(&dest_path, algo);

                match actual {
                    Ok(actual_hash) => {
                        if actual_hash.verify(expected_hash) {
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                    Err(e) => Err((path.clone(), e.to_string())),
                }
            })
            .collect();

        let mut verified = 0u64;
        let mut passed = 0u64;
        let mut failed = 0u64;
        let mut mismatches = Vec::new();

        for result in results {
            verified += 1;
            match result {
                Ok(true) => passed += 1,
                Ok(false) => {
                    failed += 1;
                }
                Err((path, error)) => {
                    failed += 1;
                    mismatches.push((path, String::new(), error));
                }
            }
        }

        Ok(VerificationSummary {
            verified,
            passed,
            failed,
            mismatches,
        })
    }
}

/// Simple synchronous copy for small operations
pub fn simple_copy(source: &Path, dest: &Path) -> Result<CopyResult> {
    let config = CopyConfig {
        source: source.to_path_buf(),
        destination: dest.to_path_buf(),
        ..Default::default()
    };

    let engine = CopyEngine::new(config);
    engine.execute()
}

/// Copy with default settings and progress
pub fn copy_with_progress(
    source: &Path,
    dest: &Path,
    threads: usize,
    verify: Option<HashAlgorithm>,
) -> Result<CopyResult> {
    let config = CopyConfig {
        source: source.to_path_buf(),
        destination: dest.to_path_buf(),
        threads,
        verify,
        ordering: OrderingStrategy::SmallestFirst,
        ..Default::default()
    };

    let progress = ProgressReporter::new();
    let engine = CopyEngine::new(config).with_progress(progress);

    engine.execute()
}

/// Worker thread for task-based copy
pub fn spawn_copy_workers(
    scheduler: Arc<TaskScheduler>,
    threads: usize,
    copy_options: CopyOptions,
    verify_algo: Option<HashAlgorithm>,
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(threads);

    for worker_id in 0..threads {
        let _sched = Arc::clone(&scheduler);
        let opts = copy_options.clone();
        let task_rx = scheduler.task_receiver();
        let result_tx = scheduler.result_sender();
        let shutdown = scheduler.shutdown_flag();

        let handle = thread::spawn(move || {
            let copier = FileCopier::new(opts);

            while !shutdown.load(Ordering::SeqCst) {
                // Try to receive a task
                match task_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(task) => {
                        let _start = Instant::now();
                        let dest_path = task.dest_path();

                        // Ensure parent directory
                        if let Some(parent) = dest_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }

                        // Copy the file
                        let result = if let Some(algo) = verify_algo {
                            let mut hasher = StreamingHasher::new(algo);
                            copier.copy_with_hash(&task.entry.path, &dest_path, &mut hasher)
                                .map(|stats| TaskSuccess {
                                    bytes_copied: stats.bytes_copied,
                                    duration: stats.duration,
                                    hash: Some(hasher.finalize().hash),
                                })
                        } else {
                            copier.copy(&task.entry.path, &dest_path)
                                .map(|stats| TaskSuccess {
                                    bytes_copied: stats.bytes_copied,
                                    duration: stats.duration,
                                    hash: None,
                                })
                        };

                        let task_result = TaskResult {
                            task_id: task.id,
                            result,
                            retries: task.retries,
                        };

                        let _ = result_tx.send(task_result);
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                }
            }

            tracing::debug!("Worker {} shutting down", worker_id);
        });

        handles.push(handle);
    }

    handles
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;

    fn create_test_structure(dir: &Path) {
        // Create subdirectories
        std::fs::create_dir_all(dir.join("subdir1")).unwrap();
        std::fs::create_dir_all(dir.join("subdir2/nested")).unwrap();

        // Create files of various sizes
        File::create(dir.join("tiny.txt")).unwrap()
            .write_all(b"tiny").unwrap();

        let mut small = File::create(dir.join("small.bin")).unwrap();
        small.write_all(&vec![0xABu8; 10 * 1024]).unwrap();

        let mut medium = File::create(dir.join("subdir1/medium.bin")).unwrap();
        medium.write_all(&vec![0xCDu8; 100 * 1024]).unwrap();

        File::create(dir.join("subdir2/nested/deep.txt")).unwrap()
            .write_all(b"deep file content").unwrap();
    }

    #[test]
    fn test_simple_copy() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_structure(src.path());

        let result = simple_copy(src.path(), dst.path()).unwrap();

        assert!(result.is_success());
        assert!(result.files_copied >= 4);
        assert!(dst.path().join("tiny.txt").exists());
        assert!(dst.path().join("subdir2/nested/deep.txt").exists());
    }

    #[test]
    fn test_copy_with_verification() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_structure(src.path());

        let config = CopyConfig {
            source: src.path().to_path_buf(),
            destination: dst.path().to_path_buf(),
            verify: Some(HashAlgorithm::XXHash3),
            ..Default::default()
        };

        let engine = CopyEngine::new(config);
        let result = engine.execute().unwrap();

        assert!(result.is_success());
        assert!(result.verification.is_some());

        let verification = result.verification.unwrap();
        assert!(verification.failed == 0);
    }

    #[test]
    fn test_incremental_copy() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_structure(src.path());

        // First copy
        let config = CopyConfig {
            source: src.path().to_path_buf(),
            destination: dst.path().to_path_buf(),
            ..Default::default()
        };

        let engine = CopyEngine::new(config);
        let result1 = engine.execute().unwrap();

        // Second copy (incremental)
        let config = CopyConfig {
            source: src.path().to_path_buf(),
            destination: dst.path().to_path_buf(),
            incremental: true,
            ..Default::default()
        };

        let engine = CopyEngine::new(config);
        let result2 = engine.execute().unwrap();

        // Incremental should skip files
        assert!(result2.bytes_copied < result1.bytes_copied);
    }

    #[test]
    fn test_parallel_copy() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_structure(src.path());

        let config = CopyConfig {
            source: src.path().to_path_buf(),
            destination: dst.path().to_path_buf(),
            threads: 4,
            ..Default::default()
        };

        let engine = CopyEngine::new(config);
        let result = engine.execute().unwrap();

        assert!(result.is_success());
    }
}
