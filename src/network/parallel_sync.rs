//! Multithreaded Remote Sync
//!
//! Provides parallel file transfer capabilities for remote synchronization
//! using connection pooling and work-stealing thread pools.
//!
//! ## Features
//!
//! - Connection pooling for SSH/TCP/QUIC connections
//! - Work-stealing parallelism using Rayon
//! - Chunked large file transfer across multiple connections
//! - Progress tracking per connection
//! - Automatic retry with exponential backoff
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    ParallelRemoteSync                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
//! │  │ Connection 1 │  │ Connection 2 │  │ Connection N │   ...    │
//! │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
//! │         │                 │                 │                   │
//! │         ▼                 ▼                 ▼                   │
//! │  ┌──────────────────────────────────────────────────────────┐  │
//! │  │                  Rayon Thread Pool                        │  │
//! │  │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │  │
//! │  │   │ Worker1 │ │ Worker2 │ │ Worker3 │ │ WorkerN │        │  │
//! │  │   └─────────┘ └─────────┘ └─────────┘ └─────────┘        │  │
//! │  └──────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::config::RemoteConfig;
use crate::error::{Result, SmartCopyError};
use crate::fs::FileEntry;
use crate::network::agent::{AgentClient, AgentRequest, AgentResponse};
use rayon::prelude::*;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Default number of parallel connections
pub const DEFAULT_PARALLEL_CONNECTIONS: usize = 4;

/// Default chunk size for large file transfers (64 MB)
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Minimum file size for chunked transfer (100 MB)
pub const MIN_CHUNKED_SIZE: u64 = 100 * 1024 * 1024;

/// Connection pool for remote transfers
pub struct ConnectionPool<C> {
    /// Available connections
    connections: Mutex<VecDeque<C>>,
    /// Total connections in pool
    total: usize,
    /// Active connections count
    active: AtomicU64,
    /// Pool configuration
    config: RemoteConfig,
    /// Factory function to create new connections
    factory: Box<dyn Fn(&RemoteConfig) -> Result<C> + Send + Sync>,
}

impl<C> ConnectionPool<C> {
    /// Create a new connection pool
    pub fn new<F>(config: RemoteConfig, size: usize, factory: F) -> Result<Self>
    where
        F: Fn(&RemoteConfig) -> Result<C> + Send + Sync + 'static,
    {
        let mut connections = VecDeque::with_capacity(size);

        // Pre-create connections
        for _ in 0..size {
            connections.push_back(factory(&config)?);
        }

        Ok(Self {
            connections: Mutex::new(connections),
            total: size,
            active: AtomicU64::new(0),
            config,
            factory: Box::new(factory),
        })
    }

    /// Get a connection from the pool
    pub fn get(&self) -> Result<PooledConnection<'_, C>> {
        let conn = {
            let mut pool = self.connections.lock().unwrap();
            pool.pop_front()
        };

        match conn {
            Some(c) => {
                self.active.fetch_add(1, Ordering::SeqCst);
                Ok(PooledConnection {
                    pool: self,
                    connection: Some(c),
                })
            }
            None => {
                // Create new connection if pool is exhausted
                let c = (self.factory)(&self.config)?;
                self.active.fetch_add(1, Ordering::SeqCst);
                Ok(PooledConnection {
                    pool: self,
                    connection: Some(c),
                })
            }
        }
    }

    /// Return a connection to the pool
    fn return_connection(&self, conn: C) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut pool = self.connections.lock().unwrap();
        if pool.len() < self.total {
            pool.push_back(conn);
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let pool = self.connections.lock().unwrap();
        PoolStats {
            total: self.total,
            available: pool.len(),
            active: self.active.load(Ordering::SeqCst) as usize,
        }
    }
}

/// Pooled connection wrapper
pub struct PooledConnection<'a, C> {
    pool: &'a ConnectionPool<C>,
    connection: Option<C>,
}

impl<C> PooledConnection<'_, C> {
    /// Get reference to the connection
    pub fn get(&self) -> Option<&C> {
        self.connection.as_ref()
    }

    /// Get mutable reference to the connection
    pub fn get_mut(&mut self) -> Option<&mut C> {
        self.connection.as_mut()
    }
}

impl<C> Drop for PooledConnection<'_, C> {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.return_connection(conn);
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total: usize,
    pub available: usize,
    pub active: usize,
}

/// Parallel remote sync configuration
#[derive(Debug, Clone)]
pub struct ParallelSyncConfig {
    /// Number of parallel connections
    pub connections: usize,
    /// Chunk size for large files
    pub chunk_size: usize,
    /// Minimum size for chunked transfer
    pub min_chunked_size: u64,
    /// Enable compression
    pub compression: bool,
    /// Maximum retries per file
    pub max_retries: usize,
    /// Retry delay (exponential backoff base)
    pub retry_delay: Duration,
    /// Bandwidth limit per connection (bytes/sec, 0 = unlimited)
    pub bandwidth_limit: u64,
}

impl Default for ParallelSyncConfig {
    fn default() -> Self {
        Self {
            connections: DEFAULT_PARALLEL_CONNECTIONS,
            chunk_size: DEFAULT_CHUNK_SIZE,
            min_chunked_size: MIN_CHUNKED_SIZE,
            compression: false,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            bandwidth_limit: 0,
        }
    }
}

/// Transfer operation for a single file or chunk
#[derive(Debug, Clone)]
pub struct TransferOp {
    /// Source path
    pub source: PathBuf,
    /// Destination path
    pub dest: PathBuf,
    /// File size
    pub size: u64,
    /// Offset for chunked transfer (None = full file)
    pub offset: Option<u64>,
    /// Chunk size for chunked transfer
    pub chunk_size: Option<usize>,
    /// Priority (lower = higher priority)
    pub priority: u32,
}

/// Transfer result
#[derive(Debug, Clone)]
pub struct TransferResult {
    /// Source path
    pub source: PathBuf,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Transfer duration
    pub duration: Duration,
    /// Throughput (bytes/sec)
    pub throughput: f64,
    /// Success flag
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Retries used
    pub retries: usize,
}

/// Parallel remote sync engine
pub struct ParallelRemoteSync {
    /// Configuration
    config: ParallelSyncConfig,
    /// Remote configuration
    remote_config: RemoteConfig,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Progress tracking
    progress: Arc<SyncProgress>,
}

impl ParallelRemoteSync {
    /// Create a new parallel sync engine
    pub fn new(remote_config: RemoteConfig, config: ParallelSyncConfig) -> Self {
        Self {
            config,
            remote_config,
            shutdown: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(SyncProgress::new()),
        }
    }

    /// Get progress tracker
    pub fn progress(&self) -> Arc<SyncProgress> {
        Arc::clone(&self.progress)
    }

    /// Get shutdown flag
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Sync files to remote using TCP agent
    pub fn sync_to_remote_tcp(&self, files: Vec<FileEntry>, remote_base: &Path) -> Result<SyncResult> {
        let start = Instant::now();

        // Create connection pool
        let host = self.remote_config.host.clone();
        let port = self.remote_config.tcp_port;

        let pool: ConnectionPool<AgentClient> = ConnectionPool::new(
            self.remote_config.clone(),
            self.config.connections,
            move |_config| {
                AgentClient::connect_tcp(&host, port)
            },
        )?;

        // Prepare transfer operations
        let ops = self.prepare_operations(&files, remote_base);

        // Execute transfers in parallel
        let results = self.execute_parallel_transfers(&pool, ops)?;

        // Aggregate results
        let mut total_bytes = 0u64;
        let mut success_count = 0u64;
        let mut failure_count = 0u64;
        let mut failures = Vec::new();

        for result in &results {
            if result.success {
                total_bytes += result.bytes_transferred;
                success_count += 1;
            } else {
                failure_count += 1;
                if let Some(ref err) = result.error {
                    failures.push((result.source.display().to_string(), err.clone()));
                }
            }
        }

        let duration = start.elapsed();
        let throughput = if duration.as_secs_f64() > 0.0 {
            total_bytes as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Ok(SyncResult {
            files_transferred: success_count,
            files_failed: failure_count,
            bytes_transferred: total_bytes,
            duration,
            throughput,
            failures,
            pool_stats: pool.stats(),
        })
    }

    /// Prepare transfer operations from file list
    fn prepare_operations(&self, files: &[FileEntry], remote_base: &Path) -> Vec<TransferOp> {
        let mut ops = Vec::new();

        for file in files {
            let rel_path = file.path.strip_prefix(&file.path.parent().unwrap_or(&file.path))
                .unwrap_or(&file.path);
            let dest = remote_base.join(rel_path);

            if file.size >= self.config.min_chunked_size {
                // Split large file into chunks
                let num_chunks = (file.size as usize + self.config.chunk_size - 1) / self.config.chunk_size;

                for i in 0..num_chunks {
                    let offset = (i * self.config.chunk_size) as u64;
                    let remaining = file.size - offset;
                    let chunk_size = remaining.min(self.config.chunk_size as u64) as usize;

                    ops.push(TransferOp {
                        source: file.path.clone(),
                        dest: dest.clone(),
                        size: chunk_size as u64,
                        offset: Some(offset),
                        chunk_size: Some(chunk_size),
                        priority: 1, // Large files have lower priority
                    });
                }
            } else {
                ops.push(TransferOp {
                    source: file.path.clone(),
                    dest: dest.clone(),
                    size: file.size,
                    offset: None,
                    chunk_size: None,
                    priority: 0, // Small files have higher priority
                });
            }
        }

        // Sort by priority (small files first for quick wins)
        ops.sort_by_key(|op| (op.priority, op.size));

        ops
    }

    /// Execute transfers in parallel using the connection pool
    fn execute_parallel_transfers(
        &self,
        pool: &ConnectionPool<AgentClient>,
        ops: Vec<TransferOp>,
    ) -> Result<Vec<TransferResult>> {
        let progress = Arc::clone(&self.progress);
        let shutdown = Arc::clone(&self.shutdown);
        let config = self.config.clone();

        // Use Rayon for parallel execution
        let results: Vec<TransferResult> = ops
            .into_par_iter()
            .map(|op| {
                if shutdown.load(Ordering::SeqCst) {
                    return TransferResult {
                        source: op.source,
                        bytes_transferred: 0,
                        duration: Duration::ZERO,
                        throughput: 0.0,
                        success: false,
                        error: Some("Shutdown requested".to_string()),
                        retries: 0,
                    };
                }

                let start = Instant::now();
                let mut retries = 0;
                let mut last_error = None;

                while retries <= config.max_retries {
                    match Self::execute_single_transfer(pool, &op, &progress) {
                        Ok(bytes) => {
                            let duration = start.elapsed();
                            let throughput = if duration.as_secs_f64() > 0.0 {
                                bytes as f64 / duration.as_secs_f64()
                            } else {
                                0.0
                            };

                            return TransferResult {
                                source: op.source.clone(),
                                bytes_transferred: bytes,
                                duration,
                                throughput,
                                success: true,
                                error: None,
                                retries,
                            };
                        }
                        Err(e) => {
                            last_error = Some(e.to_string());
                            retries += 1;

                            if retries <= config.max_retries {
                                // Exponential backoff
                                let delay = config.retry_delay * (1 << retries);
                                std::thread::sleep(delay);
                            }
                        }
                    }
                }

                TransferResult {
                    source: op.source,
                    bytes_transferred: 0,
                    duration: start.elapsed(),
                    throughput: 0.0,
                    success: false,
                    error: last_error,
                    retries,
                }
            })
            .collect();

        Ok(results)
    }

    /// Execute a single transfer operation
    fn execute_single_transfer(
        pool: &ConnectionPool<AgentClient>,
        op: &TransferOp,
        progress: &Arc<SyncProgress>,
    ) -> Result<u64> {
        let mut conn = pool.get()?;
        let client = conn.get_mut()
            .ok_or_else(|| SmartCopyError::RemoteTransferError("No connection available".to_string()))?;

        // Read file data
        let data = if let Some(offset) = op.offset {
            let chunk_size = op.chunk_size.unwrap_or(op.size as usize);
            Self::read_file_chunk(&op.source, offset, chunk_size)?
        } else {
            std::fs::read(&op.source)
                .map_err(|e| SmartCopyError::io(&op.source, e))?
        };

        let bytes_len = data.len() as u64;

        // Write to remote
        let create = op.offset.is_none() || op.offset == Some(0);
        client.write_chunk(&op.dest, op.offset.unwrap_or(0), data, create)?;

        // Update progress
        progress.add_bytes(bytes_len);
        if op.offset.is_none() || op.offset == Some(0) {
            progress.add_file();
        }

        Ok(bytes_len)
    }

    /// Read a chunk from a file
    fn read_file_chunk(path: &Path, offset: u64, size: usize) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(path)
            .map_err(|e| SmartCopyError::io(path, e))?;

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| SmartCopyError::io(path, e))?;

        let mut buffer = vec![0u8; size];
        let bytes_read = file.read(&mut buffer)
            .map_err(|e| SmartCopyError::io(path, e))?;

        buffer.truncate(bytes_read);
        Ok(buffer)
    }
}

/// Sync progress tracking
pub struct SyncProgress {
    /// Bytes transferred
    bytes: AtomicU64,
    /// Files transferred
    files: AtomicU64,
    /// Start time
    start: RwLock<Option<Instant>>,
    /// Total bytes to transfer
    total_bytes: AtomicU64,
    /// Total files to transfer
    total_files: AtomicU64,
}

impl SyncProgress {
    /// Create new progress tracker
    pub fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            files: AtomicU64::new(0),
            start: RwLock::new(None),
            total_bytes: AtomicU64::new(0),
            total_files: AtomicU64::new(0),
        }
    }

    /// Set total expected values
    pub fn set_totals(&self, files: u64, bytes: u64) {
        self.total_files.store(files, Ordering::SeqCst);
        self.total_bytes.store(bytes, Ordering::SeqCst);
        *self.start.write().unwrap() = Some(Instant::now());
    }

    /// Add transferred bytes
    pub fn add_bytes(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Add transferred file
    pub fn add_file(&self) {
        self.files.fetch_add(1, Ordering::SeqCst);
    }

    /// Get current progress snapshot
    pub fn snapshot(&self) -> ProgressSnapshot {
        let bytes = self.bytes.load(Ordering::SeqCst);
        let files = self.files.load(Ordering::SeqCst);
        let total_bytes = self.total_bytes.load(Ordering::SeqCst);
        let total_files = self.total_files.load(Ordering::SeqCst);

        let elapsed = self.start.read().unwrap()
            .map(|s| s.elapsed())
            .unwrap_or(Duration::ZERO);

        let throughput = if elapsed.as_secs_f64() > 0.0 {
            bytes as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let percent = if total_bytes > 0 {
            (bytes as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };

        let eta = if throughput > 0.0 && bytes < total_bytes {
            Some(Duration::from_secs_f64((total_bytes - bytes) as f64 / throughput))
        } else {
            None
        };

        ProgressSnapshot {
            bytes,
            files,
            total_bytes,
            total_files,
            elapsed,
            throughput,
            percent,
            eta,
        }
    }
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Progress snapshot
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub bytes: u64,
    pub files: u64,
    pub total_bytes: u64,
    pub total_files: u64,
    pub elapsed: Duration,
    pub throughput: f64,
    pub percent: f64,
    pub eta: Option<Duration>,
}

/// Sync result
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Files successfully transferred
    pub files_transferred: u64,
    /// Files that failed
    pub files_failed: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Total duration
    pub duration: Duration,
    /// Overall throughput (bytes/sec)
    pub throughput: f64,
    /// List of failures (path, error)
    pub failures: Vec<(String, String)>,
    /// Connection pool statistics
    pub pool_stats: PoolStats,
}

impl SyncResult {
    /// Print a summary of the sync result
    pub fn print_summary(&self) {
        use humansize::{format_size, BINARY};

        println!("\n=== Parallel Sync Summary ===");
        println!("Files transferred: {}", self.files_transferred);
        println!("Files failed:      {}", self.files_failed);
        println!("Bytes transferred: {}", format_size(self.bytes_transferred, BINARY));
        println!("Duration:          {:.2}s", self.duration.as_secs_f64());
        println!("Throughput:        {}/s", format_size(self.throughput as u64, BINARY));
        println!("Connections used:  {}", self.pool_stats.total);

        if !self.failures.is_empty() {
            println!("\nFailures:");
            for (path, err) in &self.failures {
                println!("  {} - {}", path, err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_sync_config_default() {
        let config = ParallelSyncConfig::default();
        assert_eq!(config.connections, DEFAULT_PARALLEL_CONNECTIONS);
        assert_eq!(config.chunk_size, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn test_progress_tracking() {
        let progress = SyncProgress::new();
        progress.set_totals(100, 1000);

        progress.add_bytes(500);
        progress.add_file();

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.bytes, 500);
        assert_eq!(snapshot.files, 1);
        assert_eq!(snapshot.total_bytes, 1000);
        assert_eq!(snapshot.total_files, 100);
        assert_eq!(snapshot.percent, 50.0);
    }

    #[test]
    fn test_pool_stats() {
        let stats = PoolStats {
            total: 4,
            available: 2,
            active: 2,
        };

        assert_eq!(stats.total, 4);
        assert_eq!(stats.available, 2);
        assert_eq!(stats.active, 2);
    }
}
