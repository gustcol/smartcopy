//! Task scheduling and work distribution
//!
//! Provides intelligent task scheduling that optimizes for:
//! - Smallest files first (quick wins)
//! - Parallel large file chunking
//! - Work-stealing for load balancing

use crate::error::{Result, SmartCopyError};
use crate::fs::{FileEntry, FileSizeCategory};
use crossbeam::channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// A single copy task
#[derive(Debug, Clone)]
pub struct CopyTask {
    /// Unique task ID
    pub id: u64,
    /// Source file entry
    pub entry: FileEntry,
    /// Destination root path
    pub dest_root: std::path::PathBuf,
    /// Priority (lower = higher priority)
    pub priority: u32,
    /// Retry count
    pub retries: usize,
}

impl CopyTask {
    /// Create a new copy task
    pub fn new(id: u64, entry: FileEntry, dest_root: std::path::PathBuf) -> Self {
        // Calculate priority based on file size (smaller = higher priority)
        let priority = match FileSizeCategory::from_size(entry.size) {
            FileSizeCategory::Tiny => 0,
            FileSizeCategory::Small => 1,
            FileSizeCategory::Medium => 2,
            FileSizeCategory::Large => 3,
            FileSizeCategory::Huge => 4,
        };

        Self {
            id,
            entry,
            dest_root,
            priority,
            retries: 0,
        }
    }

    /// Get destination path
    pub fn dest_path(&self) -> std::path::PathBuf {
        self.dest_root.join(&self.entry.relative_path)
    }

    /// Increment retry count
    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }
}

/// Result of a completed task
#[derive(Debug)]
pub struct TaskResult {
    /// Task ID
    pub task_id: u64,
    /// Success or failure
    pub result: Result<TaskSuccess>,
    /// Retry count used
    pub retries: usize,
}

/// Successful task completion info
#[derive(Debug, Clone)]
pub struct TaskSuccess {
    /// Bytes copied
    pub bytes_copied: u64,
    /// Time taken
    pub duration: std::time::Duration,
    /// Hash result (if verification enabled)
    pub hash: Option<String>,
}

/// Task scheduler configuration
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Number of worker threads
    pub threads: usize,
    /// Maximum queue size
    pub queue_size: usize,
    /// Maximum retries per task
    pub max_retries: usize,
    /// Enable work stealing
    pub work_stealing: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            threads: num_cpus::get(),
            queue_size: 10000,
            max_retries: 3,
            work_stealing: true,
        }
    }
}

/// Task scheduler statistics
#[derive(Debug, Default)]
pub struct SchedulerStats {
    /// Total tasks submitted
    pub tasks_submitted: AtomicU64,
    /// Tasks completed successfully
    pub tasks_completed: AtomicU64,
    /// Tasks failed
    pub tasks_failed: AtomicU64,
    /// Tasks currently in progress
    pub tasks_in_progress: AtomicUsize,
    /// Total bytes copied
    pub bytes_copied: AtomicU64,
    /// Total bytes remaining
    pub bytes_remaining: AtomicU64,
}

impl SchedulerStats {
    /// Get completion percentage
    pub fn completion_percentage(&self) -> f64 {
        let completed = self.tasks_completed.load(Ordering::Relaxed);
        let total = self.tasks_submitted.load(Ordering::Relaxed);

        if total == 0 {
            0.0
        } else {
            (completed as f64 / total as f64) * 100.0
        }
    }

    /// Get throughput in bytes/second
    pub fn throughput(&self, elapsed: std::time::Duration) -> f64 {
        let bytes = self.bytes_copied.load(Ordering::Relaxed);
        bytes as f64 / elapsed.as_secs_f64()
    }
}

/// Task scheduler for managing copy operations
pub struct TaskScheduler {
    /// Configuration
    config: SchedulerConfig,
    /// Task sender
    task_sender: Sender<CopyTask>,
    /// Task receiver for workers
    task_receiver: Receiver<CopyTask>,
    /// Result sender from workers
    result_sender: Sender<TaskResult>,
    /// Result receiver
    result_receiver: Receiver<TaskResult>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<SchedulerStats>,
    /// Next task ID
    next_task_id: AtomicU64,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        let (task_sender, task_receiver) = bounded(config.queue_size);
        let (result_sender, result_receiver) = bounded(config.queue_size);

        Self {
            config,
            task_sender,
            task_receiver,
            result_sender,
            result_receiver,
            shutdown: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(SchedulerStats::default()),
            next_task_id: AtomicU64::new(0),
        }
    }

    /// Get a clone of the task receiver for workers
    pub fn task_receiver(&self) -> Receiver<CopyTask> {
        self.task_receiver.clone()
    }

    /// Get a clone of the result sender for workers
    pub fn result_sender(&self) -> Sender<TaskResult> {
        self.result_sender.clone()
    }

    /// Get the shutdown flag
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Get statistics
    pub fn stats(&self) -> Arc<SchedulerStats> {
        Arc::clone(&self.stats)
    }

    /// Submit a task
    pub fn submit(&self, entry: FileEntry, dest_root: std::path::PathBuf) -> Result<u64> {
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let task = CopyTask::new(task_id, entry.clone(), dest_root);

        self.stats.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_remaining.fetch_add(entry.size, Ordering::Relaxed);

        self.task_sender
            .send(task)
            .map_err(|_| SmartCopyError::ThreadPoolError("Failed to submit task".to_string()))?;

        Ok(task_id)
    }

    /// Submit multiple tasks from a list of entries
    pub fn submit_batch(
        &self,
        entries: Vec<FileEntry>,
        dest_root: std::path::PathBuf,
    ) -> Result<Vec<u64>> {
        // Sort entries by size (smallest first)
        let mut sorted_entries = entries;
        sorted_entries.sort_by_key(|e| e.size);

        let mut task_ids = Vec::with_capacity(sorted_entries.len());

        for entry in sorted_entries {
            let id = self.submit(entry, dest_root.clone())?;
            task_ids.push(id);
        }

        Ok(task_ids)
    }

    /// Receive a result (blocking)
    pub fn receive_result(&self) -> Option<TaskResult> {
        self.result_receiver.recv().ok()
    }

    /// Try to receive a result (non-blocking)
    pub fn try_receive_result(&self) -> Option<TaskResult> {
        self.result_receiver.try_recv().ok()
    }

    /// Record a completed task
    pub fn record_completion(&self, bytes: u64, success: bool) {
        if success {
            self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_copied.fetch_add(bytes, Ordering::Relaxed);
            self.stats.bytes_remaining.fetch_sub(bytes, Ordering::Relaxed);
        } else {
            self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.tasks_in_progress.fetch_sub(1, Ordering::Relaxed);
    }

    /// Requeue a failed task for retry
    pub fn requeue(&self, mut task: CopyTask) -> Result<()> {
        task.increment_retries();

        if task.retries > self.config.max_retries {
            return Err(SmartCopyError::ThreadPoolError(format!(
                "Task {} exceeded max retries",
                task.id
            )));
        }

        self.task_sender
            .send(task)
            .map_err(|_| SmartCopyError::ThreadPoolError("Failed to requeue task".to_string()))?;

        Ok(())
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was signaled
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Get number of pending tasks
    pub fn pending_count(&self) -> usize {
        self.task_sender.len()
    }

    /// Check if all work is done
    pub fn is_complete(&self) -> bool {
        let submitted = self.stats.tasks_submitted.load(Ordering::Relaxed);
        let completed = self.stats.tasks_completed.load(Ordering::Relaxed);
        let failed = self.stats.tasks_failed.load(Ordering::Relaxed);

        submitted > 0 && (completed + failed) == submitted
    }
}

/// Priority queue wrapper for tasks
pub struct PriorityTaskQueue {
    /// Tasks sorted by priority
    tasks: Vec<CopyTask>,
}

impl PriorityTaskQueue {
    /// Create a new priority queue
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    /// Create from a list of entries
    pub fn from_entries(entries: Vec<FileEntry>, dest_root: std::path::PathBuf) -> Self {
        let mut queue = Self::new();

        for (id, entry) in entries.into_iter().enumerate() {
            queue.push(CopyTask::new(id as u64, entry, dest_root.clone()));
        }

        queue.sort();
        queue
    }

    /// Add a task
    pub fn push(&mut self, task: CopyTask) {
        self.tasks.push(task);
    }

    /// Sort by priority
    pub fn sort(&mut self) {
        self.tasks.sort_by_key(|t| t.priority);
    }

    /// Pop the highest priority task
    pub fn pop(&mut self) -> Option<CopyTask> {
        if self.tasks.is_empty() {
            None
        } else {
            Some(self.tasks.remove(0))
        }
    }

    /// Get number of tasks
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get total size of all tasks
    pub fn total_size(&self) -> u64 {
        self.tasks.iter().map(|t| t.entry.size).sum()
    }

    /// Partition into batches by size category
    pub fn partition_by_size(self) -> TaskPartition {
        let mut tiny = Vec::new();
        let mut small = Vec::new();
        let mut medium = Vec::new();
        let mut large = Vec::new();
        let mut huge = Vec::new();

        for task in self.tasks {
            match FileSizeCategory::from_size(task.entry.size) {
                FileSizeCategory::Tiny => tiny.push(task),
                FileSizeCategory::Small => small.push(task),
                FileSizeCategory::Medium => medium.push(task),
                FileSizeCategory::Large => large.push(task),
                FileSizeCategory::Huge => huge.push(task),
            }
        }

        TaskPartition {
            tiny,
            small,
            medium,
            large,
            huge,
        }
    }
}

impl Default for PriorityTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Tasks partitioned by size category
#[derive(Debug)]
pub struct TaskPartition {
    /// Tiny files (< 4KB)
    pub tiny: Vec<CopyTask>,
    /// Small files (< 1MB)
    pub small: Vec<CopyTask>,
    /// Medium files (< 100MB)
    pub medium: Vec<CopyTask>,
    /// Large files (< 1GB)
    pub large: Vec<CopyTask>,
    /// Huge files (>= 1GB)
    pub huge: Vec<CopyTask>,
}

impl TaskPartition {
    /// Get total task count
    pub fn total_count(&self) -> usize {
        self.tiny.len() + self.small.len() + self.medium.len() + self.large.len() + self.huge.len()
    }

    /// Get total size
    pub fn total_size(&self) -> u64 {
        self.tiny.iter().map(|t| t.entry.size).sum::<u64>()
            + self.small.iter().map(|t| t.entry.size).sum::<u64>()
            + self.medium.iter().map(|t| t.entry.size).sum::<u64>()
            + self.large.iter().map(|t| t.entry.size).sum::<u64>()
            + self.huge.iter().map(|t| t.entry.size).sum::<u64>()
    }

    /// Print partition summary
    pub fn print_summary(&self) {
        println!("Task Partition Summary:");
        println!("  Tiny (< 4KB):    {} files", self.tiny.len());
        println!("  Small (< 1MB):   {} files", self.small.len());
        println!("  Medium (< 100MB):{} files", self.medium.len());
        println!("  Large (< 1GB):   {} files", self.large.len());
        println!("  Huge (>= 1GB):   {} files", self.huge.len());
        println!("  Total: {} files, {}",
            self.total_count(),
            humansize::format_size(self.total_size(), humansize::BINARY)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn create_test_entry(size: u64) -> FileEntry {
        FileEntry {
            path: PathBuf::from("/test/file"),
            relative_path: PathBuf::from("file"),
            size,
            modified: SystemTime::now(),
            created: None,
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            permissions: 0o644,
        }
    }

    #[test]
    fn test_task_priority() {
        let tiny = CopyTask::new(0, create_test_entry(100), PathBuf::from("/dest"));
        let huge = CopyTask::new(1, create_test_entry(2_000_000_000), PathBuf::from("/dest"));

        assert!(tiny.priority < huge.priority);
    }

    #[test]
    fn test_priority_queue() {
        let mut queue = PriorityTaskQueue::new();

        queue.push(CopyTask::new(0, create_test_entry(1_000_000_000), PathBuf::from("/dest")));
        queue.push(CopyTask::new(1, create_test_entry(100), PathBuf::from("/dest")));
        queue.push(CopyTask::new(2, create_test_entry(10_000), PathBuf::from("/dest")));

        queue.sort();

        let first = queue.pop().unwrap();
        assert_eq!(first.entry.size, 100); // Smallest first
    }

    #[test]
    fn test_task_partition() {
        let entries = vec![
            create_test_entry(100),           // Tiny
            create_test_entry(500_000),       // Small
            create_test_entry(50_000_000),    // Medium
            create_test_entry(500_000_000),   // Large
            create_test_entry(2_000_000_000), // Huge
        ];

        let queue = PriorityTaskQueue::from_entries(entries, PathBuf::from("/dest"));
        let partition = queue.partition_by_size();

        assert_eq!(partition.tiny.len(), 1);
        assert_eq!(partition.small.len(), 1);
        assert_eq!(partition.medium.len(), 1);
        assert_eq!(partition.large.len(), 1);
        assert_eq!(partition.huge.len(), 1);
    }

    #[test]
    fn test_scheduler_stats() {
        let stats = SchedulerStats::default();

        stats.tasks_submitted.store(100, Ordering::Relaxed);
        stats.tasks_completed.store(50, Ordering::Relaxed);

        assert_eq!(stats.completion_percentage(), 50.0);
    }
}
