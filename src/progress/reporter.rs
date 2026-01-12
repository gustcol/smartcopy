//! Progress reporter implementation
//!
//! Uses indicatif for beautiful progress bars with:
//! - File count progress
//! - Byte transfer progress
//! - Throughput and ETA display
//! - Multi-bar support for parallel operations

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Progress reporter for copy operations
pub struct ProgressReporter {
    /// Multi-progress container
    multi: MultiProgress,
    /// Main progress bar (bytes)
    bytes_bar: ProgressBar,
    /// File count progress bar
    files_bar: ProgressBar,
    /// Current status message
    status: ProgressBar,
    /// Start time
    start_time: Instant,
    /// Total bytes to copy
    total_bytes: AtomicU64,
    /// Total files to copy
    total_files: AtomicU64,
    /// Bytes copied so far
    bytes_copied: AtomicU64,
    /// Files copied so far
    files_copied: AtomicU64,
    /// Is progress enabled
    enabled: AtomicBool,
}

impl ProgressReporter {
    /// Create a new progress reporter
    pub fn new() -> Self {
        let multi = MultiProgress::new();

        // Status line
        let status = multi.add(ProgressBar::new_spinner());
        status.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .expect("Invalid template")
        );

        // Files progress bar
        let files_bar = multi.add(ProgressBar::new(0));
        files_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:.bold.dim} [{bar:40.cyan/blue}] {pos}/{len} files ({percent}%)")
                .expect("Invalid template")
                .progress_chars("=> ")
        );
        files_bar.set_prefix("Files");

        // Bytes progress bar
        let bytes_bar = multi.add(ProgressBar::new(0));
        bytes_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:.bold.dim} [{bar:40.green/white}] {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})")
                .expect("Invalid template")
                .progress_chars("=> ")
        );
        bytes_bar.set_prefix("Data ");

        Self {
            multi,
            bytes_bar,
            files_bar,
            status,
            start_time: Instant::now(),
            total_bytes: AtomicU64::new(0),
            total_files: AtomicU64::new(0),
            bytes_copied: AtomicU64::new(0),
            files_copied: AtomicU64::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    /// Create a disabled progress reporter (for quiet mode)
    pub fn disabled() -> Self {
        let reporter = Self::new();
        reporter.enabled.store(false, Ordering::SeqCst);
        reporter.multi.set_draw_target(ProgressDrawTarget::hidden());
        reporter
    }

    /// Set total bytes to transfer
    pub fn set_total_bytes(&self, total: u64) {
        self.total_bytes.store(total, Ordering::Relaxed);
        self.bytes_bar.set_length(total);
    }

    /// Set total files to transfer
    pub fn set_total_files(&self, total: u64) {
        self.total_files.store(total, Ordering::Relaxed);
        self.files_bar.set_length(total);
    }

    /// Increment bytes copied
    pub fn increment_bytes(&self, bytes: u64) {
        self.bytes_copied.fetch_add(bytes, Ordering::Relaxed);
        self.bytes_bar.inc(bytes);
    }

    /// Increment files copied
    pub fn increment_files(&self, count: u64) {
        self.files_copied.fetch_add(count, Ordering::Relaxed);
        self.files_bar.inc(count);
    }

    /// Set current status message
    pub fn set_status(&self, msg: &str) {
        self.status.set_message(msg.to_string());
    }

    /// Set current file being copied
    pub fn set_current_file(&self, path: &str) {
        // Truncate long paths
        let display = if path.len() > 60 {
            format!("...{}", &path[path.len() - 57..])
        } else {
            path.to_string()
        };
        self.status.set_message(display);
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get current throughput in bytes/second
    pub fn throughput(&self) -> f64 {
        let bytes = self.bytes_copied.load(Ordering::Relaxed);
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Get ETA in seconds
    pub fn eta_seconds(&self) -> Option<u64> {
        let bytes_copied = self.bytes_copied.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);

        if bytes_copied == 0 || total_bytes == 0 {
            return None;
        }

        let throughput = self.throughput();
        if throughput <= 0.0 {
            return None;
        }

        let remaining = total_bytes.saturating_sub(bytes_copied);
        Some((remaining as f64 / throughput) as u64)
    }

    /// Finish progress with success message
    pub fn finish_success(&self, message: &str) {
        self.status.finish_with_message(format!("✓ {}", message));
        self.files_bar.finish();
        self.bytes_bar.finish();
    }

    /// Finish progress with error message
    pub fn finish_error(&self, message: &str) {
        self.status.finish_with_message(format!("✗ {}", message));
        self.files_bar.abandon();
        self.bytes_bar.abandon();
    }

    /// Check if progress is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Get progress summary
    pub fn summary(&self) -> ProgressSummary {
        ProgressSummary {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            bytes_copied: self.bytes_copied.load(Ordering::Relaxed),
            total_files: self.total_files.load(Ordering::Relaxed),
            files_copied: self.files_copied.load(Ordering::Relaxed),
            elapsed: self.elapsed(),
            throughput: self.throughput(),
        }
    }
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Progress summary
#[derive(Debug, Clone)]
pub struct ProgressSummary {
    /// Total bytes to transfer
    pub total_bytes: u64,
    /// Bytes copied so far
    pub bytes_copied: u64,
    /// Total files to transfer
    pub total_files: u64,
    /// Files copied so far
    pub files_copied: u64,
    /// Elapsed time
    pub elapsed: Duration,
    /// Throughput in bytes/second
    pub throughput: f64,
}

impl ProgressSummary {
    /// Get completion percentage
    pub fn percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.bytes_copied as f64 / self.total_bytes as f64) * 100.0
        }
    }

    /// Print summary to console
    pub fn print(&self) {
        println!("Progress: {:.1}%", self.percentage());
        println!("Files:    {}/{}", self.files_copied, self.total_files);
        println!("Bytes:    {}/{}",
            humansize::format_size(self.bytes_copied, humansize::BINARY),
            humansize::format_size(self.total_bytes, humansize::BINARY)
        );
        println!("Elapsed:  {:.1?}", self.elapsed);
        println!("Speed:    {}/s", humansize::format_size(self.throughput as u64, humansize::BINARY));
    }
}

/// Simple text-based progress for non-TTY environments
pub struct SimpleProgress {
    /// Start time
    start_time: Instant,
    /// Last report time
    last_report: AtomicU64,
    /// Report interval in milliseconds
    report_interval_ms: u64,
    /// Total bytes
    total_bytes: AtomicU64,
    /// Bytes copied
    bytes_copied: AtomicU64,
    /// Files copied
    files_copied: AtomicU64,
}

impl SimpleProgress {
    /// Create a new simple progress reporter
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            last_report: AtomicU64::new(0),
            report_interval_ms: 1000,
            total_bytes: AtomicU64::new(0),
            bytes_copied: AtomicU64::new(0),
            files_copied: AtomicU64::new(0),
        }
    }

    /// Set total bytes
    pub fn set_total_bytes(&self, total: u64) {
        self.total_bytes.store(total, Ordering::Relaxed);
    }

    /// Update progress
    pub fn update(&self, bytes: u64, files: u64) {
        self.bytes_copied.fetch_add(bytes, Ordering::Relaxed);
        self.files_copied.fetch_add(files, Ordering::Relaxed);

        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        let last = self.last_report.load(Ordering::Relaxed);

        if elapsed_ms - last >= self.report_interval_ms {
            self.last_report.store(elapsed_ms, Ordering::Relaxed);
            self.print_progress();
        }
    }

    /// Print current progress
    fn print_progress(&self) {
        let bytes = self.bytes_copied.load(Ordering::Relaxed);
        let total = self.total_bytes.load(Ordering::Relaxed);
        let files = self.files_copied.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();

        let percent = if total > 0 {
            (bytes as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        let speed = if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        };

        println!(
            "[{:.1}%] {} files, {}/{} @ {}/s",
            percent,
            files,
            humansize::format_size(bytes, humansize::BINARY),
            humansize::format_size(total, humansize::BINARY),
            humansize::format_size(speed as u64, humansize::BINARY)
        );
    }

    /// Finish and print final stats
    pub fn finish(&self) {
        let bytes = self.bytes_copied.load(Ordering::Relaxed);
        let files = self.files_copied.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();

        println!(
            "Completed: {} files, {} in {:.1?} ({}/s)",
            files,
            humansize::format_size(bytes, humansize::BINARY),
            elapsed,
            humansize::format_size((bytes as f64 / elapsed.as_secs_f64()) as u64, humansize::BINARY)
        );
    }
}

impl Default for SimpleProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_reporter() {
        let reporter = ProgressReporter::disabled();

        reporter.set_total_bytes(1000);
        reporter.set_total_files(10);

        reporter.increment_bytes(500);
        reporter.increment_files(5);

        let summary = reporter.summary();
        assert_eq!(summary.bytes_copied, 500);
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.percentage(), 50.0);
    }

    #[test]
    fn test_simple_progress() {
        let progress = SimpleProgress::new();
        progress.set_total_bytes(1000);
        progress.update(500, 5);

        assert_eq!(progress.bytes_copied.load(Ordering::Relaxed), 500);
        assert_eq!(progress.files_copied.load(Ordering::Relaxed), 5);
    }
}
