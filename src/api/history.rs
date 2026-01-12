//! Transfer History Tracking and Comparison
//!
//! Provides persistent storage and analysis of transfer operations for
//! performance monitoring, auditing, and optimization in large-scale
//! HPC environments.
//!
//! ## Features
//!
//! - Persistent history storage (JSON/SQLite)
//! - Performance trend analysis
//! - Transfer comparison between runs
//! - Anomaly detection
//! - Aggregate statistics
//!
//! ## History Data Retention
//!
//! - Last 30 days: Full detail (file-level)
//! - Last 90 days: Aggregate only (job-level)
//! - Beyond 90 days: Summary statistics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use chrono::{DateTime, Utc, Duration};
use crate::error::{Result, SmartCopyError};

/// History entry for a completed transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Unique entry ID
    pub id: String,
    /// Job ID that created this entry
    pub job_id: String,
    /// Job name
    pub name: String,
    /// Source path/URI
    pub source: String,
    /// Destination path/URI
    pub destination: String,
    /// Transfer type
    pub transfer_type: TransferType,
    /// Start time
    pub started_at: DateTime<Utc>,
    /// End time
    pub ended_at: DateTime<Utc>,
    /// Duration in seconds
    pub duration_seconds: f64,
    /// Result status
    pub status: TransferStatus,
    /// Error message if failed
    pub error: Option<String>,
    /// Transfer statistics
    pub stats: TransferStats,
    /// Configuration used
    pub config: TransferConfig,
    /// System snapshot at time of transfer
    pub system_snapshot: Option<SystemSnapshot>,
}

/// Transfer type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Local,
    Ssh,
    Tcp,
    Quic,
    Agent,
}

/// Transfer status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferStatus {
    Success,
    PartialSuccess,
    Failed,
    Cancelled,
}

/// Transfer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferStats {
    /// Total files processed
    pub total_files: u64,
    /// Files successfully transferred
    pub files_transferred: u64,
    /// Files failed
    pub files_failed: u64,
    /// Files skipped (already up to date)
    pub files_skipped: u64,
    /// Directories created
    pub directories_created: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Total source size
    pub total_source_bytes: u64,
    /// Average throughput (bytes/sec)
    pub avg_throughput: f64,
    /// Peak throughput (bytes/sec)
    pub peak_throughput: f64,
    /// Minimum throughput (bytes/sec)
    pub min_throughput: f64,
    /// Throughput standard deviation
    pub throughput_stddev: f64,
    /// Files per second
    pub files_per_second: f64,
    /// Verification results (if enabled)
    pub verification: Option<VerificationStats>,
    /// Compression stats (if enabled)
    pub compression: Option<CompressionStats>,
    /// Delta transfer stats (if enabled)
    pub delta: Option<DeltaStats>,
}

/// Verification statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationStats {
    /// Algorithm used
    pub algorithm: String,
    /// Files verified
    pub files_verified: u64,
    /// Verification failures
    pub verification_failures: u64,
    /// Verification overhead (seconds)
    pub overhead_seconds: f64,
}

/// Compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    /// Algorithm used
    pub algorithm: String,
    /// Original size
    pub original_bytes: u64,
    /// Compressed size
    pub compressed_bytes: u64,
    /// Compression ratio
    pub ratio: f64,
    /// Compression overhead (seconds)
    pub overhead_seconds: f64,
}

/// Delta transfer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaStats {
    /// Files analyzed for delta
    pub files_analyzed: u64,
    /// Files using delta transfer
    pub files_delta_transferred: u64,
    /// Bytes saved by delta
    pub bytes_saved: u64,
    /// Total chunks
    pub total_chunks: u64,
    /// Matched chunks (reused)
    pub matched_chunks: u64,
}

/// Transfer configuration snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferConfig {
    /// Threads used
    pub threads: usize,
    /// Buffer size
    pub buffer_size: usize,
    /// Parallel connections
    pub parallel_connections: usize,
    /// Verification enabled
    pub verify: bool,
    /// Compression enabled
    pub compression: bool,
    /// Incremental enabled
    pub incremental: bool,
    /// Delta enabled
    pub delta: bool,
    /// Bandwidth limit
    pub bandwidth_limit: u64,
}

/// System snapshot at time of transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSnapshot {
    /// Hostname
    pub hostname: String,
    /// CPU usage (0-100)
    pub cpu_usage: f64,
    /// Memory usage (0-100)
    pub memory_usage: f64,
    /// Disk I/O utilization (0-100)
    pub disk_io_usage: f64,
    /// Network utilization (0-100)
    pub network_usage: f64,
    /// Load average (1 min)
    pub load_average: f64,
}

/// Transfer comparison result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferComparison {
    /// Entries being compared
    pub entries: Vec<HistoryEntrySummary>,
    /// Comparison metrics
    pub comparison: ComparisonMetrics,
    /// Performance trend
    pub trend: PerformanceTrend,
    /// Anomalies detected
    pub anomalies: Vec<Anomaly>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Summary of a history entry for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntrySummary {
    /// Entry ID
    pub id: String,
    /// Transfer timestamp
    pub timestamp: DateTime<Utc>,
    /// Duration
    pub duration_seconds: f64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Files transferred
    pub files_transferred: u64,
    /// Average throughput
    pub avg_throughput: f64,
    /// Status
    pub status: TransferStatus,
}

/// Comparison metrics between transfers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonMetrics {
    /// Throughput comparison
    pub throughput: MetricComparison,
    /// Duration comparison
    pub duration: MetricComparison,
    /// Success rate comparison
    pub success_rate: MetricComparison,
    /// Files per second comparison
    pub files_per_second: MetricComparison,
}

/// Single metric comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Average value
    pub avg: f64,
    /// Standard deviation
    pub stddev: f64,
    /// Percent change from first to last
    pub percent_change: f64,
    /// Values for each entry
    pub values: Vec<f64>,
}

/// Performance trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTrend {
    /// Trend direction
    pub direction: TrendDirection,
    /// Trend strength (0-1)
    pub strength: f64,
    /// Predicted next value
    pub prediction: Option<f64>,
    /// Confidence (0-1)
    pub confidence: f64,
}

/// Trend direction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrendDirection {
    Improving,
    Stable,
    Degrading,
    Volatile,
}

/// Detected anomaly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Entry ID with anomaly
    pub entry_id: String,
    /// Anomaly type
    pub anomaly_type: AnomalyType,
    /// Severity (0-1)
    pub severity: f64,
    /// Description
    pub description: String,
    /// Metric name
    pub metric: String,
    /// Expected value
    pub expected: f64,
    /// Actual value
    pub actual: f64,
}

/// Anomaly types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyType {
    ThroughputDrop,
    ThroughputSpike,
    HighFailureRate,
    UnusualDuration,
    ResourceBottleneck,
}

/// History manager for persistent storage
pub struct HistoryManager {
    /// Storage path
    storage_path: PathBuf,
    /// In-memory cache
    cache: std::sync::RwLock<Vec<HistoryEntry>>,
    /// Maximum entries in cache
    max_cache_size: usize,
}

impl HistoryManager {
    /// Create a new history manager
    pub fn new(storage_path: impl AsRef<Path>) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();

        // Ensure storage directory exists
        if let Some(parent) = storage_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        let mut manager = Self {
            storage_path,
            cache: std::sync::RwLock::new(Vec::new()),
            max_cache_size: 1000,
        };

        // Load existing history
        manager.load_history()?;

        Ok(manager)
    }

    /// Load history from storage
    fn load_history(&mut self) -> Result<()> {
        if self.storage_path.exists() {
            let content = fs::read_to_string(&self.storage_path)
                .map_err(|e| SmartCopyError::io(&self.storage_path, e))?;

            let entries: Vec<HistoryEntry> = serde_json::from_str(&content)
                .unwrap_or_default();

            *self.cache.write().unwrap() = entries;
        }
        Ok(())
    }

    /// Save history to storage
    fn save_history(&self) -> Result<()> {
        let entries = self.cache.read().unwrap();
        let content = serde_json::to_string_pretty(&*entries)
            .map_err(|e| SmartCopyError::config(e.to_string()))?;

        fs::write(&self.storage_path, content)
            .map_err(|e| SmartCopyError::io(&self.storage_path, e))?;

        Ok(())
    }

    /// Add a new history entry
    pub fn add_entry(&self, entry: HistoryEntry) -> Result<()> {
        {
            let mut cache = self.cache.write().unwrap();
            cache.push(entry);

            // Trim old entries
            self.cleanup_old_entries(&mut cache);

            // Limit cache size
            if cache.len() > self.max_cache_size {
                let drain_count = cache.len() - self.max_cache_size;
                cache.drain(0..drain_count);
            }
        }

        self.save_history()
    }

    /// Cleanup entries older than retention period
    fn cleanup_old_entries(&self, entries: &mut Vec<HistoryEntry>) {
        let cutoff = Utc::now() - Duration::days(90);
        entries.retain(|e| e.ended_at > cutoff);
    }

    /// Get recent entries
    pub fn get_recent(&self, limit: usize) -> Vec<HistoryEntry> {
        let cache = self.cache.read().unwrap();
        cache.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get entry by ID
    pub fn get_by_id(&self, id: &str) -> Option<HistoryEntry> {
        let cache = self.cache.read().unwrap();
        cache.iter()
            .find(|e| e.id == id)
            .cloned()
    }

    /// Get entries for a specific source/destination pair
    pub fn get_by_path(&self, source: &str, destination: &str) -> Vec<HistoryEntry> {
        let cache = self.cache.read().unwrap();
        cache.iter()
            .filter(|e| e.source == source && e.destination == destination)
            .cloned()
            .collect()
    }

    /// Compare multiple transfers
    pub fn compare(&self, entry_ids: &[String]) -> Result<TransferComparison> {
        let cache = self.cache.read().unwrap();

        let entries: Vec<_> = entry_ids.iter()
            .filter_map(|id| cache.iter().find(|e| &e.id == id).cloned())
            .collect();

        if entries.len() < 2 {
            return Err(SmartCopyError::config("Need at least 2 entries to compare"));
        }

        let summaries: Vec<HistoryEntrySummary> = entries.iter()
            .map(|e| HistoryEntrySummary {
                id: e.id.clone(),
                timestamp: e.started_at,
                duration_seconds: e.duration_seconds,
                bytes_transferred: e.stats.bytes_transferred,
                files_transferred: e.stats.files_transferred,
                avg_throughput: e.stats.avg_throughput,
                status: e.status.clone(),
            })
            .collect();

        // Calculate comparison metrics
        let throughputs: Vec<f64> = entries.iter().map(|e| e.stats.avg_throughput).collect();
        let durations: Vec<f64> = entries.iter().map(|e| e.duration_seconds).collect();
        let success_rates: Vec<f64> = entries.iter().map(|e| {
            if e.stats.total_files > 0 {
                (e.stats.files_transferred as f64 / e.stats.total_files as f64) * 100.0
            } else {
                100.0
            }
        }).collect();
        let files_per_sec: Vec<f64> = entries.iter().map(|e| e.stats.files_per_second).collect();

        let comparison = ComparisonMetrics {
            throughput: self.calc_metric_comparison(&throughputs),
            duration: self.calc_metric_comparison(&durations),
            success_rate: self.calc_metric_comparison(&success_rates),
            files_per_second: self.calc_metric_comparison(&files_per_sec),
        };

        // Analyze trend
        let trend = self.analyze_trend(&throughputs);

        // Detect anomalies
        let anomalies = self.detect_anomalies(&entries, &throughputs);

        // Generate recommendations
        let recommendations = self.generate_recommendations(&entries, &comparison, &trend, &anomalies);

        Ok(TransferComparison {
            entries: summaries,
            comparison,
            trend,
            anomalies,
            recommendations,
        })
    }

    /// Calculate metric comparison statistics
    fn calc_metric_comparison(&self, values: &[f64]) -> MetricComparison {
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg = values.iter().sum::<f64>() / values.len() as f64;

        let variance = values.iter()
            .map(|v| (v - avg).powi(2))
            .sum::<f64>() / values.len() as f64;
        let stddev = variance.sqrt();

        let percent_change = if !values.is_empty() && values[0] != 0.0 {
            ((values[values.len() - 1] - values[0]) / values[0]) * 100.0
        } else {
            0.0
        };

        MetricComparison {
            min,
            max,
            avg,
            stddev,
            percent_change,
            values: values.to_vec(),
        }
    }

    /// Analyze performance trend
    fn analyze_trend(&self, values: &[f64]) -> PerformanceTrend {
        if values.len() < 2 {
            return PerformanceTrend {
                direction: TrendDirection::Stable,
                strength: 0.0,
                prediction: None,
                confidence: 0.0,
            };
        }

        // Simple linear regression
        let n = values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = values.iter().sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        let slope = if denominator != 0.0 { numerator / denominator } else { 0.0 };
        let intercept = y_mean - slope * x_mean;

        // Determine trend direction
        let avg = y_mean;
        let relative_slope = if avg != 0.0 { slope / avg } else { 0.0 };

        let direction = if relative_slope > 0.05 {
            TrendDirection::Improving
        } else if relative_slope < -0.05 {
            TrendDirection::Degrading
        } else {
            TrendDirection::Stable
        };

        // Calculate R-squared for confidence
        let ss_tot: f64 = values.iter().map(|y| (y - y_mean).powi(2)).sum();
        let ss_res: f64 = values.iter()
            .enumerate()
            .map(|(i, &y)| {
                let predicted = intercept + slope * i as f64;
                (y - predicted).powi(2)
            })
            .sum();

        let r_squared = if ss_tot != 0.0 { 1.0 - (ss_res / ss_tot) } else { 0.0 };

        // Predict next value
        let prediction = Some(intercept + slope * n);

        PerformanceTrend {
            direction,
            strength: relative_slope.abs().min(1.0),
            prediction,
            confidence: r_squared.max(0.0).min(1.0),
        }
    }

    /// Detect anomalies in transfer history
    fn detect_anomalies(&self, entries: &[HistoryEntry], throughputs: &[f64]) -> Vec<Anomaly> {
        let mut anomalies = Vec::new();

        if throughputs.len() < 3 {
            return anomalies;
        }

        let avg = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
        let stddev = (throughputs.iter()
            .map(|t| (t - avg).powi(2))
            .sum::<f64>() / throughputs.len() as f64).sqrt();

        for (i, entry) in entries.iter().enumerate() {
            let throughput = throughputs[i];
            let z_score = if stddev > 0.0 { (throughput - avg) / stddev } else { 0.0 };

            // Throughput drop (more than 2 standard deviations below mean)
            if z_score < -2.0 {
                anomalies.push(Anomaly {
                    entry_id: entry.id.clone(),
                    anomaly_type: AnomalyType::ThroughputDrop,
                    severity: (-z_score / 4.0).min(1.0),
                    description: format!(
                        "Throughput {:.1} MB/s is {:.1}σ below average {:.1} MB/s",
                        throughput / 1_000_000.0,
                        -z_score,
                        avg / 1_000_000.0
                    ),
                    metric: "throughput".to_string(),
                    expected: avg,
                    actual: throughput,
                });
            }

            // Throughput spike (more than 2 standard deviations above mean)
            if z_score > 2.0 {
                anomalies.push(Anomaly {
                    entry_id: entry.id.clone(),
                    anomaly_type: AnomalyType::ThroughputSpike,
                    severity: (z_score / 4.0).min(1.0),
                    description: format!(
                        "Throughput {:.1} MB/s is {:.1}σ above average {:.1} MB/s",
                        throughput / 1_000_000.0,
                        z_score,
                        avg / 1_000_000.0
                    ),
                    metric: "throughput".to_string(),
                    expected: avg,
                    actual: throughput,
                });
            }

            // High failure rate
            let failure_rate = if entry.stats.total_files > 0 {
                entry.stats.files_failed as f64 / entry.stats.total_files as f64
            } else {
                0.0
            };

            if failure_rate > 0.05 {
                anomalies.push(Anomaly {
                    entry_id: entry.id.clone(),
                    anomaly_type: AnomalyType::HighFailureRate,
                    severity: (failure_rate * 2.0).min(1.0),
                    description: format!(
                        "{:.1}% of files failed ({} of {})",
                        failure_rate * 100.0,
                        entry.stats.files_failed,
                        entry.stats.total_files
                    ),
                    metric: "failure_rate".to_string(),
                    expected: 0.0,
                    actual: failure_rate,
                });
            }
        }

        anomalies
    }

    /// Generate recommendations based on analysis
    fn generate_recommendations(
        &self,
        entries: &[HistoryEntry],
        comparison: &ComparisonMetrics,
        trend: &PerformanceTrend,
        anomalies: &[Anomaly],
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Trend-based recommendations
        match trend.direction {
            TrendDirection::Degrading if trend.strength > 0.1 => {
                recommendations.push(
                    "Performance is degrading over time. Consider investigating storage or network issues.".to_string()
                );
            }
            TrendDirection::Volatile => {
                recommendations.push(
                    "Performance is highly variable. Check for resource contention or system load variations.".to_string()
                );
            }
            _ => {}
        }

        // Throughput recommendations
        let avg_throughput = comparison.throughput.avg;
        if avg_throughput < 100_000_000.0 { // Less than 100 MB/s
            recommendations.push(
                "Average throughput is below 100 MB/s. Consider increasing parallel connections or buffer size.".to_string()
            );
        }

        // High variance
        if comparison.throughput.stddev / comparison.throughput.avg > 0.5 {
            recommendations.push(
                "High throughput variance detected. Consider using bandwidth limiting for consistent performance.".to_string()
            );
        }

        // Anomaly-based recommendations
        let throughput_drops: Vec<_> = anomalies.iter()
            .filter(|a| matches!(a.anomaly_type, AnomalyType::ThroughputDrop))
            .collect();

        if throughput_drops.len() > entries.len() / 4 {
            recommendations.push(
                "Frequent throughput drops detected. Check for network congestion or disk I/O bottlenecks.".to_string()
            );
        }

        let high_failures: Vec<_> = anomalies.iter()
            .filter(|a| matches!(a.anomaly_type, AnomalyType::HighFailureRate))
            .collect();

        if !high_failures.is_empty() {
            recommendations.push(
                "High failure rates detected. Review error logs and check destination permissions/space.".to_string()
            );
        }

        // Configuration recommendations
        if let Some(last) = entries.last() {
            if !last.config.verify && last.stats.bytes_transferred > 10_000_000_000 {
                recommendations.push(
                    "Large transfers without verification. Consider enabling --verify for data integrity.".to_string()
                );
            }

            if last.config.threads < 4 && avg_throughput < 500_000_000.0 {
                recommendations.push(
                    "Low thread count. Try increasing --threads for better parallelism.".to_string()
                );
            }
        }

        recommendations
    }

    /// Get aggregate statistics
    pub fn get_aggregate_stats(&self, days: i64) -> AggregateStats {
        let cache = self.cache.read().unwrap();
        let cutoff = Utc::now() - Duration::days(days);

        let entries: Vec<_> = cache.iter()
            .filter(|e| e.ended_at > cutoff)
            .collect();

        let total_jobs = entries.len();
        let successful_jobs = entries.iter()
            .filter(|e| e.status == TransferStatus::Success)
            .count();

        let total_bytes: u64 = entries.iter().map(|e| e.stats.bytes_transferred).sum();
        let total_files: u64 = entries.iter().map(|e| e.stats.files_transferred).sum();
        let total_duration: f64 = entries.iter().map(|e| e.duration_seconds).sum();

        let throughputs: Vec<f64> = entries.iter().map(|e| e.stats.avg_throughput).collect();
        let avg_throughput = if !throughputs.is_empty() {
            throughputs.iter().sum::<f64>() / throughputs.len() as f64
        } else {
            0.0
        };

        AggregateStats {
            period_days: days,
            total_jobs,
            successful_jobs,
            failed_jobs: total_jobs - successful_jobs,
            success_rate: if total_jobs > 0 { successful_jobs as f64 / total_jobs as f64 * 100.0 } else { 0.0 },
            total_bytes_transferred: total_bytes,
            total_files_transferred: total_files,
            total_duration_seconds: total_duration,
            avg_throughput,
            avg_job_duration: if total_jobs > 0 { total_duration / total_jobs as f64 } else { 0.0 },
        }
    }
}

/// Aggregate statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateStats {
    /// Period in days
    pub period_days: i64,
    /// Total jobs
    pub total_jobs: usize,
    /// Successful jobs
    pub successful_jobs: usize,
    /// Failed jobs
    pub failed_jobs: usize,
    /// Success rate (0-100)
    pub success_rate: f64,
    /// Total bytes transferred
    pub total_bytes_transferred: u64,
    /// Total files transferred
    pub total_files_transferred: u64,
    /// Total duration
    pub total_duration_seconds: f64,
    /// Average throughput
    pub avg_throughput: f64,
    /// Average job duration
    pub avg_job_duration: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_comparison() {
        let manager = HistoryManager {
            storage_path: PathBuf::from("/tmp/test_history.json"),
            cache: std::sync::RwLock::new(Vec::new()),
            max_cache_size: 1000,
        };

        let values = vec![100.0, 120.0, 110.0, 130.0, 150.0];
        let comparison = manager.calc_metric_comparison(&values);

        assert_eq!(comparison.min, 100.0);
        assert_eq!(comparison.max, 150.0);
        assert!((comparison.avg - 122.0).abs() < 0.1);
        assert!(comparison.percent_change > 0.0);
    }

    #[test]
    fn test_trend_analysis() {
        let manager = HistoryManager {
            storage_path: PathBuf::from("/tmp/test_history.json"),
            cache: std::sync::RwLock::new(Vec::new()),
            max_cache_size: 1000,
        };

        // Improving trend
        let values = vec![100.0, 110.0, 120.0, 130.0, 140.0];
        let trend = manager.analyze_trend(&values);
        assert!(matches!(trend.direction, TrendDirection::Improving));

        // Degrading trend
        let values = vec![140.0, 130.0, 120.0, 110.0, 100.0];
        let trend = manager.analyze_trend(&values);
        assert!(matches!(trend.direction, TrendDirection::Degrading));
    }
}
