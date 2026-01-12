//! API Data Models
//!
//! Data structures for the REST API endpoints.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use chrono::{DateTime, Utc};

/// System status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    /// SmartCopy version
    pub version: String,
    /// Server uptime
    pub uptime_seconds: u64,
    /// Number of active jobs
    pub active_jobs: usize,
    /// Number of connected agents
    pub connected_agents: usize,
    /// Total bytes transferred since start
    pub total_bytes_transferred: u64,
    /// Total files transferred since start
    pub total_files_transferred: u64,
    /// System health status
    pub health: HealthStatus,
    /// Last update timestamp
    pub timestamp: DateTime<Utc>,
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Transfer job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferJob {
    /// Unique job ID
    pub id: String,
    /// Job name/description
    pub name: String,
    /// Source path or URI
    pub source: String,
    /// Destination path or URI
    pub destination: String,
    /// Job status
    pub status: JobStatus,
    /// Job configuration
    pub config: JobConfig,
    /// Progress information
    pub progress: JobProgress,
    /// Start time
    pub started_at: Option<DateTime<Utc>>,
    /// End time
    pub ended_at: Option<DateTime<Utc>>,
    /// Error message if failed
    pub error: Option<String>,
    /// Created at
    pub created_at: DateTime<Utc>,
}

/// Job status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused,
}

/// Job configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// Number of parallel threads
    pub threads: usize,
    /// Buffer size in bytes
    pub buffer_size: usize,
    /// Enable verification
    pub verify: bool,
    /// Verification algorithm
    pub verify_algorithm: Option<String>,
    /// Enable compression
    pub compression: bool,
    /// Enable incremental sync
    pub incremental: bool,
    /// Enable delta transfer
    pub delta: bool,
    /// Bandwidth limit (bytes/sec, 0 = unlimited)
    pub bandwidth_limit: u64,
    /// Include patterns
    pub include_patterns: Vec<String>,
    /// Exclude patterns
    pub exclude_patterns: Vec<String>,
    /// Delete extra files at destination
    pub delete_extra: bool,
    /// Number of parallel connections for remote transfers
    pub parallel_connections: usize,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            threads: 0, // auto-detect
            buffer_size: 64 * 1024 * 1024, // 64 MB
            verify: false,
            verify_algorithm: None,
            compression: false,
            incremental: false,
            delta: false,
            bandwidth_limit: 0,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            delete_extra: false,
            parallel_connections: 4,
        }
    }
}

/// Job progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    /// Total files to transfer
    pub total_files: u64,
    /// Files transferred
    pub files_transferred: u64,
    /// Files failed
    pub files_failed: u64,
    /// Files skipped (already up to date)
    pub files_skipped: u64,
    /// Total bytes to transfer
    pub total_bytes: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Current throughput (bytes/sec)
    pub throughput: f64,
    /// Estimated time remaining (seconds)
    pub eta_seconds: Option<u64>,
    /// Progress percentage (0-100)
    pub percent: f64,
    /// Current file being transferred
    pub current_file: Option<String>,
}

impl Default for JobProgress {
    fn default() -> Self {
        Self {
            total_files: 0,
            files_transferred: 0,
            files_failed: 0,
            files_skipped: 0,
            total_bytes: 0,
            bytes_transferred: 0,
            throughput: 0.0,
            eta_seconds: None,
            percent: 0.0,
            current_file: None,
        }
    }
}

/// Create job request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobRequest {
    /// Job name
    pub name: Option<String>,
    /// Source path or URI
    pub source: String,
    /// Destination path or URI
    pub destination: String,
    /// Job configuration
    #[serde(default)]
    pub config: JobConfig,
    /// Start immediately
    #[serde(default = "default_true")]
    pub start_immediately: bool,
}

fn default_true() -> bool {
    true
}

/// Connected agent information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    /// Agent ID
    pub id: String,
    /// Hostname
    pub hostname: String,
    /// IP address
    pub ip_address: String,
    /// Port
    pub port: u16,
    /// Protocol (tcp, quic, ssh)
    pub protocol: String,
    /// Agent version
    pub version: String,
    /// Connection status
    pub status: AgentStatus,
    /// Connected since
    pub connected_at: DateTime<Utc>,
    /// Last heartbeat
    pub last_heartbeat: DateTime<Utc>,
    /// System information
    pub system_info: Option<AgentSystemInfo>,
}

/// Agent connection status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Connected,
    Disconnected,
    Busy,
    Error,
}

/// Agent system information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSystemInfo {
    /// OS name
    pub os: String,
    /// CPU cores
    pub cpu_cores: usize,
    /// Total memory (bytes)
    pub total_memory: u64,
    /// Available memory (bytes)
    pub available_memory: u64,
    /// Disk space available (bytes)
    pub disk_available: u64,
    /// Network interfaces
    pub network_interfaces: Vec<NetworkInterface>,
}

/// Network interface information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterface {
    /// Interface name
    pub name: String,
    /// IP addresses
    pub addresses: Vec<String>,
    /// Speed (Mbps)
    pub speed_mbps: Option<u64>,
}

/// System resource information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Hostname
    pub hostname: String,
    /// OS name and version
    pub os: String,
    /// CPU model
    pub cpu_model: String,
    /// Physical CPU cores
    pub cpu_cores_physical: usize,
    /// Logical CPU cores
    pub cpu_cores_logical: usize,
    /// Total memory (bytes)
    pub total_memory: u64,
    /// Available memory (bytes)
    pub available_memory: u64,
    /// Storage devices
    pub storage_devices: Vec<StorageDevice>,
    /// Network interfaces
    pub network_interfaces: Vec<NetworkInterface>,
    /// NUMA nodes (if applicable)
    pub numa_nodes: Option<usize>,
    /// io_uring support
    pub io_uring_supported: bool,
    /// Kernel version (Linux only)
    pub kernel_version: Option<String>,
}

/// Storage device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDevice {
    /// Device name
    pub name: String,
    /// Mount point
    pub mount_point: String,
    /// Filesystem type
    pub fs_type: String,
    /// Total space (bytes)
    pub total_bytes: u64,
    /// Available space (bytes)
    pub available_bytes: u64,
    /// Device type (nvme, ssd, hdd, network)
    pub device_type: String,
}

/// Performance metrics (Prometheus-compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Metric entries
    pub metrics: HashMap<String, MetricValue>,
}

/// Metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(HistogramValue),
}

/// Histogram metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramValue {
    pub count: u64,
    pub sum: f64,
    pub buckets: Vec<(f64, u64)>,
}

/// API error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
    /// Additional details
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "NOT_FOUND".to_string(),
            message: message.into(),
            details: None,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: "BAD_REQUEST".to_string(),
            message: message.into(),
            details: None,
        }
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self {
            code: "INTERNAL_ERROR".to_string(),
            message: message.into(),
            details: None,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: "CONFLICT".to_string(),
            message: message.into(),
            details: None,
        }
    }
}

/// Pagination parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationParams {
    /// Page number (1-indexed)
    #[serde(default = "default_page")]
    pub page: usize,
    /// Items per page
    #[serde(default = "default_per_page")]
    pub per_page: usize,
}

fn default_page() -> usize {
    1
}

fn default_per_page() -> usize {
    20
}

/// Paginated response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    /// Items
    pub items: Vec<T>,
    /// Total items
    pub total: usize,
    /// Current page
    pub page: usize,
    /// Items per page
    pub per_page: usize,
    /// Total pages
    pub total_pages: usize,
}

impl<T> PaginatedResponse<T> {
    pub fn new(items: Vec<T>, total: usize, page: usize, per_page: usize) -> Self {
        let total_pages = (total + per_page - 1) / per_page;
        Self {
            items,
            total,
            page,
            per_page,
            total_pages,
        }
    }
}
