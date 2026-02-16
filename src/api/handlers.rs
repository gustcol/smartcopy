//! API Request Handlers
//!
//! HTTP request handlers for all API endpoints.

use crate::api::models::*;
use crate::api::history::*;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use chrono::Utc;

/// Application state shared across handlers
pub struct AppState {
    /// Active transfer jobs
    pub jobs: RwLock<HashMap<String, TransferJob>>,
    /// Connected agents
    pub agents: RwLock<HashMap<String, AgentInfo>>,
    /// History manager
    pub history: HistoryManager,
    /// Server start time
    pub start_time: std::time::Instant,
    /// Total bytes transferred
    pub total_bytes: std::sync::atomic::AtomicU64,
    /// Total files transferred
    pub total_files: std::sync::atomic::AtomicU64,
}

impl AppState {
    /// Create new application state
    pub fn new(history_path: &std::path::Path) -> Result<Self> {
        Ok(Self {
            jobs: RwLock::new(HashMap::new()),
            agents: RwLock::new(HashMap::new()),
            history: HistoryManager::new(history_path)?,
            start_time: std::time::Instant::now(),
            total_bytes: std::sync::atomic::AtomicU64::new(0),
            total_files: std::sync::atomic::AtomicU64::new(0),
        })
    }
}

/// Handler for GET /api/status
pub fn handle_status(state: &AppState) -> SystemStatus {
    use std::sync::atomic::Ordering;

    let jobs = state.jobs.read().unwrap();
    let agents = state.agents.read().unwrap();

    let active_jobs = jobs.values()
        .filter(|j| j.status == JobStatus::Running)
        .count();

    let connected_agents = agents.values()
        .filter(|a| matches!(a.status, AgentStatus::Connected | AgentStatus::Busy))
        .count();

    // Determine health
    let health = if active_jobs > 0 && connected_agents == 0 {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };

    SystemStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.start_time.elapsed().as_secs(),
        active_jobs,
        connected_agents,
        total_bytes_transferred: state.total_bytes.load(Ordering::Relaxed),
        total_files_transferred: state.total_files.load(Ordering::Relaxed),
        health,
        timestamp: Utc::now(),
    }
}

/// Handler for GET /api/jobs
pub fn handle_list_jobs(state: &AppState, params: &PaginationParams) -> PaginatedResponse<TransferJob> {
    let jobs = state.jobs.read().unwrap();

    let mut all_jobs: Vec<_> = jobs.values().cloned().collect();
    all_jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let total = all_jobs.len();
    let start = (params.page - 1) * params.per_page;
    let items: Vec<_> = all_jobs.into_iter()
        .skip(start)
        .take(params.per_page)
        .collect();

    PaginatedResponse::new(items, total, params.page, params.per_page)
}

/// Handler for GET /api/jobs/{id}
pub fn handle_get_job(state: &AppState, job_id: &str) -> Option<TransferJob> {
    let jobs = state.jobs.read().unwrap();
    jobs.get(job_id).cloned()
}

/// Handler for POST /api/jobs
pub fn handle_create_job(state: &AppState, request: CreateJobRequest) -> Result<TransferJob> {
    let job_id = uuid::Uuid::new_v4().to_string();

    let job = TransferJob {
        id: job_id.clone(),
        name: request.name.unwrap_or_else(|| format!("Job {}", &job_id[..8])),
        source: request.source,
        destination: request.destination,
        status: if request.start_immediately { JobStatus::Running } else { JobStatus::Pending },
        config: request.config,
        progress: JobProgress::default(),
        started_at: None,
        ended_at: None,
        error: None,
        created_at: Utc::now(),
    };

    {
        let mut jobs = state.jobs.write().unwrap();
        jobs.insert(job_id, job.clone());
    }

    Ok(job)
}

/// Handler for DELETE /api/jobs/{id}
pub fn handle_cancel_job(state: &AppState, job_id: &str) -> Option<TransferJob> {
    let mut jobs = state.jobs.write().unwrap();

    if let Some(job) = jobs.get_mut(job_id) {
        if job.status == JobStatus::Running || job.status == JobStatus::Pending {
            job.status = JobStatus::Cancelled;
            job.ended_at = Some(Utc::now());
            return Some(job.clone());
        }
    }

    None
}

/// Handler for GET /api/history
pub fn handle_list_history(
    state: &AppState,
    params: &PaginationParams,
    source: Option<&str>,
    destination: Option<&str>,
) -> PaginatedResponse<HistoryEntry> {
    let entries = if let (Some(src), Some(dst)) = (source, destination) {
        state.history.get_by_path(src, dst)
    } else {
        state.history.get_recent(1000)
    };

    let total = entries.len();
    let start = (params.page - 1) * params.per_page;
    let items: Vec<_> = entries.into_iter()
        .skip(start)
        .take(params.per_page)
        .collect();

    PaginatedResponse::new(items, total, params.page, params.per_page)
}

/// Handler for GET /api/history/{id}
pub fn handle_get_history_entry(state: &AppState, entry_id: &str) -> Option<HistoryEntry> {
    state.history.get_by_id(entry_id)
}

/// Handler for GET /api/compare
pub fn handle_compare_transfers(
    state: &AppState,
    entry_ids: &[String],
) -> Result<TransferComparison> {
    state.history.compare(entry_ids)
}

/// Handler for GET /api/history/stats
pub fn handle_history_stats(state: &AppState, days: i64) -> AggregateStats {
    state.history.get_aggregate_stats(days)
}

/// Handler for GET /api/agents
pub fn handle_list_agents(state: &AppState) -> Vec<AgentInfo> {
    let agents = state.agents.read().unwrap();
    agents.values().cloned().collect()
}

/// Handler for GET /api/system
pub fn handle_system_info() -> SystemInfo {
    use sysinfo::{System, Disks};

    let mut sys = System::new_all();
    sys.refresh_all();

    let disks = Disks::new_with_refreshed_list();

    let storage_devices: Vec<StorageDevice> = disks.iter()
        .map(|d| StorageDevice {
            name: d.name().to_string_lossy().to_string(),
            mount_point: d.mount_point().to_string_lossy().to_string(),
            fs_type: d.file_system().to_string_lossy().to_string(),
            total_bytes: d.total_space(),
            available_bytes: d.available_space(),
            device_type: detect_storage_type(d.name().to_string_lossy().as_ref()),
        })
        .collect();

    SystemInfo {
        hostname: hostname::get().map(|h| h.to_string_lossy().to_string()).unwrap_or_default(),
        os: format!("{} {}", System::name().unwrap_or_default(), System::os_version().unwrap_or_default()),
        cpu_model: sys.cpus().first().map(|c| c.brand().to_string()).unwrap_or_default(),
        cpu_cores_physical: sys.physical_core_count().unwrap_or(0),
        cpu_cores_logical: sys.cpus().len(),
        total_memory: sys.total_memory(),
        available_memory: sys.available_memory(),
        storage_devices,
        network_interfaces: Vec::new(), // Would need additional implementation
        numa_nodes: None, // Would need hwloc
        io_uring_supported: cfg!(target_os = "linux"),
        kernel_version: System::kernel_version(),
    }
}

/// Detect storage type from device name
fn detect_storage_type(name: &str) -> String {
    let name_lower = name.to_lowercase();
    if name_lower.contains("nvme") {
        "nvme".to_string()
    } else if name_lower.contains("ssd") {
        "ssd".to_string()
    } else if name_lower.contains("hd") || name_lower.contains("disk") {
        "hdd".to_string()
    } else if name_lower.contains("nfs") || name_lower.contains("smb") || name_lower.contains("cifs") {
        "network".to_string()
    } else {
        "unknown".to_string()
    }
}

/// Handler for GET /api/metrics (Prometheus format)
pub fn handle_metrics(state: &AppState) -> String {
    use std::sync::atomic::Ordering;

    let jobs = state.jobs.read().unwrap();
    let agents = state.agents.read().unwrap();

    let active_jobs = jobs.values().filter(|j| j.status == JobStatus::Running).count();
    let pending_jobs = jobs.values().filter(|j| j.status == JobStatus::Pending).count();
    let completed_jobs = jobs.values().filter(|j| j.status == JobStatus::Completed).count();
    let failed_jobs = jobs.values().filter(|j| j.status == JobStatus::Failed).count();

    let connected_agents = agents.values()
        .filter(|a| matches!(a.status, AgentStatus::Connected | AgentStatus::Busy))
        .count();

    format!(
        r#"# HELP smartcopy_jobs_active Number of active transfer jobs
# TYPE smartcopy_jobs_active gauge
smartcopy_jobs_active {}

# HELP smartcopy_jobs_pending Number of pending transfer jobs
# TYPE smartcopy_jobs_pending gauge
smartcopy_jobs_pending {}

# HELP smartcopy_jobs_completed_total Total number of completed jobs
# TYPE smartcopy_jobs_completed_total counter
smartcopy_jobs_completed_total {}

# HELP smartcopy_jobs_failed_total Total number of failed jobs
# TYPE smartcopy_jobs_failed_total counter
smartcopy_jobs_failed_total {}

# HELP smartcopy_agents_connected Number of connected agents
# TYPE smartcopy_agents_connected gauge
smartcopy_agents_connected {}

# HELP smartcopy_bytes_transferred_total Total bytes transferred
# TYPE smartcopy_bytes_transferred_total counter
smartcopy_bytes_transferred_total {}

# HELP smartcopy_files_transferred_total Total files transferred
# TYPE smartcopy_files_transferred_total counter
smartcopy_files_transferred_total {}

# HELP smartcopy_uptime_seconds Server uptime in seconds
# TYPE smartcopy_uptime_seconds gauge
smartcopy_uptime_seconds {}
"#,
        active_jobs,
        pending_jobs,
        completed_jobs,
        failed_jobs,
        connected_agents,
        state.total_bytes.load(Ordering::Relaxed),
        state.total_files.load(Ordering::Relaxed),
        state.start_time.elapsed().as_secs(),
    )
}

/// Generate a new UUID
mod uuid {
    pub struct Uuid;

    impl Uuid {
        pub fn new_v4() -> UuidValue {
            use std::time::{SystemTime, UNIX_EPOCH};

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            // Mix timestamp with process ID and pointer address for better uniqueness
            let random: u64 = now as u64
                ^ (std::process::id() as u64).rotate_left(32)
                ^ (&now as *const _ as u64);

            UuidValue(format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                (now >> 96) as u32,
                (now >> 80) as u16,
                ((now >> 64) as u16 & 0x0FFF) | 0x4000, // version 4 marker
                (random >> 48) as u16 | 0x8000, // variant bits
                random & 0xFFFFFFFFFFFF
            ))
        }
    }

    pub struct UuidValue(String);

    impl UuidValue {
        pub fn to_string(&self) -> String {
            self.0.clone()
        }
    }
}
