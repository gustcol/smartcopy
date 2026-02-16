//! NUMA (Non-Uniform Memory Access) awareness for multi-socket systems
//!
//! Provides thread affinity and memory locality optimizations for HPC environments.
//! On multi-socket systems, this can significantly improve performance by:
//! - Pinning threads to specific CPU cores
//! - Allocating memory on the local NUMA node
//! - Reducing cross-socket memory traffic


/// NUMA node information
#[derive(Debug, Clone)]
pub struct NumaNode {
    /// Node ID
    pub id: usize,
    /// CPUs belonging to this node
    pub cpus: Vec<usize>,
    /// Total memory in bytes
    pub memory_total: u64,
    /// Free memory in bytes
    pub memory_free: u64,
}

/// NUMA topology information
#[derive(Debug, Clone)]
pub struct NumaTopology {
    /// Number of NUMA nodes
    pub num_nodes: usize,
    /// NUMA nodes
    pub nodes: Vec<NumaNode>,
    /// Total CPUs across all nodes
    pub total_cpus: usize,
    /// Is NUMA actually available/meaningful
    pub is_numa_system: bool,
}

impl Default for NumaTopology {
    fn default() -> Self {
        Self::detect()
    }
}

impl NumaTopology {
    /// Detect NUMA topology from the system
    #[cfg(target_os = "linux")]
    pub fn detect() -> Self {
        let mut nodes = Vec::new();
        let mut total_cpus = 0;

        // Try to read from /sys/devices/system/node/
        if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") {
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with("node") {
                    if let Ok(node_id) = name_str.trim_start_matches("node").parse::<usize>() {
                        let node_path = entry.path();

                        // Read CPUs for this node
                        let cpus = Self::read_node_cpus(&node_path);
                        total_cpus += cpus.len();

                        // Read memory info
                        let (mem_total, mem_free) = Self::read_node_memory(&node_path);

                        nodes.push(NumaNode {
                            id: node_id,
                            cpus,
                            memory_total: mem_total,
                            memory_free: mem_free,
                        });
                    }
                }
            }
        }

        // Sort by node ID
        nodes.sort_by_key(|n| n.id);

        let num_nodes = nodes.len();
        let is_numa_system = num_nodes > 1;

        // Fallback if no NUMA info found
        if nodes.is_empty() {
            let num_cpus = num_cpus::get();
            nodes.push(NumaNode {
                id: 0,
                cpus: (0..num_cpus).collect(),
                memory_total: 0,
                memory_free: 0,
            });
            total_cpus = num_cpus;
        }

        Self {
            num_nodes: nodes.len(),
            nodes,
            total_cpus,
            is_numa_system,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn detect() -> Self {
        // On non-Linux, assume single NUMA node
        let num_cpus = num_cpus::get();

        Self {
            num_nodes: 1,
            nodes: vec![NumaNode {
                id: 0,
                cpus: (0..num_cpus).collect(),
                memory_total: 0,
                memory_free: 0,
            }],
            total_cpus: num_cpus,
            is_numa_system: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn read_node_cpus(node_path: &std::path::Path) -> Vec<usize> {
        let cpulist_path = node_path.join("cpulist");

        if let Ok(content) = std::fs::read_to_string(&cpulist_path) {
            Self::parse_cpu_list(content.trim())
        } else {
            Vec::new()
        }
    }

    #[cfg(target_os = "linux")]
    fn read_node_memory(node_path: &std::path::Path) -> (u64, u64) {
        let meminfo_path = node_path.join("meminfo");

        let mut total = 0u64;
        let mut free = 0u64;

        if let Ok(content) = std::fs::read_to_string(&meminfo_path) {
            for line in content.lines() {
                if line.contains("MemTotal:") {
                    if let Some(kb) = Self::parse_meminfo_value(line) {
                        total = kb * 1024;
                    }
                } else if line.contains("MemFree:") {
                    if let Some(kb) = Self::parse_meminfo_value(line) {
                        free = kb * 1024;
                    }
                }
            }
        }

        (total, free)
    }

    #[cfg(target_os = "linux")]
    fn parse_meminfo_value(line: &str) -> Option<u64> {
        line.split_whitespace()
            .nth(3) // Format: "Node X MemTotal: 12345 kB"
            .and_then(|s| s.parse().ok())
    }

    /// Parse CPU list format (e.g., "0-3,8-11" -> [0,1,2,3,8,9,10,11])
    fn parse_cpu_list(s: &str) -> Vec<usize> {
        let mut cpus = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.contains('-') {
                let mut range = part.split('-');
                if let (Some(start), Some(end)) = (range.next(), range.next()) {
                    if let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) {
                        cpus.extend(start..=end);
                    }
                }
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            }
        }

        cpus
    }

    /// Get the best node for a given path (based on the storage controller)
    pub fn best_node_for_path(&self, _path: &std::path::Path) -> usize {
        // Simplified: return node 0
        // Full implementation would check storage controller NUMA locality
        0
    }

    /// Get CPUs for worker distribution
    pub fn get_worker_cpus(&self, num_workers: usize) -> Vec<usize> {
        if !self.is_numa_system {
            return (0..num_workers.min(self.total_cpus)).collect();
        }

        // Distribute workers evenly across NUMA nodes
        let mut cpus = Vec::with_capacity(num_workers);
        let workers_per_node = (num_workers + self.num_nodes - 1) / self.num_nodes;

        for node in &self.nodes {
            let take = workers_per_node.min(node.cpus.len());
            cpus.extend(node.cpus.iter().take(take).copied());

            if cpus.len() >= num_workers {
                break;
            }
        }

        cpus.truncate(num_workers);
        cpus
    }

    /// Print NUMA topology summary
    pub fn print_summary(&self) {
        println!("NUMA Topology:");
        println!("  Nodes: {}", self.num_nodes);
        println!("  Total CPUs: {}", self.total_cpus);
        println!("  NUMA System: {}", self.is_numa_system);

        for node in &self.nodes {
            println!("  Node {}:", node.id);
            println!("    CPUs: {:?}", node.cpus);
            if node.memory_total > 0 {
                println!("    Memory: {} / {} free",
                    humansize::format_size(node.memory_free, humansize::BINARY),
                    humansize::format_size(node.memory_total, humansize::BINARY));
            }
        }
    }
}

/// Thread affinity helper
pub struct ThreadAffinity;

impl ThreadAffinity {
    /// Pin current thread to a specific CPU
    #[cfg(target_os = "linux")]
    pub fn pin_to_cpu(cpu: usize) -> std::io::Result<()> {
        use std::mem::MaybeUninit;

        unsafe {
            let mut set = MaybeUninit::<libc::cpu_set_t>::zeroed().assume_init();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);

            let result = libc::sched_setaffinity(
                0, // current thread
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );

            if result == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn pin_to_cpu(_cpu: usize) -> std::io::Result<()> {
        // Thread affinity not supported on this platform
        Ok(())
    }

    /// Pin current thread to a set of CPUs
    #[cfg(target_os = "linux")]
    pub fn pin_to_cpus(cpus: &[usize]) -> std::io::Result<()> {
        use std::mem::MaybeUninit;

        if cpus.is_empty() {
            return Ok(());
        }

        unsafe {
            let mut set = MaybeUninit::<libc::cpu_set_t>::zeroed().assume_init();
            libc::CPU_ZERO(&mut set);

            for &cpu in cpus {
                libc::CPU_SET(cpu, &mut set);
            }

            let result = libc::sched_setaffinity(
                0,
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );

            if result == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn pin_to_cpus(_cpus: &[usize]) -> std::io::Result<()> {
        Ok(())
    }

    /// Pin current thread to a NUMA node
    pub fn pin_to_node(topology: &NumaTopology, node_id: usize) -> std::io::Result<()> {
        if let Some(node) = topology.nodes.iter().find(|n| n.id == node_id) {
            Self::pin_to_cpus(&node.cpus)
        } else {
            Ok(())
        }
    }
}

/// NUMA-aware memory allocator hint
#[cfg(target_os = "linux")]
pub fn set_memory_policy_local() -> std::io::Result<()> {
    // Use libc::set_mempolicy with MPOL_LOCAL if available
    // This is a simplified version - full implementation would use libnuma
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_memory_policy_local() -> std::io::Result<()> {
    Ok(())
}

/// Read the set of allowed CPUs from cgroup v2 (used in containers).
///
/// Parses `/proc/self/status` for `Cpus_allowed_list` to determine
/// which CPUs are available to this process (important in containerized
/// environments where cgroups restrict CPU access).
#[cfg(target_os = "linux")]
pub fn read_cgroup_allowed_cpus() -> Vec<usize> {
    if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
        for line in content.lines() {
            if line.starts_with("Cpus_allowed_list:") {
                let list = line.trim_start_matches("Cpus_allowed_list:").trim();
                return NumaTopology::parse_cpu_list(list);
            }
        }
    }
    // Fallback: all CPUs
    (0..num_cpus::get()).collect()
}

#[cfg(not(target_os = "linux"))]
pub fn read_cgroup_allowed_cpus() -> Vec<usize> {
    (0..num_cpus::get()).collect()
}

/// Detect the CPU quota from cgroup v1 or v2 configuration.
///
/// Returns the fractional number of CPUs available (e.g., 2.5 means
/// two and a half CPU cores). Returns `None` if no cgroup quota is set
/// or if running outside a container.
///
/// - **cgroup v2**: reads `/sys/fs/cgroup/cpu.max` (format: `max_us period_us` or `max period_us`)
/// - **cgroup v1**: reads `cpu.cfs_quota_us` and `cpu.cfs_period_us` from the cpu controller
#[cfg(target_os = "linux")]
pub fn get_container_cpu_quota() -> Option<f64> {
    // Try cgroup v2 first: /sys/fs/cgroup/cpu.max
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let content = content.trim();
        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() == 2 {
            if parts[0] == "max" {
                // "max" means unlimited
                return None;
            }
            if let (Ok(quota), Ok(period)) = (
                parts[0].parse::<f64>(),
                parts[1].parse::<f64>(),
            ) {
                if period > 0.0 {
                    return Some(quota / period);
                }
            }
        }
    }

    // Try cgroup v1: /sys/fs/cgroup/cpu/cpu.cfs_quota_us and cpu.cfs_period_us
    let quota_path = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    let period_path = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    if let (Ok(quota_str), Ok(period_str)) = (
        std::fs::read_to_string(quota_path),
        std::fs::read_to_string(period_path),
    ) {
        if let (Ok(quota), Ok(period)) = (
            quota_str.trim().parse::<i64>(),
            period_str.trim().parse::<i64>(),
        ) {
            if quota < 0 {
                // -1 means unlimited
                return None;
            }
            if period > 0 {
                return Some(quota as f64 / period as f64);
            }
        }
    }

    None
}

#[cfg(not(target_os = "linux"))]
pub fn get_container_cpu_quota() -> Option<f64> {
    None
}

/// Get the effective number of available CPUs, respecting container quotas.
///
/// Prefers the cgroup CPU quota if set, otherwise falls back to
/// `std::thread::available_parallelism()` then `num_cpus::get()`.
pub fn get_available_cpus() -> usize {
    // Check cgroup quota first
    if let Some(quota) = get_container_cpu_quota() {
        let cpus = quota.ceil() as usize;
        if cpus > 0 {
            return cpus;
        }
    }

    // Fall back to standard detection
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or_else(|_| num_cpus::get())
}

/// Manages pinning rayon worker threads to specific CPU cores for
/// maximum cache locality and minimal cross-core traffic.
pub struct WorkerPinner {
    cpus: Vec<usize>,
}

impl WorkerPinner {
    /// Create a pinner using cgroup-allowed CPUs.
    pub fn from_allowed_cpus() -> Self {
        Self {
            cpus: read_cgroup_allowed_cpus(),
        }
    }

    /// Create a pinner from a specific CPU list.
    pub fn from_cpus(cpus: Vec<usize>) -> Self {
        Self { cpus }
    }

    /// Create a pinner from NUMA topology with the given worker count.
    pub fn from_topology(topology: &NumaTopology, num_workers: usize) -> Self {
        Self {
            cpus: topology.get_worker_cpus(num_workers),
        }
    }

    /// Get the number of available CPUs.
    pub fn num_cpus(&self) -> usize {
        self.cpus.len()
    }

    /// Build a rayon thread pool with workers pinned to specific cores.
    #[cfg(feature = "numa")]
    pub fn build_pinned_pool(
        &self,
        num_threads: usize,
    ) -> Result<rayon::ThreadPool, rayon::ThreadPoolBuildError> {
        let cpus = self.cpus.clone();
        let effective_threads = num_threads.min(cpus.len());

        rayon::ThreadPoolBuilder::new()
            .num_threads(effective_threads)
            .start_handler(move |idx| {
                if idx < cpus.len() {
                    let cpu_id = cpus[idx];
                    if let Some(core_ids) = core_affinity::get_core_ids() {
                        if let Some(core_id) = core_ids.iter().find(|c| c.id == cpu_id) {
                            core_affinity::set_for_current(*core_id);
                        }
                    }
                }
            })
            .build()
    }

    /// Build a standard (unpinned) rayon thread pool as fallback.
    #[cfg(not(feature = "numa"))]
    pub fn build_pinned_pool(
        &self,
        num_threads: usize,
    ) -> Result<rayon::ThreadPool, rayon::ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.min(self.cpus.len()))
            .build()
    }
}

/// Convenience function: pin workers to cores and return a rayon pool.
pub fn pin_workers_to_cores(num_workers: usize) -> Result<rayon::ThreadPool, rayon::ThreadPoolBuildError> {
    let pinner = WorkerPinner::from_allowed_cpus();
    pinner.build_pinned_pool(num_workers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_detection() {
        let topology = NumaTopology::detect();
        assert!(topology.num_nodes >= 1);
        assert!(topology.total_cpus >= 1);
        assert!(!topology.nodes.is_empty());
    }

    #[test]
    fn test_cpu_list_parsing() {
        assert_eq!(NumaTopology::parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(NumaTopology::parse_cpu_list("0,2,4"), vec![0, 2, 4]);
        assert_eq!(NumaTopology::parse_cpu_list("0-2,4-6"), vec![0, 1, 2, 4, 5, 6]);
    }

    #[test]
    fn test_worker_distribution() {
        let topology = NumaTopology::detect();
        let cpus = topology.get_worker_cpus(4);
        assert!(cpus.len() <= 4);
        assert!(!cpus.is_empty());
    }

    #[test]
    fn test_cgroup_allowed_cpus() {
        let cpus = read_cgroup_allowed_cpus();
        assert!(!cpus.is_empty());
    }

    #[test]
    fn test_worker_pinner_creation() {
        let pinner = WorkerPinner::from_allowed_cpus();
        assert!(pinner.num_cpus() > 0);
    }

    #[test]
    fn test_worker_pinner_from_topology() {
        let topology = NumaTopology::detect();
        let pinner = WorkerPinner::from_topology(&topology, 4);
        assert!(pinner.num_cpus() > 0);
        assert!(pinner.num_cpus() <= 4);
    }

    #[test]
    fn test_pin_workers_to_cores() {
        let pool = pin_workers_to_cores(2);
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        assert!(pool.current_num_threads() > 0);
    }

    #[test]
    fn test_get_available_cpus() {
        let cpus = get_available_cpus();
        assert!(cpus >= 1, "Should detect at least 1 CPU");
    }

    #[test]
    fn test_container_cpu_quota_returns_option() {
        // On most dev machines this returns None (no cgroup quota).
        // In containers it returns Some(fractional_cpus).
        let quota = get_container_cpu_quota();
        if let Some(q) = quota {
            assert!(q > 0.0, "CPU quota should be positive if set");
        }
        // Either way, get_available_cpus should still work
        assert!(get_available_cpus() >= 1);
    }
}
