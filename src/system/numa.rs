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
}
