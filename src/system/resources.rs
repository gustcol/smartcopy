//! System resource detection
//!
//! Detects CPU, memory, storage, and network capabilities
//! to optimize copy operations for the current system.

use serde::{Deserialize, Serialize};
use std::path::Path;
use sysinfo::System;

/// Complete system information snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// CPU information
    pub cpu: CpuInfo,
    /// Memory information
    pub memory: MemoryInfo,
    /// Storage information for relevant paths
    pub storage: Vec<StorageInfo>,
    /// Network interfaces
    pub network: Vec<NetworkInfo>,
    /// NUMA topology (if available)
    pub numa: Option<NumaInfo>,
}

/// CPU information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuInfo {
    /// Total number of logical CPUs
    pub logical_cores: usize,
    /// Number of physical cores
    pub physical_cores: usize,
    /// CPU model name
    pub model: String,
    /// CPU frequency in MHz (if available)
    pub frequency_mhz: Option<u64>,
    /// CPU architecture
    pub arch: String,
    /// Vendor (Intel, AMD, ARM, etc.)
    pub vendor: String,
}

/// Memory information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    /// Total physical memory in bytes
    pub total: u64,
    /// Available memory in bytes
    pub available: u64,
    /// Used memory in bytes
    pub used: u64,
    /// Swap total in bytes
    pub swap_total: u64,
    /// Swap used in bytes
    pub swap_used: u64,
}

/// Storage device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageInfo {
    /// Mount point path
    pub mount_point: String,
    /// Device name/path
    pub device: String,
    /// Filesystem type
    pub fs_type: String,
    /// Total space in bytes
    pub total_bytes: u64,
    /// Available space in bytes
    pub available_bytes: u64,
    /// Storage type classification
    pub storage_type: StorageType,
    /// Is this a remote/network filesystem?
    pub is_remote: bool,
}

/// Storage type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageType {
    /// NVMe SSD - highest performance
    NVMe,
    /// SATA/SAS SSD - high performance
    SSD,
    /// Traditional spinning disk
    HDD,
    /// Network filesystem (NFS, CIFS, etc.)
    Network,
    /// RAM disk / tmpfs
    RamDisk,
    /// Unknown storage type
    Unknown,
}

impl StorageType {
    /// Get recommended thread count for this storage type
    pub fn recommended_threads(&self) -> usize {
        match self {
            StorageType::NVMe => num_cpus::get(),
            StorageType::SSD => num_cpus::get().min(8),
            StorageType::HDD => 2, // HDDs don't benefit from parallelism
            StorageType::Network => num_cpus::get().min(16),
            StorageType::RamDisk => num_cpus::get(),
            StorageType::Unknown => 4,
        }
    }

    /// Get recommended buffer size for this storage type
    pub fn recommended_buffer_size(&self) -> usize {
        match self {
            StorageType::NVMe => 4 * 1024 * 1024,      // 4MB
            StorageType::SSD => 1 * 1024 * 1024,       // 1MB
            StorageType::HDD => 8 * 1024 * 1024,       // 8MB - larger for sequential
            StorageType::Network => 256 * 1024,        // 256KB
            StorageType::RamDisk => 4 * 1024 * 1024,   // 4MB
            StorageType::Unknown => 1 * 1024 * 1024,   // 1MB default
        }
    }

    /// Get recommended queue depth for async I/O
    pub fn recommended_queue_depth(&self) -> usize {
        match self {
            StorageType::NVMe => 64,
            StorageType::SSD => 32,
            StorageType::HDD => 4,
            StorageType::Network => 16,
            StorageType::RamDisk => 64,
            StorageType::Unknown => 8,
        }
    }
}

/// Network interface information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInfo {
    /// Interface name
    pub name: String,
    /// MAC address
    pub mac_address: Option<String>,
    /// IPv4 addresses
    pub ipv4: Vec<String>,
    /// IPv6 addresses
    pub ipv6: Vec<String>,
    /// Link speed in Mbps (if detectable)
    pub link_speed_mbps: Option<u64>,
    /// Is this interface up?
    pub is_up: bool,
}

/// NUMA topology information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaInfo {
    /// Number of NUMA nodes
    pub num_nodes: usize,
    /// CPUs per node
    pub cpus_per_node: Vec<Vec<usize>>,
    /// Memory per node in bytes
    pub memory_per_node: Vec<u64>,
}

impl SystemInfo {
    /// Collect complete system information
    pub fn collect() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        SystemInfo {
            cpu: CpuInfo::collect(&sys),
            memory: MemoryInfo::collect(&sys),
            storage: Vec::new(), // Will be populated on demand
            network: NetworkInfo::collect(),
            numa: NumaInfo::collect(),
        }
    }

    /// Collect system info including storage for specific paths
    pub fn collect_with_paths(paths: &[&Path]) -> Self {
        let mut info = Self::collect();
        for path in paths {
            if let Some(storage_info) = StorageInfo::for_path(path) {
                // Avoid duplicates
                if !info.storage.iter().any(|s| s.mount_point == storage_info.mount_point) {
                    info.storage.push(storage_info);
                }
            }
        }
        info
    }

    /// Get optimal thread count based on system and storage
    pub fn optimal_thread_count(&self) -> usize {
        let cpu_threads = self.cpu.logical_cores;

        // Consider storage type if available
        let storage_threads = self
            .storage
            .iter()
            .map(|s| s.storage_type.recommended_threads())
            .min()
            .unwrap_or(cpu_threads);

        // Don't exceed CPU cores, but consider I/O bound nature
        cpu_threads.min(storage_threads * 2).max(2)
    }

    /// Get optimal buffer size based on storage
    pub fn optimal_buffer_size(&self) -> usize {
        self.storage
            .iter()
            .map(|s| s.storage_type.recommended_buffer_size())
            .max()
            .unwrap_or(1024 * 1024)
    }

    /// Calculate available memory for buffers (50% of available)
    pub fn available_buffer_memory(&self) -> u64 {
        self.memory.available / 2
    }

    /// Print system summary to console
    pub fn print_summary(&self) {
        println!("=== System Information ===\n");

        println!("CPU:");
        println!("  Model: {}", self.cpu.model);
        println!("  Logical cores: {}", self.cpu.logical_cores);
        println!("  Physical cores: {}", self.cpu.physical_cores);
        println!("  Architecture: {}", self.cpu.arch);
        if let Some(freq) = self.cpu.frequency_mhz {
            println!("  Frequency: {} MHz", freq);
        }

        println!("\nMemory:");
        println!("  Total: {}", humansize::format_size(self.memory.total, humansize::BINARY));
        println!("  Available: {}", humansize::format_size(self.memory.available, humansize::BINARY));
        println!("  Used: {}", humansize::format_size(self.memory.used, humansize::BINARY));

        if !self.storage.is_empty() {
            println!("\nStorage:");
            for storage in &self.storage {
                println!("  {}:", storage.mount_point);
                println!("    Device: {}", storage.device);
                println!("    Type: {:?}", storage.storage_type);
                println!("    Filesystem: {}", storage.fs_type);
                println!("    Total: {}", humansize::format_size(storage.total_bytes, humansize::BINARY));
                println!("    Available: {}", humansize::format_size(storage.available_bytes, humansize::BINARY));
            }
        }

        if let Some(numa) = &self.numa {
            println!("\nNUMA Topology:");
            println!("  Nodes: {}", numa.num_nodes);
            for (i, cpus) in numa.cpus_per_node.iter().enumerate() {
                println!("  Node {}: CPUs {:?}, Memory: {}",
                    i, cpus,
                    humansize::format_size(numa.memory_per_node.get(i).copied().unwrap_or(0), humansize::BINARY)
                );
            }
        }

        println!("\nRecommended Settings:");
        println!("  Threads: {}", self.optimal_thread_count());
        println!("  Buffer size: {}", humansize::format_size(self.optimal_buffer_size() as u64, humansize::BINARY));
    }
}

impl CpuInfo {
    /// Collect CPU information
    pub fn collect(sys: &System) -> Self {
        let cpus = sys.cpus();

        let model = cpus
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let frequency_mhz = cpus.first().map(|c| c.frequency());

        let vendor = cpus
            .first()
            .map(|c| c.vendor_id().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        CpuInfo {
            logical_cores: num_cpus::get(),
            physical_cores: num_cpus::get_physical(),
            model,
            frequency_mhz,
            arch: std::env::consts::ARCH.to_string(),
            vendor,
        }
    }
}

impl MemoryInfo {
    /// Collect memory information
    pub fn collect(sys: &System) -> Self {
        MemoryInfo {
            total: sys.total_memory(),
            available: sys.available_memory(),
            used: sys.used_memory(),
            swap_total: sys.total_swap(),
            swap_used: sys.used_swap(),
        }
    }
}

impl StorageInfo {
    /// Get storage info for a specific path
    pub fn for_path(path: &Path) -> Option<Self> {
        use sysinfo::Disks;

        let disks = Disks::new_with_refreshed_list();

        // Find the disk that contains this path
        let mut best_match: Option<&sysinfo::Disk> = None;
        let mut best_len = 0;

        let path_str = path.to_string_lossy();

        for disk in disks.iter() {
            let mount = disk.mount_point().to_string_lossy();
            if path_str.starts_with(mount.as_ref()) && mount.len() > best_len {
                best_match = Some(disk);
                best_len = mount.len();
            }
        }

        best_match.map(|disk| {
            let device_name = disk.name().to_string_lossy().to_string();
            let fs_type = disk.file_system().to_string_lossy().to_string();
            let mount_point = disk.mount_point().to_string_lossy().to_string();

            let storage_type = Self::detect_storage_type(&device_name, &fs_type, &mount_point);
            let is_remote = Self::is_remote_fs(&fs_type);

            StorageInfo {
                mount_point,
                device: device_name,
                fs_type,
                total_bytes: disk.total_space(),
                available_bytes: disk.available_space(),
                storage_type,
                is_remote,
            }
        })
    }

    /// Detect storage type from device name and filesystem
    fn detect_storage_type(device: &str, fs_type: &str, mount: &str) -> StorageType {
        let device_lower = device.to_lowercase();
        let fs_lower = fs_type.to_lowercase();
        let mount_lower = mount.to_lowercase();

        // Check for network filesystems
        if Self::is_remote_fs(&fs_lower) {
            return StorageType::Network;
        }

        // Check for RAM disk
        if fs_lower.contains("tmpfs") || fs_lower.contains("ramfs") || mount_lower.contains("/dev/shm") {
            return StorageType::RamDisk;
        }

        // Check for NVMe
        if device_lower.contains("nvme") {
            return StorageType::NVMe;
        }

        // Try to detect SSD vs HDD on Linux
        #[cfg(target_os = "linux")]
        {
            if let Some(is_ssd) = Self::is_ssd_linux(&device_lower) {
                return if is_ssd { StorageType::SSD } else { StorageType::HDD };
            }
        }

        // macOS APFS is typically SSD
        #[cfg(target_os = "macos")]
        {
            if fs_lower.contains("apfs") {
                return StorageType::SSD;
            }
        }

        StorageType::Unknown
    }

    /// Check if filesystem is a remote/network type
    fn is_remote_fs(fs_type: &str) -> bool {
        let fs_lower = fs_type.to_lowercase();
        fs_lower.contains("nfs")
            || fs_lower.contains("cifs")
            || fs_lower.contains("smb")
            || fs_lower.contains("fuse")
            || fs_lower.contains("sshfs")
            || fs_lower.contains("gpfs")
            || fs_lower.contains("lustre")
            || fs_lower.contains("gluster")
            || fs_lower.contains("ceph")
            || fs_lower.contains("beegfs")
    }

    /// Linux-specific SSD detection
    #[cfg(target_os = "linux")]
    fn is_ssd_linux(device: &str) -> Option<bool> {
        // Extract device name (e.g., "sda" from "/dev/sda1")
        let dev_name = device
            .trim_start_matches("/dev/")
            .chars()
            .take_while(|c| c.is_alphabetic())
            .collect::<String>();

        if dev_name.is_empty() {
            return None;
        }

        // Check rotational flag
        let rotational_path = format!("/sys/block/{}/queue/rotational", dev_name);
        if let Ok(content) = std::fs::read_to_string(&rotational_path) {
            if let Ok(rotational) = content.trim().parse::<u8>() {
                return Some(rotational == 0);
            }
        }

        None
    }

    #[cfg(not(target_os = "linux"))]
    fn is_ssd_linux(_device: &str) -> Option<bool> {
        None
    }
}

impl NetworkInfo {
    /// Collect network interface information
    pub fn collect() -> Vec<Self> {
        use sysinfo::Networks;

        let networks = Networks::new_with_refreshed_list();

        networks
            .iter()
            .map(|(name, _data)| {
                NetworkInfo {
                    name: name.clone(),
                    mac_address: None, // sysinfo doesn't provide this easily
                    ipv4: Vec::new(),
                    ipv6: Vec::new(),
                    link_speed_mbps: None,
                    is_up: true,
                }
            })
            .collect()
    }
}

impl NumaInfo {
    /// Collect NUMA topology (Linux-specific)
    pub fn collect() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::collect_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_linux() -> Option<Self> {
        let numa_path = Path::new("/sys/devices/system/node");

        if !numa_path.exists() {
            return None;
        }

        let mut nodes = Vec::new();
        let mut cpus_per_node = Vec::new();
        let mut memory_per_node = Vec::new();

        if let Ok(entries) = std::fs::read_dir(numa_path) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with("node") && name_str[4..].chars().all(|c| c.is_ascii_digit()) {
                    if let Ok(node_num) = name_str[4..].parse::<usize>() {
                        nodes.push(node_num);

                        // Get CPUs for this node
                        let cpulist_path = entry.path().join("cpulist");
                        let cpus = if let Ok(content) = std::fs::read_to_string(&cpulist_path) {
                            Self::parse_cpu_list(&content)
                        } else {
                            Vec::new()
                        };
                        cpus_per_node.push(cpus);

                        // Get memory for this node
                        let meminfo_path = entry.path().join("meminfo");
                        let memory = if let Ok(content) = std::fs::read_to_string(&meminfo_path) {
                            Self::parse_node_memory(&content)
                        } else {
                            0
                        };
                        memory_per_node.push(memory);
                    }
                }
            }
        }

        if nodes.is_empty() {
            return None;
        }

        Some(NumaInfo {
            num_nodes: nodes.len(),
            cpus_per_node,
            memory_per_node,
        })
    }

    #[cfg(target_os = "linux")]
    fn parse_cpu_list(content: &str) -> Vec<usize> {
        let mut cpus = Vec::new();
        for part in content.trim().split(',') {
            if let Some((start, end)) = part.split_once('-') {
                if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    cpus.extend(s..=e);
                }
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            }
        }
        cpus
    }

    #[cfg(target_os = "linux")]
    fn parse_node_memory(content: &str) -> u64 {
        for line in content.lines() {
            if line.contains("MemTotal:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    if let Ok(kb) = parts[3].parse::<u64>() {
                        return kb * 1024; // Convert KB to bytes
                    }
                }
            }
        }
        0
    }
}

/// Bandwidth tester for network interfaces
pub struct BandwidthTester {
    /// Test data size in bytes
    pub test_size: usize,
}

impl BandwidthTester {
    /// Create a new bandwidth tester
    pub fn new(test_size: usize) -> Self {
        Self { test_size }
    }

    /// Estimate local I/O bandwidth for a path
    pub fn test_local_io(&self, path: &Path) -> std::io::Result<IoMetrics> {
        use std::io::{Read, Write};
        use std::time::Instant;

        let test_file = path.join(".smartcopy_bandwidth_test");
        let data: Vec<u8> = (0..self.test_size).map(|i| (i % 256) as u8).collect();

        // Write test
        let write_start = Instant::now();
        {
            let mut file = std::fs::File::create(&test_file)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        let write_duration = write_start.elapsed();

        // Read test
        let read_start = Instant::now();
        {
            let mut file = std::fs::File::open(&test_file)?;
            let mut buffer = vec![0u8; self.test_size];
            file.read_exact(&mut buffer)?;
        }
        let read_duration = read_start.elapsed();

        // Cleanup
        let _ = std::fs::remove_file(&test_file);

        let write_mbps = (self.test_size as f64 / (1024.0 * 1024.0))
            / write_duration.as_secs_f64();
        let read_mbps = (self.test_size as f64 / (1024.0 * 1024.0))
            / read_duration.as_secs_f64();

        Ok(IoMetrics {
            write_mbps,
            read_mbps,
            iops_estimate: None,
        })
    }
}

/// I/O performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoMetrics {
    /// Write speed in MB/s
    pub write_mbps: f64,
    /// Read speed in MB/s
    pub read_mbps: f64,
    /// Estimated IOPS (if measured)
    pub iops_estimate: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_info_collection() {
        let info = SystemInfo::collect();
        assert!(info.cpu.logical_cores > 0);
        assert!(info.memory.total > 0);
    }

    #[test]
    fn test_storage_type_recommendations() {
        assert!(StorageType::NVMe.recommended_threads() >= StorageType::HDD.recommended_threads());
        assert!(StorageType::NVMe.recommended_queue_depth() > StorageType::HDD.recommended_queue_depth());
    }

    #[test]
    fn test_remote_fs_detection() {
        assert!(StorageInfo::is_remote_fs("nfs4"));
        assert!(StorageInfo::is_remote_fs("cifs"));
        assert!(StorageInfo::is_remote_fs("lustre"));
        assert!(!StorageInfo::is_remote_fs("ext4"));
        assert!(!StorageInfo::is_remote_fs("xfs"));
    }
}
