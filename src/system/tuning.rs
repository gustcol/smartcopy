//! System tuning recommendations
//!
//! Analyzes system configuration and provides recommendations
//! for optimal file copy performance in HPC environments.

use super::SystemInfo;
use crate::config::WorkloadType;
use serde::{Deserialize, Serialize};

/// Tuning recommendation with severity and implementation details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningRecommendation {
    /// Category of the recommendation
    pub category: TuningCategory,
    /// Severity/importance level
    pub severity: TuningSeverity,
    /// Short title
    pub title: String,
    /// Detailed description
    pub description: String,
    /// Current value (if applicable)
    pub current_value: Option<String>,
    /// Recommended value
    pub recommended_value: Option<String>,
    /// Command to implement the change
    pub implementation: Option<String>,
    /// Is this a persistent change?
    pub persistent: bool,
}

/// Category of tuning recommendation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TuningCategory {
    /// Kernel parameters
    Kernel,
    /// TCP/Network settings
    Network,
    /// Filesystem/storage settings
    Storage,
    /// Memory management
    Memory,
    /// CPU/scheduler settings
    Cpu,
    /// Application-level settings
    Application,
}

/// Severity level of recommendation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TuningSeverity {
    /// Critical - significant performance impact
    Critical,
    /// High - notable improvement expected
    High,
    /// Medium - moderate improvement
    Medium,
    /// Low - minor optimization
    Low,
    /// Info - informational only
    Info,
}

/// Tuning analyzer that generates recommendations
pub struct TuningAnalyzer {
    /// System information
    system_info: SystemInfo,
    /// Target workload type
    workload: WorkloadType,
}

impl TuningAnalyzer {
    /// Create a new tuning analyzer
    pub fn new(system_info: SystemInfo, workload: WorkloadType) -> Self {
        Self {
            system_info,
            workload,
        }
    }

    /// Generate all tuning recommendations
    pub fn analyze(&self) -> Vec<TuningRecommendation> {
        let mut recommendations = Vec::new();

        // Analyze each category
        recommendations.extend(self.analyze_network());
        recommendations.extend(self.analyze_storage());
        recommendations.extend(self.analyze_memory());
        recommendations.extend(self.analyze_cpu());
        recommendations.extend(self.analyze_application());

        // Sort by severity
        recommendations.sort_by(|a, b| {
            (a.severity as u8).cmp(&(b.severity as u8))
        });

        recommendations
    }

    /// Network tuning recommendations
    fn analyze_network(&self) -> Vec<TuningRecommendation> {
        let recs = Vec::new();

        #[cfg(target_os = "linux")]
        {
            // TCP buffer sizes
            let current_rmem = Self::read_sysctl("net.core.rmem_max").unwrap_or_default();
            let current_wmem = Self::read_sysctl("net.core.wmem_max").unwrap_or_default();

            let recommended_buffer = match self.workload {
                WorkloadType::Network | WorkloadType::LargeFiles => 67108864, // 64MB
                _ => 16777216, // 16MB
            };

            if current_rmem.parse::<u64>().unwrap_or(0) < recommended_buffer {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Network,
                    severity: TuningSeverity::High,
                    title: "Increase TCP receive buffer".to_string(),
                    description: "Larger receive buffers improve throughput for high-bandwidth transfers".to_string(),
                    current_value: Some(current_rmem),
                    recommended_value: Some(recommended_buffer.to_string()),
                    implementation: Some(format!(
                        "sysctl -w net.core.rmem_max={}\necho 'net.core.rmem_max={}' >> /etc/sysctl.conf",
                        recommended_buffer, recommended_buffer
                    )),
                    persistent: true,
                });
            }

            if current_wmem.parse::<u64>().unwrap_or(0) < recommended_buffer {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Network,
                    severity: TuningSeverity::High,
                    title: "Increase TCP send buffer".to_string(),
                    description: "Larger send buffers improve throughput for high-bandwidth transfers".to_string(),
                    current_value: Some(current_wmem),
                    recommended_value: Some(recommended_buffer.to_string()),
                    implementation: Some(format!(
                        "sysctl -w net.core.wmem_max={}\necho 'net.core.wmem_max={}' >> /etc/sysctl.conf",
                        recommended_buffer, recommended_buffer
                    )),
                    persistent: true,
                });
            }

            // TCP congestion control
            let current_cc = Self::read_sysctl("net.ipv4.tcp_congestion_control").unwrap_or_default();
            if current_cc != "bbr" {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Network,
                    severity: TuningSeverity::Medium,
                    title: "Use BBR congestion control".to_string(),
                    description: "BBR provides better throughput especially on networks with some packet loss".to_string(),
                    current_value: Some(current_cc),
                    recommended_value: Some("bbr".to_string()),
                    implementation: Some(
                        "modprobe tcp_bbr\nsysctl -w net.ipv4.tcp_congestion_control=bbr".to_string()
                    ),
                    persistent: true,
                });
            }

            // Jumbo frames hint
            recs.push(TuningRecommendation {
                category: TuningCategory::Network,
                severity: TuningSeverity::Info,
                title: "Consider jumbo frames".to_string(),
                description: "If your network supports it, MTU 9000 can improve throughput by reducing packet overhead".to_string(),
                current_value: None,
                recommended_value: Some("MTU 9000".to_string()),
                implementation: Some("ip link set <interface> mtu 9000".to_string()),
                persistent: false,
            });

            // Netdev budget for high-speed NICs
            let current_budget = Self::read_sysctl("net.core.netdev_budget").unwrap_or_default();
            if current_budget.parse::<u64>().unwrap_or(0) < 600 {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Network,
                    severity: TuningSeverity::Medium,
                    title: "Increase netdev budget".to_string(),
                    description: "Higher budget allows processing more packets per softirq cycle".to_string(),
                    current_value: Some(current_budget),
                    recommended_value: Some("600".to_string()),
                    implementation: Some("sysctl -w net.core.netdev_budget=600".to_string()),
                    persistent: true,
                });
            }
        }

        recs
    }

    /// Storage tuning recommendations
    fn analyze_storage(&self) -> Vec<TuningRecommendation> {
        let mut recs = Vec::new();

        #[cfg(target_os = "linux")]
        {
            // Dirty page settings
            let current_ratio = Self::read_sysctl("vm.dirty_ratio").unwrap_or_default();
            let current_background = Self::read_sysctl("vm.dirty_background_ratio").unwrap_or_default();

            let (rec_ratio, rec_bg) = match self.workload {
                WorkloadType::LargeFiles => (40, 10),
                WorkloadType::SmallFiles => (10, 5),
                _ => (20, 5),
            };

            if current_ratio.parse::<u32>().unwrap_or(0) != rec_ratio {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Storage,
                    severity: TuningSeverity::Medium,
                    title: "Optimize dirty page ratio".to_string(),
                    description: "Controls when the kernel starts flushing dirty pages".to_string(),
                    current_value: Some(current_ratio),
                    recommended_value: Some(rec_ratio.to_string()),
                    implementation: Some(format!("sysctl -w vm.dirty_ratio={}", rec_ratio)),
                    persistent: true,
                });
            }

            if current_background.parse::<u32>().unwrap_or(0) != rec_bg {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Storage,
                    severity: TuningSeverity::Medium,
                    title: "Optimize dirty background ratio".to_string(),
                    description: "Controls when background writeback starts".to_string(),
                    current_value: Some(current_background),
                    recommended_value: Some(rec_bg.to_string()),
                    implementation: Some(format!("sysctl -w vm.dirty_background_ratio={}", rec_bg)),
                    persistent: true,
                });
            }

            // I/O scheduler recommendations based on storage type
            for storage in &self.system_info.storage {
                let rec_scheduler = match storage.storage_type {
                    StorageType::NVMe | StorageType::SSD => "none",
                    StorageType::HDD => "mq-deadline",
                    _ => "none",
                };

                recs.push(TuningRecommendation {
                    category: TuningCategory::Storage,
                    severity: TuningSeverity::Medium,
                    title: format!("I/O scheduler for {}", storage.device),
                    description: format!("Optimal scheduler for {:?} storage", storage.storage_type),
                    current_value: None,
                    recommended_value: Some(rec_scheduler.to_string()),
                    implementation: Some(format!(
                        "echo {} > /sys/block/{}/queue/scheduler",
                        rec_scheduler,
                        storage.device.trim_start_matches("/dev/")
                    )),
                    persistent: false,
                });
            }

            // Read-ahead settings
            for storage in &self.system_info.storage {
                let rec_readahead = match storage.storage_type {
                    StorageType::NVMe => 256,
                    StorageType::SSD => 256,
                    StorageType::HDD => 4096,
                    StorageType::Network => 1024,
                    _ => 256,
                };

                recs.push(TuningRecommendation {
                    category: TuningCategory::Storage,
                    severity: TuningSeverity::Low,
                    title: format!("Read-ahead for {}", storage.device),
                    description: "Optimize read-ahead for sequential workloads".to_string(),
                    current_value: None,
                    recommended_value: Some(format!("{} KB", rec_readahead)),
                    implementation: Some(format!(
                        "blockdev --setra {} /dev/{}",
                        rec_readahead * 2, // blockdev uses 512-byte sectors
                        storage.device.trim_start_matches("/dev/")
                    )),
                    persistent: false,
                });
            }
        }

        // Filesystem recommendations
        recs.push(TuningRecommendation {
            category: TuningCategory::Storage,
            severity: TuningSeverity::Info,
            title: "Filesystem mount options".to_string(),
            description: "Consider noatime/nodiratime mount options to reduce metadata writes".to_string(),
            current_value: None,
            recommended_value: Some("noatime,nodiratime".to_string()),
            implementation: Some("Add 'noatime,nodiratime' to mount options in /etc/fstab".to_string()),
            persistent: true,
        });

        recs
    }

    /// Memory tuning recommendations
    fn analyze_memory(&self) -> Vec<TuningRecommendation> {
        let recs = Vec::new();

        #[cfg(target_os = "linux")]
        {
            // Swappiness
            let current_swappiness = Self::read_sysctl("vm.swappiness").unwrap_or_default();
            if current_swappiness.parse::<u32>().unwrap_or(60) > 10 {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Memory,
                    severity: TuningSeverity::Medium,
                    title: "Reduce swappiness".to_string(),
                    description: "Lower swappiness keeps more file cache in memory, beneficial for I/O workloads".to_string(),
                    current_value: Some(current_swappiness),
                    recommended_value: Some("10".to_string()),
                    implementation: Some("sysctl -w vm.swappiness=10".to_string()),
                    persistent: true,
                });
            }

            // VFS cache pressure
            let current_pressure = Self::read_sysctl("vm.vfs_cache_pressure").unwrap_or_default();
            let rec_pressure = match self.workload {
                WorkloadType::SmallFiles => 50, // More aggressive caching
                _ => 100, // Default
            };

            if current_pressure.parse::<u32>().unwrap_or(100) != rec_pressure {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Memory,
                    severity: TuningSeverity::Low,
                    title: "Optimize VFS cache pressure".to_string(),
                    description: "Controls how aggressively the kernel reclaims directory/inode caches".to_string(),
                    current_value: Some(current_pressure),
                    recommended_value: Some(rec_pressure.to_string()),
                    implementation: Some(format!("sysctl -w vm.vfs_cache_pressure={}", rec_pressure)),
                    persistent: true,
                });
            }

            // Transparent Huge Pages recommendation
            recs.push(TuningRecommendation {
                category: TuningCategory::Memory,
                severity: TuningSeverity::Info,
                title: "Transparent Huge Pages".to_string(),
                description: "THP can improve performance for large buffer operations, but may cause latency spikes".to_string(),
                current_value: None,
                recommended_value: Some("madvise".to_string()),
                implementation: Some("echo madvise > /sys/kernel/mm/transparent_hugepage/enabled".to_string()),
                persistent: false,
            });
        }

        recs
    }

    /// CPU tuning recommendations
    fn analyze_cpu(&self) -> Vec<TuningRecommendation> {
        let mut recs = Vec::new();

        // NUMA recommendations
        if let Some(numa) = &self.system_info.numa {
            if numa.num_nodes > 1 {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Cpu,
                    severity: TuningSeverity::High,
                    title: "NUMA-aware execution".to_string(),
                    description: format!(
                        "System has {} NUMA nodes. SmartCopy will automatically use NUMA-aware memory allocation",
                        numa.num_nodes
                    ),
                    current_value: None,
                    recommended_value: None,
                    implementation: Some("numactl --interleave=all smartcopy ...".to_string()),
                    persistent: false,
                });
            }
        }

        #[cfg(target_os = "linux")]
        {
            // CPU governor
            recs.push(TuningRecommendation {
                category: TuningCategory::Cpu,
                severity: TuningSeverity::Medium,
                title: "CPU frequency governor".to_string(),
                description: "Performance governor ensures maximum CPU frequency during transfers".to_string(),
                current_value: None,
                recommended_value: Some("performance".to_string()),
                implementation: Some(
                    "for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance > $cpu; done".to_string()
                ),
                persistent: false,
            });

            // IRQ affinity hint for network-heavy workloads
            if matches!(self.workload, WorkloadType::Network) {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Cpu,
                    severity: TuningSeverity::Medium,
                    title: "IRQ affinity".to_string(),
                    description: "Distribute network IRQs across CPUs for better parallelism".to_string(),
                    current_value: None,
                    recommended_value: None,
                    implementation: Some("Use irqbalance or set manually with /proc/irq/*/smp_affinity".to_string()),
                    persistent: false,
                });
            }
        }

        recs
    }

    /// Application-level recommendations
    fn analyze_application(&self) -> Vec<TuningRecommendation> {
        let mut recs = Vec::new();

        // Thread count recommendation
        let optimal_threads = self.system_info.optimal_thread_count();
        recs.push(TuningRecommendation {
            category: TuningCategory::Application,
            severity: TuningSeverity::Info,
            title: "Recommended thread count".to_string(),
            description: format!(
                "Based on {} CPU cores and storage type",
                self.system_info.cpu.logical_cores
            ),
            current_value: None,
            recommended_value: Some(optimal_threads.to_string()),
            implementation: Some(format!("smartcopy --threads {}", optimal_threads)),
            persistent: false,
        });

        // Buffer size recommendation
        let optimal_buffer = self.system_info.optimal_buffer_size();
        recs.push(TuningRecommendation {
            category: TuningCategory::Application,
            severity: TuningSeverity::Info,
            title: "Recommended buffer size".to_string(),
            description: "Optimal buffer size for your storage type".to_string(),
            current_value: None,
            recommended_value: Some(format!("{}", humansize::format_size(optimal_buffer as u64, humansize::BINARY))),
            implementation: Some(format!("smartcopy --buffer-size {}", optimal_buffer)),
            persistent: false,
        });

        // Workload-specific recommendations
        match self.workload {
            WorkloadType::SmallFiles => {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Application,
                    severity: TuningSeverity::High,
                    title: "Small files optimization".to_string(),
                    description: "Use higher thread count and smaller buffers for many small files".to_string(),
                    current_value: None,
                    recommended_value: None,
                    implementation: Some(format!(
                        "smartcopy --threads {} --buffer-size 64K",
                        self.system_info.cpu.logical_cores
                    )),
                    persistent: false,
                });
            }
            WorkloadType::LargeFiles => {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Application,
                    severity: TuningSeverity::High,
                    title: "Large files optimization".to_string(),
                    description: "Use delta transfers and larger buffers for large files".to_string(),
                    current_value: None,
                    recommended_value: None,
                    implementation: Some("smartcopy --delta --buffer-size 8M".to_string()),
                    persistent: false,
                });
            }
            WorkloadType::Network => {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Application,
                    severity: TuningSeverity::High,
                    title: "Network transfer optimization".to_string(),
                    description: "Use compression and multiple streams for network transfers".to_string(),
                    current_value: None,
                    recommended_value: None,
                    implementation: Some("smartcopy --compress --ssh-streams 8".to_string()),
                    persistent: false,
                });
            }
            WorkloadType::Mixed => {
                recs.push(TuningRecommendation {
                    category: TuningCategory::Application,
                    severity: TuningSeverity::Info,
                    title: "Mixed workload".to_string(),
                    description: "Default settings are optimized for mixed workloads".to_string(),
                    current_value: None,
                    recommended_value: None,
                    implementation: None,
                    persistent: false,
                });
            }
        }

        recs
    }

    /// Read a sysctl value
    #[cfg(target_os = "linux")]
    fn read_sysctl(name: &str) -> Option<String> {
        let path = format!("/proc/sys/{}", name.replace('.', "/"));
        std::fs::read_to_string(&path).ok().map(|s| s.trim().to_string())
    }

    #[cfg(not(target_os = "linux"))]
    fn read_sysctl(_name: &str) -> Option<String> {
        None
    }

    /// Print recommendations to console
    pub fn print_recommendations(&self) {
        let recommendations = self.analyze();

        println!("=== Tuning Recommendations for {:?} Workload ===\n", self.workload);

        let mut current_category = None;

        for rec in &recommendations {
            if current_category != Some(rec.category) {
                println!("\n## {:?} ##\n", rec.category);
                current_category = Some(rec.category);
            }

            let severity_icon = match rec.severity {
                TuningSeverity::Critical => "[!!!]",
                TuningSeverity::High => "[!!]",
                TuningSeverity::Medium => "[!]",
                TuningSeverity::Low => "[~]",
                TuningSeverity::Info => "[i]",
            };

            println!("{} {}", severity_icon, rec.title);
            println!("   {}", rec.description);

            if let Some(current) = &rec.current_value {
                println!("   Current: {}", current);
            }
            if let Some(recommended) = &rec.recommended_value {
                println!("   Recommended: {}", recommended);
            }
            if let Some(impl_cmd) = &rec.implementation {
                println!("   Implementation:");
                for line in impl_cmd.lines() {
                    println!("     $ {}", line);
                }
            }
            println!();
        }

        // Summary
        let critical = recommendations.iter().filter(|r| r.severity == TuningSeverity::Critical).count();
        let high = recommendations.iter().filter(|r| r.severity == TuningSeverity::High).count();
        let medium = recommendations.iter().filter(|r| r.severity == TuningSeverity::Medium).count();

        println!("=== Summary ===");
        println!("Critical: {}, High: {}, Medium: {}", critical, high, medium);

        if critical > 0 || high > 0 {
            println!("\nApplying the critical and high priority recommendations could significantly improve performance.");
        }
    }
}

/// Network speed tier for high-speed tuning recommendations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkSpeedTier {
    /// 10 Gigabit Ethernet (~1.25 GB/s)
    Gbps10,
    /// 100 Gigabit Ethernet (~12.5 GB/s)
    Gbps100,
    /// 200 Gigabit Ethernet (~25 GB/s)
    Gbps200,
    /// 400 Gigabit Ethernet (~50 GB/s)
    Gbps400,
}

impl NetworkSpeedTier {
    /// Get the theoretical throughput in GB/s
    pub fn throughput_gbps(&self) -> f64 {
        match self {
            NetworkSpeedTier::Gbps10 => 1.25,
            NetworkSpeedTier::Gbps100 => 12.5,
            NetworkSpeedTier::Gbps200 => 25.0,
            NetworkSpeedTier::Gbps400 => 50.0,
        }
    }

    /// Get the speed in Gbps
    pub fn speed_gbps(&self) -> u32 {
        match self {
            NetworkSpeedTier::Gbps10 => 10,
            NetworkSpeedTier::Gbps100 => 100,
            NetworkSpeedTier::Gbps200 => 200,
            NetworkSpeedTier::Gbps400 => 400,
        }
    }
}

/// High-speed network tuning guide
pub struct HighSpeedNetworkGuide;

impl HighSpeedNetworkGuide {
    /// Get tuning recommendations for a specific network speed tier
    pub fn get_recommendations(tier: NetworkSpeedTier) -> HighSpeedConfig {
        match tier {
            NetworkSpeedTier::Gbps10 => Self::config_10g(),
            NetworkSpeedTier::Gbps100 => Self::config_100g(),
            NetworkSpeedTier::Gbps200 => Self::config_200g(),
            NetworkSpeedTier::Gbps400 => Self::config_400g(),
        }
    }

    /// 10 Gbps configuration (~1.25 GB/s)
    fn config_10g() -> HighSpeedConfig {
        HighSpeedConfig {
            tier: NetworkSpeedTier::Gbps10,
            threads: 8,
            buffer_size_mb: 16,
            tcp_buffer_size_mb: 64,
            mtu: 9000,
            streams: 4,
            congestion_control: "bbr".to_string(),
            ring_buffer_size: 4096,
            irq_affinity: false,
            numa_aware: true,
            kernel_bypass: false,
            use_io_uring: true,
            use_quic: true,
            use_rdma: false,
            nics_required: 1,
            storage_requirements: vec![
                "NVMe SSD (min 2 GB/s read/write)".to_string(),
            ],
            kernel_params: vec![
                ("net.core.rmem_max", "67108864"),
                ("net.core.wmem_max", "67108864"),
                ("net.ipv4.tcp_rmem", "4096 87380 67108864"),
                ("net.ipv4.tcp_wmem", "4096 65536 67108864"),
                ("net.core.netdev_max_backlog", "30000"),
                ("net.ipv4.tcp_congestion_control", "bbr"),
                ("net.ipv4.tcp_mtu_probing", "1"),
            ].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            notes: vec![
                "Standard modern NVMe SSDs can sustain this throughput".to_string(),
                "Enable jumbo frames (MTU 9000) on all network paths".to_string(),
                "Use BBR congestion control for best throughput".to_string(),
                "SSH with aes128-gcm cipher can achieve near line rate".to_string(),
                "QUIC transport recommended for lower latency".to_string(),
            ],
        }
    }

    /// 100 Gbps configuration (~12.5 GB/s)
    fn config_100g() -> HighSpeedConfig {
        HighSpeedConfig {
            tier: NetworkSpeedTier::Gbps100,
            threads: 32,
            buffer_size_mb: 64,
            tcp_buffer_size_mb: 512,
            mtu: 9000,
            streams: 16,
            congestion_control: "bbr".to_string(),
            ring_buffer_size: 16384,
            irq_affinity: true,
            numa_aware: true,
            kernel_bypass: false,
            use_io_uring: true,
            use_quic: true,
            use_rdma: false,
            nics_required: 1,
            storage_requirements: vec![
                "Multiple NVMe SSDs in RAID-0 or parallel access".to_string(),
                "Parallel filesystem (Lustre, GPFS, BeeGFS) recommended".to_string(),
                "Minimum 16 GB/s storage throughput required".to_string(),
            ],
            kernel_params: vec![
                ("net.core.rmem_max", "536870912"),
                ("net.core.wmem_max", "536870912"),
                ("net.ipv4.tcp_rmem", "4096 87380 536870912"),
                ("net.ipv4.tcp_wmem", "4096 65536 536870912"),
                ("net.core.netdev_max_backlog", "250000"),
                ("net.core.netdev_budget", "600"),
                ("net.ipv4.tcp_congestion_control", "bbr"),
                ("net.ipv4.tcp_mtu_probing", "1"),
                ("net.ipv4.tcp_timestamps", "1"),
                ("net.ipv4.tcp_sack", "1"),
                ("net.core.somaxconn", "65535"),
            ].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            notes: vec![
                "NUMA pinning is critical - pin threads to same NUMA node as NIC".to_string(),
                "Configure IRQ affinity to distribute network interrupts".to_string(),
                "Use multiple parallel QUIC connections for best throughput".to_string(),
                "io_uring with batched I/O is essential".to_string(),
                "Consider CPU isolation for network threads".to_string(),
                "Storage must be on same NUMA node as NIC for best performance".to_string(),
            ],
        }
    }

    /// 200 Gbps configuration (~25 GB/s)
    fn config_200g() -> HighSpeedConfig {
        HighSpeedConfig {
            tier: NetworkSpeedTier::Gbps200,
            threads: 64,
            buffer_size_mb: 128,
            tcp_buffer_size_mb: 1024,
            mtu: 9000,
            streams: 32,
            congestion_control: "bbr".to_string(),
            ring_buffer_size: 32768,
            irq_affinity: true,
            numa_aware: true,
            kernel_bypass: true,
            use_io_uring: true,
            use_quic: true,
            use_rdma: true,
            nics_required: 2,
            storage_requirements: vec![
                "Enterprise NVMe array or parallel filesystem".to_string(),
                "Lustre/GPFS with multiple OSTs recommended".to_string(),
                "Minimum 30 GB/s storage throughput required".to_string(),
                "Consider NVMe-oF for distributed storage".to_string(),
            ],
            kernel_params: vec![
                ("net.core.rmem_max", "1073741824"),
                ("net.core.wmem_max", "1073741824"),
                ("net.ipv4.tcp_rmem", "4096 87380 1073741824"),
                ("net.ipv4.tcp_wmem", "4096 65536 1073741824"),
                ("net.core.netdev_max_backlog", "500000"),
                ("net.core.netdev_budget", "1200"),
                ("net.core.optmem_max", "134217728"),
                ("net.ipv4.tcp_congestion_control", "bbr"),
                ("net.ipv4.tcp_low_latency", "1"),
                ("net.ipv4.tcp_fastopen", "3"),
            ].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            notes: vec![
                "Multiple NICs required - use bonding or separate streams per NIC".to_string(),
                "Consider RDMA (RoCE v2) for lowest latency".to_string(),
                "Kernel bypass (DPDK/XDP) may be needed for full line rate".to_string(),
                "Use CPU isolation and real-time scheduling for network threads".to_string(),
                "NUMA topology must match: NIC -> PCIe -> CPU -> Memory -> Storage".to_string(),
                "Consider dedicated network CPU cores".to_string(),
            ],
        }
    }

    /// 400 Gbps configuration (~50 GB/s)
    fn config_400g() -> HighSpeedConfig {
        HighSpeedConfig {
            tier: NetworkSpeedTier::Gbps400,
            threads: 128,
            buffer_size_mb: 256,
            tcp_buffer_size_mb: 2048,
            mtu: 9000,
            streams: 64,
            congestion_control: "bbr".to_string(),
            ring_buffer_size: 65536,
            irq_affinity: true,
            numa_aware: true,
            kernel_bypass: true,
            use_io_uring: true,
            use_quic: true,
            use_rdma: true,
            nics_required: 4,
            storage_requirements: vec![
                "High-performance parallel filesystem (Lustre, GPFS, Spectrum Scale)".to_string(),
                "Multiple storage servers with aggregate 60+ GB/s throughput".to_string(),
                "NVMe-oF or direct-attached NVMe arrays".to_string(),
                "Consider all-flash storage with dedicated fabric".to_string(),
            ],
            kernel_params: vec![
                ("net.core.rmem_max", "2147483647"),
                ("net.core.wmem_max", "2147483647"),
                ("net.ipv4.tcp_rmem", "4096 87380 2147483647"),
                ("net.ipv4.tcp_wmem", "4096 65536 2147483647"),
                ("net.core.netdev_max_backlog", "1000000"),
                ("net.core.netdev_budget", "2400"),
                ("net.core.optmem_max", "268435456"),
                ("net.ipv4.tcp_congestion_control", "bbr"),
                ("net.ipv4.tcp_low_latency", "1"),
                ("net.ipv4.tcp_fastopen", "3"),
                ("net.ipv4.tcp_window_scaling", "1"),
            ].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            notes: vec![
                "Requires specialized 400G NICs (Mellanox ConnectX-7 or similar)".to_string(),
                "RDMA (InfiniBand or RoCE v2) strongly recommended".to_string(),
                "Multiple NICs across multiple NUMA nodes required".to_string(),
                "Custom network stack (DPDK, XDP, io_uring) essential".to_string(),
                "Use SR-IOV for virtual function isolation".to_string(),
                "Consider dedicated FPGA-based NICs for protocol offload".to_string(),
                "End-to-end fabric design is critical".to_string(),
                "Real-world throughput typically 70-80% of line rate".to_string(),
            ],
        }
    }

    /// Print recommendations for a specific speed tier
    pub fn print_recommendations(tier: NetworkSpeedTier) {
        let config = Self::get_recommendations(tier);
        config.print();
    }

    /// Print all speed tier recommendations
    pub fn print_all_recommendations() {
        for tier in [
            NetworkSpeedTier::Gbps10,
            NetworkSpeedTier::Gbps100,
            NetworkSpeedTier::Gbps200,
            NetworkSpeedTier::Gbps400,
        ] {
            Self::print_recommendations(tier);
            println!();
        }
    }
}

/// Configuration for high-speed network transfers
#[derive(Debug, Clone)]
pub struct HighSpeedConfig {
    /// Network speed tier
    pub tier: NetworkSpeedTier,
    /// Recommended thread count
    pub threads: usize,
    /// Buffer size in MB
    pub buffer_size_mb: usize,
    /// TCP buffer size in MB
    pub tcp_buffer_size_mb: usize,
    /// MTU size
    pub mtu: usize,
    /// Number of parallel streams
    pub streams: usize,
    /// Congestion control algorithm
    pub congestion_control: String,
    /// Ring buffer size
    pub ring_buffer_size: usize,
    /// IRQ affinity required
    pub irq_affinity: bool,
    /// NUMA awareness required
    pub numa_aware: bool,
    /// Kernel bypass required
    pub kernel_bypass: bool,
    /// Use io_uring
    pub use_io_uring: bool,
    /// Use QUIC transport
    pub use_quic: bool,
    /// Use RDMA
    pub use_rdma: bool,
    /// Number of NICs required
    pub nics_required: usize,
    /// Storage requirements
    pub storage_requirements: Vec<String>,
    /// Kernel parameters
    pub kernel_params: Vec<(String, String)>,
    /// Additional notes
    pub notes: Vec<String>,
}

impl HighSpeedConfig {
    /// Print the configuration as a guide
    pub fn print(&self) {
        println!("╔══════════════════════════════════════════════════════════════════════════╗");
        println!("║  HIGH-SPEED TRANSFER GUIDE: {} Gbps (~{:.1} GB/s)                      ║",
                 self.tier.speed_gbps(), self.tier.throughput_gbps());
        println!("╚══════════════════════════════════════════════════════════════════════════╝");
        println!();

        // SmartCopy command
        println!("## SmartCopy Command ##");
        println!();
        println!("  smartcopy /source /dest \\");
        println!("    --threads {} \\", self.threads);
        println!("    --buffer-size {}M \\", self.buffer_size_mb);
        if self.use_quic {
            println!("    --quic \\");
        }
        println!("    --streams {} \\", self.streams);
        println!("    --progress");
        println!();

        // System requirements
        println!("## System Requirements ##");
        println!();
        println!("  NICs Required:        {}", self.nics_required);
        println!("  MTU:                  {}", self.mtu);
        println!("  Threads:              {}", self.threads);
        println!("  Parallel Streams:     {}", self.streams);
        println!("  Buffer Size:          {} MB", self.buffer_size_mb);
        println!("  TCP Buffer:           {} MB", self.tcp_buffer_size_mb);
        println!("  Ring Buffer:          {}", self.ring_buffer_size);
        println!();

        // Features
        println!("## Required Features ##");
        println!();
        println!("  NUMA Awareness:       {}", if self.numa_aware { "Required" } else { "Recommended" });
        println!("  IRQ Affinity:         {}", if self.irq_affinity { "Required" } else { "Optional" });
        println!("  Kernel Bypass:        {}", if self.kernel_bypass { "Required" } else { "Optional" });
        println!("  io_uring:             {}", if self.use_io_uring { "Recommended" } else { "Optional" });
        println!("  QUIC Transport:       {}", if self.use_quic { "Recommended" } else { "Optional" });
        println!("  RDMA (RoCE/IB):       {}", if self.use_rdma { "Recommended" } else { "Optional" });
        println!();

        // Storage requirements
        println!("## Storage Requirements ##");
        println!();
        for req in &self.storage_requirements {
            println!("  • {}", req);
        }
        println!();

        // Kernel parameters
        println!("## Kernel Tuning (sysctl) ##");
        println!();
        for (param, value) in &self.kernel_params {
            println!("  sysctl -w {}={}", param, value);
        }
        println!();

        // Persistent configuration
        println!("## Persistent Configuration (/etc/sysctl.d/99-smartcopy.conf) ##");
        println!();
        println!("  cat << 'EOF' | sudo tee /etc/sysctl.d/99-smartcopy.conf");
        for (param, value) in &self.kernel_params {
            println!("  {}={}", param, value);
        }
        println!("  EOF");
        println!("  sudo sysctl --system");
        println!();

        // Network interface tuning
        println!("## Network Interface Tuning ##");
        println!();
        println!("  # Set MTU (on all interfaces in path)");
        println!("  ip link set eth0 mtu {}", self.mtu);
        println!();
        println!("  # Increase ring buffer size");
        println!("  ethtool -G eth0 rx {} tx {}", self.ring_buffer_size, self.ring_buffer_size);
        println!();
        if self.irq_affinity {
            println!("  # Set IRQ affinity (distribute across cores)");
            println!("  # Find IRQs: grep eth0 /proc/interrupts");
            println!("  # Set affinity: echo <cpu_mask> > /proc/irq/<irq>/smp_affinity");
            println!();
        }

        // Notes
        println!("## Performance Notes ##");
        println!();
        for note in &self.notes {
            println!("  → {}", note);
        }
        println!();

        // Verification commands
        println!("## Verification Commands ##");
        println!();
        println!("  # Check current MTU");
        println!("  ip link show eth0 | grep mtu");
        println!();
        println!("  # Check TCP buffer sizes");
        println!("  sysctl net.core.rmem_max net.core.wmem_max");
        println!();
        println!("  # Check congestion control");
        println!("  sysctl net.ipv4.tcp_congestion_control");
        println!();
        println!("  # Check ring buffer size");
        println!("  ethtool -g eth0");
        println!();
        println!("  # Test with iperf3");
        println!("  iperf3 -c <server> -P {} -t 60", self.streams);
        println!();
    }

    /// Get the SmartCopy command for this configuration
    pub fn smartcopy_command(&self, source: &str, dest: &str) -> String {
        let mut cmd = format!(
            "smartcopy {} {} --threads {} --buffer-size {}M --streams {}",
            source, dest, self.threads, self.buffer_size_mb, self.streams
        );
        if self.use_quic {
            cmd.push_str(" --quic");
        }
        cmd.push_str(" --progress");
        cmd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuning_analyzer() {
        let system_info = SystemInfo::collect();
        let analyzer = TuningAnalyzer::new(system_info, WorkloadType::Mixed);
        let recommendations = analyzer.analyze();

        // Should always have some application-level recommendations
        assert!(!recommendations.is_empty());
        assert!(recommendations.iter().any(|r| r.category == TuningCategory::Application));
    }

    #[test]
    fn test_recommendation_sorting() {
        let system_info = SystemInfo::collect();
        let analyzer = TuningAnalyzer::new(system_info, WorkloadType::Network);
        let recommendations = analyzer.analyze();

        // Check that recommendations are sorted by severity
        for window in recommendations.windows(2) {
            assert!(window[0].severity as u8 <= window[1].severity as u8);
        }
    }
}
