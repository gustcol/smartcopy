//! Configuration settings for SmartCopy
//!
//! Defines all configuration options, CLI arguments, and defaults
//! for the copy operation.

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// SmartCopy - High-performance file copy utility for HPC environments
#[derive(Parser, Debug, Clone)]
#[command(name = "smartcopy")]
#[command(author = "SmartCopy Team")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Blazingly fast, intelligent file copy for HPC")]
#[command(long_about = r#"
SmartCopy is a high-performance file copy utility designed for HPC environments.

Features:
  - Multi-threaded parallel copying
  - Intelligent resource detection and auto-tuning
  - Smart file ordering (smallest first)
  - Integrity verification (XXHash3, BLAKE3, SHA-256)
  - Incremental/delta synchronization
  - SSH/SFTP remote transfers
  - LZ4 compression for network transfers
  - System tuning recommendations

Examples:
  smartcopy /source /destination              # Basic copy
  smartcopy /src /dst --threads 16 --verify   # Parallel with verification
  smartcopy /local user@server:/remote --ssh  # Remote copy
  smartcopy --analyze-system                  # System analysis
"#)]
pub struct CliArgs {
    /// Source path (local or remote: user@host:/path)
    #[arg(value_name = "SOURCE")]
    pub source: Option<String>,

    /// Destination path (local or remote: user@host:/path)
    #[arg(value_name = "DESTINATION")]
    pub destination: Option<String>,

    /// Number of parallel threads (0 = auto-detect)
    #[arg(short = 't', long, default_value = "0", value_name = "NUM")]
    pub threads: usize,

    /// Buffer size for file operations (e.g., 1M, 64K)
    #[arg(short = 'b', long, default_value = "1M", value_name = "SIZE")]
    pub buffer_size: String,

    /// Hash algorithm for verification
    #[arg(long, value_enum, value_name = "ALGO")]
    pub verify: Option<HashAlgorithm>,

    /// Enable incremental/delta sync mode
    #[arg(short = 'i', long)]
    pub incremental: bool,

    /// Use delta transfer for large files (rsync-like)
    #[arg(long)]
    pub delta: bool,

    /// Minimum file size for delta transfer (e.g., 10M)
    #[arg(long, default_value = "10M", value_name = "SIZE")]
    pub delta_threshold: String,

    /// Enable LZ4 compression for transfers
    #[arg(short = 'c', long)]
    pub compress: bool,

    /// Compression level (1-12, higher = better ratio)
    #[arg(long, default_value = "1", value_name = "LEVEL")]
    pub compress_level: u32,

    /// Use SSH/SFTP for remote transfers
    #[arg(long)]
    pub ssh: bool,

    /// SSH port (default: 22)
    #[arg(long, default_value = "22", value_name = "PORT")]
    pub ssh_port: u16,

    /// SSH private key path
    #[arg(long, value_name = "PATH")]
    pub ssh_key: Option<PathBuf>,

    /// Number of parallel SSH streams
    #[arg(long, default_value = "4", value_name = "NUM")]
    pub ssh_streams: usize,

    /// Enable direct TCP mode for LAN transfers
    #[arg(long)]
    pub tcp_direct: bool,

    /// TCP port for direct mode
    #[arg(long, default_value = "9876", value_name = "PORT")]
    pub tcp_port: u16,

    // === QUIC Transport Options ===
    /// Enable QUIC transport (HTTP/3-like, requires remote agent)
    #[arg(long)]
    pub quic: bool,

    /// QUIC port for transfers
    #[arg(long, default_value = "9877", value_name = "PORT")]
    pub quic_port: u16,

    // === SSH Agent Options ===
    /// Enable SSH agent mode (spawn remote smartcopy for delta sync)
    #[arg(long)]
    pub agent: bool,

    // === SSH Tuning Options ===
    /// Enable SSH ControlMaster for connection multiplexing
    #[arg(long)]
    pub ssh_control_master: bool,

    /// SSH ControlPersist timeout in seconds
    #[arg(long, default_value = "600", value_name = "SECS")]
    pub ssh_control_persist: u32,

    /// SSH cipher (chacha20-poly1305, aes128-gcm, aes256-gcm, aes128-ctr, aes256-ctr)
    #[arg(long, value_enum, default_value = "chacha20-poly1305")]
    pub ssh_cipher: SshCipher,

    /// Enable SSH compression (usually slower for fast networks)
    #[arg(long)]
    pub ssh_compress: bool,

    /// Show detailed progress information
    #[arg(short = 'p', long)]
    pub progress: bool,

    /// Verbose output (can be repeated: -v, -vv, -vvv)
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Quiet mode (suppress non-error output)
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Dry run (show what would be copied)
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Delete extra files in destination (sync mode)
    #[arg(long)]
    pub delete_extra: bool,

    /// Preserve file attributes (permissions, timestamps)
    #[arg(long, default_value = "true")]
    pub preserve: bool,

    /// Follow symbolic links
    #[arg(short = 'L', long)]
    pub follow_symlinks: bool,

    /// Include hidden files
    #[arg(long)]
    pub include_hidden: bool,

    /// File pattern to include (glob)
    #[arg(long, value_name = "PATTERN")]
    pub include: Vec<String>,

    /// File pattern to exclude (glob)
    #[arg(long, value_name = "PATTERN")]
    pub exclude: Vec<String>,

    /// Maximum file size to copy (e.g., 1G)
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Minimum file size to copy (e.g., 1K)
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    /// Bandwidth limit (e.g., 100M for 100 MB/s)
    #[arg(long, value_name = "RATE")]
    pub bandwidth_limit: Option<String>,

    /// Retry failed operations N times
    #[arg(long, default_value = "3", value_name = "NUM")]
    pub retries: usize,

    /// Retry delay in seconds
    #[arg(long, default_value = "1", value_name = "SECS")]
    pub retry_delay: u64,

    /// Continue on errors (don't abort)
    #[arg(long)]
    pub continue_on_error: bool,

    /// Path to manifest file for tracking
    #[arg(long, value_name = "PATH")]
    pub manifest: Option<PathBuf>,

    /// Output format for reports
    #[arg(long, value_enum, default_value = "text")]
    pub output_format: OutputFormat,

    /// Log file path
    #[arg(long, value_name = "PATH")]
    pub log_file: Option<PathBuf>,

    /// Subcommands
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Available subcommands
#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Analyze system resources and capabilities
    #[command(name = "analyze")]
    AnalyzeSystem {
        /// Output detailed analysis
        #[arg(short, long)]
        detailed: bool,
    },

    /// Show tuning recommendations
    #[command(name = "tuning")]
    Tuning {
        /// Target workload type
        #[arg(short, long, value_enum, default_value = "mixed")]
        workload: WorkloadType,
    },

    /// Verify integrity of a previous copy
    #[command(name = "verify")]
    Verify {
        /// Source path
        source: String,
        /// Destination path
        destination: String,
        /// Hash algorithm
        #[arg(long, value_enum, default_value = "xxhash3")]
        algorithm: HashAlgorithm,
    },

    /// Show manifest/sync status
    #[command(name = "status")]
    Status {
        /// Manifest file path
        manifest: PathBuf,
    },

    /// Run as TCP server for direct transfers
    #[command(name = "server")]
    Server {
        /// Listen port
        #[arg(short, long, default_value = "9876")]
        port: u16,
        /// Bind address
        #[arg(short, long, default_value = "0.0.0.0")]
        bind: String,
    },

    /// Run benchmarks
    #[command(name = "benchmark")]
    Benchmark {
        /// Path for benchmark files
        path: PathBuf,
        /// Test file size
        #[arg(long, default_value = "1G")]
        size: String,
    },

    /// Run as remote agent (for delta sync over SSH)
    #[command(name = "agent")]
    Agent {
        /// Protocol to use (stdio for SSH pipe, tcp for standalone)
        #[arg(long, value_enum, default_value = "stdio")]
        protocol: AgentProtocol,
        /// TCP port when using tcp protocol
        #[arg(long, default_value = "9878")]
        port: u16,
        /// Bind address for TCP
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },

    /// Run QUIC server for high-speed remote transfers
    #[command(name = "quic-server")]
    QuicServer {
        /// Listen port
        #[arg(short, long, default_value = "9877")]
        port: u16,
        /// Bind address
        #[arg(short, long, default_value = "0.0.0.0")]
        bind: String,
        /// Path to TLS certificate (auto-generated if not provided)
        #[arg(long)]
        cert: Option<PathBuf>,
        /// Path to TLS private key
        #[arg(long)]
        key: Option<PathBuf>,
    },

    /// Show high-speed network tuning guide (10G/100G/200G/400G)
    #[command(name = "highspeed")]
    HighSpeed {
        /// Network speed tier
        #[arg(value_enum, default_value = "10g")]
        speed: HighSpeedTier,
    },
}

/// High-speed network tier for CLI
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum HighSpeedTier {
    /// 10 Gigabit Ethernet (~1.25 GB/s)
    #[value(name = "10g")]
    Gbps10,
    /// 100 Gigabit Ethernet (~12.5 GB/s)
    #[value(name = "100g")]
    Gbps100,
    /// 200 Gigabit Ethernet (~25 GB/s)
    #[value(name = "200g")]
    Gbps200,
    /// 400 Gigabit Ethernet (~50 GB/s)
    #[value(name = "400g")]
    Gbps400,
}

/// Agent protocol type
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AgentProtocol {
    /// Stdio (for SSH pipe - default)
    #[default]
    Stdio,
    /// TCP socket
    Tcp,
}

/// Hash algorithm for integrity verification
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    /// XXHash3 - Ultra fast, non-cryptographic (128-bit)
    #[default]
    #[value(name = "xxhash3")]
    XXHash3,
    /// XXHash64 - Fast, non-cryptographic (64-bit)
    #[value(name = "xxhash64")]
    XXHash64,
    /// BLAKE3 - Fast and cryptographically secure
    #[value(name = "blake3")]
    Blake3,
    /// SHA-256 - Standard cryptographic hash
    #[value(name = "sha256")]
    Sha256,
}

impl HashAlgorithm {
    /// Get the output size in bytes
    pub fn output_size(&self) -> usize {
        match self {
            Self::XXHash3 => 16,
            Self::XXHash64 => 8,
            Self::Blake3 => 32,
            Self::Sha256 => 32,
        }
    }

    /// Get human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            Self::XXHash3 => "XXHash3",
            Self::XXHash64 => "XXHash64",
            Self::Blake3 => "BLAKE3",
            Self::Sha256 => "SHA-256",
        }
    }
}

/// Output format for reports
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Human-readable text
    #[default]
    Text,
    /// JSON format
    Json,
    /// CSV format
    Csv,
}

/// Workload type for tuning recommendations
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkloadType {
    /// Many small files
    SmallFiles,
    /// Few large files
    LargeFiles,
    /// Mixed workload
    #[default]
    Mixed,
    /// Network-heavy transfers
    Network,
}

/// File ordering strategy
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OrderingStrategy {
    /// Smallest files first (quick wins)
    #[default]
    SmallestFirst,
    /// Largest files first
    LargestFirst,
    /// Newest files first
    NewestFirst,
    /// Oldest files first
    OldestFirst,
    /// No specific ordering
    None,
}

/// Runtime configuration derived from CLI args
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyConfig {
    /// Source path
    pub source: PathBuf,
    /// Destination path
    pub destination: PathBuf,
    /// Remote host info if applicable
    pub remote: Option<RemoteConfig>,
    /// Thread count
    pub threads: usize,
    /// Buffer size in bytes
    pub buffer_size: usize,
    /// Hash algorithm for verification
    pub verify: Option<HashAlgorithm>,
    /// Enable incremental sync
    pub incremental: bool,
    /// Enable delta transfer
    pub delta: bool,
    /// Delta threshold in bytes
    pub delta_threshold: u64,
    /// Enable compression
    pub compress: bool,
    /// Compression level
    pub compress_level: u32,
    /// Preserve attributes
    pub preserve: bool,
    /// Follow symlinks
    pub follow_symlinks: bool,
    /// Include hidden files
    pub include_hidden: bool,
    /// Include patterns
    pub include_patterns: Vec<String>,
    /// Exclude patterns
    pub exclude_patterns: Vec<String>,
    /// Max file size
    pub max_size: Option<u64>,
    /// Min file size
    pub min_size: Option<u64>,
    /// Bandwidth limit in bytes/sec
    pub bandwidth_limit: Option<u64>,
    /// Retry count
    pub retries: usize,
    /// Retry delay in seconds
    pub retry_delay: u64,
    /// Continue on error
    pub continue_on_error: bool,
    /// Dry run mode
    pub dry_run: bool,
    /// Delete extra files
    pub delete_extra: bool,
    /// Manifest path
    pub manifest_path: Option<PathBuf>,
    /// File ordering strategy
    pub ordering: OrderingStrategy,
}

/// Remote host configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Remote hostname or IP
    pub host: String,
    /// Username
    pub user: String,
    /// Port
    pub port: u16,
    /// SSH key path
    pub key_path: Option<PathBuf>,
    /// Number of parallel streams
    pub streams: usize,
    /// Use TCP direct mode
    pub tcp_direct: bool,
    /// TCP port for direct mode
    pub tcp_port: u16,
    /// Use QUIC transport
    pub quic: bool,
    /// QUIC port
    pub quic_port: u16,
    /// Use SSH agent mode (spawn remote agent for delta sync)
    pub use_agent: bool,
    /// SSH tuning configuration
    pub ssh_tuning: Option<SshTuningConfig>,
}

/// SSH tuning configuration for optimal performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTuningConfig {
    /// Enable ControlMaster for connection multiplexing
    pub control_master: bool,
    /// Path for ControlMaster socket
    pub control_path: Option<PathBuf>,
    /// ControlPersist timeout in seconds (0 = disabled)
    pub control_persist: u32,
    /// Preferred cipher (fast ciphers: chacha20-poly1305, aes128-gcm)
    pub cipher: SshCipher,
    /// Enable compression (usually off for fast networks)
    pub compression: bool,
    /// TCP keep-alive interval in seconds
    pub keep_alive_interval: u32,
    /// Server alive count max
    pub server_alive_count_max: u32,
    /// Disable strict host key checking (use with caution)
    pub strict_host_key_checking: bool,
    /// Batch mode (disable password prompts)
    pub batch_mode: bool,
    /// Custom SSH options
    pub custom_options: Vec<(String, String)>,
}

impl Default for SshTuningConfig {
    fn default() -> Self {
        Self {
            control_master: true,
            control_path: None, // Will use ~/.ssh/smartcopy-%r@%h:%p
            control_persist: 600, // 10 minutes
            cipher: SshCipher::ChaCha20Poly1305,
            compression: false, // Usually slower for fast networks
            keep_alive_interval: 30,
            server_alive_count_max: 3,
            strict_host_key_checking: true,
            batch_mode: true,
            custom_options: Vec::new(),
        }
    }
}

/// SSH cipher options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum SshCipher {
    /// ChaCha20-Poly1305 (fastest on CPUs without AES-NI)
    #[default]
    #[value(name = "chacha20-poly1305")]
    ChaCha20Poly1305,
    /// AES-128-GCM (fastest on CPUs with AES-NI)
    #[value(name = "aes128-gcm")]
    Aes128Gcm,
    /// AES-256-GCM
    #[value(name = "aes256-gcm")]
    Aes256Gcm,
    /// AES-128-CTR (legacy, widely compatible)
    #[value(name = "aes128-ctr")]
    Aes128Ctr,
    /// AES-256-CTR (legacy, widely compatible)
    #[value(name = "aes256-ctr")]
    Aes256Ctr,
}

impl SshCipher {
    /// Get the OpenSSH cipher name
    pub fn as_ssh_string(&self) -> &'static str {
        match self {
            Self::ChaCha20Poly1305 => "chacha20-poly1305@openssh.com",
            Self::Aes128Gcm => "aes128-gcm@openssh.com",
            Self::Aes256Gcm => "aes256-gcm@openssh.com",
            Self::Aes128Ctr => "aes128-ctr",
            Self::Aes256Ctr => "aes256-ctr",
        }
    }

    /// Get the libssh2 method name
    pub fn as_libssh2_method(&self) -> &'static str {
        match self {
            // libssh2 uses different naming
            Self::ChaCha20Poly1305 => "chacha20-poly1305@openssh.com",
            Self::Aes128Gcm => "aes128-gcm@openssh.com",
            Self::Aes256Gcm => "aes256-gcm@openssh.com",
            Self::Aes128Ctr => "aes128-ctr",
            Self::Aes256Ctr => "aes256-ctr",
        }
    }
}

impl Default for CopyConfig {
    fn default() -> Self {
        Self {
            source: PathBuf::new(),
            destination: PathBuf::new(),
            remote: None,
            threads: 0, // Auto-detect
            buffer_size: 1024 * 1024, // 1MB
            verify: None,
            incremental: false,
            delta: false,
            delta_threshold: 10 * 1024 * 1024, // 10MB
            compress: false,
            compress_level: 1,
            preserve: true,
            follow_symlinks: false,
            include_hidden: false,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            max_size: None,
            min_size: None,
            bandwidth_limit: None,
            retries: 3,
            retry_delay: 1,
            continue_on_error: false,
            dry_run: false,
            delete_extra: false,
            manifest_path: None,
            ordering: OrderingStrategy::SmallestFirst,
        }
    }
}

/// Parse human-readable size string to bytes
pub fn parse_size(size: &str) -> Result<u64, String> {
    let size = size.trim().to_uppercase();

    if size.is_empty() {
        return Err("Empty size string".to_string());
    }

    let (num_str, multiplier) = if size.ends_with("TB") || size.ends_with("T") {
        let num = size.trim_end_matches(|c| c == 'T' || c == 'B');
        (num, 1024u64 * 1024 * 1024 * 1024)
    } else if size.ends_with("GB") || size.ends_with("G") {
        let num = size.trim_end_matches(|c| c == 'G' || c == 'B');
        (num, 1024u64 * 1024 * 1024)
    } else if size.ends_with("MB") || size.ends_with("M") {
        let num = size.trim_end_matches(|c| c == 'M' || c == 'B');
        (num, 1024u64 * 1024)
    } else if size.ends_with("KB") || size.ends_with("K") {
        let num = size.trim_end_matches(|c| c == 'K' || c == 'B');
        (num, 1024u64)
    } else if size.ends_with('B') {
        let num = size.trim_end_matches('B');
        (num, 1u64)
    } else {
        // Assume bytes if no suffix
        (size.as_str(), 1u64)
    };

    let num: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| format!("Invalid number: {}", num_str))?;

    Ok((num * multiplier as f64) as u64)
}

/// Parse remote path (user@host:/path)
pub fn parse_remote_path(path: &str) -> Option<(String, String, PathBuf)> {
    // Pattern: user@host:/path or user@host:path
    if let Some((user_host, remote_path)) = path.split_once(':') {
        if let Some((user, host)) = user_host.split_once('@') {
            return Some((
                user.to_string(),
                host.to_string(),
                PathBuf::from(remote_path),
            ));
        }
    }
    None
}

impl CopyConfig {
    /// Create config from CLI arguments
    pub fn from_cli(args: &CliArgs) -> Result<Self, String> {
        let source = args.source.as_ref().ok_or("Source path required")?;
        let destination = args.destination.as_ref().ok_or("Destination path required")?;

        let mut config = Self::default();

        // Build SSH tuning config if any SSH tuning options are set
        let ssh_tuning = if args.ssh_control_master || args.ssh_compress {
            Some(SshTuningConfig {
                control_master: args.ssh_control_master,
                control_persist: args.ssh_control_persist,
                cipher: args.ssh_cipher,
                compression: args.ssh_compress,
                ..Default::default()
            })
        } else {
            None
        };

        // Parse source
        if let Some((user, host, path)) = parse_remote_path(source) {
            config.source = path;
            config.remote = Some(RemoteConfig {
                user,
                host,
                port: args.ssh_port,
                key_path: args.ssh_key.clone(),
                streams: args.ssh_streams,
                tcp_direct: args.tcp_direct,
                tcp_port: args.tcp_port,
                quic: args.quic,
                quic_port: args.quic_port,
                use_agent: args.agent,
                ssh_tuning: ssh_tuning.clone(),
            });
        } else {
            config.source = PathBuf::from(source);
        }

        // Parse destination
        if let Some((user, host, path)) = parse_remote_path(destination) {
            config.destination = path;
            if config.remote.is_none() {
                config.remote = Some(RemoteConfig {
                    user,
                    host,
                    port: args.ssh_port,
                    key_path: args.ssh_key.clone(),
                    streams: args.ssh_streams,
                    tcp_direct: args.tcp_direct,
                    tcp_port: args.tcp_port,
                    quic: args.quic,
                    quic_port: args.quic_port,
                    use_agent: args.agent,
                    ssh_tuning,
                });
            }
        } else {
            config.destination = PathBuf::from(destination);
        }

        config.threads = args.threads;
        config.buffer_size = parse_size(&args.buffer_size).map_err(|e| format!("Invalid buffer size: {}", e))? as usize;
        config.verify = args.verify;
        config.incremental = args.incremental;
        config.delta = args.delta;
        config.delta_threshold = parse_size(&args.delta_threshold).map_err(|e| format!("Invalid delta threshold: {}", e))?;
        config.compress = args.compress;
        config.compress_level = args.compress_level;
        config.preserve = args.preserve;
        config.follow_symlinks = args.follow_symlinks;
        config.include_hidden = args.include_hidden;
        config.include_patterns = args.include.clone();
        config.exclude_patterns = args.exclude.clone();
        config.max_size = args.max_size.as_ref().map(|s| parse_size(s)).transpose().map_err(|e| format!("Invalid max size: {}", e))?;
        config.min_size = args.min_size.as_ref().map(|s| parse_size(s)).transpose().map_err(|e| format!("Invalid min size: {}", e))?;
        config.bandwidth_limit = args.bandwidth_limit.as_ref().map(|s| parse_size(s)).transpose().map_err(|e| format!("Invalid bandwidth limit: {}", e))?;
        config.retries = args.retries;
        config.retry_delay = args.retry_delay;
        config.continue_on_error = args.continue_on_error;
        config.dry_run = args.dry_run;
        config.delete_extra = args.delete_extra;
        config.manifest_path = args.manifest.clone();

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1.5G").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as u64);
    }

    #[test]
    fn test_parse_remote_path() {
        let result = parse_remote_path("user@host:/path/to/file");
        assert!(result.is_some());
        let (user, host, path) = result.unwrap();
        assert_eq!(user, "user");
        assert_eq!(host, "host");
        assert_eq!(path, PathBuf::from("/path/to/file"));

        assert!(parse_remote_path("/local/path").is_none());
    }

    #[test]
    fn test_hash_algorithm() {
        assert_eq!(HashAlgorithm::XXHash3.output_size(), 16);
        assert_eq!(HashAlgorithm::Blake3.output_size(), 32);
        assert_eq!(HashAlgorithm::XXHash3.name(), "XXHash3");
    }
}
