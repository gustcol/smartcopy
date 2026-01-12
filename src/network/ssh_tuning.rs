//! SSH Tuning and ControlMaster Support
//!
//! This module provides advanced SSH performance optimizations:
//! - ControlMaster for connection multiplexing
//! - Fast cipher selection
//! - Connection pooling with socket reuse
//! - Optimized SSH options for high-throughput transfers

use crate::config::{RemoteConfig, SshCipher, SshTuningConfig};
use crate::error::{Result, SmartCopyError};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// SSH ControlMaster manager for connection multiplexing
///
/// ControlMaster allows multiple SSH sessions to share a single network connection,
/// dramatically reducing connection overhead for parallel transfers.
pub struct ControlMasterManager {
    /// Socket path for the control connection
    socket_path: PathBuf,
    /// Remote configuration
    config: RemoteConfig,
    /// Whether the master connection is active
    active: Arc<AtomicBool>,
}

impl ControlMasterManager {
    /// Create a new ControlMaster manager
    ///
    /// # Arguments
    /// * `config` - Remote configuration with SSH tuning settings
    ///
    /// # Returns
    /// A new ControlMasterManager instance
    pub fn new(config: &RemoteConfig) -> Result<Self> {
        let socket_path = Self::get_socket_path(config)?;

        Ok(Self {
            socket_path,
            config: config.clone(),
            active: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get the socket path for ControlMaster
    fn get_socket_path(config: &RemoteConfig) -> Result<PathBuf> {
        if let Some(ref tuning) = config.ssh_tuning {
            if let Some(ref path) = tuning.control_path {
                return Ok(path.clone());
            }
        }

        // Default path: ~/.ssh/smartcopy-{user}@{host}:{port}
        let home = std::env::var("HOME")
            .map_err(|_| SmartCopyError::config("HOME environment variable not set"))?;

        let socket_name = format!(
            "smartcopy-{}@{}:{}",
            config.user, config.host, config.port
        );

        let ssh_dir = PathBuf::from(home).join(".ssh");

        // Ensure .ssh directory exists
        if !ssh_dir.exists() {
            std::fs::create_dir_all(&ssh_dir)
                .map_err(|e| SmartCopyError::io(&ssh_dir, e))?;
        }

        Ok(ssh_dir.join(socket_name))
    }

    /// Start the ControlMaster connection
    ///
    /// This creates a persistent SSH connection that other sessions can multiplex through.
    pub fn start(&self) -> Result<()> {
        if self.active.load(Ordering::SeqCst) {
            return Ok(()); // Already active
        }

        // Check if socket already exists (from a previous session)
        if self.socket_path.exists() {
            if self.check_connection()? {
                self.active.store(true, Ordering::SeqCst);
                return Ok(());
            }
            // Socket exists but connection is dead, remove it
            let _ = std::fs::remove_file(&self.socket_path);
        }

        let tuning = self.config.ssh_tuning.as_ref()
            .cloned()
            .unwrap_or_default();

        let mut cmd = Command::new("ssh");

        // ControlMaster options
        cmd.arg("-M") // Master mode
           .arg("-N") // No command
           .arg("-f") // Background
           .arg("-o").arg(format!("ControlPath={}", self.socket_path.display()))
           .arg("-o").arg(format!("ControlPersist={}", tuning.control_persist));

        // Cipher selection
        cmd.arg("-c").arg(tuning.cipher.as_ssh_string());

        // Compression
        if tuning.compression {
            cmd.arg("-C");
        }

        // Keep-alive settings
        cmd.arg("-o").arg(format!("ServerAliveInterval={}", tuning.keep_alive_interval))
           .arg("-o").arg(format!("ServerAliveCountMax={}", tuning.server_alive_count_max));

        // Batch mode
        if tuning.batch_mode {
            cmd.arg("-o").arg("BatchMode=yes");
        }

        // Host key checking
        if !tuning.strict_host_key_checking {
            cmd.arg("-o").arg("StrictHostKeyChecking=no")
               .arg("-o").arg("UserKnownHostsFile=/dev/null");
        }

        // Custom options
        for (key, value) in &tuning.custom_options {
            cmd.arg("-o").arg(format!("{}={}", key, value));
        }

        // Port
        cmd.arg("-p").arg(self.config.port.to_string());

        // Key path
        if let Some(ref key_path) = self.config.key_path {
            cmd.arg("-i").arg(key_path);
        }

        // User@Host
        cmd.arg(format!("{}@{}", self.config.user, self.config.host));

        // Execute
        let status = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .status()
            .map_err(|e| SmartCopyError::connection(&self.config.host, e.to_string()))?;

        if !status.success() {
            return Err(SmartCopyError::connection(
                &self.config.host,
                "Failed to start ControlMaster connection",
            ));
        }

        // Wait for socket to be created
        for _ in 0..50 {
            if self.socket_path.exists() {
                self.active.store(true, Ordering::SeqCst);
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Err(SmartCopyError::connection(
            &self.config.host,
            "ControlMaster socket not created in time",
        ))
    }

    /// Check if the ControlMaster connection is still alive
    pub fn check_connection(&self) -> Result<bool> {
        let output = Command::new("ssh")
            .arg("-O").arg("check")
            .arg("-o").arg(format!("ControlPath={}", self.socket_path.display()))
            .arg(format!("{}@{}", self.config.user, self.config.host))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| SmartCopyError::connection(&self.config.host, e.to_string()))?;

        Ok(output.status.success())
    }

    /// Stop the ControlMaster connection
    pub fn stop(&self) -> Result<()> {
        if !self.active.load(Ordering::SeqCst) {
            return Ok(());
        }

        let status = Command::new("ssh")
            .arg("-O").arg("exit")
            .arg("-o").arg(format!("ControlPath={}", self.socket_path.display()))
            .arg(format!("{}@{}", self.config.user, self.config.host))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| SmartCopyError::connection(&self.config.host, e.to_string()))?;

        self.active.store(false, Ordering::SeqCst);

        if !status.success() {
            // Socket might already be gone, try to clean up
            let _ = std::fs::remove_file(&self.socket_path);
        }

        Ok(())
    }

    /// Get SSH command arguments for using this ControlMaster
    pub fn get_ssh_args(&self) -> Vec<String> {
        let tuning = self.config.ssh_tuning.as_ref()
            .cloned()
            .unwrap_or_default();

        let mut args = Vec::new();

        // Use existing control socket
        args.push("-o".to_string());
        args.push(format!("ControlPath={}", self.socket_path.display()));
        args.push("-o".to_string());
        args.push("ControlMaster=auto".to_string());

        // Cipher
        args.push("-c".to_string());
        args.push(tuning.cipher.as_ssh_string().to_string());

        // Port
        args.push("-p".to_string());
        args.push(self.config.port.to_string());

        // Key path
        if let Some(ref key_path) = self.config.key_path {
            args.push("-i".to_string());
            args.push(key_path.to_string_lossy().to_string());
        }

        args
    }

    /// Get the socket path
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Check if the master connection is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
}

impl Drop for ControlMasterManager {
    fn drop(&mut self) {
        // Don't stop the connection on drop - let ControlPersist handle it
        // This allows the connection to persist across multiple smartcopy invocations
    }
}

/// High-performance SSH transfer using ControlMaster
pub struct OptimizedSshTransfer {
    /// ControlMaster manager
    control: ControlMasterManager,
    /// Remote configuration
    config: RemoteConfig,
}

impl OptimizedSshTransfer {
    /// Create a new optimized SSH transfer
    pub fn new(config: &RemoteConfig) -> Result<Self> {
        let control = ControlMasterManager::new(config)?;

        // Start ControlMaster if configured
        if let Some(ref tuning) = config.ssh_tuning {
            if tuning.control_master {
                control.start()?;
            }
        }

        Ok(Self {
            control,
            config: config.clone(),
        })
    }

    /// Execute a remote command via SSH
    pub fn exec(&self, command: &str) -> Result<String> {
        let mut cmd = Command::new("ssh");

        // Add ControlMaster args
        for arg in self.control.get_ssh_args() {
            cmd.arg(arg);
        }

        // User@Host
        cmd.arg(format!("{}@{}", self.config.user, self.config.host));

        // Command
        cmd.arg(command);

        let output = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| SmartCopyError::connection(&self.config.host, e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SmartCopyError::connection(&self.config.host, stderr.to_string()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Transfer a file using rsync over the ControlMaster connection
    ///
    /// This provides rsync-like delta transfer while using our optimized SSH connection
    pub fn rsync_transfer(
        &self,
        local_path: &Path,
        remote_path: &Path,
        upload: bool,
    ) -> Result<TransferStats> {
        let mut cmd = Command::new("rsync");

        // Rsync options for maximum performance
        cmd.arg("-avz")  // Archive, verbose, compress
           .arg("--progress")
           .arg("--partial")  // Keep partial transfers for resume
           .arg("--inplace"); // Update files in place

        // Build SSH command with our optimizations
        let ssh_args = self.control.get_ssh_args().join(" ");
        cmd.arg("-e").arg(format!("ssh {}", ssh_args));

        // Source and destination
        let remote = format!(
            "{}@{}:{}",
            self.config.user,
            self.config.host,
            remote_path.display()
        );

        if upload {
            cmd.arg(local_path);
            cmd.arg(&remote);
        } else {
            cmd.arg(&remote);
            cmd.arg(local_path);
        }

        let start = std::time::Instant::now();

        let output = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| SmartCopyError::connection(&self.config.host, e.to_string()))?;

        let duration = start.elapsed();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SmartCopyError::RemoteTransferError(stderr.to_string()));
        }

        // Parse rsync output for stats
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stats = Self::parse_rsync_output(&stdout, duration);

        Ok(stats)
    }

    /// Parse rsync output for transfer statistics
    fn parse_rsync_output(output: &str, duration: std::time::Duration) -> TransferStats {
        let mut bytes_transferred = 0u64;
        let mut files_transferred = 0u64;

        for line in output.lines() {
            // Try to parse "sent X bytes  received Y bytes"
            if line.contains("sent") && line.contains("bytes") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                for (i, part) in parts.iter().enumerate() {
                    if *part == "sent" && i + 1 < parts.len() {
                        if let Ok(n) = parts[i + 1].replace(",", "").parse::<u64>() {
                            bytes_transferred += n;
                        }
                    }
                    if *part == "received" && i + 1 < parts.len() {
                        if let Ok(n) = parts[i + 1].replace(",", "").parse::<u64>() {
                            bytes_transferred += n;
                        }
                    }
                }
            }
            // Count files (lines that don't start with special characters)
            if !line.starts_with("sending") &&
               !line.starts_with("sent") &&
               !line.starts_with("total") &&
               !line.is_empty() {
                files_transferred += 1;
            }
        }

        let throughput = if duration.as_secs_f64() > 0.0 {
            bytes_transferred as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        TransferStats {
            bytes_transferred,
            files_transferred,
            duration,
            throughput,
        }
    }

    /// Get the underlying ControlMaster manager
    pub fn control_master(&self) -> &ControlMasterManager {
        &self.control
    }
}

/// Transfer statistics
#[derive(Debug, Clone)]
pub struct TransferStats {
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Files transferred
    pub files_transferred: u64,
    /// Transfer duration
    pub duration: std::time::Duration,
    /// Throughput in bytes/second
    pub throughput: f64,
}

/// SSH performance tuning recommendations
pub struct SshTuningRecommendations;

impl SshTuningRecommendations {
    /// Get optimal SSH configuration based on network conditions
    ///
    /// # Arguments
    /// * `latency_ms` - Network latency in milliseconds
    /// * `bandwidth_mbps` - Available bandwidth in Mbps
    /// * `has_aes_ni` - Whether the CPU has AES-NI support
    ///
    /// # Returns
    /// Recommended SSH tuning configuration
    pub fn get_optimal_config(
        latency_ms: u32,
        bandwidth_mbps: u32,
        has_aes_ni: bool,
    ) -> SshTuningConfig {
        let mut config = SshTuningConfig::default();

        // Cipher selection based on CPU capabilities
        config.cipher = if has_aes_ni {
            SshCipher::Aes128Gcm // Fastest with hardware AES
        } else {
            SshCipher::ChaCha20Poly1305 // Fastest without hardware AES
        };

        // Compression: only beneficial for high-latency, low-bandwidth links
        config.compression = latency_ms > 50 && bandwidth_mbps < 10;

        // ControlMaster: almost always beneficial
        config.control_master = true;

        // Longer persistence for high-latency links
        config.control_persist = if latency_ms > 100 {
            1800 // 30 minutes
        } else if latency_ms > 50 {
            900 // 15 minutes
        } else {
            600 // 10 minutes
        };

        // Adjust keepalive based on latency
        config.keep_alive_interval = (latency_ms / 10).max(10).min(60) as u32;
        config.server_alive_count_max = 3;

        config
    }

    /// Check if the CPU supports AES-NI
    #[cfg(target_arch = "x86_64")]
    pub fn has_aes_ni() -> bool {
        #[cfg(target_feature = "aes")]
        {
            true
        }
        #[cfg(not(target_feature = "aes"))]
        {
            // Runtime check
            is_x86_feature_detected!("aes")
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn has_aes_ni() -> bool {
        false
    }

    /// Print tuning recommendations
    pub fn print_recommendations() {
        let has_aes = Self::has_aes_ni();

        println!("SSH Tuning Recommendations:");
        println!("===========================");
        println!();
        println!("CPU AES-NI Support: {}", if has_aes { "Yes" } else { "No" });
        println!();
        println!("Recommended cipher: {}",
            if has_aes { "aes128-gcm@openssh.com" } else { "chacha20-poly1305@openssh.com" }
        );
        println!();
        println!("For optimal performance, add to ~/.ssh/config:");
        println!();
        println!("Host *");
        println!("    ControlMaster auto");
        println!("    ControlPath ~/.ssh/smartcopy-%r@%h:%p");
        println!("    ControlPersist 600");
        println!("    Ciphers {}", if has_aes { "aes128-gcm@openssh.com" } else { "chacha20-poly1305@openssh.com" });
        println!("    Compression no");
        println!("    ServerAliveInterval 30");
        println!("    ServerAliveCountMax 3");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cipher_strings() {
        assert_eq!(
            SshCipher::ChaCha20Poly1305.as_ssh_string(),
            "chacha20-poly1305@openssh.com"
        );
        assert_eq!(
            SshCipher::Aes128Gcm.as_ssh_string(),
            "aes128-gcm@openssh.com"
        );
    }

    #[test]
    fn test_optimal_config_low_latency() {
        let config = SshTuningRecommendations::get_optimal_config(5, 1000, true);
        assert!(!config.compression); // High bandwidth, no compression
        assert_eq!(config.cipher, SshCipher::Aes128Gcm);
    }

    #[test]
    fn test_optimal_config_high_latency() {
        let config = SshTuningRecommendations::get_optimal_config(100, 5, false);
        assert!(config.compression); // Low bandwidth, compression helps
        assert_eq!(config.cipher, SshCipher::ChaCha20Poly1305);
    }

    #[test]
    fn test_ssh_tuning_default() {
        let config = SshTuningConfig::default();
        assert!(config.control_master);
        assert!(!config.compression);
        assert_eq!(config.control_persist, 600);
    }
}
