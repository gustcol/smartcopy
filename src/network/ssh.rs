//! SSH/SFTP remote transfer
//!
//! Provides secure file transfer over SSH using SFTP protocol
//! with support for parallel streams and compression.

use crate::config::RemoteConfig;
use crate::error::{Result, SmartCopyError};
use crate::fs::FileEntry;
use ssh2::{Session, Sftp};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

/// SSH connection for remote transfers
pub struct SshConnection {
    /// SSH session
    session: Session,
    /// SFTP channel
    sftp: Sftp,
    /// Remote configuration
    config: RemoteConfig,
}

impl SshConnection {
    /// Connect to remote host
    pub fn connect(config: &RemoteConfig) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);
        let sock_addr = addr.to_socket_addrs()
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?
            .next()
            .ok_or_else(|| SmartCopyError::connection(&config.host, "Could not resolve address"))?;

        // Bug 2: Use connect_timeout instead of connect (10s timeout)
        let tcp = TcpStream::connect_timeout(&sock_addr, Duration::from_secs(10))
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;

        // Bug 2: Set TCP read/write timeouts (30s)
        tcp.set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;
        tcp.set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;

        let mut session = Session::new()
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;

        session.set_tcp_stream(tcp);
        session.handshake()
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;

        // Bug 3: Set SSH keepalive (every 15 seconds)
        session.set_keepalive(true, 15);

        // Authenticate
        Self::authenticate(&mut session, config)?;

        // Open SFTP channel
        let sftp = session.sftp()
            .map_err(|e| SmartCopyError::connection(&config.host, e.to_string()))?;

        Ok(Self {
            session,
            sftp,
            config: config.clone(),
        })
    }

    /// Authenticate with the remote host
    fn authenticate(session: &mut Session, config: &RemoteConfig) -> Result<()> {
        // Try key-based auth first
        if let Some(key_path) = &config.key_path {
            session.userauth_pubkey_file(&config.user, None, key_path, None)
                .map_err(|e| SmartCopyError::auth(&config.user, &config.host, e.to_string()))?;
        } else {
            // Try SSH agent
            let mut agent = session.agent()
                .map_err(|e| SmartCopyError::auth(&config.user, &config.host, e.to_string()))?;

            agent.connect()
                .map_err(|e| SmartCopyError::auth(&config.user, &config.host, e.to_string()))?;

            agent.list_identities()
                .map_err(|e| SmartCopyError::auth(&config.user, &config.host, e.to_string()))?;

            let identities: Vec<_> = agent.identities().unwrap_or_default();

            let mut authenticated = false;
            for identity in identities {
                if agent.userauth(&config.user, &identity).is_ok() {
                    authenticated = true;
                    break;
                }
            }

            if !authenticated {
                return Err(SmartCopyError::auth(
                    &config.user,
                    &config.host,
                    "No valid SSH key found in agent",
                ));
            }
        }

        if !session.authenticated() {
            return Err(SmartCopyError::auth(
                &config.user,
                &config.host,
                "Authentication failed",
            ));
        }

        Ok(())
    }

    /// Upload a file to remote host with retry and exponential backoff
    pub fn upload(&self, local_path: &Path, remote_path: &Path) -> Result<u64> {
        let max_retries = 5u32;
        let base_delay = Duration::from_secs(2);
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = base_delay * 2u32.pow(attempt - 1);
                std::thread::sleep(delay);
            }

            match self.upload_inner(local_path, remote_path) {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    tracing::warn!(
                        "Upload attempt {}/{} failed for {:?}: {}",
                        attempt + 1, max_retries + 1, local_path, e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    /// Inner upload implementation
    fn upload_inner(&self, local_path: &Path, remote_path: &Path) -> Result<u64> {
        let local_file = std::fs::File::open(local_path)
            .map_err(|e| SmartCopyError::io(local_path, e))?;
        let _size = local_file.metadata()
            .map_err(|e| SmartCopyError::io(local_path, e))?
            .len();

        // Create parent directory on remote
        if let Some(parent) = remote_path.parent() {
            self.create_remote_dir_all(parent)?;
        }

        // Open remote file for writing
        let mut remote_file = self.sftp.create(remote_path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

        let mut reader = std::io::BufReader::with_capacity(1024 * 1024, local_file);
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut bytes_copied = 0u64;

        loop {
            let bytes_read = reader.read(&mut buffer)
                .map_err(|e| SmartCopyError::io(local_path, e))?;

            if bytes_read == 0 {
                break;
            }

            remote_file.write_all(&buffer[..bytes_read])
                .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

            bytes_copied += bytes_read as u64;
        }

        Ok(bytes_copied)
    }

    /// Download a file from remote host with retry and exponential backoff
    pub fn download(&self, remote_path: &Path, local_path: &Path) -> Result<u64> {
        let max_retries = 5u32;
        let base_delay = Duration::from_secs(2);
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = base_delay * 2u32.pow(attempt - 1);
                std::thread::sleep(delay);
            }

            match self.download_inner(remote_path, local_path) {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    tracing::warn!(
                        "Download attempt {}/{} failed for {:?}: {}",
                        attempt + 1, max_retries + 1, remote_path, e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    /// Inner download implementation
    fn download_inner(&self, remote_path: &Path, local_path: &Path) -> Result<u64> {
        // Ensure local parent directory exists
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        // Open remote file for reading
        let mut remote_file = self.sftp.open(remote_path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

        let local_file = std::fs::File::create(local_path)
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        let mut writer = std::io::BufWriter::with_capacity(1024 * 1024, local_file);
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut bytes_copied = 0u64;

        loop {
            let bytes_read = remote_file.read(&mut buffer)
                .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

            if bytes_read == 0 {
                break;
            }

            writer.write_all(&buffer[..bytes_read])
                .map_err(|e| SmartCopyError::io(local_path, e))?;

            bytes_copied += bytes_read as u64;
        }

        writer.flush()
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        Ok(bytes_copied)
    }

    /// Create remote directory recursively
    fn create_remote_dir_all(&self, path: &Path) -> Result<()> {
        let mut current = std::path::PathBuf::new();

        for component in path.components() {
            current.push(component);

            // Check if directory exists
            match self.sftp.stat(&current) {
                Ok(stat) => {
                    if !stat.is_dir() {
                        return Err(SmartCopyError::RemoteTransferError(
                            format!("Path exists but is not a directory: {:?}", current)
                        ));
                    }
                }
                Err(_) => {
                    // Directory doesn't exist, create it
                    self.sftp.mkdir(&current, 0o755)
                        .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    /// List remote directory
    pub fn list_dir(&self, path: &Path) -> Result<Vec<RemoteEntry>> {
        let entries = self.sftp.readdir(path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

        Ok(entries
            .into_iter()
            .map(|(path, stat)| RemoteEntry {
                path,
                size: stat.size.unwrap_or(0),
                is_dir: stat.is_dir(),
                is_file: stat.is_file(),
                mtime: stat.mtime.unwrap_or(0),
            })
            .collect())
    }

    /// Get remote file info
    pub fn stat(&self, path: &Path) -> Result<RemoteEntry> {
        let stat = self.sftp.stat(path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

        Ok(RemoteEntry {
            path: path.to_path_buf(),
            size: stat.size.unwrap_or(0),
            is_dir: stat.is_dir(),
            is_file: stat.is_file(),
            mtime: stat.mtime.unwrap_or(0),
        })
    }

    /// Check if remote path exists
    pub fn exists(&self, path: &Path) -> bool {
        self.sftp.stat(path).is_ok()
    }

    /// Remove remote file
    pub fn remove_file(&self, path: &Path) -> Result<()> {
        self.sftp.unlink(path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))
    }

    /// Remove remote directory
    pub fn remove_dir(&self, path: &Path) -> Result<()> {
        self.sftp.rmdir(path)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))
    }
}

/// Remote file entry
#[derive(Debug, Clone)]
pub struct RemoteEntry {
    /// Full path
    pub path: std::path::PathBuf,
    /// File size
    pub size: u64,
    /// Is directory
    pub is_dir: bool,
    /// Is regular file
    pub is_file: bool,
    /// Modification time (Unix timestamp)
    pub mtime: u64,
}

/// SSH connection pool for parallel transfers
pub struct SshConnectionPool {
    /// Pool of connections
    connections: Vec<SshConnection>,
    /// Remote configuration
    config: RemoteConfig,
}

impl SshConnectionPool {
    /// Create a pool with the specified number of connections
    pub fn new(config: RemoteConfig, pool_size: usize) -> Result<Self> {
        let mut connections = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            connections.push(SshConnection::connect(&config)?);
        }

        Ok(Self { connections, config })
    }

    /// Get a connection from the pool
    pub fn get(&self, index: usize) -> Option<&SshConnection> {
        self.connections.get(index % self.connections.len())
    }

    /// Get pool size
    pub fn size(&self) -> usize {
        self.connections.len()
    }
}

/// Parallel SSH uploader
pub struct ParallelSshUploader {
    /// Connection pool
    pool: SshConnectionPool,
}

impl ParallelSshUploader {
    /// Create with the specified number of parallel streams
    pub fn new(config: RemoteConfig, streams: usize) -> Result<Self> {
        let pool = SshConnectionPool::new(config, streams)?;
        Ok(Self { pool })
    }

    /// Upload multiple files in parallel
    pub fn upload_files(
        &self,
        files: &[(FileEntry, std::path::PathBuf)],
    ) -> Vec<Result<u64>> {
        use rayon::prelude::*;

        files
            .par_iter()
            .enumerate()
            .map(|(i, (entry, remote_path))| {
                let conn = self.pool.get(i).ok_or_else(|| {
                    SmartCopyError::RemoteTransferError("No connection available".to_string())
                })?;
                conn.upload(&entry.path, remote_path)
            })
            .collect()
    }
}

/// Result of SSH transfer
#[derive(Debug, Clone)]
pub struct SshTransferResult {
    /// Files transferred
    pub files_transferred: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Transfer duration
    pub duration: std::time::Duration,
    /// Throughput in bytes/second
    pub throughput: f64,
    /// Failed transfers
    pub failures: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require an SSH server to be available
    // They are marked as ignore by default

    #[test]
    #[ignore]
    fn test_ssh_connection() {
        let config = RemoteConfig {
            host: "localhost".to_string(),
            user: "test".to_string(),
            port: 22,
            key_path: None,
            streams: 1,
            tcp_direct: false,
            tcp_port: 9876,
            quic: false,
            quic_port: 9877,
            use_agent: false,
            ssh_tuning: None,
        };

        let conn = SshConnection::connect(&config);
        assert!(conn.is_ok());
    }
}
