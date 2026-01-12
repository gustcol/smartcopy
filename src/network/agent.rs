//! SmartCopy Remote Agent
//!
//! The agent runs on the remote server and enables:
//! - Remote delta calculation (like rsync)
//! - Parallel file operations on the remote side
//! - Efficient bidirectional data transfer
//!
//! ## Protocol
//!
//! The agent communicates via a binary protocol over stdio (when spawned via SSH)
//! or TCP (when running as a standalone daemon).
//!
//! ## Usage
//!
//! Via SSH (spawned automatically):
//! ```bash
//! smartcopy /local user@remote:/path --agent
//! ```
//!
//! As standalone daemon:
//! ```bash
//! smartcopy agent --protocol tcp --port 9878
//! ```

use crate::config::AgentProtocol;
use crate::error::{Result, SmartCopyError};
use crate::sync::FileSignature;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;

/// Magic bytes for protocol identification
pub const PROTOCOL_MAGIC: &[u8; 8] = b"SCAGENT1";

/// Maximum message size (64 MB)
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Agent request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentRequest {
    /// Protocol handshake
    Handshake {
        version: u32,
        client_features: Vec<String>,
    },

    /// Request file signature for delta transfer
    GetSignature {
        path: PathBuf,
        chunk_size: usize,
    },

    /// Get file metadata
    GetMetadata {
        path: PathBuf,
    },

    /// List directory contents
    ListDirectory {
        path: PathBuf,
        recursive: bool,
    },

    /// Read file chunk
    ReadChunk {
        path: PathBuf,
        offset: u64,
        size: usize,
    },

    /// Write file chunk
    WriteChunk {
        path: PathBuf,
        offset: u64,
        data: Vec<u8>,
        create: bool,
    },

    /// Create file with preallocated size
    CreateFile {
        path: PathBuf,
        size: u64,
    },

    /// Apply delta to existing file
    ApplyDelta {
        source_path: PathBuf,
        dest_path: PathBuf,
        delta_ops: Vec<DeltaOp>,
    },

    /// Calculate hash of file
    HashFile {
        path: PathBuf,
        algorithm: String,
    },

    /// Sync file times/permissions
    SetAttributes {
        path: PathBuf,
        mtime: Option<u64>,
        permissions: Option<u32>,
    },

    /// Create directory
    CreateDirectory {
        path: PathBuf,
        recursive: bool,
    },

    /// Remove file or directory
    Remove {
        path: PathBuf,
        recursive: bool,
    },

    /// Ping for keepalive
    Ping,

    /// Shutdown the agent
    Shutdown,
}

/// Delta operation for remote application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOp {
    /// Copy a chunk from source file
    CopyChunk {
        source_offset: u64,
        dest_offset: u64,
        size: usize,
    },
    /// Write literal data
    WriteLiteral {
        dest_offset: u64,
        data: Vec<u8>,
    },
}

/// Agent response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentResponse {
    /// Handshake accepted
    HandshakeOk {
        version: u32,
        server_features: Vec<String>,
    },

    /// File signature
    Signature {
        signature: RemoteFileSignature,
    },

    /// File metadata
    Metadata {
        exists: bool,
        is_file: bool,
        is_dir: bool,
        size: u64,
        mtime: u64,
        permissions: u32,
    },

    /// Directory listing
    DirectoryListing {
        entries: Vec<AgentRemoteEntry>,
    },

    /// File chunk data
    ChunkData {
        data: Vec<u8>,
        offset: u64,
    },

    /// Write acknowledgment
    WriteAck {
        bytes_written: u64,
    },

    /// File created
    FileCreated {
        path: PathBuf,
    },

    /// Delta applied
    DeltaApplied {
        bytes_copied: u64,
        bytes_written: u64,
    },

    /// File hash
    Hash {
        algorithm: String,
        hash: String,
    },

    /// Attributes set
    AttributesSet,

    /// Directory created
    DirectoryCreated,

    /// Item removed
    Removed,

    /// Pong response
    Pong,

    /// Error response
    Error {
        code: u32,
        message: String,
    },

    /// Shutdown acknowledgment
    ShutdownAck,
}

/// Remote file signature (serializable version)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFileSignature {
    pub path: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub num_chunks: usize,
    pub chunks: Vec<RemoteChunkSignature>,
}

/// Remote chunk signature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteChunkSignature {
    pub index: usize,
    pub offset: u64,
    pub size: usize,
    pub weak_checksum: u32,
    pub strong_hash: u64,
}

/// Remote directory entry for agent protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRemoteEntry {
    pub path: PathBuf,
    pub is_file: bool,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: u64,
    pub permissions: u32,
}

impl From<FileSignature> for RemoteFileSignature {
    fn from(sig: FileSignature) -> Self {
        Self {
            path: sig.path,
            file_size: sig.file_size,
            chunk_size: sig.chunk_size,
            num_chunks: sig.num_chunks,
            chunks: sig.chunks.into_iter().map(|c| RemoteChunkSignature {
                index: c.index,
                offset: c.offset,
                size: c.size,
                weak_checksum: c.weak_checksum,
                strong_hash: c.strong_hash,
            }).collect(),
        }
    }
}

/// Agent server for handling remote requests
pub struct AgentServer {
    /// Protocol type
    protocol: AgentProtocol,
    /// TCP port (if using TCP)
    port: u16,
    /// Bind address (if using TCP)
    bind: String,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl AgentServer {
    /// Create a new agent server
    pub fn new(protocol: AgentProtocol, port: u16, bind: String) -> Self {
        Self {
            protocol,
            port,
            bind,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run the agent server
    pub fn run(&self) -> Result<()> {
        match self.protocol {
            AgentProtocol::Stdio => self.run_stdio(),
            AgentProtocol::Tcp => self.run_tcp(),
        }
    }

    /// Run in stdio mode (for SSH pipe)
    fn run_stdio(&self) -> Result<()> {
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();

        let reader = BufReader::new(stdin.lock());
        let writer = BufWriter::new(stdout.lock());

        self.handle_connection(reader, writer)
    }

    /// Run in TCP mode
    fn run_tcp(&self) -> Result<()> {
        let addr = format!("{}:{}", self.bind, self.port);
        let listener = TcpListener::bind(&addr)
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        eprintln!("SmartCopy agent listening on {}", addr);

        for stream in listener.incoming() {
            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }

            match stream {
                Ok(stream) => {
                    let reader = BufReader::new(stream.try_clone().unwrap());
                    let writer = BufWriter::new(stream);

                    if let Err(e) = self.handle_connection(reader, writer) {
                        eprintln!("Connection error: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Handle a single connection
    fn handle_connection<R: Read, W: Write>(
        &self,
        mut reader: BufReader<R>,
        mut writer: BufWriter<W>,
    ) -> Result<()> {
        // Read and verify magic bytes
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        if &magic != PROTOCOL_MAGIC {
            return Err(SmartCopyError::connection(
                "agent",
                "Invalid protocol magic bytes",
            ));
        }

        loop {
            // Read message length
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(SmartCopyError::connection("agent", e.to_string())),
            }

            let msg_len = u32::from_le_bytes(len_buf) as usize;
            if msg_len > MAX_MESSAGE_SIZE {
                return Err(SmartCopyError::connection(
                    "agent",
                    format!("Message too large: {} bytes", msg_len),
                ));
            }

            // Read message
            let mut msg_buf = vec![0u8; msg_len];
            reader.read_exact(&mut msg_buf)
                .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

            // Deserialize request
            let request: AgentRequest = bincode::deserialize(&msg_buf)
                .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

            // Handle request
            let response = self.handle_request(request)?;

            // Check for shutdown
            if matches!(response, AgentResponse::ShutdownAck) {
                self.send_response(&mut writer, &response)?;
                break;
            }

            // Send response
            self.send_response(&mut writer, &response)?;
        }

        Ok(())
    }

    /// Send a response
    fn send_response<W: Write>(
        &self,
        writer: &mut BufWriter<W>,
        response: &AgentResponse,
    ) -> Result<()> {
        let msg = bincode::serialize(response)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        let len = (msg.len() as u32).to_le_bytes();
        writer.write_all(&len)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;
        writer.write_all(&msg)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;
        writer.flush()
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        Ok(())
    }

    /// Handle a single request
    fn handle_request(&self, request: AgentRequest) -> Result<AgentResponse> {
        match request {
            AgentRequest::Handshake { version, client_features: _ } => {
                if version != PROTOCOL_VERSION {
                    return Ok(AgentResponse::Error {
                        code: 1,
                        message: format!(
                            "Protocol version mismatch: expected {}, got {}",
                            PROTOCOL_VERSION, version
                        ),
                    });
                }

                Ok(AgentResponse::HandshakeOk {
                    version: PROTOCOL_VERSION,
                    server_features: vec![
                        "delta".to_string(),
                        "parallel".to_string(),
                        "compression".to_string(),
                    ],
                })
            }

            AgentRequest::GetSignature { path, chunk_size } => {
                match FileSignature::generate_parallel(&path, chunk_size) {
                    Ok(sig) => Ok(AgentResponse::Signature {
                        signature: RemoteFileSignature::from(sig),
                    }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 2,
                        message: format!("{}", e),
                    }),
                }
            }

            AgentRequest::GetMetadata { path } => {
                match std::fs::metadata(&path) {
                    Ok(meta) => Ok(AgentResponse::Metadata {
                        exists: true,
                        is_file: meta.is_file(),
                        is_dir: meta.is_dir(),
                        size: meta.len(),
                        mtime: meta.modified()
                            .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
                            .unwrap_or(0),
                        permissions: Self::get_permissions(&meta),
                    }),
                    Err(_) => Ok(AgentResponse::Metadata {
                        exists: false,
                        is_file: false,
                        is_dir: false,
                        size: 0,
                        mtime: 0,
                        permissions: 0,
                    }),
                }
            }

            AgentRequest::ListDirectory { path, recursive } => {
                let entries = Self::list_directory(&path, recursive)?;
                Ok(AgentResponse::DirectoryListing { entries })
            }

            AgentRequest::ReadChunk { path, offset, size } => {
                match Self::read_chunk(&path, offset, size) {
                    Ok(data) => Ok(AgentResponse::ChunkData { data, offset }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 3,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::WriteChunk { path, offset, data, create } => {
                match Self::write_chunk(&path, offset, &data, create) {
                    Ok(bytes) => Ok(AgentResponse::WriteAck { bytes_written: bytes }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 4,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::CreateFile { path, size } => {
                match Self::create_file(&path, size) {
                    Ok(_) => Ok(AgentResponse::FileCreated { path }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 5,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::ApplyDelta { source_path, dest_path, delta_ops } => {
                match Self::apply_delta(&source_path, &dest_path, &delta_ops) {
                    Ok((copied, written)) => Ok(AgentResponse::DeltaApplied {
                        bytes_copied: copied,
                        bytes_written: written,
                    }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 6,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::HashFile { path, algorithm } => {
                match Self::hash_file(&path, &algorithm) {
                    Ok(hash) => Ok(AgentResponse::Hash { algorithm, hash }),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 7,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::SetAttributes { path, mtime, permissions } => {
                match Self::set_attributes(&path, mtime, permissions) {
                    Ok(_) => Ok(AgentResponse::AttributesSet),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 8,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::CreateDirectory { path, recursive } => {
                let result = if recursive {
                    std::fs::create_dir_all(&path)
                } else {
                    std::fs::create_dir(&path)
                };

                match result {
                    Ok(_) => Ok(AgentResponse::DirectoryCreated),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 9,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::Remove { path, recursive } => {
                let result = if recursive {
                    if path.is_dir() {
                        std::fs::remove_dir_all(&path)
                    } else {
                        std::fs::remove_file(&path)
                    }
                } else {
                    if path.is_dir() {
                        std::fs::remove_dir(&path)
                    } else {
                        std::fs::remove_file(&path)
                    }
                };

                match result {
                    Ok(_) => Ok(AgentResponse::Removed),
                    Err(e) => Ok(AgentResponse::Error {
                        code: 10,
                        message: e.to_string(),
                    }),
                }
            }

            AgentRequest::Ping => Ok(AgentResponse::Pong),

            AgentRequest::Shutdown => {
                self.shutdown.store(true, Ordering::SeqCst);
                Ok(AgentResponse::ShutdownAck)
            }
        }
    }

    /// Get file permissions
    #[cfg(unix)]
    fn get_permissions(meta: &std::fs::Metadata) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode()
    }

    #[cfg(not(unix))]
    fn get_permissions(_meta: &std::fs::Metadata) -> u32 {
        0o644
    }

    /// List directory contents
    fn list_directory(path: &Path, recursive: bool) -> Result<Vec<AgentRemoteEntry>> {
        let mut entries = Vec::new();

        if recursive {
            for entry in walkdir::WalkDir::new(path).follow_links(false) {
                let entry = entry.map_err(|e| SmartCopyError::io(path, e.into()))?;
                let meta = entry.metadata().map_err(|e| SmartCopyError::io(path, e.into()))?;

                entries.push(AgentRemoteEntry {
                    path: entry.path().to_path_buf(),
                    is_file: meta.is_file(),
                    is_dir: meta.is_dir(),
                    size: meta.len(),
                    mtime: meta.modified()
                        .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
                        .unwrap_or(0),
                    permissions: Self::get_permissions(&meta),
                });
            }
        } else {
            for entry in std::fs::read_dir(path).map_err(|e| SmartCopyError::io(path, e))? {
                let entry = entry.map_err(|e| SmartCopyError::io(path, e))?;
                let meta = entry.metadata().map_err(|e| SmartCopyError::io(path, e))?;

                entries.push(AgentRemoteEntry {
                    path: entry.path(),
                    is_file: meta.is_file(),
                    is_dir: meta.is_dir(),
                    size: meta.len(),
                    mtime: meta.modified()
                        .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
                        .unwrap_or(0),
                    permissions: Self::get_permissions(&meta),
                });
            }
        }

        Ok(entries)
    }

    /// Read a chunk from a file
    fn read_chunk(path: &Path, offset: u64, size: usize) -> Result<Vec<u8>> {
        use std::io::Seek;

        let mut file = std::fs::File::open(path)
            .map_err(|e| SmartCopyError::io(path, e))?;

        file.seek(std::io::SeekFrom::Start(offset))
            .map_err(|e| SmartCopyError::io(path, e))?;

        let mut buffer = vec![0u8; size];
        let bytes_read = file.read(&mut buffer)
            .map_err(|e| SmartCopyError::io(path, e))?;

        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    /// Write a chunk to a file
    fn write_chunk(path: &Path, offset: u64, data: &[u8], create: bool) -> Result<u64> {
        use std::io::Seek;

        let mut file = if create {
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .map_err(|e| SmartCopyError::io(path, e))?
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| SmartCopyError::io(path, e))?
        };

        file.seek(std::io::SeekFrom::Start(offset))
            .map_err(|e| SmartCopyError::io(path, e))?;

        file.write_all(data)
            .map_err(|e| SmartCopyError::io(path, e))?;

        Ok(data.len() as u64)
    }

    /// Create a file with preallocated size
    fn create_file(path: &Path, size: u64) -> Result<()> {
        // Create parent directory
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        let file = std::fs::File::create(path)
            .map_err(|e| SmartCopyError::io(path, e))?;

        file.set_len(size)
            .map_err(|e| SmartCopyError::io(path, e))?;

        Ok(())
    }

    /// Apply delta operations to create destination file
    fn apply_delta(
        source_path: &Path,
        dest_path: &Path,
        delta_ops: &[DeltaOp],
    ) -> Result<(u64, u64)> {
        use std::io::Seek;

        // Create parent directory
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        let source = std::fs::File::open(source_path)
            .map_err(|e| SmartCopyError::io(source_path, e))?;
        let mut source_reader = BufReader::new(source);

        let dest = std::fs::File::create(dest_path)
            .map_err(|e| SmartCopyError::io(dest_path, e))?;
        let mut dest_writer = BufWriter::new(dest);

        let mut bytes_copied = 0u64;
        let mut bytes_written = 0u64;

        for op in delta_ops {
            match op {
                DeltaOp::CopyChunk { source_offset, dest_offset, size } => {
                    let mut buffer = vec![0u8; *size];
                    source_reader.seek(std::io::SeekFrom::Start(*source_offset))
                        .map_err(|e| SmartCopyError::io(source_path, e))?;
                    source_reader.read_exact(&mut buffer)
                        .map_err(|e| SmartCopyError::io(source_path, e))?;

                    dest_writer.seek(std::io::SeekFrom::Start(*dest_offset))
                        .map_err(|e| SmartCopyError::io(dest_path, e))?;
                    dest_writer.write_all(&buffer)
                        .map_err(|e| SmartCopyError::io(dest_path, e))?;

                    bytes_copied += *size as u64;
                }
                DeltaOp::WriteLiteral { dest_offset, data } => {
                    dest_writer.seek(std::io::SeekFrom::Start(*dest_offset))
                        .map_err(|e| SmartCopyError::io(dest_path, e))?;
                    dest_writer.write_all(data)
                        .map_err(|e| SmartCopyError::io(dest_path, e))?;

                    bytes_written += data.len() as u64;
                }
            }
        }

        dest_writer.flush()
            .map_err(|e| SmartCopyError::io(dest_path, e))?;

        Ok((bytes_copied, bytes_written))
    }

    /// Calculate hash of a file
    fn hash_file(path: &Path, algorithm: &str) -> Result<String> {
        let mut file = std::fs::File::open(path)
            .map_err(|e| SmartCopyError::io(path, e))?;

        let hash = match algorithm.to_lowercase().as_str() {
            "xxhash3" | "xxh3" => {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)
                    .map_err(|e| SmartCopyError::io(path, e))?;
                format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&buffer))
            }
            "blake3" => {
                let mut hasher = blake3::Hasher::new();
                let mut buffer = [0u8; 65536];
                loop {
                    let bytes_read = file.read(&mut buffer)
                        .map_err(|e| SmartCopyError::io(path, e))?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
                hasher.finalize().to_hex().to_string()
            }
            "sha256" => {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                let mut buffer = [0u8; 65536];
                loop {
                    let bytes_read = file.read(&mut buffer)
                        .map_err(|e| SmartCopyError::io(path, e))?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
                hex::encode(hasher.finalize())
            }
            _ => {
                return Err(SmartCopyError::config(format!(
                    "Unknown hash algorithm: {}",
                    algorithm
                )));
            }
        };

        Ok(hash)
    }

    /// Set file attributes
    fn set_attributes(path: &Path, mtime: Option<u64>, permissions: Option<u32>) -> Result<()> {
        if let Some(mtime) = mtime {
            let time = filetime::FileTime::from_unix_time(mtime as i64, 0);
            filetime::set_file_mtime(path, time)
                .map_err(|e| SmartCopyError::io(path, e))?;
        }

        #[cfg(unix)]
        if let Some(mode) = permissions {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(mode);
            std::fs::set_permissions(path, perms)
                .map_err(|e| SmartCopyError::io(path, e))?;
        }

        Ok(())
    }
}

/// Agent client for communicating with remote agent
pub struct AgentClient {
    writer: Box<dyn Write + Send>,
    reader: Box<dyn Read + Send>,
}

impl AgentClient {
    /// Connect to agent via TCP
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        let reader = stream.try_clone()
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        let mut client = Self {
            writer: Box::new(stream),
            reader: Box::new(BufReader::new(reader)),
        };

        // Send magic bytes
        client.writer.write_all(PROTOCOL_MAGIC)
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        // Perform handshake
        client.handshake()?;

        Ok(client)
    }

    /// Create client from stdio (for SSH pipe)
    pub fn from_stdio(stdin: std::process::ChildStdin, stdout: std::process::ChildStdout) -> Result<Self> {
        let mut client = Self {
            writer: Box::new(stdin),
            reader: Box::new(BufReader::new(stdout)),
        };

        // Send magic bytes
        client.writer.write_all(PROTOCOL_MAGIC)
            .map_err(|e| SmartCopyError::connection("stdio", e.to_string()))?;

        // Perform handshake
        client.handshake()?;

        Ok(client)
    }

    /// Perform protocol handshake
    fn handshake(&mut self) -> Result<()> {
        let request = AgentRequest::Handshake {
            version: PROTOCOL_VERSION,
            client_features: vec!["delta".to_string(), "parallel".to_string()],
        };

        let response = self.send_request(&request)?;

        match response {
            AgentResponse::HandshakeOk { version, .. } => {
                if version != PROTOCOL_VERSION {
                    return Err(SmartCopyError::connection(
                        "agent",
                        format!("Protocol version mismatch: expected {}, got {}", PROTOCOL_VERSION, version),
                    ));
                }
                Ok(())
            }
            AgentResponse::Error { message, .. } => {
                Err(SmartCopyError::connection("agent", message))
            }
            _ => Err(SmartCopyError::connection("agent", "Unexpected handshake response")),
        }
    }

    /// Send a request and receive response
    pub fn send_request(&mut self, request: &AgentRequest) -> Result<AgentResponse> {
        // Serialize request
        let msg = bincode::serialize(request)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        // Send length + message
        let len = (msg.len() as u32).to_le_bytes();
        self.writer.write_all(&len)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;
        self.writer.write_all(&msg)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;
        self.writer.flush()
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        // Read response length
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        let msg_len = u32::from_le_bytes(len_buf) as usize;
        if msg_len > MAX_MESSAGE_SIZE {
            return Err(SmartCopyError::connection(
                "agent",
                format!("Response too large: {} bytes", msg_len),
            ));
        }

        // Read response
        let mut msg_buf = vec![0u8; msg_len];
        self.reader.read_exact(&mut msg_buf)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        // Deserialize response
        let response: AgentResponse = bincode::deserialize(&msg_buf)
            .map_err(|e| SmartCopyError::connection("agent", e.to_string()))?;

        Ok(response)
    }

    /// Get file signature from remote
    pub fn get_signature(&mut self, path: &Path, chunk_size: usize) -> Result<RemoteFileSignature> {
        let request = AgentRequest::GetSignature {
            path: path.to_path_buf(),
            chunk_size,
        };

        match self.send_request(&request)? {
            AgentResponse::Signature { signature } => Ok(signature),
            AgentResponse::Error { message, .. } => {
                Err(SmartCopyError::RemoteTransferError(message))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response".to_string()
            )),
        }
    }

    /// Get file metadata from remote
    pub fn get_metadata(&mut self, path: &Path) -> Result<AgentResponse> {
        let request = AgentRequest::GetMetadata {
            path: path.to_path_buf(),
        };
        self.send_request(&request)
    }

    /// Write a chunk to remote file
    pub fn write_chunk(
        &mut self,
        path: &Path,
        offset: u64,
        data: Vec<u8>,
        create: bool,
    ) -> Result<u64> {
        let request = AgentRequest::WriteChunk {
            path: path.to_path_buf(),
            offset,
            data,
            create,
        };

        match self.send_request(&request)? {
            AgentResponse::WriteAck { bytes_written } => Ok(bytes_written),
            AgentResponse::Error { message, .. } => {
                Err(SmartCopyError::RemoteTransferError(message))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response".to_string()
            )),
        }
    }

    /// Apply delta on remote
    pub fn apply_delta(
        &mut self,
        source_path: &Path,
        dest_path: &Path,
        delta_ops: Vec<DeltaOp>,
    ) -> Result<(u64, u64)> {
        let request = AgentRequest::ApplyDelta {
            source_path: source_path.to_path_buf(),
            dest_path: dest_path.to_path_buf(),
            delta_ops,
        };

        match self.send_request(&request)? {
            AgentResponse::DeltaApplied { bytes_copied, bytes_written } => {
                Ok((bytes_copied, bytes_written))
            }
            AgentResponse::Error { message, .. } => {
                Err(SmartCopyError::RemoteTransferError(message))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response".to_string()
            )),
        }
    }

    /// Ping the remote agent
    pub fn ping(&mut self) -> Result<bool> {
        match self.send_request(&AgentRequest::Ping)? {
            AgentResponse::Pong => Ok(true),
            _ => Ok(false),
        }
    }

    /// Shutdown the remote agent
    pub fn shutdown(&mut self) -> Result<()> {
        let _ = self.send_request(&AgentRequest::Shutdown);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_magic() {
        assert_eq!(PROTOCOL_MAGIC.len(), 8);
        assert_eq!(&PROTOCOL_MAGIC[..], b"SCAGENT1");
    }

    #[test]
    fn test_request_serialization() {
        let request = AgentRequest::Ping;
        let serialized = bincode::serialize(&request).unwrap();
        let deserialized: AgentRequest = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            AgentRequest::Ping => {}
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_response_serialization() {
        let response = AgentResponse::Pong;
        let serialized = bincode::serialize(&response).unwrap();
        let deserialized: AgentResponse = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            AgentResponse::Pong => {}
            _ => panic!("Wrong response type"),
        }
    }
}
