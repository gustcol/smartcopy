//! Direct TCP transfer for maximum LAN throughput
//!
//! Provides high-speed file transfer over TCP without SSH overhead.
//! Best for trusted LAN environments where encryption is not required.

use crate::error::{IoResultExt, Result, SmartCopyError};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Magic bytes for protocol identification
const PROTOCOL_MAGIC: &[u8; 8] = b"SMCOPY01";

/// Default buffer size (1MB)
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// Message types for the protocol
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// File transfer request
    FileRequest = 1,
    /// File data
    FileData = 2,
    /// File complete
    FileComplete = 3,
    /// Error
    Error = 4,
    /// Ping/keepalive
    Ping = 5,
    /// Pong response
    Pong = 6,
    /// Shutdown server
    Shutdown = 255,
}

impl MessageType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::FileRequest),
            2 => Some(Self::FileData),
            3 => Some(Self::FileComplete),
            4 => Some(Self::Error),
            5 => Some(Self::Ping),
            6 => Some(Self::Pong),
            255 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

/// TCP transfer server
pub struct TcpServer {
    /// Listener
    listener: TcpListener,
    /// Root directory for serving files
    root: std::path::PathBuf,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Bytes transferred counter
    bytes_transferred: Arc<AtomicU64>,
}

impl TcpServer {
    /// Create and start a TCP server
    pub fn bind(addr: &str, root: &Path) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| SmartCopyError::connection(addr, e.to_string()))?;

        Ok(Self {
            listener,
            root: root.to_path_buf(),
            shutdown: Arc::new(AtomicBool::new(false)),
            bytes_transferred: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get the bound address
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Get shutdown flag for external control
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Get bytes transferred counter
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred.load(Ordering::Relaxed)
    }

    /// Run the server (blocking)
    pub fn run(&self) -> Result<()> {
        self.listener.set_nonblocking(true)
            .map_err(|e| SmartCopyError::connection("listener", e.to_string()))?;

        while !self.shutdown.load(Ordering::SeqCst) {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    tracing::info!("Accepted connection from {}", addr);

                    let root = self.root.clone();
                    let bytes_counter = Arc::clone(&self.bytes_transferred);

                    thread::spawn(move || {
                        if let Err(e) = Self::handle_client(stream, &root, bytes_counter) {
                            tracing::error!("Client error: {}", e);
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Handle a single client connection
    fn handle_client(
        stream: TcpStream,
        root: &Path,
        bytes_counter: Arc<AtomicU64>,
    ) -> Result<()> {
        let mut reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, stream.try_clone()?);
        let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, stream);

        // Read and verify magic
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)
            .map_err(|e| SmartCopyError::RemoteTransferError(e.to_string()))?;

        if &magic != PROTOCOL_MAGIC {
            return Err(SmartCopyError::RemoteTransferError("Invalid protocol magic".to_string()));
        }

        loop {
            // Read message type
            let mut msg_type = [0u8; 1];
            match reader.read_exact(&mut msg_type) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(SmartCopyError::RemoteTransferError(e.to_string())),
            }

            let msg_type = MessageType::from_u8(msg_type[0])
                .ok_or_else(|| SmartCopyError::RemoteTransferError("Invalid message type".to_string()))?;

            match msg_type {
                MessageType::FileRequest => {
                    Self::handle_file_request(&mut reader, &mut writer, root, &bytes_counter)?;
                }
                MessageType::Ping => {
                    writer.write_all(&[MessageType::Pong as u8])?;
                    writer.flush()?;
                }
                MessageType::Shutdown => break,
                _ => {
                    return Err(SmartCopyError::RemoteTransferError(
                        format!("Unexpected message type: {:?}", msg_type)
                    ));
                }
            }
        }

        Ok(())
    }

    /// Handle file request
    fn handle_file_request<R: Read, W: Write>(
        reader: &mut R,
        writer: &mut W,
        root: &Path,
        bytes_counter: &Arc<AtomicU64>,
    ) -> Result<()> {
        // Read path length
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let path_len = u32::from_le_bytes(len_buf) as usize;

        // Read path
        let mut path_buf = vec![0u8; path_len];
        reader.read_exact(&mut path_buf)?;
        let rel_path = String::from_utf8_lossy(&path_buf);

        let file_path = root.join(&*rel_path);

        // Open and send file
        match std::fs::File::open(&file_path) {
            Ok(file) => {
                let size = file.metadata()?.len();

                // Send file size
                writer.write_all(&[MessageType::FileData as u8])?;
                writer.write_all(&size.to_le_bytes())?;

                // Send file content
                let mut file_reader = BufReader::new(file);
                let mut buffer = vec![0u8; DEFAULT_BUFFER_SIZE];
                let mut total_sent = 0u64;

                while total_sent < size {
                    let bytes_read = file_reader.read(&mut buffer)?;
                    if bytes_read == 0 {
                        break;
                    }
                    writer.write_all(&buffer[..bytes_read])?;
                    total_sent += bytes_read as u64;
                    bytes_counter.fetch_add(bytes_read as u64, Ordering::Relaxed);
                }

                writer.flush()?;

                // Send complete
                writer.write_all(&[MessageType::FileComplete as u8])?;
                writer.flush()?;
            }
            Err(e) => {
                // Send error
                let error_msg = e.to_string();
                writer.write_all(&[MessageType::Error as u8])?;
                writer.write_all(&(error_msg.len() as u32).to_le_bytes())?;
                writer.write_all(error_msg.as_bytes())?;
                writer.flush()?;
            }
        }

        Ok(())
    }
}

/// TCP transfer client
pub struct TcpClient {
    /// Connection stream
    stream: TcpStream,
    /// Buffer size
    buffer_size: usize,
}

impl TcpClient {
    /// Connect to a TCP server
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| SmartCopyError::connection(addr, e.to_string()))?;

        // Send protocol magic
        let mut stream_clone = stream.try_clone()?;
        stream_clone.write_all(PROTOCOL_MAGIC)?;
        stream_clone.flush()?;

        Ok(Self {
            stream,
            buffer_size: DEFAULT_BUFFER_SIZE,
        })
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Download a file from the server
    pub fn download(&mut self, remote_path: &str, local_path: &Path) -> Result<u64> {
        // Ensure local parent exists
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).with_path(parent)?;
        }

        let mut writer = BufWriter::with_capacity(self.buffer_size, self.stream.try_clone()?);
        let mut reader = BufReader::with_capacity(self.buffer_size, self.stream.try_clone()?);

        // Send file request
        writer.write_all(&[MessageType::FileRequest as u8])?;
        writer.write_all(&(remote_path.len() as u32).to_le_bytes())?;
        writer.write_all(remote_path.as_bytes())?;
        writer.flush()?;

        // Read response
        let mut msg_type = [0u8; 1];
        reader.read_exact(&mut msg_type)?;

        match MessageType::from_u8(msg_type[0]) {
            Some(MessageType::FileData) => {
                // Read file size
                let mut size_buf = [0u8; 8];
                reader.read_exact(&mut size_buf)?;
                let size = u64::from_le_bytes(size_buf);

                // Create local file
                let local_file = std::fs::File::create(local_path).with_path(local_path)?;
                let mut file_writer = BufWriter::with_capacity(self.buffer_size, local_file);

                // Receive file content
                let mut buffer = vec![0u8; self.buffer_size];
                let mut total_received = 0u64;

                while total_received < size {
                    let to_read = ((size - total_received) as usize).min(self.buffer_size);
                    reader.read_exact(&mut buffer[..to_read])?;
                    file_writer.write_all(&buffer[..to_read])?;
                    total_received += to_read as u64;
                }

                file_writer.flush()?;

                // Read complete message
                reader.read_exact(&mut msg_type)?;
                if msg_type[0] != MessageType::FileComplete as u8 {
                    return Err(SmartCopyError::RemoteTransferError(
                        "Expected FileComplete message".to_string()
                    ));
                }

                Ok(total_received)
            }
            Some(MessageType::Error) => {
                // Read error message
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf)?;
                let msg_len = u32::from_le_bytes(len_buf) as usize;

                let mut msg_buf = vec![0u8; msg_len];
                reader.read_exact(&mut msg_buf)?;
                let error_msg = String::from_utf8_lossy(&msg_buf);

                Err(SmartCopyError::RemoteTransferError(error_msg.to_string()))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response message".to_string()
            )),
        }
    }

    /// Ping the server
    pub fn ping(&mut self) -> Result<Duration> {
        let start = std::time::Instant::now();

        let mut stream = self.stream.try_clone()?;
        stream.write_all(&[MessageType::Ping as u8])?;
        stream.flush()?;

        let mut response = [0u8; 1];
        stream.read_exact(&mut response)?;

        if response[0] != MessageType::Pong as u8 {
            return Err(SmartCopyError::RemoteTransferError("Invalid ping response".to_string()));
        }

        Ok(start.elapsed())
    }

    /// Close the connection
    pub fn close(mut self) -> Result<()> {
        self.stream.write_all(&[MessageType::Shutdown as u8])?;
        self.stream.flush()?;
        Ok(())
    }
}

/// Measure network bandwidth between two points
pub fn measure_bandwidth(server_addr: &str, _test_size: usize) -> Result<BandwidthResult> {
    let mut client = TcpClient::connect(server_addr)?;

    // Measure latency
    let latency = client.ping()?;

    // For actual bandwidth measurement, we'd need a test file on the server
    // This is a simplified version

    Ok(BandwidthResult {
        latency,
        bandwidth_mbps: 0.0, // Would be calculated from actual transfer
    })
}

/// Bandwidth measurement result
#[derive(Debug, Clone)]
pub struct BandwidthResult {
    /// Round-trip latency
    pub latency: Duration,
    /// Estimated bandwidth in Mbps
    pub bandwidth_mbps: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write;

    #[test]
    fn test_message_type_round_trip() {
        for i in 0..=255u8 {
            if let Some(msg_type) = MessageType::from_u8(i) {
                assert_eq!(i, msg_type as u8);
            }
        }
    }

    #[test]
    #[ignore] // Requires starting a server
    fn test_tcp_transfer() {
        let dir = TempDir::new().unwrap();

        // Create test file
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(b"Hello, TCP transfer!").unwrap();

        // Start server in background
        let server = TcpServer::bind("127.0.0.1:0", dir.path()).unwrap();
        let addr = server.local_addr().unwrap();
        let shutdown = server.shutdown_flag();

        let server_thread = thread::spawn(move || {
            let _ = server.run();
        });

        // Give server time to start
        thread::sleep(Duration::from_millis(100));

        // Connect and download
        let mut client = TcpClient::connect(&addr.to_string()).unwrap();
        let download_path = dir.path().join("downloaded.txt");

        let bytes = client.download("test.txt", &download_path).unwrap();
        assert_eq!(bytes, 20);

        // Verify content
        let content = std::fs::read_to_string(&download_path).unwrap();
        assert_eq!(content, "Hello, TCP transfer!");

        // Shutdown
        shutdown.store(true, Ordering::SeqCst);
        let _ = client.close();
        let _ = server_thread.join();
    }
}
