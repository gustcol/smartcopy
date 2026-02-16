//! QUIC Transport Layer
//!
//! High-performance file transfer using QUIC protocol (HTTP/3-like).
//!
//! ## Features
//!
//! - **0-RTT Connection**: Resume connections instantly
//! - **Multiplexing**: Multiple file transfers over single connection
//! - **Modern Congestion Control**: BBR or CUBIC for optimal throughput
//! - **Connection Migration**: Seamless network changes
//! - **Built-in Encryption**: TLS 1.3 integrated
//!
//! ## Usage
//!
//! Server:
//! ```bash
//! smartcopy quic-server --port 9877
//! ```
//!
//! Client:
//! ```bash
//! smartcopy /local remote:9877:/path --quic
//! ```

use crate::error::{Result, SmartCopyError};
use quinn::{
    ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig,
    TransportConfig, VarInt,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// QUIC protocol version
pub const QUIC_PROTOCOL_VERSION: u32 = 1;

/// Default QUIC port
pub const DEFAULT_QUIC_PORT: u16 = 9877;

/// Maximum concurrent streams per connection
pub const MAX_CONCURRENT_STREAMS: u32 = 100;

/// Stream buffer size (4 MB)
pub const STREAM_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// QUIC message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuicMessage {
    /// File transfer request
    FileRequest {
        path: PathBuf,
        offset: u64,
        length: Option<u64>,
    },

    /// File metadata request
    MetadataRequest {
        path: PathBuf,
    },

    /// File metadata response
    MetadataResponse {
        exists: bool,
        size: u64,
        mtime: u64,
        is_dir: bool,
    },

    /// Directory listing request
    ListRequest {
        path: PathBuf,
        recursive: bool,
    },

    /// File data chunk
    FileData {
        offset: u64,
        data: Vec<u8>,
        is_last: bool,
    },

    /// Transfer complete
    TransferComplete {
        bytes_transferred: u64,
        hash: Option<String>,
    },

    /// Error response
    Error {
        code: u32,
        message: String,
    },

    /// Ping for keepalive
    Ping { timestamp: u64 },

    /// Pong response
    Pong { timestamp: u64 },
}

/// TLS certificate manager for QUIC
pub struct CertificateManager {
    /// Certificate chain
    cert_chain: Vec<CertificateDer<'static>>,
    /// Private key
    private_key: PrivateKeyDer<'static>,
}

impl CertificateManager {
    /// Load certificates from files
    pub fn from_files(cert_path: &Path, key_path: &Path) -> Result<Self> {
        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| SmartCopyError::io(cert_path, e))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|e| SmartCopyError::io(key_path, e))?;

        Self::from_pem(&cert_pem, &key_pem)
    }

    /// Load certificates from PEM data
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &*cert_pem)
            .filter_map(|r| r.ok())
            .collect();

        if certs.is_empty() {
            return Err(SmartCopyError::config("No certificates found in PEM"));
        }

        let key = rustls_pemfile::private_key(&mut &*key_pem)
            .map_err(|e| SmartCopyError::config(format!("Failed to parse private key: {}", e)))?
            .ok_or_else(|| SmartCopyError::config("No private key found in PEM"))?;

        Ok(Self {
            cert_chain: certs,
            private_key: key,
        })
    }

    /// Generate self-signed certificate
    pub fn generate_self_signed(hostname: &str) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec![hostname.to_string()])
            .map_err(|e| SmartCopyError::config(format!("Failed to generate certificate: {}", e)))?;

        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

        Ok(Self {
            cert_chain: vec![cert_der],
            private_key: PrivateKeyDer::Pkcs8(key_der),
        })
    }

    /// Save certificate and key to files
    pub fn save_to_files(&self, cert_path: &Path, key_path: &Path) -> Result<()> {
        use std::io::Write;

        // Save certificate
        let mut cert_file = std::fs::File::create(cert_path)
            .map_err(|e| SmartCopyError::io(cert_path, e))?;

        for cert in &self.cert_chain {
            writeln!(cert_file, "-----BEGIN CERTIFICATE-----")
                .map_err(|e| SmartCopyError::io(cert_path, e))?;
            let b64 = base64_encode(cert.as_ref());
            for chunk in b64.as_bytes().chunks(64) {
                cert_file.write_all(chunk)
                    .map_err(|e| SmartCopyError::io(cert_path, e))?;
                writeln!(cert_file)
                    .map_err(|e| SmartCopyError::io(cert_path, e))?;
            }
            writeln!(cert_file, "-----END CERTIFICATE-----")
                .map_err(|e| SmartCopyError::io(cert_path, e))?;
        }

        // Save key
        let mut key_file = std::fs::File::create(key_path)
            .map_err(|e| SmartCopyError::io(key_path, e))?;

        writeln!(key_file, "-----BEGIN PRIVATE KEY-----")
            .map_err(|e| SmartCopyError::io(key_path, e))?;
        let key_bytes = match &self.private_key {
            PrivateKeyDer::Pkcs8(key) => key.secret_pkcs8_der(),
            PrivateKeyDer::Pkcs1(key) => key.secret_pkcs1_der(),
            PrivateKeyDer::Sec1(key) => key.secret_sec1_der(),
            _ => return Err(SmartCopyError::config("Unsupported key format")),
        };
        let b64 = base64_encode(key_bytes);
        for chunk in b64.as_bytes().chunks(64) {
            key_file.write_all(chunk)
                .map_err(|e| SmartCopyError::io(key_path, e))?;
            writeln!(key_file)
                .map_err(|e| SmartCopyError::io(key_path, e))?;
        }
        writeln!(key_file, "-----END PRIVATE KEY-----")
            .map_err(|e| SmartCopyError::io(key_path, e))?;

        Ok(())
    }

    /// Get certificate chain
    pub fn cert_chain(&self) -> &[CertificateDer<'static>] {
        &self.cert_chain
    }

    /// Get private key clone
    pub fn private_key(&self) -> PrivateKeyDer<'static> {
        self.private_key.clone_key()
    }
}

/// Simple base64 encoder
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let chunks = data.chunks(3);

    for chunk in chunks {
        let mut n = 0u32;
        for (i, &byte) in chunk.iter().enumerate() {
            n |= (byte as u32) << (16 - i * 8);
        }

        result.push(ALPHABET[(n >> 18) as usize & 0x3F] as char);
        result.push(ALPHABET[(n >> 12) as usize & 0x3F] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[(n >> 6) as usize & 0x3F] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[n as usize & 0x3F] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// QUIC server for file transfers
pub struct QuicServer {
    /// Server endpoint
    endpoint: Endpoint,
    /// Bind address
    bind_addr: SocketAddr,
}

impl QuicServer {
    /// Create a new QUIC server
    pub async fn new(
        bind_addr: SocketAddr,
        cert_manager: &CertificateManager,
    ) -> Result<Self> {
        let server_config = Self::build_server_config(cert_manager)?;
        let endpoint = Endpoint::server(server_config, bind_addr)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        Ok(Self {
            endpoint,
            bind_addr,
        })
    }

    /// Build server configuration
    fn build_server_config(cert_manager: &CertificateManager) -> Result<ServerConfig> {
        let mut crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                cert_manager.cert_chain().to_vec(),
                cert_manager.private_key(),
            )
            .map_err(|e| SmartCopyError::config(format!("TLS config error: {}", e)))?;

        crypto.alpn_protocols = vec![b"smartcopy".to_vec()];

        let quic_crypto = QuicServerConfig::try_from(crypto)
            .map_err(|e| SmartCopyError::config(format!("QUIC crypto error: {}", e)))?;

        let mut transport = TransportConfig::default();
        transport.max_concurrent_bidi_streams(VarInt::from_u32(MAX_CONCURRENT_STREAMS));
        transport.max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_STREAMS));
        transport.stream_receive_window(VarInt::from_u32(STREAM_BUFFER_SIZE as u32));
        transport.receive_window(VarInt::from_u32(STREAM_BUFFER_SIZE as u32 * 4));
        transport.send_window(STREAM_BUFFER_SIZE as u64 * 4);
        transport.keep_alive_interval(Some(Duration::from_secs(10)));

        let mut server_config = ServerConfig::with_crypto(Arc::new(quic_crypto));
        server_config.transport_config(Arc::new(transport));

        Ok(server_config)
    }

    /// Run the server
    pub async fn run(&self) -> Result<()> {
        println!("QUIC server listening on {}", self.bind_addr);

        while let Some(connecting) = self.endpoint.accept().await {
            let connection = connecting
                .await
                .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

            println!("New connection from {}", connection.remote_address());

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(connection).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Handle a single connection
    async fn handle_connection(connection: quinn::Connection) -> Result<()> {
        loop {
            let stream = match connection.accept_bi().await {
                Ok(stream) => stream,
                Err(quinn::ConnectionError::ApplicationClosed(_)) => break,
                Err(e) => return Err(SmartCopyError::connection("quic", e.to_string())),
            };

            tokio::spawn(async move {
                if let Err(e) = Self::handle_stream(stream).await {
                    eprintln!("Stream error: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Handle a single stream
    async fn handle_stream((mut send, mut recv): (SendStream, RecvStream)) -> Result<()> {
        // Read message length
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let msg_len = u32::from_le_bytes(len_buf) as usize;

        // Read message
        let mut msg_buf = vec![0u8; msg_len];
        recv.read_exact(&mut msg_buf)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        // Deserialize request
        let message: QuicMessage = bincode::deserialize(&msg_buf)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        // Handle request
        match message {
            QuicMessage::FileRequest { path, offset, length } => {
                Self::handle_file_request(&mut send, &path, offset, length).await?;
            }
            QuicMessage::MetadataRequest { path } => {
                Self::handle_metadata_request(&mut send, &path).await?;
            }
            QuicMessage::ListRequest { path, recursive } => {
                Self::handle_list_request(&mut send, &path, recursive).await?;
            }
            QuicMessage::Ping { timestamp } => {
                Self::send_message(&mut send, &QuicMessage::Pong { timestamp }).await?;
            }
            _ => {
                Self::send_message(&mut send, &QuicMessage::Error {
                    code: 1,
                    message: "Unsupported request".to_string(),
                }).await?;
            }
        }

        send.finish().ok();
        Ok(())
    }

    /// Handle file request
    async fn handle_file_request(
        send: &mut SendStream,
        path: &Path,
        offset: u64,
        length: Option<u64>,
    ) -> Result<()> {
        use tokio::fs::File;
        use tokio::io::AsyncSeekExt;

        let mut file = match File::open(path).await {
            Ok(f) => f,
            Err(e) => {
                Self::send_message(send, &QuicMessage::Error {
                    code: 2,
                    message: e.to_string(),
                }).await?;
                return Ok(());
            }
        };

        // Seek to offset
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| SmartCopyError::io(path, e))?;

        let file_size = file.metadata()
            .await
            .map_err(|e| SmartCopyError::io(path, e))?
            .len();

        let remaining = file_size.saturating_sub(offset);
        let to_read = length.unwrap_or(remaining).min(remaining);

        let mut bytes_sent = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

        while bytes_sent < to_read {
            let chunk_size = ((to_read - bytes_sent) as usize).min(buffer.len());
            let bytes_read = file.read(&mut buffer[..chunk_size])
                .await
                .map_err(|e| SmartCopyError::io(path, e))?;

            if bytes_read == 0 {
                break;
            }

            let is_last = bytes_sent + bytes_read as u64 >= to_read;

            let msg = QuicMessage::FileData {
                offset: offset + bytes_sent,
                data: buffer[..bytes_read].to_vec(),
                is_last,
            };

            Self::send_message(send, &msg).await?;
            bytes_sent += bytes_read as u64;
        }

        // Send completion
        Self::send_message(send, &QuicMessage::TransferComplete {
            bytes_transferred: bytes_sent,
            hash: None,
        }).await?;

        Ok(())
    }

    /// Handle metadata request
    async fn handle_metadata_request(send: &mut SendStream, path: &Path) -> Result<()> {
        let response = match tokio::fs::metadata(path).await {
            Ok(meta) => QuicMessage::MetadataResponse {
                exists: true,
                size: meta.len(),
                mtime: meta.modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                is_dir: meta.is_dir(),
            },
            Err(_) => QuicMessage::MetadataResponse {
                exists: false,
                size: 0,
                mtime: 0,
                is_dir: false,
            },
        };

        Self::send_message(send, &response).await
    }

    /// Handle list request
    async fn handle_list_request(
        send: &mut SendStream,
        _path: &Path,
        _recursive: bool,
    ) -> Result<()> {
        // TODO: Implement directory listing
        Self::send_message(send, &QuicMessage::Error {
            code: 3,
            message: "List not implemented yet".to_string(),
        }).await
    }

    /// Send a message
    async fn send_message(send: &mut SendStream, message: &QuicMessage) -> Result<()> {
        let msg = bincode::serialize(message)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let len = (msg.len() as u32).to_le_bytes();
        send.write_all(&len)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;
        send.write_all(&msg)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        Ok(())
    }
}

/// QUIC client for file transfers
pub struct QuicClient {
    /// Client endpoint
    endpoint: Endpoint,
    /// Active connection
    connection: Option<quinn::Connection>,
}

impl QuicClient {
    /// Create a new QUIC client
    pub fn new() -> Result<Self> {
        let client_config = Self::build_client_config()?;

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            connection: None,
        })
    }

    /// Build client configuration (accepts any certificate - for testing)
    fn build_client_config() -> Result<ClientConfig> {
        let crypto = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();

        let quic_crypto = QuicClientConfig::try_from(crypto)
            .map_err(|e| SmartCopyError::config(format!("QUIC crypto error: {}", e)))?;

        let mut transport = TransportConfig::default();
        transport.max_concurrent_bidi_streams(VarInt::from_u32(MAX_CONCURRENT_STREAMS));
        transport.stream_receive_window(VarInt::from_u32(STREAM_BUFFER_SIZE as u32));
        transport.keep_alive_interval(Some(Duration::from_secs(10)));

        let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
        client_config.transport_config(Arc::new(transport));

        Ok(client_config)
    }

    /// Connect to a QUIC server
    pub async fn connect(&mut self, addr: SocketAddr, server_name: &str) -> Result<()> {
        let connection = self.endpoint
            .connect(addr, server_name)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        self.connection = Some(connection);
        Ok(())
    }

    /// Request a file from the server
    pub async fn request_file(
        &self,
        path: &Path,
        offset: u64,
        length: Option<u64>,
    ) -> Result<Vec<u8>> {
        let connection = self.connection.as_ref()
            .ok_or_else(|| SmartCopyError::connection("quic", "Not connected"))?;

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        // Send request
        let request = QuicMessage::FileRequest {
            path: path.to_path_buf(),
            offset,
            length,
        };

        Self::send_message(&mut send, &request).await?;
        send.finish().ok();

        // Receive file data
        let mut data = Vec::new();

        loop {
            let message = Self::receive_message(&mut recv).await?;

            match message {
                QuicMessage::FileData { data: chunk, is_last, .. } => {
                    data.extend_from_slice(&chunk);
                    if is_last {
                        break;
                    }
                }
                QuicMessage::TransferComplete { .. } => break,
                QuicMessage::Error { message, .. } => {
                    return Err(SmartCopyError::RemoteTransferError(message));
                }
                _ => {
                    return Err(SmartCopyError::RemoteTransferError(
                        "Unexpected response".to_string()
                    ));
                }
            }
        }

        Ok(data)
    }

    /// Get file metadata
    pub async fn get_metadata(&self, path: &Path) -> Result<(bool, u64, u64, bool)> {
        let connection = self.connection.as_ref()
            .ok_or_else(|| SmartCopyError::connection("quic", "Not connected"))?;

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let request = QuicMessage::MetadataRequest {
            path: path.to_path_buf(),
        };

        Self::send_message(&mut send, &request).await?;
        send.finish().ok();

        let message = Self::receive_message(&mut recv).await?;

        match message {
            QuicMessage::MetadataResponse { exists, size, mtime, is_dir } => {
                Ok((exists, size, mtime, is_dir))
            }
            QuicMessage::Error { message, .. } => {
                Err(SmartCopyError::RemoteTransferError(message))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response".to_string()
            )),
        }
    }

    /// Ping the server
    pub async fn ping(&self) -> Result<Duration> {
        let connection = self.connection.as_ref()
            .ok_or_else(|| SmartCopyError::connection("quic", "Not connected"))?;

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let request = QuicMessage::Ping { timestamp };
        Self::send_message(&mut send, &request).await?;
        send.finish().ok();

        let message = Self::receive_message(&mut recv).await?;

        match message {
            QuicMessage::Pong { timestamp: ts } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                Ok(Duration::from_millis(now - ts))
            }
            _ => Err(SmartCopyError::RemoteTransferError(
                "Unexpected response".to_string()
            )),
        }
    }

    /// Send a message
    async fn send_message(send: &mut SendStream, message: &QuicMessage) -> Result<()> {
        let msg = bincode::serialize(message)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let len = (msg.len() as u32).to_le_bytes();
        send.write_all(&len)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;
        send.write_all(&msg)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        Ok(())
    }

    /// Receive a message
    async fn receive_message(recv: &mut RecvStream) -> Result<QuicMessage> {
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let msg_len = u32::from_le_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; msg_len];
        recv.read_exact(&mut msg_buf)
            .await
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        let message: QuicMessage = bincode::deserialize(&msg_buf)
            .map_err(|e| SmartCopyError::connection("quic", e.to_string()))?;

        Ok(message)
    }

    /// Close the connection
    pub fn close(&mut self) {
        if let Some(conn) = self.connection.take() {
            conn.close(VarInt::from_u32(0), b"done");
        }
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        self.close();
    }
}

/// Skip server certificate verification (for self-signed certs)
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Transfer statistics for QUIC
#[derive(Debug, Clone)]
pub struct QuicTransferStats {
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Transfer duration
    pub duration: Duration,
    /// Throughput in bytes/second
    pub throughput: f64,
    /// Round-trip time
    pub rtt: Duration,
    /// Connection statistics
    pub streams_opened: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b"Hello"), "SGVsbG8=");
        assert_eq!(base64_encode(b"Hi"), "SGk=");
        assert_eq!(base64_encode(b"A"), "QQ==");
    }

    #[test]
    fn test_message_serialization() {
        let msg = QuicMessage::Ping { timestamp: 12345 };
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: QuicMessage = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            QuicMessage::Ping { timestamp } => assert_eq!(timestamp, 12345),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_certificate_generation() {
        let cert = CertificateManager::generate_self_signed("localhost").unwrap();
        assert!(!cert.cert_chain().is_empty());
    }
}
