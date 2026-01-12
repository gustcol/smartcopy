//! HTTP API Server
//!
//! Lightweight HTTP server for the SmartCopy Dashboard API.
//! Uses tokio for async I/O without external web framework dependencies.
//!
//! ## Running the Server
//!
//! ```bash
//! # Start API server on default port
//! smartcopy api-server --port 8080
//!
//! # With custom bind address
//! smartcopy api-server --bind 0.0.0.0 --port 8080
//! ```
//!
//! ## Important Notes
//!
//! The dashboard API server is designed for large-scale HPC environments
//! and enterprise deployments. For small-scale use cases, the CLI interface
//! provides all necessary functionality.
//!
//! **Do NOT run SmartCopy itself inside Docker** - this would significantly
//! impact I/O performance. Only the dashboard should be containerized.

use crate::api::handlers::*;
use crate::api::models::*;
use crate::error::{Result, SmartCopyError};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

/// API server configuration
#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    /// Bind address
    pub bind: String,
    /// Port
    pub port: u16,
    /// History storage path
    pub history_path: PathBuf,
    /// Enable CORS for all origins
    pub cors_enabled: bool,
    /// API key for authentication (optional)
    pub api_key: Option<String>,
    /// Maximum request body size (bytes)
    pub max_body_size: usize,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 8080,
            history_path: dirs::data_local_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("smartcopy")
                .join("history.json"),
            cors_enabled: true,
            api_key: None,
            max_body_size: 10 * 1024 * 1024, // 10 MB
        }
    }
}

/// API HTTP Server
pub struct ApiServer {
    /// Configuration
    config: ApiServerConfig,
    /// Shared application state
    state: Arc<AppState>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(config: ApiServerConfig) -> Result<Self> {
        let state = Arc::new(AppState::new(&config.history_path)?);

        Ok(Self {
            config,
            state,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get shutdown flag for external control
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Get shared state
    pub fn state(&self) -> Arc<AppState> {
        Arc::clone(&self.state)
    }

    /// Run the server (blocking)
    pub fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr)
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        listener.set_nonblocking(true)
            .map_err(|e| SmartCopyError::connection(&addr, e.to_string()))?;

        eprintln!("SmartCopy API server listening on http://{}", addr);
        eprintln!("Dashboard API ready for large-scale HPC environments");

        while !self.shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let state = Arc::clone(&self.state);
                    let config = self.config.clone();

                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, &state, &config) {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }

        eprintln!("API server shutting down");
        Ok(())
    }
}

/// Handle a single HTTP connection
fn handle_connection(
    mut stream: TcpStream,
    state: &AppState,
    config: &ApiServerConfig,
) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // Read request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return send_error(&mut stream, 400, "Bad Request", config);
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length = 0usize;

    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;

        if line.trim().is_empty() {
            break;
        }

        if let Some((key, value)) = line.trim().split_once(':') {
            let key = key.trim().to_lowercase();
            let value = value.trim().to_string();

            if key == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }

            headers.push((key, value));
        }
    }

    // Check API key if configured
    if let Some(ref api_key) = config.api_key {
        let auth_header = headers.iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.as_str());

        let expected = format!("Bearer {}", api_key);
        if auth_header != Some(&expected) {
            return send_error(&mut stream, 401, "Unauthorized", config);
        }
    }

    // Read body if present
    let body = if content_length > 0 && content_length <= config.max_body_size {
        let mut body = vec![0u8; content_length];
        std::io::Read::read_exact(&mut reader, &mut body)?;
        Some(String::from_utf8_lossy(&body).to_string())
    } else {
        None
    };

    // Route request
    route_request(&mut stream, method, path, body, state, config)
}

/// Route HTTP request to appropriate handler
fn route_request(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    body: Option<String>,
    state: &AppState,
    config: &ApiServerConfig,
) -> Result<()> {
    // Parse path and query string
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let query_params = parse_query_string(query);

    // Handle CORS preflight
    if method == "OPTIONS" {
        return send_cors_preflight(stream, config);
    }

    match (method, path) {
        // System status
        ("GET", "/api/status") => {
            let status = handle_status(state);
            send_json(stream, 200, &status, config)
        }

        // Jobs
        ("GET", "/api/jobs") => {
            let params = PaginationParams {
                page: query_params.get("page").and_then(|s| s.parse().ok()).unwrap_or(1),
                per_page: query_params.get("per_page").and_then(|s| s.parse().ok()).unwrap_or(20),
            };
            let jobs = handle_list_jobs(state, &params);
            send_json(stream, 200, &jobs, config)
        }

        ("POST", "/api/jobs") => {
            if let Some(body) = body {
                match serde_json::from_str::<CreateJobRequest>(&body) {
                    Ok(request) => {
                        match handle_create_job(state, request) {
                            Ok(job) => send_json(stream, 201, &job, config),
                            Err(e) => send_error(stream, 500, &e.to_string(), config),
                        }
                    }
                    Err(e) => send_error(stream, 400, &e.to_string(), config),
                }
            } else {
                send_error(stream, 400, "Request body required", config)
            }
        }

        // Job by ID
        (method, path) if path.starts_with("/api/jobs/") => {
            let job_id = &path[10..]; // Skip "/api/jobs/"

            match method {
                "GET" => {
                    match handle_get_job(state, job_id) {
                        Some(job) => send_json(stream, 200, &job, config),
                        None => send_error(stream, 404, "Job not found", config),
                    }
                }
                "DELETE" => {
                    match handle_cancel_job(state, job_id) {
                        Some(job) => send_json(stream, 200, &job, config),
                        None => send_error(stream, 404, "Job not found or cannot be cancelled", config),
                    }
                }
                _ => send_error(stream, 405, "Method not allowed", config),
            }
        }

        // History
        ("GET", "/api/history") => {
            let params = PaginationParams {
                page: query_params.get("page").and_then(|s| s.parse().ok()).unwrap_or(1),
                per_page: query_params.get("per_page").and_then(|s| s.parse().ok()).unwrap_or(20),
            };
            let source = query_params.get("source").map(|s| s.as_str());
            let dest = query_params.get("destination").map(|s| s.as_str());
            let history = handle_list_history(state, &params, source, dest);
            send_json(stream, 200, &history, config)
        }

        ("GET", "/api/history/stats") => {
            let days = query_params.get("days").and_then(|s| s.parse().ok()).unwrap_or(30);
            let stats = handle_history_stats(state, days);
            send_json(stream, 200, &stats, config)
        }

        // History entry by ID
        (method, path) if path.starts_with("/api/history/") && method == "GET" => {
            let entry_id = &path[13..]; // Skip "/api/history/"

            if entry_id == "stats" {
                // Already handled above
                send_error(stream, 404, "Entry not found", config)
            } else {
                match handle_get_history_entry(state, entry_id) {
                    Some(entry) => send_json(stream, 200, &entry, config),
                    None => send_error(stream, 404, "Entry not found", config),
                }
            }
        }

        // Compare transfers
        ("GET", "/api/compare") => {
            let ids: Vec<String> = query_params.get("ids")
                .map(|s| s.split(',').map(|s| s.to_string()).collect())
                .unwrap_or_default();

            if ids.len() < 2 {
                send_error(stream, 400, "At least 2 entry IDs required", config)
            } else {
                match handle_compare_transfers(state, &ids) {
                    Ok(comparison) => send_json(stream, 200, &comparison, config),
                    Err(e) => send_error(stream, 400, &e.to_string(), config),
                }
            }
        }

        // Agents
        ("GET", "/api/agents") => {
            let agents = handle_list_agents(state);
            send_json(stream, 200, &agents, config)
        }

        // System info
        ("GET", "/api/system") => {
            let info = handle_system_info();
            send_json(stream, 200, &info, config)
        }

        // Prometheus metrics
        ("GET", "/api/metrics") => {
            let metrics = handle_metrics(state);
            send_plain(stream, 200, &metrics, "text/plain; version=0.0.4", config)
        }

        // Health check
        ("GET", "/health") | ("GET", "/api/health") => {
            send_plain(stream, 200, "OK", "text/plain", config)
        }

        // Not found
        _ => send_error(stream, 404, "Not found", config),
    }
}

/// Parse query string into key-value pairs
fn parse_query_string(query: &str) -> std::collections::HashMap<String, String> {
    query.split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((
                urlencoding_decode(key),
                urlencoding_decode(value),
            ))
        })
        .collect()
}

/// Simple URL decoding
fn urlencoding_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
}

/// Send JSON response
fn send_json<T: serde::Serialize>(
    stream: &mut TcpStream,
    status: u16,
    data: &T,
    config: &ApiServerConfig,
) -> Result<()> {
    let body = serde_json::to_string(data)
        .map_err(|e| SmartCopyError::config(e.to_string()))?;

    send_response(stream, status, &body, "application/json", config)
}

/// Send plain text response
fn send_plain(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    content_type: &str,
    config: &ApiServerConfig,
) -> Result<()> {
    send_response(stream, status, body, content_type, config)
}

/// Send HTTP response
fn send_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    content_type: &str,
    config: &ApiServerConfig,
) -> Result<()> {
    let status_text = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let mut response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n",
        status, status_text,
        content_type,
        body.len(),
    );

    if config.cors_enabled {
        response.push_str("Access-Control-Allow-Origin: *\r\n");
        response.push_str("Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n");
        response.push_str("Access-Control-Allow-Headers: Content-Type, Authorization\r\n");
    }

    response.push_str("\r\n");
    response.push_str(body);

    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    Ok(())
}

/// Send error response
fn send_error(
    stream: &mut TcpStream,
    status: u16,
    message: &str,
    config: &ApiServerConfig,
) -> Result<()> {
    let error = ApiError {
        code: match status {
            400 => "BAD_REQUEST",
            401 => "UNAUTHORIZED",
            404 => "NOT_FOUND",
            405 => "METHOD_NOT_ALLOWED",
            _ => "INTERNAL_ERROR",
        }.to_string(),
        message: message.to_string(),
        details: None,
    };

    send_json(stream, status, &error, config)
}

/// Send CORS preflight response
fn send_cors_preflight(stream: &mut TcpStream, config: &ApiServerConfig) -> Result<()> {
    let response = format!(
        "HTTP/1.1 204 No Content\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type, Authorization\r\n\
         Access-Control-Max-Age: 86400\r\n\
         Content-Length: 0\r\n\r\n"
    );

    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    Ok(())
}

/// Directory helper for default paths
mod dirs {
    use std::path::PathBuf;

    pub fn data_local_dir() -> Option<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            std::env::var("HOME").ok().map(|h| PathBuf::from(h).join("Library/Application Support"))
        }

        #[cfg(target_os = "linux")]
        {
            std::env::var("XDG_DATA_HOME")
                .ok()
                .map(PathBuf::from)
                .or_else(|| std::env::var("HOME").ok().map(|h| PathBuf::from(h).join(".local/share")))
        }

        #[cfg(target_os = "windows")]
        {
            std::env::var("LOCALAPPDATA").ok().map(PathBuf::from)
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_string() {
        let params = parse_query_string("page=1&per_page=20&search=hello+world");
        assert_eq!(params.get("page"), Some(&"1".to_string()));
        assert_eq!(params.get("per_page"), Some(&"20".to_string()));
        assert_eq!(params.get("search"), Some(&"hello world".to_string()));
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(urlencoding_decode("hello%20world"), "hello world");
        assert_eq!(urlencoding_decode("foo+bar"), "foo bar");
        assert_eq!(urlencoding_decode("test%2Fpath"), "test/path");
    }
}
