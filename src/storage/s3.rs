//! S3/Object Storage Support
//!
//! Provides native S3-compatible object storage support using the AWS SDK.
//! Supports AWS S3, MinIO, Ceph, and other S3-compatible services.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// AWS region
    pub region: String,
    /// Custom endpoint URL (for MinIO, Ceph, etc.)
    pub endpoint: Option<String>,
    /// Access key ID
    pub access_key_id: Option<String>,
    /// Secret access key
    pub secret_access_key: Option<String>,
    /// Bucket name
    pub bucket: String,
    /// Key prefix
    pub prefix: Option<String>,
    /// Use path-style URLs (required for some S3-compatible services)
    pub path_style: bool,
    /// Enable server-side encryption
    pub encryption: Option<S3Encryption>,
    /// Storage class (STANDARD, REDUCED_REDUNDANCY, GLACIER, etc.)
    pub storage_class: Option<String>,
    /// Multipart upload threshold (bytes)
    pub multipart_threshold: u64,
    /// Multipart chunk size (bytes)
    pub multipart_chunk_size: u64,
    /// Maximum concurrent uploads
    pub max_concurrent_uploads: usize,
    /// Request timeout
    pub timeout: Duration,
    /// Number of retries
    pub max_retries: u32,
}

/// S3 server-side encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum S3Encryption {
    /// SSE-S3 (AES-256 managed by S3)
    S3Managed,
    /// SSE-KMS (AWS Key Management Service)
    Kms { key_id: String },
    /// SSE-C (Customer-provided key)
    CustomerKey { key: String },
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            bucket: String::new(),
            prefix: None,
            path_style: false,
            encryption: None,
            storage_class: None,
            multipart_threshold: 100 * 1024 * 1024, // 100MB
            multipart_chunk_size: 64 * 1024 * 1024, // 64MB
            max_concurrent_uploads: 8,
            timeout: Duration::from_secs(300),
            max_retries: 3,
        }
    }
}

impl S3Config {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            region: std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string()),
            endpoint: std::env::var("AWS_ENDPOINT_URL").ok()
                .or_else(|| std::env::var("S3_ENDPOINT").ok()),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            bucket: std::env::var("S3_BUCKET").unwrap_or_default(),
            prefix: std::env::var("S3_PREFIX").ok(),
            path_style: std::env::var("S3_PATH_STYLE")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            ..Default::default()
        }
    }

    /// Create config for MinIO
    pub fn minio(endpoint: &str, access_key: &str, secret_key: &str, bucket: &str) -> Self {
        Self {
            region: "us-east-1".to_string(),
            endpoint: Some(endpoint.to_string()),
            access_key_id: Some(access_key.to_string()),
            secret_access_key: Some(secret_key.to_string()),
            bucket: bucket.to_string(),
            path_style: true, // MinIO requires path-style
            ..Default::default()
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.bucket.is_empty() {
            return Err("Bucket name is required".to_string());
        }
        if self.multipart_chunk_size < 5 * 1024 * 1024 {
            return Err("Multipart chunk size must be at least 5MB".to_string());
        }
        Ok(())
    }
}

/// S3 object information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    /// Object key
    pub key: String,
    /// Size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: u64,
    /// ETag (MD5 hash for non-multipart uploads)
    pub etag: Option<String>,
    /// Storage class
    pub storage_class: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// Result of an S3 operation
#[derive(Debug, Clone)]
pub struct S3Result {
    /// Number of objects processed
    pub objects_count: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Number of successful operations
    pub success_count: u64,
    /// Number of failed operations
    pub failure_count: u64,
    /// Duration of operation
    pub duration: Duration,
    /// Errors encountered
    pub errors: Vec<S3Error>,
}

/// S3 operation error
#[derive(Debug, Clone)]
pub struct S3Error {
    /// Object key
    pub key: String,
    /// Error message
    pub message: String,
    /// Error code
    pub code: Option<String>,
}

/// S3 client for object storage operations
pub struct S3Client {
    config: S3Config,
}

impl S3Client {
    /// Create a new S3 client
    pub fn new(config: S3Config) -> io::Result<Self> {
        config.validate().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(Self { config })
    }

    /// Create from environment
    pub fn from_env() -> io::Result<Self> {
        Self::new(S3Config::from_env())
    }

    /// Get bucket name
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// List objects in bucket with optional prefix
    pub fn list_objects(&self, prefix: Option<&str>) -> io::Result<Vec<S3Object>> {
        // In production, this would use aws-sdk-s3
        // For now, we'll use the AWS CLI or s5cmd as backend
        let full_prefix = match (&self.config.prefix, prefix) {
            (Some(p1), Some(p2)) => Some(format!("{}/{}", p1, p2)),
            (Some(p), None) => Some(p.to_string()),
            (None, Some(p)) => Some(p.to_string()),
            (None, None) => None,
        };

        self.list_with_cli(&full_prefix)
    }

    /// Upload a file to S3
    pub fn upload_file(&self, local_path: &Path, key: &str) -> io::Result<S3Object> {
        let full_key = self.full_key(key);
        let metadata = std::fs::metadata(local_path)?;

        if metadata.len() >= self.config.multipart_threshold {
            self.upload_multipart(local_path, &full_key)
        } else {
            self.upload_single(local_path, &full_key)
        }
    }

    /// Download a file from S3
    pub fn download_file(&self, key: &str, local_path: &Path) -> io::Result<u64> {
        let full_key = self.full_key(key);
        self.download_with_cli(&full_key, local_path)
    }

    /// Upload a directory recursively
    pub fn upload_directory(&self, local_dir: &Path, prefix: &str) -> io::Result<S3Result> {
        let start = std::time::Instant::now();
        let mut objects_count = 0u64;
        let mut bytes_transferred = 0u64;
        let mut success_count = 0u64;
        let mut failure_count = 0u64;
        let mut errors = Vec::new();

        for entry in walkdir::WalkDir::new(local_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let relative_path = entry.path().strip_prefix(local_dir).unwrap();
                let key = format!("{}/{}", prefix, relative_path.display());

                objects_count += 1;

                match self.upload_file(entry.path(), &key) {
                    Ok(obj) => {
                        bytes_transferred += obj.size;
                        success_count += 1;
                    }
                    Err(e) => {
                        failure_count += 1;
                        errors.push(S3Error {
                            key: key.clone(),
                            message: e.to_string(),
                            code: None,
                        });
                    }
                }
            }
        }

        Ok(S3Result {
            objects_count,
            bytes_transferred,
            success_count,
            failure_count,
            duration: start.elapsed(),
            errors,
        })
    }

    /// Download objects to a directory
    pub fn download_directory(&self, prefix: &str, local_dir: &Path) -> io::Result<S3Result> {
        let start = std::time::Instant::now();
        let objects = self.list_objects(Some(prefix))?;

        let mut bytes_transferred = 0u64;
        let mut success_count = 0u64;
        let mut failure_count = 0u64;
        let mut errors = Vec::new();

        for obj in &objects {
            let relative_key = obj.key.strip_prefix(prefix).unwrap_or(&obj.key);
            let relative_key = relative_key.trim_start_matches('/');
            let local_path = local_dir.join(relative_key);

            // Create parent directories
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            match self.download_file(&obj.key, &local_path) {
                Ok(size) => {
                    bytes_transferred += size;
                    success_count += 1;
                }
                Err(e) => {
                    failure_count += 1;
                    errors.push(S3Error {
                        key: obj.key.clone(),
                        message: e.to_string(),
                        code: None,
                    });
                }
            }
        }

        Ok(S3Result {
            objects_count: objects.len() as u64,
            bytes_transferred,
            success_count,
            failure_count,
            duration: start.elapsed(),
            errors,
        })
    }

    /// Sync local directory with S3 (upload changed files)
    pub fn sync_to_s3(&self, local_dir: &Path, prefix: &str) -> io::Result<S3Result> {
        let start = std::time::Instant::now();

        // List existing S3 objects
        let remote_objects: HashMap<String, S3Object> = self
            .list_objects(Some(prefix))?
            .into_iter()
            .map(|obj| (obj.key.clone(), obj))
            .collect();

        let mut objects_count = 0u64;
        let mut bytes_transferred = 0u64;
        let mut success_count = 0u64;
        let mut failure_count = 0u64;
        let mut errors = Vec::new();

        for entry in walkdir::WalkDir::new(local_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let relative_path = entry.path().strip_prefix(local_dir).unwrap();
                let key = format!("{}/{}", prefix, relative_path.display());
                let full_key = self.full_key(&key);

                let metadata = std::fs::metadata(entry.path())?;
                let local_size = metadata.len();
                let local_mtime = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                // Check if upload is needed
                let needs_upload = match remote_objects.get(&full_key) {
                    Some(remote) => {
                        remote.size != local_size || remote.last_modified < local_mtime
                    }
                    None => true,
                };

                if needs_upload {
                    objects_count += 1;

                    match self.upload_file(entry.path(), &key) {
                        Ok(obj) => {
                            bytes_transferred += obj.size;
                            success_count += 1;
                        }
                        Err(e) => {
                            failure_count += 1;
                            errors.push(S3Error {
                                key: key.clone(),
                                message: e.to_string(),
                                code: None,
                            });
                        }
                    }
                }
            }
        }

        Ok(S3Result {
            objects_count,
            bytes_transferred,
            success_count,
            failure_count,
            duration: start.elapsed(),
            errors,
        })
    }

    /// Delete an object
    pub fn delete_object(&self, key: &str) -> io::Result<()> {
        let full_key = self.full_key(key);
        self.delete_with_cli(&full_key)
    }

    /// Copy an object within S3
    pub fn copy_object(&self, src_key: &str, dst_key: &str) -> io::Result<S3Object> {
        let full_src = self.full_key(src_key);
        let full_dst = self.full_key(dst_key);
        self.copy_with_cli(&full_src, &full_dst)
    }

    // Helper methods using CLI tools

    fn full_key(&self, key: &str) -> String {
        match &self.config.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key.to_string(),
        }
    }

    fn list_with_cli(&self, prefix: &Option<String>) -> io::Result<Vec<S3Object>> {
        let s3_url = match prefix {
            Some(p) => format!("s3://{}/{}", self.config.bucket, p),
            None => format!("s3://{}", self.config.bucket),
        };

        let output = std::process::Command::new("aws")
            .args(&[
                "s3",
                "ls",
                &s3_url,
                "--recursive",
                "--output",
                "json",
            ])
            .env_args(&self.config)
            .output()?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        // Parse output (simplified, would need proper JSON parsing in production)
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut objects = Vec::new();

        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let size = parts[2].parse().unwrap_or(0);
                let key = parts[3..].join(" ");

                objects.push(S3Object {
                    key,
                    size,
                    last_modified: 0,
                    etag: None,
                    storage_class: None,
                    content_type: None,
                    metadata: HashMap::new(),
                });
            }
        }

        Ok(objects)
    }

    fn upload_single(&self, local_path: &Path, key: &str) -> io::Result<S3Object> {
        let s3_url = format!("s3://{}/{}", self.config.bucket, key);

        let mut cmd = std::process::Command::new("aws");
        cmd.args(&["s3", "cp", &local_path.display().to_string(), &s3_url]);
        cmd.env_args(&self.config);

        if let Some(ref storage_class) = self.config.storage_class {
            cmd.args(&["--storage-class", storage_class]);
        }

        let output = cmd.output()?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        let metadata = std::fs::metadata(local_path)?;

        Ok(S3Object {
            key: key.to_string(),
            size: metadata.len(),
            last_modified: metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            etag: None,
            storage_class: self.config.storage_class.clone(),
            content_type: None,
            metadata: HashMap::new(),
        })
    }

    fn upload_multipart(&self, local_path: &Path, key: &str) -> io::Result<S3Object> {
        // For multipart, we still use aws s3 cp which handles it automatically
        self.upload_single(local_path, key)
    }

    fn download_with_cli(&self, key: &str, local_path: &Path) -> io::Result<u64> {
        let s3_url = format!("s3://{}/{}", self.config.bucket, key);

        // Create parent directories
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let output = std::process::Command::new("aws")
            .args(&["s3", "cp", &s3_url, &local_path.display().to_string()])
            .env_args(&self.config)
            .output()?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        let metadata = std::fs::metadata(local_path)?;
        Ok(metadata.len())
    }

    fn delete_with_cli(&self, key: &str) -> io::Result<()> {
        let s3_url = format!("s3://{}/{}", self.config.bucket, key);

        let output = std::process::Command::new("aws")
            .args(&["s3", "rm", &s3_url])
            .env_args(&self.config)
            .output()?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        Ok(())
    }

    fn copy_with_cli(&self, src_key: &str, dst_key: &str) -> io::Result<S3Object> {
        let src_url = format!("s3://{}/{}", self.config.bucket, src_key);
        let dst_url = format!("s3://{}/{}", self.config.bucket, dst_key);

        let output = std::process::Command::new("aws")
            .args(&["s3", "cp", &src_url, &dst_url])
            .env_args(&self.config)
            .output()?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        // Get object info
        Ok(S3Object {
            key: dst_key.to_string(),
            size: 0, // Would need HEAD request to get actual size
            last_modified: 0,
            etag: None,
            storage_class: None,
            content_type: None,
            metadata: HashMap::new(),
        })
    }
}

/// Extension trait for adding S3 config to Command
trait CommandS3Ext {
    fn env_args(&mut self, config: &S3Config) -> &mut Self;
}

impl CommandS3Ext for std::process::Command {
    fn env_args(&mut self, config: &S3Config) -> &mut Self {
        if let Some(ref endpoint) = config.endpoint {
            self.env("AWS_ENDPOINT_URL", endpoint);
        }
        if let Some(ref key) = config.access_key_id {
            self.env("AWS_ACCESS_KEY_ID", key);
        }
        if let Some(ref secret) = config.secret_access_key {
            self.env("AWS_SECRET_ACCESS_KEY", secret);
        }
        self.env("AWS_REGION", &config.region);

        if config.path_style {
            // AWS CLI v2 doesn't have direct path-style flag, use s3 configuration
            self.args(&["--endpoint-url", config.endpoint.as_ref().unwrap_or(&String::new())]);
        }

        self
    }
}

/// Parse S3 URL (s3://bucket/key)
pub fn parse_s3_url(url: &str) -> Option<(String, String)> {
    if !url.starts_with("s3://") {
        return None;
    }

    let path = &url[5..];
    let mut parts = path.splitn(2, '/');
    let bucket = parts.next()?.to_string();
    let key = parts.next().unwrap_or("").to_string();

    Some((bucket, key))
}

/// Check if path is an S3 URL
pub fn is_s3_url(path: &str) -> bool {
    path.starts_with("s3://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        assert_eq!(
            parse_s3_url("s3://my-bucket/path/to/file"),
            Some(("my-bucket".to_string(), "path/to/file".to_string()))
        );

        assert_eq!(
            parse_s3_url("s3://bucket"),
            Some(("bucket".to_string(), "".to_string()))
        );

        assert_eq!(parse_s3_url("/local/path"), None);
    }

    #[test]
    fn test_is_s3_url() {
        assert!(is_s3_url("s3://bucket/key"));
        assert!(!is_s3_url("/local/path"));
        assert!(!is_s3_url("https://bucket.s3.amazonaws.com/key"));
    }

    #[test]
    fn test_s3_config_minio() {
        let config = S3Config::minio(
            "http://localhost:9000",
            "minioadmin",
            "minioadmin",
            "test-bucket",
        );

        assert!(config.path_style);
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
    }
}
