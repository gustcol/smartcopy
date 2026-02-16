//! Native AWS S3 SDK integration
//!
//! Provides direct AWS SDK access for S3 operations, replacing CLI subprocess
//! calls with connection-pooled HTTP/2 requests. Supports S3-compatible
//! endpoints (MinIO, Wasabi, etc.), multipart uploads, and exponential
//! backoff retry.

use crate::error::{Result, SmartCopyError};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::Semaphore;
use std::sync::Arc;

/// Default maximum number of concurrent S3 operations.
const DEFAULT_MAX_CONCURRENT: usize = 64;

/// Default multipart upload threshold: 100 MB.
const MULTIPART_THRESHOLD: u64 = 100 * 1024 * 1024;

/// Default part size for multipart upload: 8 MB.
const MULTIPART_PART_SIZE: u64 = 8 * 1024 * 1024;

/// Maximum retry delay cap in seconds.
const MAX_RETRY_DELAY_SECS: u64 = 64;

/// Configuration for the native S3 client.
#[derive(Debug, Clone)]
pub struct NativeS3Config {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// Custom endpoint URL for S3-compatible services (MinIO, Wasabi)
    pub endpoint_url: Option<String>,
    /// Maximum number of concurrent downloads/uploads
    pub max_concurrent: usize,
    /// Maximum number of retries per operation
    pub max_retries: u32,
    /// Force path-style access (required for some S3-compatible services)
    pub force_path_style: bool,
    /// Access key ID (optional, falls back to AWS credential chain)
    pub access_key_id: Option<String>,
    /// Secret access key (optional, falls back to AWS credential chain)
    pub secret_access_key: Option<String>,
}

impl Default for NativeS3Config {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            endpoint_url: None,
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            max_retries: 5,
            force_path_style: false,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

/// S3 file entry for tracking downloads/uploads.
#[derive(Debug, Clone)]
pub struct S3FileEntry {
    /// S3 object key
    pub key: String,
    /// Object size in bytes (if known)
    pub size: Option<u64>,
    /// Local path to download to / upload from
    pub local_path: PathBuf,
}

/// Native S3 client with connection pooling and semaphore-based concurrency.
pub struct NativeS3Client {
    client: aws_sdk_s3::Client,
    config: NativeS3Config,
    semaphore: Arc<Semaphore>,
}

impl NativeS3Client {
    /// Create a new S3 client from configuration.
    pub async fn new(config: NativeS3Config) -> Result<Self> {
        let mut aws_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()));

        // Set custom endpoint for S3-compatible services
        if let Some(ref endpoint) = config.endpoint_url {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        // Set explicit credentials if provided
        if let (Some(ref key_id), Some(ref secret)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let creds = aws_credential_types::Credentials::new(
                key_id,
                secret,
                None, // session token
                None, // expiry
                "smartcopy-static",
            );
            aws_config_builder = aws_config_builder.credentials_provider(creds);
        }

        let aws_config = aws_config_builder.load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if config.force_path_style {
            s3_config = s3_config.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        Ok(Self {
            client,
            config,
            semaphore,
        })
    }

    /// Download a single object from S3 with exponential backoff retry.
    pub async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &Path,
    ) -> Result<u64> {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            SmartCopyError::RemoteTransferError(format!("Semaphore error: {}", e))
        })?;

        let mut retries = 0;
        loop {
            match self.try_download(bucket, key, local_path).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) if retries < self.config.max_retries => {
                    retries += 1;
                    let delay = exponential_backoff_delay(retries);
                    tracing::warn!(
                        "S3 download failed (attempt {}/{}): {}. Retrying in {:?}",
                        retries,
                        self.config.max_retries,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Attempt a single download.
    async fn try_download(&self, bucket: &str, key: &str, local_path: &Path) -> Result<u64> {
        // Ensure parent directory exists
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SmartCopyError::io(parent, e))?;
        }

        // Check if we can skip (size-based)
        if let Ok(meta) = tokio::fs::metadata(local_path).await {
            let head = self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| {
                    SmartCopyError::RemoteTransferError(format!("S3 head_object failed: {}", e))
                })?;

            if let Some(remote_size) = head.content_length() {
                if meta.len() == remote_size as u64 {
                    tracing::debug!("Skipping {} (size match: {} bytes)", key, meta.len());
                    return Ok(0);
                }
            }
        }

        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                SmartCopyError::RemoteTransferError(format!("S3 get_object failed: {}", e))
            })?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| {
                SmartCopyError::RemoteTransferError(format!("S3 body read failed: {}", e))
            })?;

        let bytes = body.into_bytes();
        let len = bytes.len() as u64;

        tokio::fs::write(local_path, &bytes)
            .await
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        Ok(len)
    }

    /// Upload a file to S3. Uses multipart upload for files over the threshold.
    pub async fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &Path,
    ) -> Result<u64> {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            SmartCopyError::RemoteTransferError(format!("Semaphore error: {}", e))
        })?;

        let metadata = tokio::fs::metadata(local_path)
            .await
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        let file_size = metadata.len();

        let mut retries = 0;
        loop {
            let result = if file_size > MULTIPART_THRESHOLD {
                self.try_multipart_upload(bucket, key, local_path, file_size)
                    .await
            } else {
                self.try_simple_upload(bucket, key, local_path).await
            };

            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) if retries < self.config.max_retries => {
                    retries += 1;
                    let delay = exponential_backoff_delay(retries);
                    tracing::warn!(
                        "S3 upload failed (attempt {}/{}): {}. Retrying in {:?}",
                        retries,
                        self.config.max_retries,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Simple single-part upload for small files.
    async fn try_simple_upload(
        &self,
        bucket: &str,
        key: &str,
        local_path: &Path,
    ) -> Result<u64> {
        let data = tokio::fs::read(local_path)
            .await
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        let len = data.len() as u64;

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| {
                SmartCopyError::RemoteTransferError(format!("S3 put_object failed: {}", e))
            })?;

        Ok(len)
    }

    /// Multipart upload for large files.
    async fn try_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        local_path: &Path,
        file_size: u64,
    ) -> Result<u64> {
        let create = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                SmartCopyError::RemoteTransferError(format!(
                    "S3 create_multipart_upload failed: {}",
                    e
                ))
            })?;

        let upload_id = create
            .upload_id()
            .ok_or_else(|| {
                SmartCopyError::RemoteTransferError("Missing upload_id".to_string())
            })?
            .to_string();

        let data = tokio::fs::read(local_path)
            .await
            .map_err(|e| SmartCopyError::io(local_path, e))?;

        let mut parts = Vec::new();
        let mut offset = 0usize;
        let mut part_number = 1i32;

        while offset < data.len() {
            let end = (offset + MULTIPART_PART_SIZE as usize).min(data.len());
            let chunk = data[offset..end].to_vec();

            let upload_part = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(chunk.into())
                .send()
                .await
                .map_err(|e| {
                    SmartCopyError::RemoteTransferError(format!("S3 upload_part failed: {}", e))
                })?;

            let etag = upload_part.e_tag().unwrap_or_default().to_string();
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );

            offset = end;
            part_number += 1;
        }

        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| {
                SmartCopyError::RemoteTransferError(format!(
                    "S3 complete_multipart_upload failed: {}",
                    e
                ))
            })?;

        Ok(file_size)
    }

    /// Download multiple files concurrently.
    pub async fn download_batch(
        &self,
        bucket: &str,
        entries: &[S3FileEntry],
    ) -> Vec<std::result::Result<u64, SmartCopyError>> {
        let mut handles = Vec::with_capacity(entries.len());

        for entry in entries {
            let client = self.client.clone();
            let semaphore = Arc::clone(&self.semaphore);
            let bucket = bucket.to_string();
            let key = entry.key.clone();
            let local_path = entry.local_path.clone();
            let max_retries = self.config.max_retries;

            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire().await.map_err(|e| {
                    SmartCopyError::RemoteTransferError(format!("Semaphore error: {}", e))
                })?;

                let mut retries = 0u32;
                loop {
                    match try_download_inner(&client, &bucket, &key, &local_path).await {
                        Ok(bytes) => return Ok(bytes),
                        Err(e) if retries < max_retries => {
                            retries += 1;
                            let delay = exponential_backoff_delay(retries);
                            tokio::time::sleep(delay).await;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(SmartCopyError::RemoteTransferError(format!(
                    "Task join error: {}",
                    e
                )))),
            }
        }
        results
    }
}

/// Standalone download helper for batch operations.
async fn try_download_inner(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> Result<u64> {
    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| SmartCopyError::io(parent, e))?;
    }

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            SmartCopyError::RemoteTransferError(format!("S3 get_object failed: {}", e))
        })?;

    let body = resp.body.collect().await.map_err(|e| {
        SmartCopyError::RemoteTransferError(format!("S3 body read failed: {}", e))
    })?;

    let bytes = body.into_bytes();
    let len = bytes.len() as u64;

    tokio::fs::write(local_path, &bytes)
        .await
        .map_err(|e| SmartCopyError::io(local_path, e))?;

    Ok(len)
}

/// Calculate exponential backoff delay: 2^retries seconds, capped at MAX_RETRY_DELAY_SECS.
fn exponential_backoff_delay(retries: u32) -> Duration {
    let secs = (1u64 << retries).min(MAX_RETRY_DELAY_SECS);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NativeS3Config::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.max_concurrent, DEFAULT_MAX_CONCURRENT);
        assert_eq!(config.max_retries, 5);
        assert!(!config.force_path_style);
    }

    #[test]
    fn test_exponential_backoff() {
        assert_eq!(exponential_backoff_delay(1), Duration::from_secs(2));
        assert_eq!(exponential_backoff_delay(2), Duration::from_secs(4));
        assert_eq!(exponential_backoff_delay(3), Duration::from_secs(8));
        assert_eq!(exponential_backoff_delay(6), Duration::from_secs(64));
        // Should cap at 64 seconds
        assert_eq!(exponential_backoff_delay(10), Duration::from_secs(64));
    }

    #[test]
    fn test_s3_file_entry() {
        let entry = S3FileEntry {
            key: "data/file.txt".to_string(),
            size: Some(1024),
            local_path: PathBuf::from("/tmp/file.txt"),
        };
        assert_eq!(entry.key, "data/file.txt");
        assert_eq!(entry.size, Some(1024));
    }
}
