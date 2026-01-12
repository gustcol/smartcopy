//! Error types for SmartCopy
//!
//! This module defines all error types used throughout the application,
//! providing detailed error information for debugging and user feedback.

use std::path::PathBuf;
use thiserror::Error;

/// Main error type for SmartCopy operations
#[derive(Error, Debug)]
pub enum SmartCopyError {
    /// I/O error during file operations
    #[error("I/O error at '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// File or directory not found
    #[error("Path not found: {0}")]
    NotFound(PathBuf),

    /// Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(PathBuf),

    /// Source and destination are the same
    #[error("Source and destination are the same: {0}")]
    SameSourceAndDestination(PathBuf),

    /// Invalid path format
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// Hash verification failed
    #[error("Integrity check failed for '{path}': expected {expected}, got {actual}")]
    IntegrityMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },

    /// Hash algorithm not supported
    #[error("Unsupported hash algorithm: {0}")]
    UnsupportedHashAlgorithm(String),

    /// Network/SSH connection error
    #[error("Connection error to '{host}': {message}")]
    ConnectionError { host: String, message: String },

    /// SSH authentication failed
    #[error("SSH authentication failed for '{user}@{host}': {message}")]
    AuthenticationError {
        user: String,
        host: String,
        message: String,
    },

    /// Remote transfer error
    #[error("Remote transfer error: {0}")]
    RemoteTransferError(String),

    /// Compression/decompression error
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Manifest parsing/writing error
    #[error("Manifest error: {0}")]
    ManifestError(String),

    /// Delta sync error
    #[error("Delta sync error: {0}")]
    DeltaSyncError(String),

    /// Resource detection error
    #[error("System resource detection error: {0}")]
    ResourceDetectionError(String),

    /// Thread pool error
    #[error("Thread pool error: {0}")]
    ThreadPoolError(String),

    /// Operation cancelled by user
    #[error("Operation cancelled")]
    Cancelled,

    /// Operation timed out
    #[error("Operation timed out after {0} seconds")]
    Timeout(u64),

    /// Disk full
    #[error("Insufficient disk space at '{path}': need {required} bytes, have {available} bytes")]
    InsufficientSpace {
        path: PathBuf,
        required: u64,
        available: u64,
    },

    /// File too large
    #[error("File too large: {path} ({size} bytes exceeds limit of {limit} bytes)")]
    FileTooLarge {
        path: PathBuf,
        size: u64,
        limit: u64,
    },

    /// Symbolic link error
    #[error("Symbolic link error at '{path}': {message}")]
    SymlinkError { path: PathBuf, message: String },

    /// Unsupported file type
    #[error("Unsupported file type at '{path}': {file_type}")]
    UnsupportedFileType { path: PathBuf, file_type: String },

    /// Unsupported operation on this platform
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    /// I/O error with custom message
    #[error("I/O error at '{path}': {message}")]
    IoError { path: PathBuf, message: String },

    /// Multiple errors occurred
    #[error("Multiple errors occurred ({count} errors)")]
    MultipleErrors {
        count: usize,
        errors: Vec<SmartCopyError>,
    },

    /// Generic error with context
    #[error("{context}: {source}")]
    WithContext {
        context: String,
        #[source]
        source: Box<SmartCopyError>,
    },
}

impl SmartCopyError {
    /// Create an I/O error with path context
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Create an integrity mismatch error
    pub fn integrity_mismatch(
        path: impl Into<PathBuf>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self::IntegrityMismatch {
            path: path.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create a connection error
    pub fn connection(host: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ConnectionError {
            host: host.into(),
            message: message.into(),
        }
    }

    /// Create an authentication error
    pub fn auth(
        user: impl Into<String>,
        host: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::AuthenticationError {
            user: user.into(),
            host: host.into(),
            message: message.into(),
        }
    }

    /// Add context to an error
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Self::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// Create a configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::ConfigError(message.into())
    }

    /// Check if this error is recoverable (can be retried)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Io { .. }
                | Self::ConnectionError { .. }
                | Self::RemoteTransferError(_)
                | Self::Timeout(_)
        )
    }

    /// Check if this error is a permission issue
    pub fn is_permission_error(&self) -> bool {
        match self {
            Self::PermissionDenied(_) => true,
            Self::Io { source, .. } => source.kind() == std::io::ErrorKind::PermissionDenied,
            _ => false,
        }
    }

    /// Get the path associated with this error, if any
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::Io { path, .. }
            | Self::NotFound(path)
            | Self::PermissionDenied(path)
            | Self::SameSourceAndDestination(path)
            | Self::IntegrityMismatch { path, .. }
            | Self::InsufficientSpace { path, .. }
            | Self::FileTooLarge { path, .. }
            | Self::SymlinkError { path, .. }
            | Self::UnsupportedFileType { path, .. } => Some(path),
            _ => None,
        }
    }
}

/// Result type alias for SmartCopy operations
pub type Result<T> = std::result::Result<T, SmartCopyError>;

impl From<std::io::Error> for SmartCopyError {
    fn from(err: std::io::Error) -> Self {
        SmartCopyError::Io {
            path: std::path::PathBuf::new(),
            source: err,
        }
    }
}

impl From<serde_json::Error> for SmartCopyError {
    fn from(err: serde_json::Error) -> Self {
        SmartCopyError::ManifestError(err.to_string())
    }
}

/// Extension trait for adding path context to std::io::Result
pub trait IoResultExt<T> {
    /// Add path context to an I/O error
    fn with_path(self, path: impl Into<PathBuf>) -> Result<T>;
}

impl<T> IoResultExt<T> for std::io::Result<T> {
    fn with_path(self, path: impl Into<PathBuf>) -> Result<T> {
        self.map_err(|e| SmartCopyError::io(path, e))
    }
}

/// Collects multiple results into a single result
pub fn collect_errors<T>(results: Vec<Result<T>>) -> Result<Vec<T>> {
    let mut successes = Vec::new();
    let mut errors = Vec::new();

    for result in results {
        match result {
            Ok(value) => successes.push(value),
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        Ok(successes)
    } else if errors.len() == 1 {
        Err(errors.into_iter().next().unwrap())
    } else {
        Err(SmartCopyError::MultipleErrors {
            count: errors.len(),
            errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_with_path() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = SmartCopyError::io("/test/path", io_err);
        assert!(err.path().is_some());
        assert_eq!(err.path().unwrap(), &PathBuf::from("/test/path"));
    }

    #[test]
    fn test_error_recoverability() {
        let recoverable = SmartCopyError::Timeout(30);
        assert!(recoverable.is_recoverable());

        let non_recoverable = SmartCopyError::PermissionDenied(PathBuf::from("/test"));
        assert!(!non_recoverable.is_recoverable());
    }

    #[test]
    fn test_collect_errors() {
        let results: Vec<Result<i32>> = vec![Ok(1), Ok(2), Ok(3)];
        let collected = collect_errors(results);
        assert!(collected.is_ok());
        assert_eq!(collected.unwrap(), vec![1, 2, 3]);

        let results: Vec<Result<i32>> = vec![
            Ok(1),
            Err(SmartCopyError::Cancelled),
            Err(SmartCopyError::Timeout(10)),
        ];
        let collected = collect_errors(results);
        assert!(collected.is_err());
    }
}
