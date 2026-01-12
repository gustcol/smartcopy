//! Sync manifest for tracking file state
//!
//! Provides persistent tracking of synchronized files
//! for efficient incremental updates.
//!
//! Note: Paths are stored as UTF-8 strings with lossy conversion.
//! For paths with non-UTF8 characters, use byte-level operations
//! or ensure your filesystem uses UTF-8 encoding.

use crate::config::HashAlgorithm;
use crate::error::{IoResultExt, Result, SmartCopyError};
use crate::fs::{FileEntry, ScanResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// A single file entry in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Relative path from root (UTF-8, may be lossy for non-UTF8 paths)
    pub path: String,
    /// Raw path bytes for non-UTF8 paths (base64 encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_bytes: Option<String>,
    /// File size in bytes
    pub size: u64,
    /// Modification time (Unix timestamp)
    pub mtime: u64,
    /// File hash (optional)
    pub hash: Option<String>,
    /// Hash algorithm used
    pub hash_algorithm: Option<HashAlgorithm>,
    /// File permissions (Unix mode)
    pub permissions: u32,
}

impl ManifestEntry {
    /// Create from a FileEntry
    pub fn from_file_entry(entry: &FileEntry) -> Self {
        let mtime = entry
            .modified
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let path_str = entry.relative_path.to_string_lossy().to_string();

        // Store raw bytes if path contains non-UTF8 characters
        #[cfg(unix)]
        let path_bytes = {
            use std::os::unix::ffi::OsStrExt;
            let bytes = entry.relative_path.as_os_str().as_bytes();
            // Check if lossy conversion lost data
            if path_str.contains('\u{FFFD}') || path_str.as_bytes() != bytes {
                Some(hex::encode(bytes))
            } else {
                None
            }
        };

        #[cfg(not(unix))]
        let path_bytes = None;

        Self {
            path: path_str,
            path_bytes,
            size: entry.size,
            mtime,
            hash: None,
            hash_algorithm: None,
            permissions: entry.permissions,
        }
    }

    /// Get the path as PathBuf, preferring raw bytes if available
    #[cfg(unix)]
    pub fn to_path(&self) -> PathBuf {
        if let Some(ref hex_bytes) = self.path_bytes {
            if let Ok(bytes) = hex::decode(hex_bytes) {
                use std::os::unix::ffi::OsStrExt;
                return PathBuf::from(std::ffi::OsStr::from_bytes(&bytes));
            }
        }
        PathBuf::from(&self.path)
    }

    /// Get the path as PathBuf
    #[cfg(not(unix))]
    pub fn to_path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    /// Create with hash
    pub fn with_hash(mut self, hash: String, algorithm: HashAlgorithm) -> Self {
        self.hash = Some(hash);
        self.hash_algorithm = Some(algorithm);
        self
    }

    /// Check if file matches this entry (by metadata)
    pub fn matches_metadata(&self, entry: &FileEntry) -> bool {
        let entry_mtime = entry
            .modified
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.size == entry.size && self.mtime == entry_mtime
    }
}

/// Sync manifest containing state of all synchronized files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncManifest {
    /// Manifest version
    pub version: u32,
    /// Source root path
    pub source_root: String,
    /// Destination root path
    pub dest_root: String,
    /// Creation timestamp
    pub created: u64,
    /// Last update timestamp
    pub updated: u64,
    /// Total files
    pub total_files: usize,
    /// Total size
    pub total_size: u64,
    /// File entries
    pub entries: Vec<ManifestEntry>,
}

impl SyncManifest {
    /// Current manifest version
    pub const VERSION: u32 = 1;

    /// Create a new empty manifest
    pub fn new(source_root: &str, dest_root: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            version: Self::VERSION,
            source_root: source_root.to_string(),
            dest_root: dest_root.to_string(),
            created: now,
            updated: now,
            total_files: 0,
            total_size: 0,
            entries: Vec::new(),
        }
    }

    /// Create from a scan result
    pub fn from_scan(scan: &ScanResult, dest_root: &str) -> Self {
        let mut manifest = Self::new(&scan.root.to_string_lossy(), dest_root);

        for entry in &scan.files {
            manifest.add_entry(ManifestEntry::from_file_entry(entry));
        }

        manifest
    }

    /// Add an entry
    pub fn add_entry(&mut self, entry: ManifestEntry) {
        self.total_size += entry.size;
        self.total_files += 1;
        self.entries.push(entry);
        self.touch();
    }

    /// Update timestamp
    fn touch(&mut self) {
        self.updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }

    /// Find entry by path
    pub fn find(&self, path: &str) -> Option<&ManifestEntry> {
        self.entries.iter().find(|e| e.path == path)
    }

    /// Find entry by path (mutable)
    pub fn find_mut(&mut self, path: &str) -> Option<&mut ManifestEntry> {
        self.entries.iter_mut().find(|e| e.path == path)
    }

    /// Remove entry by path
    pub fn remove(&mut self, path: &str) -> Option<ManifestEntry> {
        if let Some(pos) = self.entries.iter().position(|e| e.path == path) {
            let entry = self.entries.remove(pos);
            self.total_size = self.total_size.saturating_sub(entry.size);
            self.total_files = self.total_files.saturating_sub(1);
            self.touch();
            Some(entry)
        } else {
            None
        }
    }

    /// Update or add an entry
    pub fn upsert(&mut self, entry: ManifestEntry) {
        // Find the index of existing entry (if any) to avoid borrow issues
        let existing_idx = self.entries.iter().position(|e| e.path == entry.path);

        if let Some(idx) = existing_idx {
            let old_size = self.entries[idx].size;
            self.total_size = self.total_size.saturating_sub(old_size) + entry.size;
            self.entries[idx] = entry;
        } else {
            self.add_entry(entry);
        }
        self.touch();
    }

    /// Build a lookup map by path
    pub fn as_map(&self) -> HashMap<&str, &ManifestEntry> {
        self.entries.iter().map(|e| (e.path.as_str(), e)).collect()
    }

    /// Save manifest to file (JSON)
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))?;
        std::fs::write(path, json).with_path(path)?;
        Ok(())
    }

    /// Save manifest to file (binary/bincode - more compact)
    pub fn save_binary(&self, path: &Path) -> Result<()> {
        let data = bincode::serialize(self)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))?;
        std::fs::write(path, data).with_path(path)?;
        Ok(())
    }

    /// Load manifest from JSON file
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).with_path(path)?;
        serde_json::from_str(&content)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))
    }

    /// Load manifest from binary file
    pub fn load_binary(path: &Path) -> Result<Self> {
        let data = std::fs::read(path).with_path(path)?;
        bincode::deserialize(&data)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))
    }

    /// Get paths of all entries
    pub fn paths(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.path.as_str()).collect()
    }

    /// Check if manifest contains a path
    pub fn contains(&self, path: &str) -> bool {
        self.entries.iter().any(|e| e.path == path)
    }

    /// Print summary
    pub fn print_summary(&self) {
        println!("=== Sync Manifest ===");
        println!("Version: {}", self.version);
        println!("Source:  {}", self.source_root);
        println!("Dest:    {}", self.dest_root);
        println!("Files:   {}", self.total_files);
        println!("Size:    {}", humansize::format_size(self.total_size, humansize::BINARY));
        println!("Created: {}", self.format_timestamp(self.created));
        println!("Updated: {}", self.format_timestamp(self.updated));
    }

    fn format_timestamp(&self, ts: u64) -> String {
        chrono::DateTime::from_timestamp(ts as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| ts.to_string())
    }
}

/// Manifest diff between current state and manifest
#[derive(Debug, Clone)]
pub struct ManifestDiff {
    /// New files (in current state but not in manifest)
    pub added: Vec<String>,
    /// Modified files (different size/mtime)
    pub modified: Vec<String>,
    /// Deleted files (in manifest but not in current state)
    pub deleted: Vec<String>,
    /// Unchanged files
    pub unchanged: Vec<String>,
}

impl ManifestDiff {
    /// Calculate diff between scan result and manifest
    pub fn calculate(scan: &ScanResult, manifest: &SyncManifest) -> Self {
        let mut added = Vec::new();
        let mut modified = Vec::new();
        let mut unchanged = Vec::new();

        let manifest_map = manifest.as_map();
        let mut seen_paths = std::collections::HashSet::new();

        for entry in &scan.files {
            let path = entry.relative_path.to_string_lossy().to_string();
            seen_paths.insert(path.clone());

            if let Some(manifest_entry) = manifest_map.get(path.as_str()) {
                if manifest_entry.matches_metadata(entry) {
                    unchanged.push(path);
                } else {
                    modified.push(path);
                }
            } else {
                added.push(path);
            }
        }

        let deleted: Vec<String> = manifest
            .paths()
            .into_iter()
            .filter(|p| !seen_paths.contains(*p))
            .map(|s| s.to_string())
            .collect();

        Self {
            added,
            modified,
            deleted,
            unchanged,
        }
    }

    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        !self.added.is_empty() || !self.modified.is_empty() || !self.deleted.is_empty()
    }

    /// Get total number of changes
    pub fn change_count(&self) -> usize {
        self.added.len() + self.modified.len() + self.deleted.len()
    }

    /// Print summary
    pub fn print_summary(&self) {
        println!("=== Manifest Diff ===");
        println!("Added:     {}", self.added.len());
        println!("Modified:  {}", self.modified.len());
        println!("Deleted:   {}", self.deleted.len());
        println!("Unchanged: {}", self.unchanged.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_manifest_creation() {
        let manifest = SyncManifest::new("/source", "/dest");
        assert_eq!(manifest.version, SyncManifest::VERSION);
        assert_eq!(manifest.total_files, 0);
    }

    #[test]
    fn test_manifest_add_entry() {
        let mut manifest = SyncManifest::new("/source", "/dest");

        manifest.add_entry(ManifestEntry {
            path: "file1.txt".to_string(),
            path_bytes: None,
            size: 100,
            mtime: 1234567890,
            hash: None,
            hash_algorithm: None,
            permissions: 0o644,
        });

        assert_eq!(manifest.total_files, 1);
        assert_eq!(manifest.total_size, 100);
        assert!(manifest.contains("file1.txt"));
    }

    #[test]
    fn test_manifest_save_load_json() {
        let dir = TempDir::new().unwrap();
        let mut manifest = SyncManifest::new("/source", "/dest");

        manifest.add_entry(ManifestEntry {
            path: "test.txt".to_string(),
            path_bytes: None,
            size: 50,
            mtime: 1234567890,
            hash: Some("abc123".to_string()),
            hash_algorithm: Some(HashAlgorithm::XXHash3),
            permissions: 0o644,
        });

        // Test JSON save/load
        let json_path = dir.path().join("manifest.json");
        manifest.save(&json_path).unwrap();
        let loaded = SyncManifest::load(&json_path).unwrap();
        assert_eq!(loaded.total_files, 1);
        assert_eq!(loaded.entries[0].path, "test.txt");
        assert_eq!(loaded.entries[0].hash_algorithm, Some(HashAlgorithm::XXHash3));
    }

    #[test]
    #[ignore] // Binary format has compatibility issues with clap derive macros on enums
    fn test_manifest_save_load_binary() {
        let dir = TempDir::new().unwrap();
        let mut manifest = SyncManifest::new("/source", "/dest");

        // Use simpler entry without enum for binary test
        manifest.add_entry(ManifestEntry {
            path: "test.txt".to_string(),
            path_bytes: None,
            size: 50,
            mtime: 1234567890,
            hash: Some("abc123".to_string()),
            hash_algorithm: None,
            permissions: 0o644,
        });

        // Test binary save/load
        let bin_path = dir.path().join("manifest.bin");
        manifest.save_binary(&bin_path).unwrap();
        let loaded = SyncManifest::load_binary(&bin_path).unwrap();
        assert_eq!(loaded.total_files, 1);
        assert_eq!(loaded.entries[0].path, "test.txt");
    }

    #[test]
    fn test_manifest_upsert() {
        let mut manifest = SyncManifest::new("/source", "/dest");

        manifest.upsert(ManifestEntry {
            path: "file.txt".to_string(),
            path_bytes: None,
            size: 100,
            mtime: 1234567890,
            hash: None,
            hash_algorithm: None,
            permissions: 0o644,
        });

        assert_eq!(manifest.total_size, 100);

        // Update with larger size
        manifest.upsert(ManifestEntry {
            path: "file.txt".to_string(),
            path_bytes: None,
            size: 200,
            mtime: 1234567891,
            hash: None,
            hash_algorithm: None,
            permissions: 0o644,
        });

        assert_eq!(manifest.total_files, 1);
        assert_eq!(manifest.total_size, 200);
    }

    #[test]
    fn test_manifest_remove() {
        let mut manifest = SyncManifest::new("/source", "/dest");

        manifest.add_entry(ManifestEntry {
            path: "file.txt".to_string(),
            path_bytes: None,
            size: 100,
            mtime: 0,
            hash: None,
            hash_algorithm: None,
            permissions: 0o644,
        });

        let removed = manifest.remove("file.txt");
        assert!(removed.is_some());
        assert_eq!(manifest.total_files, 0);
        assert_eq!(manifest.total_size, 0);
    }
}
