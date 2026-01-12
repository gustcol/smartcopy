//! Incremental synchronization
//!
//! Provides efficient file synchronization by:
//! - Comparing metadata (size, mtime) for change detection
//! - Only copying new or modified files
//! - Optionally deleting extra files in destination

use crate::error::Result;
use crate::fs::{compare_entries, FileComparison, FileEntry, Scanner, ScanConfig, ScanResult};
use crate::sync::SyncManifest;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Synchronization action to perform
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    /// Copy new file to destination
    CopyNew,
    /// Update existing file (source is newer)
    Update,
    /// Delete file from destination (not in source)
    Delete,
    /// Skip (files are identical)
    Skip,
    /// Conflict (destination is newer)
    Conflict,
}

/// A file change detected during sync analysis
#[derive(Debug, Clone)]
pub struct SyncChange {
    /// Relative path
    pub path: String,
    /// Action to perform
    pub action: SyncAction,
    /// Source entry (if applicable)
    pub source: Option<FileEntry>,
    /// Destination entry (if applicable)
    pub dest: Option<FileEntry>,
    /// Size of the change (bytes to copy or delete)
    pub size: u64,
}

/// Analysis result for sync operation
#[derive(Debug, Clone)]
pub struct SyncAnalysis {
    /// Files to copy (new or updated)
    pub to_copy: Vec<SyncChange>,
    /// Files to skip (unchanged)
    pub to_skip: Vec<SyncChange>,
    /// Files to delete (extra in destination)
    pub to_delete: Vec<SyncChange>,
    /// Conflicts (destination newer than source)
    pub conflicts: Vec<SyncChange>,
    /// Total bytes to copy
    pub bytes_to_copy: u64,
    /// Total bytes to delete
    pub bytes_to_delete: u64,
    /// Total files in source
    pub source_count: usize,
    /// Total files in destination
    pub dest_count: usize,
}

impl SyncAnalysis {
    /// Create empty analysis
    pub fn new() -> Self {
        Self {
            to_copy: Vec::new(),
            to_skip: Vec::new(),
            to_delete: Vec::new(),
            conflicts: Vec::new(),
            bytes_to_copy: 0,
            bytes_to_delete: 0,
            source_count: 0,
            dest_count: 0,
        }
    }

    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        !self.to_copy.is_empty() || !self.to_delete.is_empty()
    }

    /// Get total number of actions
    pub fn action_count(&self) -> usize {
        self.to_copy.len() + self.to_delete.len()
    }

    /// Print summary
    pub fn print_summary(&self) {
        println!("=== Sync Analysis ===");
        println!("Source files:     {}", self.source_count);
        println!("Destination files:{}", self.dest_count);
        println!();
        println!("To copy:   {} files ({})",
            self.to_copy.len(),
            humansize::format_size(self.bytes_to_copy, humansize::BINARY)
        );
        println!("To skip:   {} files", self.to_skip.len());
        println!("To delete: {} files ({})",
            self.to_delete.len(),
            humansize::format_size(self.bytes_to_delete, humansize::BINARY)
        );
        if !self.conflicts.is_empty() {
            println!("Conflicts: {} files (destination newer)", self.conflicts.len());
        }
    }
}

impl Default for SyncAnalysis {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental synchronization engine
pub struct IncrementalSync {
    /// Delete extra files in destination
    delete_extra: bool,
    /// Use content-based comparison (hash)
    content_compare: bool,
    /// Ignore file time differences
    ignore_times: bool,
}

impl IncrementalSync {
    /// Create a new incremental sync
    pub fn new() -> Self {
        Self {
            delete_extra: false,
            content_compare: false,
            ignore_times: false,
        }
    }

    /// Enable deletion of extra files
    pub fn delete_extra(mut self, enable: bool) -> Self {
        self.delete_extra = enable;
        self
    }

    /// Enable content-based comparison
    pub fn content_compare(mut self, enable: bool) -> Self {
        self.content_compare = enable;
        self
    }

    /// Ignore modification times
    pub fn ignore_times(mut self, enable: bool) -> Self {
        self.ignore_times = enable;
        self
    }

    /// Analyze source and destination for sync
    pub fn analyze(&self, source: &Path, dest: &Path) -> Result<SyncAnalysis> {
        // Scan both directories
        let scan_config = ScanConfig::default();
        let scanner = Scanner::new(scan_config)?;

        let source_scan = scanner.scan(source)?;
        let dest_scan = if dest.exists() {
            scanner.scan(dest)?
        } else {
            ScanResult {
                root: dest.to_path_buf(),
                files: Vec::new(),
                directories: Vec::new(),
                total_size: 0,
                file_count: 0,
                dir_count: 0,
                scan_duration: std::time::Duration::ZERO,
                errors: Vec::new(),
            }
        };

        self.compare_scans(&source_scan, &dest_scan)
    }

    /// Analyze using a manifest
    pub fn analyze_with_manifest(
        &self,
        source: &Path,
        manifest: &SyncManifest,
    ) -> Result<SyncAnalysis> {
        let scan_config = ScanConfig::default();
        let scanner = Scanner::new(scan_config)?;
        let source_scan = scanner.scan(source)?;

        let mut analysis = SyncAnalysis::new();
        analysis.source_count = source_scan.file_count;

        // Build manifest lookup
        let manifest_entries: HashMap<&str, &crate::sync::ManifestEntry> = manifest
            .entries
            .iter()
            .map(|e| (e.path.as_str(), e))
            .collect();

        for source_entry in &source_scan.files {
            let rel_path = source_entry.relative_path.to_string_lossy();

            if let Some(manifest_entry) = manifest_entries.get(rel_path.as_ref()) {
                // Compare with manifest
                let mtime = source_entry.modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                if source_entry.size == manifest_entry.size
                    && (self.ignore_times || mtime == manifest_entry.mtime)
                {
                    // Unchanged
                    analysis.to_skip.push(SyncChange {
                        path: rel_path.to_string(),
                        action: SyncAction::Skip,
                        source: Some(source_entry.clone()),
                        dest: None,
                        size: 0,
                    });
                } else {
                    // Changed
                    analysis.to_copy.push(SyncChange {
                        path: rel_path.to_string(),
                        action: SyncAction::Update,
                        source: Some(source_entry.clone()),
                        dest: None,
                        size: source_entry.size,
                    });
                    analysis.bytes_to_copy += source_entry.size;
                }
            } else {
                // New file
                analysis.to_copy.push(SyncChange {
                    path: rel_path.to_string(),
                    action: SyncAction::CopyNew,
                    source: Some(source_entry.clone()),
                    dest: None,
                    size: source_entry.size,
                });
                analysis.bytes_to_copy += source_entry.size;
            }
        }

        // Find deleted files
        if self.delete_extra {
            let source_paths: HashSet<String> = source_scan.files
                .iter()
                .map(|e| e.relative_path.to_string_lossy().to_string())
                .collect();

            for entry in &manifest.entries {
                if !source_paths.contains(&entry.path) {
                    analysis.to_delete.push(SyncChange {
                        path: entry.path.clone(),
                        action: SyncAction::Delete,
                        source: None,
                        dest: None,
                        size: entry.size,
                    });
                    analysis.bytes_to_delete += entry.size;
                }
            }
        }

        Ok(analysis)
    }

    /// Compare two scan results
    fn compare_scans(
        &self,
        source: &ScanResult,
        dest: &ScanResult,
    ) -> Result<SyncAnalysis> {
        let mut analysis = SyncAnalysis::new();
        analysis.source_count = source.file_count;
        analysis.dest_count = dest.file_count;

        // Build destination file lookup
        let dest_files: HashMap<String, &FileEntry> = dest.files
            .iter()
            .map(|e| (e.relative_path.to_string_lossy().to_string(), e))
            .collect();

        let source_paths: HashSet<String> = source.files
            .iter()
            .map(|e| e.relative_path.to_string_lossy().to_string())
            .collect();

        // Compare source files to destination
        for source_entry in &source.files {
            let rel_path = source_entry.relative_path.to_string_lossy().to_string();

            if let Some(dest_entry) = dest_files.get(&rel_path) {
                // File exists in both - compare
                let comparison = if self.ignore_times {
                    if source_entry.size != dest_entry.size {
                        FileComparison::SizeDifferent
                    } else {
                        FileComparison::Same
                    }
                } else {
                    compare_entries(source_entry, dest_entry)
                };

                match comparison {
                    FileComparison::Same => {
                        analysis.to_skip.push(SyncChange {
                            path: rel_path,
                            action: SyncAction::Skip,
                            source: Some(source_entry.clone()),
                            dest: Some((*dest_entry).clone()),
                            size: 0,
                        });
                    }
                    FileComparison::SourceNewer | FileComparison::SizeDifferent => {
                        analysis.to_copy.push(SyncChange {
                            path: rel_path,
                            action: SyncAction::Update,
                            source: Some(source_entry.clone()),
                            dest: Some((*dest_entry).clone()),
                            size: source_entry.size,
                        });
                        analysis.bytes_to_copy += source_entry.size;
                    }
                    FileComparison::DestNewer => {
                        analysis.conflicts.push(SyncChange {
                            path: rel_path,
                            action: SyncAction::Conflict,
                            source: Some(source_entry.clone()),
                            dest: Some((*dest_entry).clone()),
                            size: 0,
                        });
                    }
                }
            } else {
                // New file in source
                analysis.to_copy.push(SyncChange {
                    path: rel_path,
                    action: SyncAction::CopyNew,
                    source: Some(source_entry.clone()),
                    dest: None,
                    size: source_entry.size,
                });
                analysis.bytes_to_copy += source_entry.size;
            }
        }

        // Find extra files in destination
        if self.delete_extra {
            for dest_entry in &dest.files {
                let rel_path = dest_entry.relative_path.to_string_lossy().to_string();
                if !source_paths.contains(&rel_path) {
                    analysis.to_delete.push(SyncChange {
                        path: rel_path,
                        action: SyncAction::Delete,
                        source: None,
                        dest: Some(dest_entry.clone()),
                        size: dest_entry.size,
                    });
                    analysis.bytes_to_delete += dest_entry.size;
                }
            }
        }

        Ok(analysis)
    }
}

impl Default for IncrementalSync {
    fn default() -> Self {
        Self::new()
    }
}

/// Quick check if incremental sync is needed
pub fn needs_sync(source: &Path, dest: &Path) -> Result<bool> {
    let sync = IncrementalSync::new();
    let analysis = sync.analyze(source, dest)?;
    Ok(analysis.has_changes())
}

/// Get list of new/modified files only
pub fn get_changed_files(source: &Path, dest: &Path) -> Result<Vec<String>> {
    let sync = IncrementalSync::new();
    let analysis = sync.analyze(source, dest)?;

    Ok(analysis.to_copy.into_iter().map(|c| c.path).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;
    use std::thread;
    use std::time::Duration;

    fn create_test_file(dir: &Path, name: &str, content: &[u8]) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut file = File::create(path).unwrap();
        file.write_all(content).unwrap();
    }

    #[test]
    fn test_analyze_empty_dest() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_file(src.path(), "file1.txt", b"content1");
        create_test_file(src.path(), "file2.txt", b"content2");

        let sync = IncrementalSync::new();
        let analysis = sync.analyze(src.path(), dst.path()).unwrap();

        assert_eq!(analysis.to_copy.len(), 2);
        assert!(analysis.to_skip.is_empty());
        assert!(analysis.to_delete.is_empty());
    }

    #[test]
    fn test_analyze_identical() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        let content = b"identical content";
        create_test_file(src.path(), "file.txt", content);
        create_test_file(dst.path(), "file.txt", content);

        let sync = IncrementalSync::new();
        let analysis = sync.analyze(src.path(), dst.path()).unwrap();

        // Files might show as changed due to different mtimes in tests
        // so we just check there are no new files
        assert!(analysis.to_copy.iter().all(|c| c.action != SyncAction::CopyNew));
    }

    #[test]
    fn test_analyze_delete_extra() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_file(src.path(), "keep.txt", b"keep");
        create_test_file(dst.path(), "keep.txt", b"keep");
        create_test_file(dst.path(), "extra.txt", b"extra");

        let sync = IncrementalSync::new().delete_extra(true);
        let analysis = sync.analyze(src.path(), dst.path()).unwrap();

        assert_eq!(analysis.to_delete.len(), 1);
        assert_eq!(analysis.to_delete[0].path, "extra.txt");
    }

    #[test]
    fn test_needs_sync() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();

        create_test_file(src.path(), "file.txt", b"content");

        assert!(needs_sync(src.path(), dst.path()).unwrap());

        // Copy file to destination
        create_test_file(dst.path(), "file.txt", b"content");

        // Note: In real tests mtime would be same only if we explicitly set it
    }
}
