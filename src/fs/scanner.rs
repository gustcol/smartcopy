//! Directory scanner with parallel traversal
//!
//! High-performance directory scanning using parallel iteration
//! with smart file ordering and filtering capabilities.

use crate::config::OrderingStrategy;
use crate::error::{Result, SmartCopyError};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use walkdir::{DirEntry, WalkDir};

/// Metadata for a single file entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Absolute path to the file
    pub path: PathBuf,
    /// Relative path from source root
    pub relative_path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Modification time
    pub modified: SystemTime,
    /// Creation time (if available)
    pub created: Option<SystemTime>,
    /// Is this a directory?
    pub is_dir: bool,
    /// Is this a symlink?
    pub is_symlink: bool,
    /// Symlink target (if applicable)
    pub symlink_target: Option<PathBuf>,
    /// Unix permissions (if available)
    #[cfg(unix)]
    pub permissions: u32,
    /// File mode placeholder for non-Unix
    #[cfg(not(unix))]
    pub permissions: u32,
}

impl FileEntry {
    /// Create a FileEntry from a path
    pub fn from_path(path: &Path, source_root: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|e| SmartCopyError::io(path, e))?;

        let relative_path = path
            .strip_prefix(source_root)
            .unwrap_or(path)
            .to_path_buf();

        let is_symlink = metadata.is_symlink();
        let symlink_target = if is_symlink {
            std::fs::read_link(path).ok()
        } else {
            None
        };

        #[cfg(unix)]
        let permissions = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode()
        };

        #[cfg(not(unix))]
        let permissions = 0o644;

        Ok(FileEntry {
            path: path.to_path_buf(),
            relative_path,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            created: metadata.created().ok(),
            is_dir: metadata.is_dir(),
            is_symlink,
            symlink_target,
            permissions,
        })
    }

    /// Get file extension
    pub fn extension(&self) -> Option<&str> {
        self.path.extension().and_then(|e| e.to_str())
    }

    /// Check if this is a hidden file (Unix convention)
    pub fn is_hidden(&self) -> bool {
        self.path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with('.'))
            .unwrap_or(false)
    }
}

/// Result of a directory scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    /// Root path that was scanned
    pub root: PathBuf,
    /// All file entries (excluding directories)
    pub files: Vec<FileEntry>,
    /// All directory entries
    pub directories: Vec<FileEntry>,
    /// Total size of all files
    pub total_size: u64,
    /// Total number of files
    pub file_count: usize,
    /// Total number of directories
    pub dir_count: usize,
    /// Scan duration
    pub scan_duration: std::time::Duration,
    /// Any errors encountered during scan
    pub errors: Vec<String>,
}

impl ScanResult {
    /// Sort files by the given strategy
    pub fn sort_files(&mut self, strategy: OrderingStrategy) {
        match strategy {
            OrderingStrategy::SmallestFirst => {
                self.files.sort_by(|a, b| a.size.cmp(&b.size));
            }
            OrderingStrategy::LargestFirst => {
                self.files.sort_by(|a, b| b.size.cmp(&a.size));
            }
            OrderingStrategy::NewestFirst => {
                self.files.sort_by(|a, b| b.modified.cmp(&a.modified));
            }
            OrderingStrategy::OldestFirst => {
                self.files.sort_by(|a, b| a.modified.cmp(&b.modified));
            }
            OrderingStrategy::None => {}
        }
    }

    /// Get files partitioned by size threshold
    pub fn partition_by_size(&self, threshold: u64) -> (Vec<&FileEntry>, Vec<&FileEntry>) {
        self.files.iter().partition(|f| f.size < threshold)
    }

    /// Filter files by include/exclude patterns
    pub fn filter(&self, include: &GlobSet, exclude: &GlobSet) -> Vec<&FileEntry> {
        self.files
            .iter()
            .filter(|f| {
                let path_str = f.relative_path.to_string_lossy();
                let included = include.is_empty() || include.is_match(&*path_str);
                let excluded = exclude.is_match(&*path_str);
                included && !excluded
            })
            .collect()
    }
}

/// Configuration for directory scanning
#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Follow symbolic links
    pub follow_symlinks: bool,
    /// Include hidden files
    pub include_hidden: bool,
    /// Maximum depth (None = unlimited)
    pub max_depth: Option<usize>,
    /// Include patterns
    pub include_patterns: Vec<String>,
    /// Exclude patterns
    pub exclude_patterns: Vec<String>,
    /// Minimum file size
    pub min_size: Option<u64>,
    /// Maximum file size
    pub max_size: Option<u64>,
    /// Number of threads for parallel scanning
    pub threads: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            follow_symlinks: false,
            include_hidden: false,
            max_depth: None,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            min_size: None,
            max_size: None,
            threads: num_cpus::get(),
        }
    }
}

/// High-performance directory scanner
pub struct Scanner {
    config: ScanConfig,
    include_matcher: GlobSet,
    exclude_matcher: GlobSet,
}

impl Scanner {
    /// Create a new scanner with the given configuration
    pub fn new(config: ScanConfig) -> Result<Self> {
        let include_matcher = Self::build_globset(&config.include_patterns)?;
        let exclude_matcher = Self::build_globset(&config.exclude_patterns)?;

        Ok(Self {
            config,
            include_matcher,
            exclude_matcher,
        })
    }

    /// Build a GlobSet from patterns
    fn build_globset(patterns: &[String]) -> Result<GlobSet> {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            let glob = Glob::new(pattern)
                .map_err(|e| SmartCopyError::ConfigError(format!("Invalid glob pattern '{}': {}", pattern, e)))?;
            builder.add(glob);
        }
        builder
            .build()
            .map_err(|e| SmartCopyError::ConfigError(format!("Failed to build glob set: {}", e)))
    }

    /// Scan a directory and return all entries
    pub fn scan(&self, root: &Path) -> Result<ScanResult> {
        let start_time = std::time::Instant::now();

        if !root.exists() {
            return Err(SmartCopyError::NotFound(root.to_path_buf()));
        }

        let root = root.canonicalize()
            .map_err(|e| SmartCopyError::io(root, e))?;

        // Configure WalkDir
        let mut walker = WalkDir::new(&root)
            .follow_links(self.config.follow_symlinks);

        if let Some(max_depth) = self.config.max_depth {
            walker = walker.max_depth(max_depth);
        }

        // Collect entries in parallel
        let entries: Vec<_> = walker.into_iter().collect();

        // Process entries in parallel using rayon
        let results: Vec<_> = entries
            .into_par_iter()
            .filter_map(|entry| {
                match entry {
                    Ok(e) => self.process_entry(&e, &root),
                    Err(err) => {
                        Some(Err(err.to_string()))
                    }
                }
            })
            .collect();

        // Separate successes and errors
        let mut files = Vec::new();
        let mut directories = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(entry) => {
                    if entry.is_dir {
                        directories.push(entry);
                    } else {
                        files.push(entry);
                    }
                }
                Err(e) => errors.push(e),
            }
        }

        // Calculate totals
        let total_size: u64 = files.iter().map(|f| f.size).sum();
        let file_count = files.len();
        let dir_count = directories.len();

        Ok(ScanResult {
            root,
            files,
            directories,
            total_size,
            file_count,
            dir_count,
            scan_duration: start_time.elapsed(),
            errors,
        })
    }

    /// Process a single directory entry
    fn process_entry(&self, entry: &DirEntry, root: &Path) -> Option<std::result::Result<FileEntry, String>> {
        let path = entry.path();

        // Skip hidden files if configured
        if !self.config.include_hidden && self.is_hidden(entry) {
            return None;
        }

        // Create file entry
        let file_entry = match FileEntry::from_path(path, root) {
            Ok(e) => e,
            Err(err) => return Some(Err(err.to_string())),
        };

        // Skip directories here (we want to traverse them, but filter files)
        if file_entry.is_dir {
            return Some(Ok(file_entry));
        }

        // Apply size filters
        if let Some(min_size) = self.config.min_size {
            if file_entry.size < min_size {
                return None;
            }
        }
        if let Some(max_size) = self.config.max_size {
            if file_entry.size > max_size {
                return None;
            }
        }

        // Apply pattern filters
        let path_str = file_entry.relative_path.to_string_lossy();

        // Check include patterns (if any)
        if !self.include_matcher.is_empty() && !self.include_matcher.is_match(&*path_str) {
            return None;
        }

        // Check exclude patterns
        if self.exclude_matcher.is_match(&*path_str) {
            return None;
        }

        Some(Ok(file_entry))
    }

    /// Check if an entry is hidden
    fn is_hidden(&self, entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .map(|s| s.starts_with('.'))
            .unwrap_or(false)
    }

    /// Scan and sort by the given strategy
    pub fn scan_sorted(&self, root: &Path, strategy: OrderingStrategy) -> Result<ScanResult> {
        let mut result = self.scan(root)?;
        result.sort_files(strategy);
        Ok(result)
    }
}

/// Quick estimate of directory size without full metadata
pub fn estimate_directory_size(path: &Path) -> std::io::Result<(u64, usize)> {
    let mut total_size = 0u64;
    let mut file_count = 0usize;

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            if let Ok(metadata) = entry.metadata() {
                total_size += metadata.len();
                file_count += 1;
            }
        }
    }

    Ok((total_size, file_count))
}

/// Compare two file entries for sync purposes
pub fn compare_entries(source: &FileEntry, dest: &FileEntry) -> FileComparison {
    if source.size != dest.size {
        return FileComparison::SizeDifferent;
    }

    match source.modified.cmp(&dest.modified) {
        Ordering::Greater => FileComparison::SourceNewer,
        Ordering::Less => FileComparison::DestNewer,
        Ordering::Equal => FileComparison::Same,
    }
}

/// Result of comparing two file entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileComparison {
    /// Files are identical (same size and mtime)
    Same,
    /// Source is newer
    SourceNewer,
    /// Destination is newer
    DestNewer,
    /// Files have different sizes
    SizeDifferent,
}

/// File size categories for optimized handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSizeCategory {
    /// Tiny files (< 4KB) - best copied with simple read/write
    Tiny,
    /// Small files (< 1MB) - can use buffered I/O
    Small,
    /// Medium files (< 100MB) - use memory mapping
    Medium,
    /// Large files (< 1GB) - use chunked transfer
    Large,
    /// Huge files (>= 1GB) - use parallel chunks or delta
    Huge,
}

impl FileSizeCategory {
    /// Categorize a file by size
    pub fn from_size(size: u64) -> Self {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if size < 4 * KB {
            Self::Tiny
        } else if size < 1 * MB {
            Self::Small
        } else if size < 100 * MB {
            Self::Medium
        } else if size < 1 * GB {
            Self::Large
        } else {
            Self::Huge
        }
    }

    /// Get recommended buffer size for this category
    pub fn recommended_buffer_size(&self) -> usize {
        match self {
            Self::Tiny => 4 * 1024,
            Self::Small => 64 * 1024,
            Self::Medium => 256 * 1024,
            Self::Large => 1024 * 1024,
            Self::Huge => 4 * 1024 * 1024,
        }
    }

    /// Should use memory mapping?
    pub fn use_mmap(&self) -> bool {
        matches!(self, Self::Medium | Self::Large | Self::Huge)
    }

    /// Should use parallel chunks?
    pub fn use_parallel_chunks(&self) -> bool {
        matches!(self, Self::Huge)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;

    fn create_test_dir() -> TempDir {
        let dir = TempDir::new().unwrap();

        // Create some test files
        File::create(dir.path().join("small.txt")).unwrap()
            .write_all(b"small file").unwrap();

        let mut medium = File::create(dir.path().join("medium.bin")).unwrap();
        medium.write_all(&vec![0u8; 1024 * 100]).unwrap();

        // Create subdirectory
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        File::create(dir.path().join("subdir/nested.txt")).unwrap()
            .write_all(b"nested").unwrap();

        // Create hidden file
        File::create(dir.path().join(".hidden")).unwrap()
            .write_all(b"hidden").unwrap();

        dir
    }

    #[test]
    fn test_scanner_basic() {
        let dir = create_test_dir();
        let config = ScanConfig::default();
        let scanner = Scanner::new(config).unwrap();

        let result = scanner.scan(dir.path()).unwrap();

        assert!(result.file_count >= 2); // small.txt, medium.bin, nested.txt (hidden excluded)
        assert!(result.dir_count >= 1);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_scanner_with_hidden() {
        let dir = create_test_dir();
        let config = ScanConfig {
            include_hidden: true,
            ..Default::default()
        };
        let scanner = Scanner::new(config).unwrap();

        let result = scanner.scan(dir.path()).unwrap();

        // Should include .hidden file
        assert!(result.files.iter().any(|f| f.is_hidden()));
    }

    #[test]
    fn test_scanner_exclude_pattern() {
        let dir = create_test_dir();
        let config = ScanConfig {
            exclude_patterns: vec!["*.bin".to_string()],
            ..Default::default()
        };
        let scanner = Scanner::new(config).unwrap();

        let result = scanner.scan(dir.path()).unwrap();

        // Should not include .bin files
        assert!(!result.files.iter().any(|f| f.extension() == Some("bin")));
    }

    #[test]
    fn test_file_size_category() {
        assert_eq!(FileSizeCategory::from_size(100), FileSizeCategory::Tiny);
        assert_eq!(FileSizeCategory::from_size(100_000), FileSizeCategory::Small);
        assert_eq!(FileSizeCategory::from_size(10_000_000), FileSizeCategory::Medium);
        assert_eq!(FileSizeCategory::from_size(500_000_000), FileSizeCategory::Large);
        assert_eq!(FileSizeCategory::from_size(2_000_000_000), FileSizeCategory::Huge);
    }

    #[test]
    fn test_sort_strategies() {
        let dir = create_test_dir();
        let config = ScanConfig::default();
        let scanner = Scanner::new(config).unwrap();

        let mut result = scanner.scan(dir.path()).unwrap();

        // Sort smallest first
        result.sort_files(OrderingStrategy::SmallestFirst);
        for window in result.files.windows(2) {
            assert!(window[0].size <= window[1].size);
        }

        // Sort largest first
        result.sort_files(OrderingStrategy::LargestFirst);
        for window in result.files.windows(2) {
            assert!(window[0].size >= window[1].size);
        }
    }
}
