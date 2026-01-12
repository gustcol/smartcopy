//! Integrity verification using multiple hash algorithms
//!
//! Supports XXHash3 (ultra-fast), BLAKE3 (fast + secure), and SHA-256.
//! All hashers support streaming for single-pass copy-and-hash operations.

use crate::config::HashAlgorithm;
use crate::error::{IoResultExt, Result, SmartCopyError};
use crate::fs::HashWriter;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::hash::Hasher as StdHasher;
use std::io::{BufReader, Read};
use std::path::Path;

/// Hash result as hex string
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashResult {
    /// The hash algorithm used
    pub algorithm: HashAlgorithm,
    /// Hash value as lowercase hex string
    pub hash: String,
    /// File size in bytes
    pub size: u64,
}

impl HashResult {
    /// Create a new hash result
    pub fn new(algorithm: HashAlgorithm, hash: String, size: u64) -> Self {
        Self { algorithm, hash, size }
    }

    /// Verify against another hash result
    pub fn verify(&self, other: &HashResult) -> bool {
        self.algorithm == other.algorithm && self.hash == other.hash
    }
}

impl std::fmt::Display for HashResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hash)
    }
}

/// Unified hasher that supports all algorithms
pub enum Hasher {
    /// XXHash3 128-bit
    XXHash3(xxhash_rust::xxh3::Xxh3),
    /// XXHash64
    XXHash64(xxhash_rust::xxh64::Xxh64),
    /// BLAKE3
    Blake3(blake3::Hasher),
    /// SHA-256
    Sha256(sha2::Sha256),
}

impl Hasher {
    /// Create a new hasher for the given algorithm
    pub fn new(algorithm: HashAlgorithm) -> Self {
        match algorithm {
            HashAlgorithm::XXHash3 => Self::XXHash3(xxhash_rust::xxh3::Xxh3::new()),
            HashAlgorithm::XXHash64 => Self::XXHash64(xxhash_rust::xxh64::Xxh64::new(0)),
            HashAlgorithm::Blake3 => Self::Blake3(blake3::Hasher::new()),
            HashAlgorithm::Sha256 => {
                use sha2::Digest;
                Self::Sha256(sha2::Sha256::new())
            }
        }
    }

    /// Get the algorithm this hasher uses
    pub fn algorithm(&self) -> HashAlgorithm {
        match self {
            Self::XXHash3(_) => HashAlgorithm::XXHash3,
            Self::XXHash64(_) => HashAlgorithm::XXHash64,
            Self::Blake3(_) => HashAlgorithm::Blake3,
            Self::Sha256(_) => HashAlgorithm::Sha256,
        }
    }

    /// Update the hasher with more data
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::XXHash3(h) => h.update(data),
            Self::XXHash64(h) => h.update(data),
            Self::Blake3(h) => { h.update(data); }
            Self::Sha256(h) => {
                use sha2::Digest;
                h.update(data);
            }
        }
    }

    /// Finalize and get the hash as hex string
    pub fn finalize(self) -> String {
        match self {
            Self::XXHash3(h) => format!("{:032x}", h.digest128()),
            Self::XXHash64(h) => format!("{:016x}", h.digest()),
            Self::Blake3(h) => h.finalize().to_hex().to_string(),
            Self::Sha256(h) => {
                use sha2::Digest;
                let result = h.finalize();
                hex::encode(result)
            }
        }
    }

    /// Reset the hasher for reuse
    pub fn reset(&mut self) {
        match self {
            Self::XXHash3(h) => h.reset(),
            Self::XXHash64(h) => *h = xxhash_rust::xxh64::Xxh64::new(0),
            Self::Blake3(h) => { h.reset(); }
            Self::Sha256(h) => {
                
                sha2::Digest::reset(h);
            }
        }
    }
}

impl HashWriter for Hasher {
    fn update(&mut self, data: &[u8]) {
        Hasher::update(self, data);
    }
}

/// Compute hash of a file
pub fn hash_file(path: &Path, algorithm: HashAlgorithm) -> Result<HashResult> {
    hash_file_with_buffer(path, algorithm, 1024 * 1024) // 1MB buffer
}

/// Compute hash of a file with custom buffer size
pub fn hash_file_with_buffer(
    path: &Path,
    algorithm: HashAlgorithm,
    buffer_size: usize,
) -> Result<HashResult> {
    let file = File::open(path).with_path(path)?;
    let size = file.metadata().with_path(path)?.len();
    let mut reader = BufReader::with_capacity(buffer_size, file);
    let mut hasher = Hasher::new(algorithm);
    let mut buffer = vec![0u8; buffer_size];

    loop {
        let bytes_read = reader.read(&mut buffer)
            .map_err(|e| SmartCopyError::io(path, e))?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    Ok(HashResult::new(algorithm, hasher.finalize(), size))
}

/// Compute hash of data in memory
pub fn hash_bytes(data: &[u8], algorithm: HashAlgorithm) -> HashResult {
    let mut hasher = Hasher::new(algorithm);
    hasher.update(data);
    HashResult::new(algorithm, hasher.finalize(), data.len() as u64)
}

/// Verify file integrity by comparing hashes
pub fn verify_file(path: &Path, expected: &HashResult) -> Result<bool> {
    let actual = hash_file(path, expected.algorithm)?;
    Ok(actual.verify(expected))
}

/// Verify two files have identical content
pub fn verify_files_match(
    source: &Path,
    dest: &Path,
    algorithm: HashAlgorithm,
) -> Result<VerificationResult> {
    let source_hash = hash_file(source, algorithm)?;
    let dest_hash = hash_file(dest, algorithm)?;

    let matches = source_hash.verify(&dest_hash);

    Ok(VerificationResult {
        source_hash,
        dest_hash,
        matches,
    })
}

/// Result of verifying two files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Hash of the source file
    pub source_hash: HashResult,
    /// Hash of the destination file
    pub dest_hash: HashResult,
    /// Whether the hashes match
    pub matches: bool,
}

/// Batch hash multiple files in parallel
pub fn hash_files_parallel(
    paths: &[&Path],
    algorithm: HashAlgorithm,
) -> Vec<Result<HashResult>> {
    paths
        .par_iter()
        .map(|path| hash_file(path, algorithm))
        .collect()
}

/// Streaming hasher for copy-and-hash operations
pub struct StreamingHasher {
    hasher: Hasher,
    bytes_processed: u64,
}

impl StreamingHasher {
    /// Create a new streaming hasher
    pub fn new(algorithm: HashAlgorithm) -> Self {
        Self {
            hasher: Hasher::new(algorithm),
            bytes_processed: 0,
        }
    }

    /// Process a chunk of data
    pub fn process(&mut self, data: &[u8]) {
        self.hasher.update(data);
        self.bytes_processed += data.len() as u64;
    }

    /// Get bytes processed so far
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed
    }

    /// Finalize and get the result
    pub fn finalize(self) -> HashResult {
        let algorithm = self.hasher.algorithm();
        HashResult::new(algorithm, self.hasher.finalize(), self.bytes_processed)
    }
}

impl HashWriter for StreamingHasher {
    fn update(&mut self, data: &[u8]) {
        self.process(data);
    }
}

/// Hash entry for manifest files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHashEntry {
    /// Relative path
    pub path: String,
    /// File size
    pub size: u64,
    /// Hash value
    pub hash: String,
    /// Algorithm used
    pub algorithm: HashAlgorithm,
    /// Modification time (Unix timestamp)
    pub mtime: u64,
}

/// Collection of file hashes (manifest)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashManifest {
    /// Algorithm used for all hashes
    pub algorithm: HashAlgorithm,
    /// Creation timestamp
    pub created: u64,
    /// Root path
    pub root: String,
    /// File entries
    pub entries: Vec<FileHashEntry>,
}

impl HashManifest {
    /// Create a new empty manifest
    pub fn new(algorithm: HashAlgorithm, root: &str) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        Self {
            algorithm,
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            root: root.to_string(),
            entries: Vec::new(),
        }
    }

    /// Add a file entry
    pub fn add_entry(&mut self, entry: FileHashEntry) {
        self.entries.push(entry);
    }

    /// Find entry by path
    pub fn find_entry(&self, path: &str) -> Option<&FileHashEntry> {
        self.entries.iter().find(|e| e.path == path)
    }

    /// Save manifest to JSON file
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))?;
        std::fs::write(path, json).with_path(path)?;
        Ok(())
    }

    /// Load manifest from JSON file
    pub fn load(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path).with_path(path)?;
        serde_json::from_str(&json)
            .map_err(|e| SmartCopyError::ManifestError(e.to_string()))
    }
}

/// Quick hash for change detection (uses XXHash3 by default)
pub fn quick_hash(path: &Path) -> Result<u64> {
    let file = File::open(path).with_path(path)?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buffer = vec![0u8; 64 * 1024];

    loop {
        let bytes_read = reader.read(&mut buffer)
            .map_err(|e| SmartCopyError::io(path, e))?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.digest())
}

/// Benchmark hash algorithms
pub fn benchmark_algorithms(data_size: usize) -> Vec<(HashAlgorithm, std::time::Duration, f64)> {
    let data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();
    let mut results = Vec::new();

    for algorithm in [
        HashAlgorithm::XXHash3,
        HashAlgorithm::XXHash64,
        HashAlgorithm::Blake3,
        HashAlgorithm::Sha256,
    ] {
        let start = std::time::Instant::now();
        let iterations = 10;

        for _ in 0..iterations {
            hash_bytes(&data, algorithm);
        }

        let duration = start.elapsed() / iterations;
        let throughput = (data_size as f64) / duration.as_secs_f64() / (1024.0 * 1024.0);

        results.push((algorithm, duration, throughput));
    }

    results.sort_by(|a, b| a.1.cmp(&b.1));
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write;

    fn create_test_file(dir: &Path, content: &[u8]) -> std::path::PathBuf {
        let path = dir.join("test.bin");
        let mut file = File::create(&path).unwrap();
        file.write_all(content).unwrap();
        path
    }

    #[test]
    fn test_hash_algorithms() {
        let data = b"Hello, World!";

        for algorithm in [
            HashAlgorithm::XXHash3,
            HashAlgorithm::XXHash64,
            HashAlgorithm::Blake3,
            HashAlgorithm::Sha256,
        ] {
            let hash = hash_bytes(data, algorithm);
            assert!(!hash.hash.is_empty());
            assert_eq!(hash.size, data.len() as u64);

            // Verify determinism
            let hash2 = hash_bytes(data, algorithm);
            assert_eq!(hash, hash2);
        }
    }

    #[test]
    fn test_hash_file() {
        let dir = TempDir::new().unwrap();
        let content = b"Test file content for hashing";
        let path = create_test_file(dir.path(), content);

        let file_hash = hash_file(&path, HashAlgorithm::Blake3).unwrap();
        let memory_hash = hash_bytes(content, HashAlgorithm::Blake3);

        assert_eq!(file_hash.hash, memory_hash.hash);
    }

    #[test]
    fn test_verify_files() {
        let dir = TempDir::new().unwrap();
        let content = b"Identical content";

        let path1 = dir.path().join("file1.bin");
        let path2 = dir.path().join("file2.bin");

        std::fs::write(&path1, content).unwrap();
        std::fs::write(&path2, content).unwrap();

        let result = verify_files_match(&path1, &path2, HashAlgorithm::XXHash3).unwrap();
        assert!(result.matches);

        // Modify one file
        std::fs::write(&path2, b"Different content").unwrap();
        let result = verify_files_match(&path1, &path2, HashAlgorithm::XXHash3).unwrap();
        assert!(!result.matches);
    }

    #[test]
    fn test_streaming_hasher() {
        let mut hasher = StreamingHasher::new(HashAlgorithm::Blake3);

        hasher.process(b"Hello, ");
        hasher.process(b"World!");

        let result = hasher.finalize();
        let direct = hash_bytes(b"Hello, World!", HashAlgorithm::Blake3);

        assert_eq!(result.hash, direct.hash);
    }

    #[test]
    fn test_manifest() {
        let dir = TempDir::new().unwrap();
        let mut manifest = HashManifest::new(HashAlgorithm::Blake3, "/test/root");

        manifest.add_entry(FileHashEntry {
            path: "file1.txt".to_string(),
            size: 100,
            hash: "abc123".to_string(),
            algorithm: HashAlgorithm::Blake3,
            mtime: 1234567890,
        });

        let manifest_path = dir.path().join("manifest.json");
        manifest.save(&manifest_path).unwrap();

        let loaded = HashManifest::load(&manifest_path).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].path, "file1.txt");
    }

    #[test]
    fn test_quick_hash() {
        let dir = TempDir::new().unwrap();
        let path = create_test_file(dir.path(), b"Quick hash test content");

        let hash1 = quick_hash(&path).unwrap();
        let hash2 = quick_hash(&path).unwrap();

        assert_eq!(hash1, hash2);
    }
}
