//! Resume interrupted transfers module
//!
//! Provides the ability to resume file transfers that were interrupted
//! due to network issues, system crashes, or manual cancellation.
//! Uses checkpointing and partial file tracking.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// State of a resumable transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferState {
    /// Unique transfer ID
    pub id: String,
    /// Source path
    pub source: PathBuf,
    /// Destination path
    pub destination: PathBuf,
    /// Total size in bytes
    pub total_size: u64,
    /// Bytes transferred so far
    pub bytes_transferred: u64,
    /// File states for multi-file transfers
    pub files: HashMap<PathBuf, FileTransferState>,
    /// Transfer started timestamp
    pub started_at: u64,
    /// Last checkpoint timestamp
    pub last_checkpoint: u64,
    /// Transfer options hash (to detect config changes)
    pub options_hash: u64,
    /// Transfer status
    pub status: TransferStatus,
}

/// State of an individual file transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransferState {
    /// Relative path from source root
    pub relative_path: PathBuf,
    /// File size
    pub size: u64,
    /// Bytes written to destination
    pub bytes_written: u64,
    /// Source file modification time
    pub source_mtime: u64,
    /// Checksum of transferred portion (for verification)
    pub partial_checksum: Option<String>,
    /// File status
    pub status: FileStatus,
}

/// Status of a transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    /// Transfer in progress
    InProgress,
    /// Transfer paused/interrupted
    Interrupted,
    /// Transfer completed successfully
    Completed,
    /// Transfer failed
    Failed,
    /// Transfer cancelled by user
    Cancelled,
}

/// Status of an individual file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileStatus {
    /// Not started
    Pending,
    /// Partially transferred
    Partial,
    /// Fully transferred
    Complete,
    /// Transfer failed
    Failed,
    /// Skipped (already exists and matches)
    Skipped,
}

/// Result of a resume operation
#[derive(Debug, Clone)]
pub struct ResumeResult {
    /// Whether resume was possible
    pub resumed: bool,
    /// Number of files skipped (already complete)
    pub files_skipped: u64,
    /// Bytes skipped (already transferred)
    pub bytes_skipped: u64,
    /// Number of files to transfer
    pub files_remaining: u64,
    /// Bytes remaining to transfer
    pub bytes_remaining: u64,
}

/// Manager for resumable transfers
pub struct ResumeManager {
    /// Directory for storing transfer states
    state_dir: PathBuf,
    /// Checkpoint interval in bytes
    checkpoint_interval: u64,
    /// Buffer size for file operations
    buffer_size: usize,
}

impl ResumeManager {
    /// Create a new resume manager
    pub fn new<P: AsRef<Path>>(state_dir: P) -> io::Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&state_dir)?;

        Ok(Self {
            state_dir,
            checkpoint_interval: 64 * 1024 * 1024, // 64MB default
            buffer_size: 1024 * 1024,              // 1MB buffer
        })
    }

    /// Set checkpoint interval
    pub fn with_checkpoint_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Generate a transfer ID
    pub fn generate_id(source: &Path, destination: &Path) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        source.hash(&mut hasher);
        destination.hash(&mut hasher);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);

        format!("{:016x}", hasher.finish())
    }

    /// Create a new transfer state
    pub fn create_transfer(
        &self,
        id: &str,
        source: &Path,
        destination: &Path,
        files: Vec<(PathBuf, u64)>,
    ) -> io::Result<TransferState> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let total_size: u64 = files.iter().map(|(_, size)| size).sum();

        let file_states: HashMap<PathBuf, FileTransferState> = files
            .into_iter()
            .map(|(path, size)| {
                (
                    path.clone(),
                    FileTransferState {
                        relative_path: path,
                        size,
                        bytes_written: 0,
                        source_mtime: 0,
                        partial_checksum: None,
                        status: FileStatus::Pending,
                    },
                )
            })
            .collect();

        let state = TransferState {
            id: id.to_string(),
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            total_size,
            bytes_transferred: 0,
            files: file_states,
            started_at: now,
            last_checkpoint: now,
            options_hash: 0,
            status: TransferStatus::InProgress,
        };

        self.save_state(&state)?;
        Ok(state)
    }

    /// Load existing transfer state
    pub fn load_state(&self, id: &str) -> io::Result<Option<TransferState>> {
        let path = self.state_path(id);
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let state: TransferState = serde_json::from_reader(reader)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(state))
    }

    /// Save transfer state
    pub fn save_state(&self, state: &TransferState) -> io::Result<()> {
        let path = self.state_path(&state.id);
        let temp_path = path.with_extension("tmp");

        let file = File::create(&temp_path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        std::fs::rename(&temp_path, &path)?;
        Ok(())
    }

    /// Delete transfer state
    pub fn delete_state(&self, id: &str) -> io::Result<()> {
        let path = self.state_path(id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// List all transfer states
    pub fn list_transfers(&self) -> io::Result<Vec<TransferState>> {
        let mut transfers = Vec::new();

        for entry in std::fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Ok(file) = File::open(&path) {
                    let reader = BufReader::new(file);
                    if let Ok(state) = serde_json::from_reader::<_, TransferState>(reader) {
                        transfers.push(state);
                    }
                }
            }
        }

        Ok(transfers)
    }

    /// Check if a transfer can be resumed
    pub fn can_resume(&self, state: &TransferState) -> ResumeResult {
        let mut files_skipped = 0u64;
        let mut bytes_skipped = 0u64;
        let mut files_remaining = 0u64;
        let mut bytes_remaining = 0u64;

        for file_state in state.files.values() {
            match file_state.status {
                FileStatus::Complete | FileStatus::Skipped => {
                    files_skipped += 1;
                    bytes_skipped += file_state.size;
                }
                FileStatus::Partial => {
                    files_remaining += 1;
                    bytes_remaining += file_state.size - file_state.bytes_written;
                    bytes_skipped += file_state.bytes_written;
                }
                FileStatus::Pending | FileStatus::Failed => {
                    files_remaining += 1;
                    bytes_remaining += file_state.size;
                }
            }
        }

        let resumed = state.status == TransferStatus::Interrupted
            && files_remaining > 0
            && state.bytes_transferred > 0;

        ResumeResult {
            resumed,
            files_skipped,
            bytes_skipped,
            files_remaining,
            bytes_remaining,
        }
    }

    /// Resume a file transfer from where it left off
    pub fn resume_file<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        src: P,
        dst: Q,
        file_state: &FileTransferState,
    ) -> io::Result<u64> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        // Open source file
        let mut src_file = File::open(src)?;
        let src_size = src_file.metadata()?.len();

        // Verify source hasn't changed
        if src_size != file_state.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Source file size changed",
            ));
        }

        // Open or create destination
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(dst)?;

        let resume_offset = file_state.bytes_written;

        // Seek to resume position
        if resume_offset > 0 {
            src_file.seek(SeekFrom::Start(resume_offset))?;
            dst_file.seek(SeekFrom::Start(resume_offset))?;

            // Verify partial content matches (optional)
            if let Some(ref checksum) = file_state.partial_checksum {
                if !self.verify_partial(&dst, resume_offset, checksum)? {
                    // Partial content doesn't match, restart from beginning
                    src_file.seek(SeekFrom::Start(0))?;
                    dst_file.seek(SeekFrom::Start(0))?;
                    dst_file.set_len(0)?;
                    return self.copy_with_progress(&mut src_file, &mut dst_file, src_size, 0);
                }
            }
        }

        self.copy_with_progress(&mut src_file, &mut dst_file, src_size, resume_offset)
    }

    /// Copy file with progress tracking
    fn copy_with_progress(
        &self,
        src: &mut File,
        dst: &mut File,
        total_size: u64,
        start_offset: u64,
    ) -> io::Result<u64> {
        let mut buffer = vec![0u8; self.buffer_size];
        let mut bytes_copied = 0u64;
        let remaining = total_size - start_offset;

        while bytes_copied < remaining {
            let to_read = std::cmp::min(self.buffer_size as u64, remaining - bytes_copied) as usize;
            let bytes_read = src.read(&mut buffer[..to_read])?;

            if bytes_read == 0 {
                break;
            }

            dst.write_all(&buffer[..bytes_read])?;
            bytes_copied += bytes_read as u64;
        }

        dst.flush()?;
        Ok(bytes_copied)
    }

    /// Verify partial file content
    fn verify_partial(&self, path: &Path, size: u64, expected_checksum: &str) -> io::Result<bool> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut file = File::open(path)?;
        let mut hasher = DefaultHasher::new();
        let mut buffer = vec![0u8; self.buffer_size];
        let mut bytes_read_total = 0u64;

        while bytes_read_total < size {
            let to_read =
                std::cmp::min(self.buffer_size as u64, size - bytes_read_total) as usize;
            let bytes_read = file.read(&mut buffer[..to_read])?;

            if bytes_read == 0 {
                break;
            }

            hasher.write(&buffer[..bytes_read]);
            bytes_read_total += bytes_read as u64;
        }

        let checksum = format!("{:016x}", hasher.finish());
        Ok(checksum == expected_checksum)
    }

    /// Calculate checksum of file portion
    pub fn calculate_partial_checksum(&self, path: &Path, size: u64) -> io::Result<String> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut file = File::open(path)?;
        let mut hasher = DefaultHasher::new();
        let mut buffer = vec![0u8; self.buffer_size];
        let mut bytes_read_total = 0u64;

        while bytes_read_total < size {
            let to_read =
                std::cmp::min(self.buffer_size as u64, size - bytes_read_total) as usize;
            let bytes_read = file.read(&mut buffer[..to_read])?;

            if bytes_read == 0 {
                break;
            }

            hasher.write(&buffer[..bytes_read]);
            bytes_read_total += bytes_read as u64;
        }

        Ok(format!("{:016x}", hasher.finish()))
    }

    /// Get state file path
    fn state_path(&self, id: &str) -> PathBuf {
        self.state_dir.join(format!("{}.json", id))
    }

    /// Update file state
    pub fn update_file_state(
        &self,
        state: &mut TransferState,
        path: &Path,
        bytes_written: u64,
        status: FileStatus,
    ) -> io::Result<()> {
        if let Some(file_state) = state.files.get_mut(path) {
            file_state.bytes_written = bytes_written;
            file_state.status = status;

            // Recalculate total bytes transferred
            state.bytes_transferred = state
                .files
                .values()
                .map(|f| f.bytes_written)
                .sum();

            state.last_checkpoint = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        // Save checkpoint if needed
        if state.bytes_transferred % self.checkpoint_interval < self.buffer_size as u64 {
            self.save_state(state)?;
        }

        Ok(())
    }

    /// Mark transfer as complete
    pub fn complete_transfer(&self, state: &mut TransferState) -> io::Result<()> {
        state.status = TransferStatus::Completed;
        self.save_state(state)?;
        Ok(())
    }

    /// Mark transfer as interrupted
    pub fn interrupt_transfer(&self, state: &mut TransferState) -> io::Result<()> {
        state.status = TransferStatus::Interrupted;
        self.save_state(state)?;
        Ok(())
    }

    /// Clean up old/completed transfers
    pub fn cleanup(&self, max_age_days: u64) -> io::Result<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let max_age_secs = max_age_days * 24 * 60 * 60;
        let mut cleaned = 0;

        for entry in std::fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Ok(file) = File::open(&path) {
                    let reader = BufReader::new(file);
                    if let Ok(state) = serde_json::from_reader::<_, TransferState>(reader) {
                        let age = now.saturating_sub(state.last_checkpoint);
                        let should_clean = age > max_age_secs
                            || state.status == TransferStatus::Completed
                            || state.status == TransferStatus::Cancelled;

                        if should_clean {
                            std::fs::remove_file(&path)?;
                            cleaned += 1;
                        }
                    }
                }
            }
        }

        Ok(cleaned)
    }
}

/// Resumable file writer with automatic checkpointing
pub struct ResumableWriter {
    file: File,
    path: PathBuf,
    bytes_written: u64,
    checkpoint_interval: u64,
    last_checkpoint: u64,
}

impl ResumableWriter {
    /// Create a new resumable writer
    pub fn new<P: AsRef<Path>>(path: P, resume_offset: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)?;

        if resume_offset > 0 {
            file.seek(SeekFrom::Start(resume_offset))?;
        }

        Ok(Self {
            file,
            path,
            bytes_written: resume_offset,
            checkpoint_interval: 64 * 1024 * 1024,
            last_checkpoint: resume_offset,
        })
    }

    /// Write data
    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let written = self.file.write(data)?;
        self.bytes_written += written as u64;
        Ok(written)
    }

    /// Flush and checkpoint
    pub fn checkpoint(&mut self) -> io::Result<u64> {
        self.file.flush()?;
        self.file.sync_data()?;
        self.last_checkpoint = self.bytes_written;
        Ok(self.bytes_written)
    }

    /// Check if checkpoint is needed
    pub fn needs_checkpoint(&self) -> bool {
        self.bytes_written - self.last_checkpoint >= self.checkpoint_interval
    }

    /// Get bytes written
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Finish writing
    pub fn finish(mut self) -> io::Result<u64> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(self.bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_resume_manager_create_and_load() {
        let dir = tempdir().unwrap();
        let manager = ResumeManager::new(dir.path()).unwrap();

        let files = vec![
            (PathBuf::from("file1.txt"), 1000),
            (PathBuf::from("file2.txt"), 2000),
        ];

        let state = manager
            .create_transfer(
                "test123",
                Path::new("/src"),
                Path::new("/dst"),
                files,
            )
            .unwrap();

        assert_eq!(state.total_size, 3000);
        assert_eq!(state.files.len(), 2);

        let loaded = manager.load_state("test123").unwrap().unwrap();
        assert_eq!(loaded.id, "test123");
        assert_eq!(loaded.total_size, 3000);
    }

    #[test]
    fn test_can_resume() {
        let dir = tempdir().unwrap();
        let manager = ResumeManager::new(dir.path()).unwrap();

        let files = vec![(PathBuf::from("file1.txt"), 1000)];

        let mut state = manager
            .create_transfer("test456", Path::new("/src"), Path::new("/dst"), files)
            .unwrap();

        state.status = TransferStatus::Interrupted;
        state.bytes_transferred = 500;
        state.files.get_mut(&PathBuf::from("file1.txt")).unwrap().bytes_written = 500;
        state.files.get_mut(&PathBuf::from("file1.txt")).unwrap().status = FileStatus::Partial;

        let result = manager.can_resume(&state);
        assert!(result.resumed);
        assert_eq!(result.bytes_skipped, 500);
        assert_eq!(result.bytes_remaining, 500);
    }
}
