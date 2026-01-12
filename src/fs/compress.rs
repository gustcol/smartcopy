//! LZ4 compression support for network transfers
//!
//! Provides ultra-fast compression with minimal CPU overhead.
//! LZ4 can compress/decompress at 500+ MB/s, making it ideal
//! for network transfers where bandwidth is the bottleneck.

use crate::error::{IoResultExt, Result, SmartCopyError};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Default compression block size (4 MB)
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Original size before compression
    pub original_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Compression ratio (compressed / original)
    pub ratio: f64,
    /// Compression speed in bytes/second (original bytes)
    pub speed: f64,
}

impl CompressionStats {
    /// Calculate compression ratio
    pub fn calculate_ratio(&mut self) {
        if self.original_size > 0 {
            self.ratio = self.compressed_size as f64 / self.original_size as f64;
        }
    }

    /// Get space saved as a percentage
    pub fn space_saved_percent(&self) -> f64 {
        if self.original_size > 0 {
            (1.0 - self.ratio) * 100.0
        } else {
            0.0
        }
    }
}

/// LZ4 compressor for file operations
pub struct Lz4Compressor {
    block_size: usize,
}

impl Default for Lz4Compressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Lz4Compressor {
    /// Create a new LZ4 compressor with default block size
    pub fn new() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    /// Create with custom block size
    pub fn with_block_size(block_size: usize) -> Self {
        Self { block_size }
    }

    /// Compress a file to another file
    pub fn compress_file(&self, source: &Path, dest: &Path) -> Result<CompressionStats> {
        let start = std::time::Instant::now();

        let src_file = File::open(source).with_path(source)?;
        let _src_size = src_file.metadata().with_path(source)?.len();
        let mut reader = BufReader::with_capacity(self.block_size, src_file);

        let dst_file = File::create(dest).with_path(dest)?;
        let mut writer = BufWriter::with_capacity(self.block_size, dst_file);

        let mut buffer = vec![0u8; self.block_size];
        let mut total_original = 0u64;
        let mut total_compressed = 0u64;

        loop {
            let bytes_read = reader.read(&mut buffer)
                .map_err(|e| SmartCopyError::io(source, e))?;

            if bytes_read == 0 {
                break;
            }

            // Compress the block
            let compressed = compress_prepend_size(&buffer[..bytes_read]);

            // Write block size header (for streaming decompression)
            let block_len = compressed.len() as u32;
            writer.write_all(&block_len.to_le_bytes())
                .map_err(|e| SmartCopyError::io(dest, e))?;

            // Write compressed data
            writer.write_all(&compressed)
                .map_err(|e| SmartCopyError::io(dest, e))?;

            total_original += bytes_read as u64;
            total_compressed += 4 + compressed.len() as u64; // 4 bytes for length header
        }

        writer.flush().with_path(dest)?;

        let duration = start.elapsed();
        let mut stats = CompressionStats {
            original_size: total_original,
            compressed_size: total_compressed,
            ratio: 0.0,
            speed: total_original as f64 / duration.as_secs_f64(),
        };
        stats.calculate_ratio();

        Ok(stats)
    }

    /// Decompress a file to another file
    pub fn decompress_file(&self, source: &Path, dest: &Path) -> Result<CompressionStats> {
        let start = std::time::Instant::now();

        let src_file = File::open(source).with_path(source)?;
        let _src_size = src_file.metadata().with_path(source)?.len();
        let mut reader = BufReader::with_capacity(self.block_size, src_file);

        let dst_file = File::create(dest).with_path(dest)?;
        let mut writer = BufWriter::with_capacity(self.block_size, dst_file);

        let mut total_compressed = 0u64;
        let mut total_original = 0u64;
        let mut len_buf = [0u8; 4];

        loop {
            // Read block length header
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(SmartCopyError::io(source, e)),
            }

            let block_len = u32::from_le_bytes(len_buf) as usize;
            total_compressed += 4 + block_len as u64;

            // Read compressed block
            let mut compressed = vec![0u8; block_len];
            reader.read_exact(&mut compressed)
                .map_err(|e| SmartCopyError::io(source, e))?;

            // Decompress
            let decompressed = decompress_size_prepended(&compressed)
                .map_err(|e| SmartCopyError::CompressionError(
                    format!("LZ4 decompression failed: {}", e)
                ))?;

            writer.write_all(&decompressed)
                .map_err(|e| SmartCopyError::io(dest, e))?;

            total_original += decompressed.len() as u64;
        }

        writer.flush().with_path(dest)?;

        let duration = start.elapsed();
        let mut stats = CompressionStats {
            original_size: total_original,
            compressed_size: total_compressed,
            ratio: 0.0,
            speed: total_original as f64 / duration.as_secs_f64(),
        };
        stats.calculate_ratio();

        Ok(stats)
    }

    /// Compress data in memory
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        compress_prepend_size(data)
    }

    /// Decompress data in memory
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        decompress_size_prepended(data)
            .map_err(|e| SmartCopyError::CompressionError(
                format!("LZ4 decompression failed: {}", e)
            ))
    }

    /// Copy a file with on-the-fly compression
    /// Writes compressed data to destination, returns original size
    pub fn copy_compressed(&self, source: &Path, dest: &Path) -> Result<(u64, CompressionStats)> {
        let stats = self.compress_file(source, dest)?;
        Ok((stats.original_size, stats))
    }
}

/// Streaming LZ4 compressor for network transfers
pub struct Lz4StreamCompressor {
    buffer: Vec<u8>,
    block_size: usize,
}

impl Lz4StreamCompressor {
    pub fn new(block_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(block_size),
            block_size,
        }
    }

    /// Add data to the compression buffer
    /// Returns compressed blocks if buffer is full
    pub fn write(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        let mut blocks = Vec::new();
        let mut remaining = data;

        while !remaining.is_empty() {
            let space = self.block_size - self.buffer.len();
            let to_copy = remaining.len().min(space);

            self.buffer.extend_from_slice(&remaining[..to_copy]);
            remaining = &remaining[to_copy..];

            if self.buffer.len() >= self.block_size {
                blocks.push(self.flush_block());
            }
        }

        blocks
    }

    /// Flush remaining data as a compressed block
    pub fn finish(mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(self.flush_block())
        }
    }

    fn flush_block(&mut self) -> Vec<u8> {
        let compressed = compress_prepend_size(&self.buffer);
        self.buffer.clear();

        // Prepend length header
        let mut result = Vec::with_capacity(4 + compressed.len());
        result.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        result.extend_from_slice(&compressed);
        result
    }
}

/// Streaming LZ4 decompressor for network transfers
pub struct Lz4StreamDecompressor {
    pending: Vec<u8>,
}

impl Lz4StreamDecompressor {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Feed compressed data and get decompressed blocks
    pub fn write(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.pending.extend_from_slice(data);

        let mut blocks = Vec::new();

        while self.pending.len() >= 4 {
            let block_len = u32::from_le_bytes([
                self.pending[0],
                self.pending[1],
                self.pending[2],
                self.pending[3],
            ]) as usize;

            if self.pending.len() < 4 + block_len {
                break; // Wait for more data
            }

            let compressed = &self.pending[4..4 + block_len];
            let decompressed = decompress_size_prepended(compressed)
                .map_err(|e| SmartCopyError::CompressionError(
                    format!("LZ4 decompression failed: {}", e)
                ))?;

            blocks.push(decompressed);
            self.pending.drain(..4 + block_len);
        }

        Ok(blocks)
    }

    /// Check if there's pending data
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

impl Default for Lz4StreamDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write as IoWrite;

    #[test]
    fn test_compress_decompress_memory() {
        let compressor = Lz4Compressor::new();

        let original = b"Hello, World! This is a test of LZ4 compression.".repeat(100);

        let compressed = compressor.compress(&original);
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(original.as_slice(), decompressed.as_slice());
        assert!(compressed.len() < original.len()); // Should compress
    }

    #[test]
    fn test_compress_decompress_file() {
        let dir = TempDir::new().unwrap();

        // Create test file
        let src_path = dir.path().join("source.txt");
        let compressed_path = dir.path().join("compressed.lz4");
        let decompressed_path = dir.path().join("decompressed.txt");

        let original_data = b"Test data for compression ".repeat(1000);
        std::fs::write(&src_path, &original_data).unwrap();

        let compressor = Lz4Compressor::new();

        // Compress
        let compress_stats = compressor.compress_file(&src_path, &compressed_path).unwrap();
        assert!(compress_stats.ratio < 1.0); // Should compress

        // Decompress
        let decompress_stats = compressor.decompress_file(&compressed_path, &decompressed_path).unwrap();

        // Verify
        let decompressed = std::fs::read(&decompressed_path).unwrap();
        assert_eq!(original_data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_streaming_compress() {
        let mut compressor = Lz4StreamCompressor::new(1024);

        let data = b"Hello, streaming world!".repeat(100);

        let blocks = compressor.write(&data);
        let final_block = compressor.finish();

        // Should have produced some blocks
        assert!(!blocks.is_empty() || final_block.is_some());
    }
}
