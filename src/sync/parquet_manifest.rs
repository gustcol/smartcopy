//! Parquet-based manifest storage for high-performance metadata handling
//!
//! Uses Apache Arrow columnar format with Parquet file storage for efficient
//! manifest operations on large file sets (millions of files).
//! Provides ZSTD compression for optimal storage density.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;

/// Maximum records per Arrow RecordBatch chunk.
/// Prevents StringBuilder overflow on very long file paths.
const CHUNK_SIZE: usize = 500_000;

/// Schema definition for the manifest Parquet file.
fn manifest_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("mtime_secs", DataType::Int64, false),
        Field::new("mtime_nsecs", DataType::UInt32, true),
        Field::new("permissions", DataType::UInt32, true),
        Field::new("uid", DataType::UInt32, true),
        Field::new("gid", DataType::UInt32, true),
        Field::new("setuid", DataType::Boolean, true),
        Field::new("setgid", DataType::Boolean, true),
        Field::new("sticky", DataType::Boolean, true),
        Field::new("file_type", DataType::Utf8, true),
        Field::new("symlink_target", DataType::Utf8, true),
        Field::new("xxhash3", DataType::Utf8, true),
        Field::new("blake3", DataType::Utf8, true),
    ]))
}

/// A single file entry in the manifest.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nsecs: Option<u32>,
    pub permissions: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub setuid: Option<bool>,
    pub setgid: Option<bool>,
    pub sticky: Option<bool>,
    pub file_type: Option<String>,
    pub symlink_target: Option<String>,
    pub xxhash3: Option<String>,
    pub blake3: Option<String>,
}

/// Writes manifest entries to a Parquet file with ZSTD compression.
pub struct ParquetManifestWriter {
    writer: ArrowWriter<File>,
    buffer: Vec<ManifestEntry>,
    total_written: usize,
}

impl ParquetManifestWriter {
    /// Create a new writer that outputs to the given path.
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let schema = manifest_schema();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(CHUNK_SIZE)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            buffer: Vec::with_capacity(CHUNK_SIZE),
            total_written: 0,
        })
    }

    /// Add a single entry to the buffer. Flushes automatically at CHUNK_SIZE.
    pub fn add_entry(&mut self, entry: ManifestEntry) -> Result<(), Box<dyn std::error::Error>> {
        self.buffer.push(entry);
        if self.buffer.len() >= CHUNK_SIZE {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Flush the internal buffer as a RecordBatch.
    fn flush_buffer(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let len = self.buffer.len();
        let mut path_builder = StringBuilder::with_capacity(len, len * 64);
        let mut size_builder = UInt64Builder::with_capacity(len);
        let mut mtime_secs_builder = Int64Builder::with_capacity(len);
        let mut mtime_nsecs_builder = UInt32Builder::with_capacity(len);
        let mut perms_builder = UInt32Builder::with_capacity(len);
        let mut uid_builder = UInt32Builder::with_capacity(len);
        let mut gid_builder = UInt32Builder::with_capacity(len);
        let mut setuid_builder = BooleanBuilder::with_capacity(len);
        let mut setgid_builder = BooleanBuilder::with_capacity(len);
        let mut sticky_builder = BooleanBuilder::with_capacity(len);
        let mut ftype_builder = StringBuilder::with_capacity(len, len * 8);
        let mut symlink_builder = StringBuilder::with_capacity(len, len * 32);
        let mut xxhash3_builder = StringBuilder::with_capacity(len, len * 32);
        let mut blake3_builder = StringBuilder::with_capacity(len, len * 64);

        for entry in self.buffer.drain(..) {
            path_builder.append_value(&entry.path);
            size_builder.append_value(entry.size);
            mtime_secs_builder.append_value(entry.mtime_secs);
            match entry.mtime_nsecs {
                Some(v) => mtime_nsecs_builder.append_value(v),
                None => mtime_nsecs_builder.append_null(),
            }
            match entry.permissions {
                Some(v) => perms_builder.append_value(v),
                None => perms_builder.append_null(),
            }
            match entry.uid {
                Some(v) => uid_builder.append_value(v),
                None => uid_builder.append_null(),
            }
            match entry.gid {
                Some(v) => gid_builder.append_value(v),
                None => gid_builder.append_null(),
            }
            match entry.setuid {
                Some(v) => setuid_builder.append_value(v),
                None => setuid_builder.append_null(),
            }
            match entry.setgid {
                Some(v) => setgid_builder.append_value(v),
                None => setgid_builder.append_null(),
            }
            match entry.sticky {
                Some(v) => sticky_builder.append_value(v),
                None => sticky_builder.append_null(),
            }
            match entry.file_type {
                Some(ref v) => ftype_builder.append_value(v),
                None => ftype_builder.append_null(),
            }
            match entry.symlink_target {
                Some(ref v) => symlink_builder.append_value(v),
                None => symlink_builder.append_null(),
            }
            match entry.xxhash3 {
                Some(ref v) => xxhash3_builder.append_value(v),
                None => xxhash3_builder.append_null(),
            }
            match entry.blake3 {
                Some(ref v) => blake3_builder.append_value(v),
                None => blake3_builder.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(mtime_secs_builder.finish()),
            Arc::new(mtime_nsecs_builder.finish()),
            Arc::new(perms_builder.finish()),
            Arc::new(uid_builder.finish()),
            Arc::new(gid_builder.finish()),
            Arc::new(setuid_builder.finish()),
            Arc::new(setgid_builder.finish()),
            Arc::new(sticky_builder.finish()),
            Arc::new(ftype_builder.finish()),
            Arc::new(symlink_builder.finish()),
            Arc::new(xxhash3_builder.finish()),
            Arc::new(blake3_builder.finish()),
        ];

        let batch = RecordBatch::try_new(manifest_schema(), columns)?;
        self.writer.write(&batch)?;
        self.total_written += len;

        Ok(())
    }

    /// Finalize and close the Parquet file.
    pub fn finish(mut self) -> Result<usize, Box<dyn std::error::Error>> {
        self.flush_buffer()?;
        self.writer.close()?;
        Ok(self.total_written)
    }
}

/// Reads manifest entries from a Parquet file.
pub struct ParquetManifestReader;

impl ParquetManifestReader {
    /// Read all entries from a Parquet manifest file.
    pub fn read_all(path: &Path) -> Result<Vec<ManifestEntry>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(CHUNK_SIZE)
            .build()?;

        let mut entries = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            let path_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("path column");
            let size_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .expect("size column");
            let mtime_secs_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("mtime_secs column");
            let mtime_nsecs_col = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .expect("mtime_nsecs column");
            let perms_col = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .expect("permissions column");
            let uid_col = batch
                .column(5)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .expect("uid column");
            let gid_col = batch
                .column(6)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .expect("gid column");
            let setuid_col = batch
                .column(7)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("setuid column");
            let setgid_col = batch
                .column(8)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("setgid column");
            let sticky_col = batch
                .column(9)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("sticky column");
            let ftype_col = batch
                .column(10)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("file_type column");
            let symlink_col = batch
                .column(11)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("symlink_target column");
            let xxhash3_col = batch
                .column(12)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("xxhash3 column");
            let blake3_col = batch
                .column(13)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("blake3 column");

            for i in 0..num_rows {
                entries.push(ManifestEntry {
                    path: path_col.value(i).to_string(),
                    size: size_col.value(i),
                    mtime_secs: mtime_secs_col.value(i),
                    mtime_nsecs: if mtime_nsecs_col.is_null(i) {
                        None
                    } else {
                        Some(mtime_nsecs_col.value(i))
                    },
                    permissions: if perms_col.is_null(i) {
                        None
                    } else {
                        Some(perms_col.value(i))
                    },
                    uid: if uid_col.is_null(i) {
                        None
                    } else {
                        Some(uid_col.value(i))
                    },
                    gid: if gid_col.is_null(i) {
                        None
                    } else {
                        Some(gid_col.value(i))
                    },
                    setuid: if setuid_col.is_null(i) {
                        None
                    } else {
                        Some(setuid_col.value(i))
                    },
                    setgid: if setgid_col.is_null(i) {
                        None
                    } else {
                        Some(setgid_col.value(i))
                    },
                    sticky: if sticky_col.is_null(i) {
                        None
                    } else {
                        Some(sticky_col.value(i))
                    },
                    file_type: if ftype_col.is_null(i) {
                        None
                    } else {
                        Some(ftype_col.value(i).to_string())
                    },
                    symlink_target: if symlink_col.is_null(i) {
                        None
                    } else {
                        Some(symlink_col.value(i).to_string())
                    },
                    xxhash3: if xxhash3_col.is_null(i) {
                        None
                    } else {
                        Some(xxhash3_col.value(i).to_string())
                    },
                    blake3: if blake3_col.is_null(i) {
                        None
                    } else {
                        Some(blake3_col.value(i).to_string())
                    },
                });
            }
        }

        Ok(entries)
    }

    /// Get the number of entries without loading all data.
    pub fn count_entries(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = parquet::file::reader::SerializedFileReader::new(file)?;
        let parquet_meta = reader.metadata();
        let total: i64 = parquet_meta
            .row_groups()
            .iter()
            .map(|rg: &parquet::file::metadata::RowGroupMetaData| rg.num_rows())
            .sum();
        Ok(total as usize)
    }

    /// Get the manifest file path with .parquet extension.
    pub fn manifest_path(base_dir: &Path) -> PathBuf {
        base_dir.join(".smartcopy-manifest.parquet")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_parquet_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("test-manifest.parquet");

        // Write entries
        let mut writer = ParquetManifestWriter::new(&manifest_path).unwrap();
        for i in 0..100 {
            writer
                .add_entry(ManifestEntry {
                    path: format!("/data/file_{}.txt", i),
                    size: i as u64 * 1024,
                    mtime_secs: 1700000000 + i as i64,
                    mtime_nsecs: Some(123456789),
                    permissions: Some(0o644),
                    uid: Some(1000),
                    gid: Some(1000),
                    setuid: Some(false),
                    setgid: Some(false),
                    sticky: Some(false),
                    file_type: Some("file".to_string()),
                    symlink_target: None,
                    xxhash3: Some(format!("{:032x}", i)),
                    blake3: None,
                })
                .unwrap();
        }
        let written = writer.finish().unwrap();
        assert_eq!(written, 100);

        // Read back
        let entries = ParquetManifestReader::read_all(&manifest_path).unwrap();
        assert_eq!(entries.len(), 100);
        assert_eq!(entries[0].path, "/data/file_0.txt");
        assert_eq!(entries[99].size, 99 * 1024);
        assert_eq!(entries[0].permissions, Some(0o644));

        // Count
        let count = ParquetManifestReader::count_entries(&manifest_path).unwrap();
        assert_eq!(count, 100);
    }
}
