//! Performance benchmarks for SmartCopy
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

/// Create a test file of the specified size
fn create_test_file(dir: &std::path::Path, name: &str, size: usize) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut file = File::create(&path).unwrap();

    let chunk_size = 64 * 1024;
    let chunk: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();
    let mut remaining = size;

    while remaining > 0 {
        let to_write = remaining.min(chunk_size);
        file.write_all(&chunk[..to_write]).unwrap();
        remaining -= to_write;
    }

    path
}

fn bench_copy_small_files(c: &mut Criterion) {
    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();

    // Create 100 small files
    for i in 0..100 {
        create_test_file(src_dir.path(), &format!("file_{}.txt", i), 1024);
    }

    c.bench_function("copy_100_small_files", |b| {
        b.iter(|| {
            let config = smartcopy::config::CopyConfig {
                source: src_dir.path().to_path_buf(),
                destination: dst_dir.path().to_path_buf(),
                threads: 4,
                ..Default::default()
            };

            let engine = smartcopy::core::CopyEngine::new(config);
            let _ = black_box(engine.execute());

            // Clean destination for next iteration
            for entry in std::fs::read_dir(dst_dir.path()).unwrap() {
                let _ = std::fs::remove_file(entry.unwrap().path());
            }
        });
    });
}

fn bench_copy_large_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_file_copy");

    for size in [1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024].iter() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();

        let src_file = create_test_file(src_dir.path(), "large.bin", *size);

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("standard", humansize::format_size(*size as u64, humansize::BINARY)),
            size,
            |b, _| {
                let dst_file = dst_dir.path().join("large.bin");

                b.iter(|| {
                    let copier = smartcopy::fs::FileCopier::default_copier();
                    let _ = black_box(copier.copy(&src_file, &dst_file));
                    let _ = std::fs::remove_file(&dst_file);
                });
            },
        );
    }

    group.finish();
}

fn bench_hash_algorithms(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_algorithms");

    let data_size = 10 * 1024 * 1024; // 10 MB
    let data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

    group.throughput(Throughput::Bytes(data_size as u64));

    for algo in [
        smartcopy::config::HashAlgorithm::XXHash3,
        smartcopy::config::HashAlgorithm::XXHash64,
        smartcopy::config::HashAlgorithm::Blake3,
        smartcopy::config::HashAlgorithm::Sha256,
    ] {
        group.bench_with_input(
            BenchmarkId::new("hash", algo.name()),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(smartcopy::hash::hash_bytes(data, algo))
                });
            },
        );
    }

    group.finish();
}

fn bench_directory_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();

    // Create test structure
    for i in 0..10 {
        let subdir = dir.path().join(format!("subdir_{}", i));
        std::fs::create_dir_all(&subdir).unwrap();

        for j in 0..100 {
            create_test_file(&subdir, &format!("file_{}.txt", j), 1024);
        }
    }

    c.bench_function("scan_1000_files", |b| {
        b.iter(|| {
            let config = smartcopy::fs::ScanConfig::default();
            let scanner = smartcopy::fs::Scanner::new(config).unwrap();
            black_box(scanner.scan(dir.path()).unwrap())
        });
    });
}

criterion_group!(
    benches,
    bench_copy_small_files,
    bench_copy_large_file,
    bench_hash_algorithms,
    bench_directory_scan
);

criterion_main!(benches);
