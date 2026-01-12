//! SmartCopy CLI - High-Performance File Copy Utility
//!
//! A blazingly fast, intelligent file copy utility for HPC environments.

use clap::Parser;
use smartcopy::config::{AgentProtocol, CliArgs, Commands, CopyConfig, HighSpeedTier, WorkloadType};
use smartcopy::core::CopyEngine;
use smartcopy::error::Result;
use smartcopy::hash::{benchmark_algorithms, verify_files_match};
use smartcopy::network::{AgentServer, CertificateManager, QuicServer, SshTuningRecommendations};
use smartcopy::progress::ProgressReporter;
use smartcopy::sync::{IncrementalSync, SyncManifest};
use smartcopy::system::{HighSpeedNetworkGuide, NetworkSpeedTier, SystemInfo, TuningAnalyzer};
use std::path::Path;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Handle result
    if let Err(e) = run(args) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(args: CliArgs) -> Result<()> {
    // Handle subcommands
    if let Some(command) = &args.command {
        return handle_command(command, &args);
    }

    // Require source and destination for copy
    if args.source.is_none() || args.destination.is_none() {
        eprintln!("Usage: smartcopy <SOURCE> <DESTINATION> [OPTIONS]");
        eprintln!("       smartcopy --help for more information");
        eprintln!("       smartcopy analyze    - Analyze system resources");
        eprintln!("       smartcopy tuning     - Show tuning recommendations");
        std::process::exit(1);
    }

    // Build configuration
    let config = CopyConfig::from_cli(&args)
        .map_err(|e| smartcopy::error::SmartCopyError::ConfigError(e))?;

    // Print configuration if verbose
    if args.verbose > 0 {
        print_config(&config);
    }

    // Create progress reporter
    let progress = if args.quiet {
        ProgressReporter::disabled()
    } else if args.progress {
        ProgressReporter::new()
    } else {
        ProgressReporter::disabled()
    };

    // Create and run copy engine
    let engine = CopyEngine::new(config.clone()).with_progress(progress);

    if args.dry_run {
        println!("=== Dry Run Mode ===");
        println!("No files will be copied.");
        println!();
    }

    let result = engine.execute()?;

    // Print results
    if !args.quiet {
        result.print_summary();
    }

    if !result.is_success() {
        std::process::exit(1);
    }

    Ok(())
}

fn handle_command(command: &Commands, _args: &CliArgs) -> Result<()> {
    match command {
        Commands::AnalyzeSystem { detailed } => {
            cmd_analyze_system(*detailed)
        }
        Commands::Tuning { workload } => {
            cmd_tuning(*workload)
        }
        Commands::Verify { source, destination, algorithm } => {
            cmd_verify(source, destination, *algorithm)
        }
        Commands::Status { manifest } => {
            cmd_status(manifest)
        }
        Commands::Server { port, bind } => {
            cmd_server(*port, bind)
        }
        Commands::Benchmark { path, size } => {
            cmd_benchmark(path, size)
        }
        Commands::Agent { protocol, port, bind } => {
            cmd_agent(*protocol, *port, bind)
        }
        Commands::QuicServer { port, bind, cert, key } => {
            cmd_quic_server(*port, bind, cert.as_deref(), key.as_deref())
        }
        Commands::HighSpeed { speed } => {
            cmd_highspeed(*speed)
        }
    }
}

fn cmd_analyze_system(detailed: bool) -> Result<()> {
    println!("Analyzing system resources...\n");

    let system_info = if detailed {
        // Include storage info for current directory
        let cwd = std::env::current_dir().unwrap_or_default();
        SystemInfo::collect_with_paths(&[cwd.as_path()])
    } else {
        SystemInfo::collect()
    };

    system_info.print_summary();

    // Run I/O benchmark if detailed
    if detailed {
        println!("\n=== I/O Benchmark ===");
        let tester = smartcopy::system::BandwidthTester::new(64 * 1024 * 1024); // 64MB
        let temp_dir = std::env::temp_dir();

        match tester.test_local_io(&temp_dir) {
            Ok(metrics) => {
                println!("Write speed: {:.1} MB/s", metrics.write_mbps);
                println!("Read speed:  {:.1} MB/s", metrics.read_mbps);
            }
            Err(e) => {
                println!("I/O benchmark failed: {}", e);
            }
        }

        println!("\n=== Hash Algorithm Benchmark ===");
        let results = benchmark_algorithms(10 * 1024 * 1024); // 10MB
        for (algo, duration, throughput) in results {
            println!("{:12} {:>10.2?}  {:>8.1} MB/s", algo.name(), duration, throughput);
        }
    }

    Ok(())
}

fn cmd_tuning(workload: WorkloadType) -> Result<()> {
    println!("Generating tuning recommendations for {:?} workload...\n", workload);

    let system_info = SystemInfo::collect();
    let analyzer = TuningAnalyzer::new(system_info, workload);
    analyzer.print_recommendations();

    Ok(())
}

fn cmd_verify(source: &str, destination: &str, algorithm: smartcopy::config::HashAlgorithm) -> Result<()> {
    let source_path = Path::new(source);
    let dest_path = Path::new(destination);

    if source_path.is_file() && dest_path.is_file() {
        // Verify single files
        println!("Verifying {} and {}...", source, destination);

        let result = verify_files_match(source_path, dest_path, algorithm)?;

        println!("Source hash:      {}", result.source_hash);
        println!("Destination hash: {}", result.dest_hash);
        println!("Match: {}", if result.matches { "YES ✓" } else { "NO ✗" });

        if !result.matches {
            std::process::exit(1);
        }
    } else if source_path.is_dir() && dest_path.is_dir() {
        // Verify directories
        println!("Verifying directories {} and {}...", source, destination);

        let sync = IncrementalSync::new();
        let analysis = sync.analyze(source_path, dest_path)?;

        println!("\nSource files:      {}", analysis.source_count);
        println!("Destination files: {}", analysis.dest_count);
        println!("Matching:          {}", analysis.to_skip.len());
        println!("Different:         {}", analysis.to_copy.len());

        if !analysis.to_copy.is_empty() {
            println!("\nDifferent files:");
            for change in &analysis.to_copy {
                println!("  {} ({:?})", change.path, change.action);
            }
            std::process::exit(1);
        }
    } else {
        eprintln!("Source and destination must both be files or both be directories");
        std::process::exit(1);
    }

    Ok(())
}

fn cmd_status(manifest_path: &Path) -> Result<()> {
    let manifest = SyncManifest::load(manifest_path)?;
    manifest.print_summary();

    println!("\nRecent entries:");
    for entry in manifest.entries.iter().take(10) {
        println!("  {} ({} bytes)", entry.path, entry.size);
    }

    if manifest.entries.len() > 10 {
        println!("  ... and {} more", manifest.entries.len() - 10);
    }

    Ok(())
}

fn cmd_server(port: u16, bind: &str) -> Result<()> {
    use smartcopy::network::TcpServer;

    let addr = format!("{}:{}", bind, port);
    let cwd = std::env::current_dir().unwrap_or_default();

    println!("Starting SmartCopy TCP server...");
    println!("Listening on: {}", addr);
    println!("Serving from: {:?}", cwd);
    println!("Press Ctrl+C to stop.");

    let server = TcpServer::bind(&addr, &cwd)?;
    server.run()?;

    Ok(())
}

fn cmd_benchmark(path: &Path, size: &str) -> Result<()> {
    use smartcopy::config::parse_size;
    use smartcopy::fs::FileCopier;
    use std::fs::File;
    use std::io::Write;

    let size_bytes = parse_size(size)
        .map_err(|e| smartcopy::error::SmartCopyError::ConfigError(e))?;

    println!("=== SmartCopy Benchmark ===");
    println!("Test file size: {}", humansize::format_size(size_bytes, humansize::BINARY));
    println!("Path: {:?}\n", path);

    // Create test directory
    std::fs::create_dir_all(path)?;

    let source_file = path.join("benchmark_source.bin");
    let dest_file = path.join("benchmark_dest.bin");

    // Create source file
    print!("Creating test file... ");
    std::io::stdout().flush()?;

    {
        let mut file = File::create(&source_file)?;
        let chunk_size = 1024 * 1024; // 1MB
        let chunk: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();
        let mut remaining = size_bytes;

        while remaining > 0 {
            let to_write = (remaining as usize).min(chunk_size);
            file.write_all(&chunk[..to_write])?;
            remaining -= to_write as u64;
        }
        file.sync_all()?;
    }
    println!("done");

    // Benchmark different copy methods
    println!("\nRunning benchmarks...\n");

    let copier = FileCopier::default_copier();

    // Warm up
    let _ = copier.copy(&source_file, &dest_file);
    std::fs::remove_file(&dest_file).ok();

    // Test 1: Standard copy
    let start = std::time::Instant::now();
    let stats = copier.copy(&source_file, &dest_file)?;
    let duration = start.elapsed();

    println!("Standard copy:");
    println!("  Method:     {:?}", stats.method);
    println!("  Duration:   {:.2?}", duration);
    println!("  Throughput: {}/s", humansize::format_size(stats.throughput as u64, humansize::BINARY));

    std::fs::remove_file(&dest_file).ok();

    // Test 2: Parallel chunked copy
    let chunk_copier = smartcopy::sync::ChunkedCopier::new(4 * 1024 * 1024, num_cpus::get());
    let start = std::time::Instant::now();
    let chunk_result = chunk_copier.copy_parallel(&source_file, &dest_file)?;
    let duration = start.elapsed();

    println!("\nParallel chunked copy ({} workers):", num_cpus::get());
    println!("  Chunks:     {}", chunk_result.chunks_processed);
    println!("  Duration:   {:.2?}", duration);
    println!("  Throughput: {}/s", humansize::format_size(chunk_result.throughput as u64, humansize::BINARY));

    // Cleanup
    std::fs::remove_file(&source_file).ok();
    std::fs::remove_file(&dest_file).ok();

    println!("\nBenchmark complete.");

    Ok(())
}

fn cmd_agent(protocol: AgentProtocol, port: u16, bind: &str) -> Result<()> {
    println!("Starting SmartCopy Agent...");
    println!("Protocol: {:?}", protocol);

    if protocol == AgentProtocol::Tcp {
        println!("Listening on: {}:{}", bind, port);
    } else {
        println!("Using stdio (for SSH pipe)");
    }

    println!("Press Ctrl+C to stop.\n");

    // Show SSH tuning recommendations
    println!("=== SSH Tuning Recommendations ===");
    SshTuningRecommendations::print_recommendations();
    println!();

    let server = AgentServer::new(protocol, port, bind.to_string());
    server.run()?;

    Ok(())
}

fn cmd_quic_server(port: u16, bind: &str, cert: Option<&Path>, key: Option<&Path>) -> Result<()> {
    use std::net::SocketAddr;

    let addr: SocketAddr = format!("{}:{}", bind, port).parse()
        .map_err(|e| smartcopy::error::SmartCopyError::config(format!("Invalid address: {}", e)))?;

    // Load or generate certificates
    let cert_manager = match (cert, key) {
        (Some(cert_path), Some(key_path)) => {
            println!("Loading certificates from files...");
            CertificateManager::from_files(cert_path, key_path)?
        }
        _ => {
            println!("Generating self-signed certificate...");
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "localhost".to_string());
            let manager = CertificateManager::generate_self_signed(&hostname)?;

            // Save for future use
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            let cert_dir = std::path::PathBuf::from(home).join(".smartcopy");
            std::fs::create_dir_all(&cert_dir)?;

            let cert_file = cert_dir.join("quic-cert.pem");
            let key_file = cert_dir.join("quic-key.pem");

            if !cert_file.exists() {
                manager.save_to_files(&cert_file, &key_file)?;
                println!("Saved certificate to: {:?}", cert_file);
                println!("Saved key to: {:?}", key_file);
            }

            manager
        }
    };

    println!("\nStarting SmartCopy QUIC server...");
    println!("Listening on: {}", addr);
    println!("Press Ctrl+C to stop.\n");

    // Run async server
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| smartcopy::error::SmartCopyError::config(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async {
        let server = QuicServer::new(addr, &cert_manager).await?;
        server.run().await
    })
}

fn cmd_highspeed(speed: HighSpeedTier) -> Result<()> {
    let tier = match speed {
        HighSpeedTier::Gbps10 => NetworkSpeedTier::Gbps10,
        HighSpeedTier::Gbps100 => NetworkSpeedTier::Gbps100,
        HighSpeedTier::Gbps200 => NetworkSpeedTier::Gbps200,
        HighSpeedTier::Gbps400 => NetworkSpeedTier::Gbps400,
    };

    HighSpeedNetworkGuide::print_recommendations(tier);
    Ok(())
}

fn print_config(config: &CopyConfig) {
    println!("=== Configuration ===");
    println!("Source:      {:?}", config.source);
    println!("Destination: {:?}", config.destination);
    println!("Threads:     {}", if config.threads == 0 { num_cpus::get() } else { config.threads });
    println!("Buffer:      {}", humansize::format_size(config.buffer_size as u64, humansize::BINARY));
    println!("Verify:      {:?}", config.verify);
    println!("Incremental: {}", config.incremental);
    println!("Delta:       {}", config.delta);
    println!("Compress:    {}", config.compress);
    println!("Ordering:    {:?}", config.ordering);

    // Print remote config if present
    if let Some(ref remote) = config.remote {
        println!("\n=== Remote Configuration ===");
        println!("Host:        {}@{}:{}", remote.user, remote.host, remote.port);
        println!("Streams:     {}", remote.streams);
        println!("TCP Direct:  {}", remote.tcp_direct);
        println!("QUIC:        {}", remote.quic);
        println!("Use Agent:   {}", remote.use_agent);

        if let Some(ref tuning) = remote.ssh_tuning {
            println!("\n=== SSH Tuning ===");
            println!("ControlMaster: {}", tuning.control_master);
            println!("ControlPersist: {}s", tuning.control_persist);
            println!("Cipher:        {:?}", tuning.cipher);
            println!("Compression:   {}", tuning.compression);
        }
    }

    println!();
}
