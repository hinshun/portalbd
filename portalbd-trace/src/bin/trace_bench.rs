//! Trace replay benchmark with CPU profiling.
//!
//! Uses the actual NBD protocol stack (NbdServer + NbdClient) to benchmark
//! realistic workload performance, matching how real clients use portalbd.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use pprof::protos::Message;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

use portalbd::config::LsmConfig;
use portalbd::daemon::Daemon;
use portalbd_trace::{NbdReplayer, OpTrace, ReplayStats, load_trace};
use slatedb::config::{CompressionCodec, SstBlockSize};

#[derive(Parser, Debug)]
#[command(name = "trace-bench", about = "Replay traces with optional profiling")]
struct Args {
    /// Path to trace.json file
    #[arg(required_unless_present = "all")]
    trace: Option<PathBuf>,

    /// Analyze all traces in directory
    #[arg(long, value_name = "DIR")]
    all: Option<PathBuf>,

    /// Number of NBD connections (simulates nbd-client -C <N>)
    #[arg(short = 'C', long, default_value = "1")]
    connections: usize,

    /// Number of replay iterations
    #[arg(short, long, default_value = "3")]
    iterations: usize,

    /// Warmup iterations (not measured)
    #[arg(long, default_value = "1")]
    warmup: usize,

    /// Output directory for profiles
    #[arg(long, value_name = "DIR")]
    profile_dir: Option<PathBuf>,

    /// Enable CPU profiling and save flamegraph
    #[arg(long)]
    profile: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Compression codec (Lz4, Snappy, Zstd, or "none"). Default: Lz4
    #[arg(long, value_name = "CODEC", default_value = "Lz4")]
    compression: String,

    /// SST block size (Block1Kib, Block2Kib, Block4Kib, Block8Kib, Block16Kib, Block32Kib, Block64Kib)
    #[arg(long, value_name = "SIZE", value_parser = parse_block_size)]
    block_size: Option<SstBlockSize>,

    /// Max unflushed bytes (memtable size before flush). Default: 64MB
    #[arg(long, value_name = "BYTES")]
    max_unflushed_bytes: Option<u64>,

    /// L0 SST size in bytes. Default: 64MB
    #[arg(long, value_name = "BYTES")]
    l0_sst_size: Option<u64>,

    /// Flush interval in ms. Set to 0 for manual-only flush. Default: 100ms
    #[arg(long, value_name = "MS")]
    flush_interval_ms: Option<u64>,
}

fn parse_compression(s: &str) -> Option<CompressionCodec> {
    if s == "none" {
        return None;
    }
    let quoted = format!("\"{s}\"");
    serde_json::from_str(&quoted)
        .unwrap_or_else(|_| panic!("invalid compression: {s}. Use: Lz4, Snappy, Zstd, or none"))
}

fn parse_block_size(s: &str) -> Result<SstBlockSize, String> {
    // Use serde to parse the same format as TOML config
    let quoted = format!("\"{s}\"");
    serde_json::from_str(&quoted).map_err(|_| {
        format!("invalid block size: {s}. Use: Block1Kib, Block2Kib, Block4Kib, Block8Kib, Block16Kib, Block32Kib, Block64Kib")
    })
}

struct LsmOptions {
    compression: Option<CompressionCodec>,
    block_size: Option<SstBlockSize>,
    max_unflushed_bytes: Option<u64>,
    l0_sst_size_bytes: Option<u64>,
    flush_interval_ms: Option<u64>,
}

async fn make_daemon(size_bytes: u64, opts: &LsmOptions) -> Daemon {
    use portalbd::config::{Config, DeviceConfig};

    let lsm_config = LsmConfig {
        compression_codec: opts.compression,
        sst_block_size: opts.block_size,
        max_unflushed_bytes: opts.max_unflushed_bytes,
        l0_sst_size_bytes: opts.l0_sst_size_bytes,
        flush_interval_ms: opts.flush_interval_ms,
        ..Default::default()
    };
    let config = Config {
        device: DeviceConfig {
            disk_size_gb: size_bytes.div_ceil(1024 * 1024 * 1024),
        },
        lsm: lsm_config,
        ..Default::default()
    };
    Daemon::from_config(config)
        .await
        .expect("failed to create daemon")
}

async fn run_replay(trace: &OpTrace, opts: &LsmOptions, num_connections: usize) -> ReplayStats {
    let daemon = make_daemon(trace.device_size_bytes, opts).await;
    let replayer = NbdReplayer::new(trace.clone(), &daemon, num_connections)
        .await
        .expect("failed to create replayer");
    let stats = replayer.replay().await;
    replayer.disconnect().await;
    stats
}

fn format_throughput(bytes: u64, duration_ns: u64) -> String {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let secs = duration_ns as f64 / 1_000_000_000.0;
    if secs > 0.0 {
        format!("{:.2} MB/s", mb / secs)
    } else {
        "N/A".to_string()
    }
}

fn print_stats(name: &str, stats: &ReplayStats) {
    let duration_ms = stats.duration_ns as f64 / 1_000_000.0;
    let total_bytes = stats.bytes_read + stats.bytes_written;

    println!("  {name}:");
    println!("    Duration: {:.2} ms", duration_ms);
    println!(
        "    Ops: {} ({:.0} ops/s)",
        stats.ops_total,
        stats.ops_per_sec()
    );
    println!(
        "    Throughput: {} (read: {}, write: {})",
        format_throughput(total_bytes, stats.duration_ns),
        format_throughput(stats.bytes_read, stats.duration_ns),
        format_throughput(stats.bytes_written, stats.duration_ns),
    );
}

async fn benchmark_trace(
    trace_path: &Path,
    args: &Args,
    opts: &LsmOptions,
) -> Result<Vec<ReplayStats>> {
    let trace = load_trace(trace_path).context("Failed to load trace")?;

    println!("\n=== {} ===", trace.workload);
    println!(
        "Device size: {} MB",
        trace.device_size_bytes / (1024 * 1024)
    );
    println!("Operations: {}", trace.ops.len());
    println!("Connections: {}", args.connections);

    // Warmup runs
    if args.warmup > 0 {
        println!("\nWarming up ({} iterations)...", args.warmup);
        for _ in 0..args.warmup {
            run_replay(&trace, opts, args.connections).await;
        }
    }

    // Start profiler if enabled
    let guard = if args.profile {
        println!("\nCPU profiling enabled");
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .expect("failed to create profiler"),
        )
    } else {
        None
    };

    // Measured runs
    println!("\nRunning {} iterations...", args.iterations);
    let mut results = Vec::with_capacity(args.iterations);

    for i in 0..args.iterations {
        let start = Instant::now();
        let stats = run_replay(&trace, opts, args.connections).await;
        let elapsed = start.elapsed();

        info!(
            iteration = i + 1,
            duration_ms = elapsed.as_millis(),
            ops_per_sec = stats.ops_per_sec() as u64,
            "Iteration complete"
        );

        print_stats(&format!("Iteration {}", i + 1), &stats);
        results.push(stats);
    }

    // Save profile if enabled
    if let Some(guard) = guard {
        let report = guard.report().build()?;

        let profile_dir = args.profile_dir.as_deref().unwrap_or(Path::new("."));
        let workload_name = trace.workload.replace('.', "_");

        // Save flamegraph SVG
        let svg_path = profile_dir.join(format!("{}.svg", workload_name));
        let mut svg_file = File::create(&svg_path)?;
        report.flamegraph(&mut svg_file)?;
        println!("\nFlamegraph saved to: {}", svg_path.display());

        // Save protobuf for pprof tool
        let pb_path = profile_dir.join(format!("{}.pb", workload_name));
        let profile = report.pprof()?;
        std::fs::write(&pb_path, profile.encode_to_vec())?;
        println!("Protobuf profile saved to: {}", pb_path.display());
    }

    // Summary
    if results.len() > 1 {
        println!("\n--- Summary ---");
        let avg_duration: f64 =
            results.iter().map(|s| s.duration_ns as f64).sum::<f64>() / results.len() as f64;
        let avg_ops: f64 =
            results.iter().map(|s| s.ops_per_sec()).sum::<f64>() / results.len() as f64;

        let min_duration = results.iter().map(|s| s.duration_ns).min().unwrap();
        let max_duration = results.iter().map(|s| s.duration_ns).max().unwrap();

        println!("  Avg duration: {:.2} ms", avg_duration / 1_000_000.0);
        println!(
            "  Min duration: {:.2} ms",
            min_duration as f64 / 1_000_000.0
        );
        println!(
            "  Max duration: {:.2} ms",
            max_duration as f64 / 1_000_000.0
        );
        println!("  Avg ops/sec: {:.0}", avg_ops);
    }

    Ok(results)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Setup logging
    let level = if args.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create profile directory if specified
    if let Some(ref dir) = args.profile_dir {
        std::fs::create_dir_all(dir)?;
    }

    let compression = parse_compression(&args.compression);
    let lsm_opts = LsmOptions {
        compression,
        block_size: args.block_size,
        max_unflushed_bytes: args.max_unflushed_bytes,
        l0_sst_size_bytes: args.l0_sst_size,
        flush_interval_ms: args.flush_interval_ms,
    };

    // Print config
    if let Some(codec) = compression {
        println!("Compression: {:?}", codec);
    } else {
        println!("Compression: none");
    }
    if let Some(bs) = args.block_size {
        println!("SST block size: {:?}", bs);
    }
    if let Some(mub) = args.max_unflushed_bytes {
        println!("Max unflushed bytes: {} MB", mub / (1024 * 1024));
    }
    if let Some(l0) = args.l0_sst_size {
        println!("L0 SST size: {} MB", l0 / (1024 * 1024));
    }
    if let Some(flush_ms) = args.flush_interval_ms {
        if flush_ms == 0 {
            println!("Flush interval: manual only");
        } else {
            println!("Flush interval: {} ms", flush_ms);
        }
    }

    if let Some(ref all_dir) = args.all {
        // Benchmark all traces in directory
        let mut traces: Vec<PathBuf> = std::fs::read_dir(all_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        traces.sort();

        println!("Found {} trace files", traces.len());

        let mut all_results = Vec::new();
        for trace_path in &traces {
            let results = benchmark_trace(trace_path, &args, &lsm_opts).await?;
            all_results.push((trace_path.clone(), results));
        }

        println!(
            "\n{:<15} {:>12} {:>12} {:>12}",
            "Workload", "Avg (ms)", "Ops/s", "MB/s"
        );

        for (path, results) in &all_results {
            let name = path.file_stem().unwrap().to_string_lossy();
            let name = name.strip_suffix(".trace").unwrap_or(&name);

            let avg_duration =
                results.iter().map(|s| s.duration_ns).sum::<u64>() / results.len() as u64;
            let avg_ops =
                results.iter().map(|s| s.ops_per_sec()).sum::<f64>() / results.len() as f64;
            let avg_bytes: u64 = results
                .iter()
                .map(|s| s.bytes_read + s.bytes_written)
                .sum::<u64>()
                / results.len() as u64;
            let mb_per_sec =
                (avg_bytes as f64 / (1024.0 * 1024.0)) / (avg_duration as f64 / 1_000_000_000.0);

            println!(
                "{:<15} {:>12.2} {:>12.0} {:>12.2}",
                name,
                avg_duration as f64 / 1_000_000.0,
                avg_ops,
                mb_per_sec
            );
        }
    } else if let Some(ref trace_path) = args.trace {
        benchmark_trace(trace_path, &args, &lsm_opts).await?;
    }

    Ok(())
}
