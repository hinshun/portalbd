//! portalbd daemon - NBD block device server with snapshot support.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use pprof::protos::Message;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use portalbd::control::{DaemonStatus, Request, Response};
use portalbd::record::save_trace;
use portalbd::{BLOCK_SIZE, Config, Daemon};

#[derive(Parser)]
#[command(
    name = "portalbd",
    about = "NBD block device daemon with snapshot support"
)]
struct Cli {
    /// Path to config file. If omitted, uses defaults (1GB in-memory device).
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Device size in GB. Overrides config file if specified.
    #[arg(long)]
    disk_size_gb: Option<u64>,

    /// Enable CPU profiling and save flamegraph on shutdown.
    #[arg(long)]
    profile: bool,

    /// Output path for CPU profile (default: portalbd.svg).
    #[arg(long, default_value = "portalbd.svg")]
    profile_output: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut config = match cli.config {
        Some(ref path) => Config::load(path)
            .with_context(|| format!("Failed to load config: {}", path.display()))?,
        None => Config::default(),
    };

    // CLI overrides
    if let Some(disk_size_gb) = cli.disk_size_gb {
        config.device.disk_size_gb = disk_size_gb;
    }

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Start CPU profiler if enabled
    let profiler_guard = if cli.profile {
        info!(output = %cli.profile_output.display(), "CPU profiling enabled");
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .context("Failed to create profiler")?,
        )
    } else {
        None
    };

    // Setup control socket
    if let Some(parent) = config.socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if config.socket.exists() {
        std::fs::remove_file(&config.socket)?;
    }

    let socket_path = config.socket.clone();

    // Create daemon from config (handles recording setup internally)
    let daemon = Arc::new(
        Daemon::from_config(config)
            .await
            .context("Failed to create daemon")?,
    );

    // Bind control socket
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("Failed to bind: {}", socket_path.display()))?;

    // Log startup info
    info!(
        socket = %socket_path.display(),
        size_gb = daemon.config().device.disk_size_gb,
        nbd = %daemon.nbd_address(),
        storage = daemon.config().storage.url.as_deref().unwrap_or("memory://"),
        recording = daemon.is_recording(),
        "portalbd started"
    );

    // Start NBD server
    let nbd_addr = daemon.nbd_address().to_string();
    let nbd_handle = {
        let daemon = daemon.clone();
        tokio::spawn(async move {
            let listener = match TcpListener::bind(&nbd_addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(error = %e, "Failed to bind NBD listener");
                    return;
                }
            };
            info!(address = %nbd_addr, "NBD server listening");
            if let Err(e) = daemon.listen(listener).await {
                error!(error = %e, "NBD server error");
            }
        })
    };

    // Spawn control socket handler
    let control_handle = tokio::spawn({
        let daemon = Arc::clone(&daemon);
        async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let daemon = Arc::clone(&daemon);
                        tokio::spawn(async move {
                            if let Err(e) = handle_control(stream, daemon).await {
                                error!(error = %e, "Control connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Accept error");
                        break;
                    }
                }
            }
        }
    });

    // Wait for shutdown signal
    let shutdown = async {
        let ctrl_c = signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => info!("Received SIGINT"),
                _ = sigterm.recv() => info!("Received SIGTERM"),
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.expect("Failed to wait for Ctrl+C");
            info!("Received SIGINT");
        }
    };

    shutdown.await;

    // Save CPU profile if profiling was enabled
    if let Some(guard) = profiler_guard {
        let report = guard
            .report()
            .build()
            .context("Failed to build profile report")?;

        // Save flamegraph SVG
        let mut svg_file = File::create(&cli.profile_output)
            .with_context(|| format!("Failed to create: {}", cli.profile_output.display()))?;
        report.flamegraph(&mut svg_file)?;
        info!(path = %cli.profile_output.display(), "Flamegraph saved");

        // Also save protobuf for pprof tool
        let pb_path = cli.profile_output.with_extension("pb");
        let profile = report.pprof()?;
        std::fs::write(&pb_path, profile.encode_to_vec())?;
        info!(path = %pb_path.display(), "Protobuf profile saved");
    }

    // Save trace if recording was enabled
    if let (Some(recorder), Some(cfg)) = (daemon.recorder(), &daemon.config().record) {
        let trace = recorder.stop();
        let op_count = trace.ops.len();
        save_trace(&trace, &cfg.path)?;

        info!(
            ops = op_count,
            path = %cfg.path.display(),
            "Trace saved"
        );
    }

    // Clean up
    nbd_handle.abort();
    control_handle.abort();

    Ok(())
}

async fn handle_control(stream: tokio::net::UnixStream, daemon: Arc<Daemon>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = serde_json::to_string(&Response::Error(e.to_string()))? + "\n";
                writer.write_all(resp.as_bytes()).await?;
                continue;
            }
        };

        let response = process_request(&daemon, request).await;
        let resp = serde_json::to_string(&response)? + "\n";
        writer.write_all(resp.as_bytes()).await?;
    }
}

async fn process_request(daemon: &Daemon, req: Request) -> Response {
    match req {
        Request::SnapshotCreate { name } => match daemon.snapshot_create(&name).await {
            Ok(s) => {
                info!(snapshot = name, "created");
                Response::Snapshot(s)
            }
            Err(e) => Response::Error(e.to_string()),
        },
        Request::SnapshotList => match daemon.snapshot_list().await {
            Ok(list) => Response::SnapshotList(list),
            Err(e) => Response::Error(e.to_string()),
        },
        Request::SnapshotGet { name } => match daemon.snapshot_get(&name).await {
            Ok(Some(s)) => Response::Snapshot(s),
            Ok(None) => Response::Error(format!("not found: {}", name)),
            Err(e) => Response::Error(e.to_string()),
        },
        Request::SnapshotDelete { name } => match daemon.snapshot_delete(&name).await {
            Ok(()) => {
                info!(snapshot = name, "deleted");
                Response::Ok
            }
            Err(e) => Response::Error(e.to_string()),
        },
        Request::SnapshotRestore { name } => match daemon.snapshot_restore(&name).await {
            Ok(()) => {
                info!(snapshot = name, "restored");
                Response::Ok
            }
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Status => {
            let count = daemon.snapshot_list().await.map(|l| l.len()).unwrap_or(0);
            Response::Status(DaemonStatus {
                size_bytes: daemon.size_bytes(),
                block_count: daemon.size_bytes() / BLOCK_SIZE as u64,
                snapshot_count: count,
                nbd_address: daemon.nbd_address().to_string(),
            })
        }
    }
}
