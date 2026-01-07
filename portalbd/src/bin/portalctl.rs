//! portalctl - CLI client for portalbd daemon.
//!
//! Usage:
//!   portalctl snapshot create <name>
//!   portalctl snapshot list
//!   portalctl snapshot delete <name>
//!   portalctl snapshot restore <name>
//!   portalctl status

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use portalbd::control::{Request, Response};

const DEFAULT_SOCKET_PATH: &str = "/run/portalbd/portalbd.sock";

#[derive(Parser)]
#[command(name = "portalctl")]
#[command(about = "Control portalbd daemon")]
struct Cli {
    /// Control socket path
    #[arg(short, long, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Snapshot management
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommands,
    },
    /// Show daemon status
    Status,
}

#[derive(Subcommand)]
enum SnapshotCommands {
    /// Create a snapshot
    Create {
        /// Snapshot name
        name: String,
    },
    /// List all snapshots
    List,
    /// Get snapshot info
    Get {
        /// Snapshot name
        name: String,
    },
    /// Delete a snapshot
    Delete {
        /// Snapshot name
        name: String,
    },
    /// Restore to a snapshot (CoW - instant, no data copying)
    Restore {
        /// Snapshot name
        name: String,
    },
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn format_timestamp(ts: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let dt = UNIX_EPOCH + Duration::from_secs(ts as u64);
    format!("{:?}", dt)
}

async fn send_request(socket: &PathBuf, request: Request) -> Result<Response> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("Failed to connect to daemon at {}", socket.display()))?;

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send request
    let req_json = serde_json::to_string(&request)? + "\n";
    writer.write_all(req_json.as_bytes()).await?;

    // Read response
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response: Response = serde_json::from_str(&line)?;

    Ok(response)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let request = match cli.command {
        Commands::Snapshot { command } => match command {
            SnapshotCommands::Create { name } => Request::SnapshotCreate { name },
            SnapshotCommands::List => Request::SnapshotList,
            SnapshotCommands::Get { name } => Request::SnapshotGet { name },
            SnapshotCommands::Delete { name } => Request::SnapshotDelete { name },
            SnapshotCommands::Restore { name } => Request::SnapshotRestore { name },
        },
        Commands::Status => Request::Status,
    };

    let response = send_request(&cli.socket, request).await?;

    match response {
        Response::Ok => {
            println!("OK");
        }
        Response::Error(msg) => {
            eprintln!("Error: {}", msg);
            std::process::exit(1);
        }
        Response::Snapshot(info) => {
            println!("Snapshot: {}", info.name);
            println!("  ID:      {}", info.id);
            println!("  Created: {}", format_timestamp(info.created_at));
        }
        Response::SnapshotList(snapshots) => {
            if snapshots.is_empty() {
                println!("No snapshots");
            } else {
                println!("{:<30} {:<40}", "NAME", "ID");
                for snap in snapshots {
                    println!("{:<30} {:<40}", snap.name, snap.id);
                }
            }
        }
        Response::Status(status) => {
            println!("portalbd daemon status:");
            println!("  Size:       {}", format_size(status.size_bytes));
            println!("  Blocks:     {}", status.block_count);
            println!("  Snapshots:  {}", status.snapshot_count);
            println!("  NBD:        {}", status.nbd_address);
        }
    }

    Ok(())
}
