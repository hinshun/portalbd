//! portalbd-csi: CSI driver for portalbd.

use std::path::PathBuf;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use portalbd_csi::{Config, Driver};

#[derive(Parser, Debug)]
#[command(name = "portalbd-csi")]
#[command(about = "CSI driver for portalbd")]
struct Args {
    /// CSI endpoint (unix:// or tcp://).
    #[arg(long, default_value = "unix:///var/run/csi/csi.sock")]
    endpoint: String,

    /// Node ID.
    #[arg(long)]
    node_id: Option<String>,

    /// Data directory for volume storage.
    #[arg(long, default_value = "/var/lib/portalbd-csi")]
    data_dir: PathBuf,

    /// Verbosity level (0-4).
    #[arg(short, default_value = "0")]
    v: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Use targeted filters to avoid verbose logs from dependencies (h2, tonic, hyper).
    // Only portalbd crates get detailed logging; everything else stays at warn.
    let filter = match args.v {
        0 => "warn".to_string(),
        1 => "portalbd_csi=info,portalbd=info,warn".to_string(),
        2 => "portalbd_csi=debug,portalbd=debug,warn".to_string(),
        3 => "portalbd_csi=trace,portalbd=trace,warn".to_string(),
        _ => "portalbd_csi=trace,portalbd=trace,info".to_string(),
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .init();

    let config = Config {
        endpoint: args.endpoint,
        node_id: args.node_id.unwrap_or_else(|| {
            hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        }),
        data_dir: args.data_dir,
        ..Default::default()
    };

    info!(
        name = %config.name,
        version = %config.version,
        node_id = %config.node_id,
        endpoint = %config.endpoint,
        "starting portalbd CSI driver"
    );

    let driver = Driver::new(config)?;
    driver.run().await?;

    Ok(())
}
