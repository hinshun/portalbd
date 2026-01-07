//! Daemon API for running portalbd.
//!
//! Provides a clean interface for starting portalbd with a given configuration.
//! Used by both the standalone daemon binary and the CSI driver.
//!
//! # Example
//!
//! ```ignore
//! use portalbd::daemon::Daemon;
//! use tokio::net::TcpListener;
//!
//! let daemon = Daemon::from_config(config).await?;
//! let listener = TcpListener::bind(&daemon.config().nbd.address).await?;
//! daemon.listen(listener).await?;
//! ```

use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::error::{Error, NbdError};
use crate::nbd::{BlockStoreHandler, Listener, NbdExport, NbdServer};
use crate::record::{OpRecorder, RecordingHandler};
use crate::store::BlockStore;
use crate::types::SnapshotInfo;

/// A running portalbd instance.
#[derive(Clone)]
pub struct Daemon {
    store: Arc<RwLock<BlockStore>>,
    server: NbdServer,
    export: NbdExport,
    config: Arc<Config>,
    recorder: Option<Arc<OpRecorder>>,
}

impl Daemon {
    /// Create a new daemon from configuration.
    ///
    /// Opens the BlockStore using the storage backend specified in config.
    /// If `config.record` is set, wraps the handler with a recording middleware.
    pub async fn from_config(config: Config) -> Result<Self, Error> {
        let (object_store, db_path) = config.storage.build_object_store()?;

        let size_bytes = config.device.disk_size_gb * 1024 * 1024 * 1024;
        let store =
            BlockStore::open_with_config(db_path, object_store, size_bytes, config.lsm.clone())
                .await?;
        let store = Arc::new(RwLock::new(store));
        let core_handler = BlockStoreHandler::new(Arc::clone(&store));

        // Wrap with recording middleware if config.record is set
        let (server, recorder) = if let Some(ref record_config) = config.record {
            let clock = Arc::new(slatedb::clock::DefaultSystemClock::new());
            let recorder = Arc::new(OpRecorder::new(&record_config.workload, size_bytes, clock));
            let handler = Arc::new(RecordingHandler::new(core_handler, Arc::clone(&recorder)));
            (NbdServer::new(handler), Some(recorder))
        } else {
            (NbdServer::new(Arc::new(core_handler)), None)
        };

        let export = NbdExport {
            name: "portalbd".to_string(),
            size_bytes,
            read_only: false,
        };

        Ok(Self {
            store,
            server,
            export,
            config: Arc::new(config),
            recorder,
        })
    }

    /// Get a reference to the underlying store.
    ///
    /// Used for direct store access when needed (e.g., control socket handling).
    pub fn store(&self) -> Arc<RwLock<BlockStore>> {
        Arc::clone(&self.store)
    }

    /// Get the daemon configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the NBD address from config.
    pub fn nbd_address(&self) -> &str {
        &self.config.nbd.address
    }

    /// Get the device size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.export.size_bytes
    }

    /// Get the recorder if recording is enabled.
    ///
    /// Returns `None` if recording was not configured.
    pub fn recorder(&self) -> Option<&Arc<OpRecorder>> {
        self.recorder.as_ref()
    }

    /// Check if recording is enabled.
    pub fn is_recording(&self) -> bool {
        self.recorder.is_some()
    }

    /// Accept NBD connections from any listener until it closes.
    ///
    /// This is the main entry point for serving NBD connections. It accepts
    /// connections in a loop, spawning a task for each one.
    ///
    /// # Arguments
    ///
    /// * `listener` - Any type implementing [`Listener`], such as:
    ///   - `TcpListener` for production TCP connections
    ///   - `UnixListener` for Unix domain sockets
    ///   - `StreamListener` for testing/benchmarks with in-memory streams
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Production: TCP listener
    /// let listener = TcpListener::bind("127.0.0.1:10809").await?;
    /// daemon.listen(listener).await?;
    ///
    /// // Testing: StreamListener with duplex channels
    /// let (tx, listener) = StreamListener::new(4);
    /// tokio::spawn(daemon.listen(listener));
    /// // ... create duplex streams and send to tx
    /// ```
    pub async fn listen<L>(&self, mut listener: L) -> Result<(), NbdError>
    where
        L: Listener,
    {
        info!(
            export = %self.export.name,
            size_bytes = self.export.size_bytes,
            "NBD server accepting connections"
        );

        loop {
            match listener.accept().await {
                Ok(stream) => {
                    let server = self.server.clone();
                    let export = self.export.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.serve(stream, &export).await {
                            // Log non-normal disconnects
                            if !matches!(e, NbdError::Io(_)) {
                                tracing::warn!(error = %e, "NBD connection error");
                            }
                        }
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                    // Channel closed (StreamListener exhausted) - normal exit
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    // Snapshot operations - delegate to BlockStore

    /// Create a snapshot with the given name.
    pub async fn snapshot_create(&self, name: &str) -> Result<SnapshotInfo, Error> {
        let store = self.store.read().await;
        store.snapshot_create(name).await.map_err(Error::from)
    }

    /// Get a snapshot by name.
    pub async fn snapshot_get(&self, name: &str) -> Result<Option<SnapshotInfo>, Error> {
        let store = self.store.read().await;
        store.snapshot_get(name).await.map_err(Error::from)
    }

    /// List all snapshots.
    pub async fn snapshot_list(&self) -> Result<Vec<SnapshotInfo>, Error> {
        let store = self.store.read().await;
        store.snapshot_list().await.map_err(Error::from)
    }

    /// Delete a snapshot by name.
    pub async fn snapshot_delete(&self, name: &str) -> Result<(), Error> {
        let store = self.store.read().await;
        store.snapshot_delete(name).await.map_err(Error::from)
    }

    /// Restore to a snapshot by name.
    ///
    /// This acquires a write lock, blocking all I/O while the restore
    /// closes and reopens the database.
    pub async fn snapshot_restore(&self, name: &str) -> Result<(), Error> {
        let mut store = self.store.write().await;
        store.snapshot_restore(name).await.map_err(Error::from)
    }

    /// Flush pending writes to storage.
    pub async fn flush(&self) -> Result<(), Error> {
        let store = self.store.read().await;
        store.flush().await.map_err(Error::from)
    }
}
