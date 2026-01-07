//! Flush queue for coalescing concurrent flush requests (group commit pattern).

use std::sync::Arc;

use slatedb::Db;
use tokio::sync::{mpsc, oneshot};

use crate::error::StoreError;

const QUEUE_CAPACITY: usize = 64;

/// Queue that coalesces concurrent flush requests into single database flushes.
#[derive(Clone)]
pub struct FlushQueue {
    sender: mpsc::Sender<oneshot::Sender<Result<(), String>>>,
}

impl FlushQueue {
    pub fn new(db: Arc<Db>) -> Self {
        let (sender, mut receiver) =
            mpsc::channel::<oneshot::Sender<Result<(), String>>>(QUEUE_CAPACITY);

        tokio::spawn(async move {
            let mut pending = Vec::new();

            while let Some(tx) = receiver.recv().await {
                pending.push(tx);
                while let Ok(tx) = receiver.try_recv() {
                    pending.push(tx);
                }

                let result = db.flush().await.map_err(|e| e.to_string());

                for tx in pending.drain(..) {
                    let _ = tx.send(result.clone());
                }
            }
        });

        Self { sender }
    }

    pub async fn flush(&self) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(tx)
            .await
            .map_err(|_| StoreError::Backend {
                message: "flush queue closed".into(),
            })?;

        rx.await
            .map_err(|_| StoreError::Backend {
                message: "flush response dropped".into(),
            })?
            .map_err(|msg| StoreError::Backend { message: msg })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn make_db() -> Arc<Db> {
        let object_store = Arc::new(InMemory::new());
        Arc::new(Db::builder("test", object_store).build().await.unwrap())
    }

    #[tokio::test]
    async fn flush_works() {
        let db = make_db().await;
        let q = FlushQueue::new(db);
        q.flush().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_flushes_coalesce() {
        let db = make_db().await;
        let q = FlushQueue::new(db);

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let q = q.clone();
                tokio::spawn(async move { q.flush().await })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }
    }
}
