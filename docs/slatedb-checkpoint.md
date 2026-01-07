# SlateDB Checkpoints

Checkpoints are SlateDB's mechanism for point-in-time recovery. A checkpoint
references a specific manifest version, protecting all SST files in that
manifest from garbage collection. portalbd uses checkpoints to implement
block device snapshots.

## Creating Checkpoints

```rust
use slatedb::{Db, CheckpointScope, CheckpointOptions};

let db = Db::open(path, object_store).await?;

// Flush pending writes, then checkpoint
let result = db
    .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
    .await?;

// Named checkpoint with expiry
let result = db
    .create_checkpoint(
        CheckpointScope::All,
        &CheckpointOptions {
            name: Some("daily_backup".to_string()),
            lifetime: Some(Duration::from_secs(86400)), // 1 day TTL
            ..Default::default()
        },
    )
    .await?;
```

`CheckpointScope::All` flushes the WAL and memtables before checkpointing,
ensuring all data at call time is captured. `CheckpointScope::Durable` skips
the flush and only includes already-persisted data (faster but may miss
recent writes).

## Managing Checkpoints

The `Admin` interface provides checkpoint management without opening the
database for writes:

```rust
use slatedb::admin::AdminBuilder;

let admin = AdminBuilder::new(path, object_store).build();

// List all checkpoints (optionally filter by name)
let checkpoints = admin.list_checkpoints(None).await?;
let named = admin.list_checkpoints(Some("daily_backup")).await?;

// Extend expiry
admin.refresh_checkpoint(checkpoint_id, Some(Duration::from_secs(7200))).await?;

// Delete
admin.delete_checkpoint(checkpoint_id).await?;

// Create without opening DB (uses latest durable state)
let result = admin.create_detached_checkpoint(&CheckpointOptions::default()).await?;
```

## Reading from Checkpoints

Open a read-only view at a specific checkpoint:

```rust
use slatedb::db_reader::{DbReader, DbReaderOptions};

let reader = DbReader::open(
    path,
    object_store.clone(),
    Some(checkpoint_id),  // None = latest
    DbReaderOptions::default(),
)
.await?;

// All reads return data as it existed at the checkpoint
let value = reader.get(b"key").await?;
```

## Restoring Checkpoints

Restore reverts the database to a checkpoint's state:

```rust
admin.restore_checkpoint(target_id).await?;
```

**Current limitations (SlateDB PR #1072):**
- Restore fails if checkpoints exist after the target. Delete them first.
- After restore, reopen with compactor disabled to avoid epoch mismatch.

portalbd handles both workarounds in `BlockStore::snapshot_restore`.

## Checkpoint Lifecycle

```
Create → Active → Expired/Deleted
           ↓
       Refresh (extends expiry)
```

- **Active:** Referenced manifest and SSTs protected from GC
- **Expired:** GC removes checkpoint, then unreferenced resources
- **Deleted:** Manual deletion via `delete_checkpoint()`

Checkpoints with `lifetime: None` never expire and must be deleted manually.

## Data Structures

```rust
pub struct Checkpoint {
    pub id: Uuid,                           // Unique identifier
    pub manifest_id: u64,                   // Referenced manifest version
    pub expire_time: Option<DateTime<Utc>>, // None = never expires
    pub create_time: DateTime<Utc>,
    pub name: Option<String>,               // For filtering
}

pub struct CheckpointOptions {
    pub lifetime: Option<Duration>,  // TTL from creation
    pub source: Option<Uuid>,        // Chain from existing checkpoint
    pub name: Option<String>,
}

pub enum CheckpointScope {
    All,      // Flush before checkpoint
    Durable,  // Only durable data (faster)
}
```

## Error Handling

```rust
match result {
    Err(SlateDBError::CheckpointMissing(id)) => {
        // Checkpoint expired or was deleted
    }
    Err(SlateDBError::CheckpointLifetimeTooShort) => {
        // Reader lifetime must be >= 2x manifest_poll_interval
    }
    // ...
}
```
