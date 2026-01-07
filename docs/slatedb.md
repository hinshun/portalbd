# SlateDB

SlateDB is an embedded LSM-tree storage engine that writes all data to object
storage (S3, GCS, ABS, MinIO). This doc covers the API patterns used in
portalbd.

## Opening a Database

```rust
use slatedb::{Db, object_store::memory::InMemory};
use std::sync::Arc;

// Simple
let db = Db::open("path", object_store).await?;

// With configuration
let db = Db::builder("path", object_store)
    .with_settings(settings)
    .with_sst_block_size(SstBlockSize::Block64Kib)
    .build()
    .await?;
```

## Read/Write Operations

```rust
// Point operations
db.put(b"key", b"value").await?;
let value: Option<Bytes> = db.get(b"key").await?;
db.delete(b"key").await?;

// Range scan
let mut iter = db.scan("a"..="z").await?;
while let Some(kv) = iter.next().await? {
    println!("{:?} = {:?}", kv.key, kv.value);
}

// Batch writes (atomic)
let mut batch = WriteBatch::new();
batch.put(b"k1", b"v1");
batch.put(b"k2", b"v2");
batch.delete(b"k3");
db.write(batch).await?;

// Non-durable write (returns before WAL flush)
db.put_with_options(b"key", b"value", &PutOptions::default(), &WriteOptions { await_durable: false }).await?;
```

## Transactions

SlateDB supports snapshot isolation and serializable snapshot isolation (SSI):

```rust
use slatedb::IsolationLevel;

let txn = db.begin(IsolationLevel::Snapshot).await?;
let value = txn.get(b"key").await?;
txn.put(b"key", b"new_value")?;
txn.commit().await?;  // Fails on write-write conflict

// SSI also detects read-write conflicts
let txn = db.begin(IsolationLevel::SerializableSnapshot).await?;
```

Handle conflicts with retry:

```rust
loop {
    let txn = db.begin(IsolationLevel::Snapshot).await?;
    txn.put(b"counter", b"1")?;
    match txn.commit().await {
        Ok(_) => break,
        Err(e) if e.kind() == ErrorKind::Transaction => continue,
        Err(e) => return Err(e),
    }
}
```

## Configuration

Key settings for block device workloads:

```rust
use slatedb::config::Settings;

let settings = Settings {
    // Manual flush control (portalbd manages durability)
    flush_interval: None,

    // Large L0 files for write batching
    l0_sst_size_bytes: 256 * 1024 * 1024,

    // Compression
    compression_codec: Some(CompressionCodec::Lz4),

    // Disable compactor (run separately or after restore)
    compactor_options: None,

    ..Default::default()
};
```

TOML configuration:

```toml
flush_interval = "100ms"
l0_sst_size_bytes = 67108864
compression_codec = "lz4"

[compactor_options]
poll_interval = "5s"
```

## Testing

Use `InMemory` for fast isolated tests:

```rust
use slatedb::object_store::memory::InMemory;

#[tokio::test]
async fn test_basic() {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("test", object_store).await.unwrap();
    db.put(b"key", b"value").await.unwrap();
    assert_eq!(db.get(b"key").await.unwrap(), Some("value".into()));
}
```

Use `LocalFileSystem` when you need persistence:

```rust
use slatedb::object_store::local::LocalFileSystem;

let temp_dir = tempfile::tempdir().unwrap();
let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
```

## Error Handling

```rust
use slatedb::{Error, ErrorKind, CloseReason};

match result {
    Err(e) => match e.kind() {
        ErrorKind::Transaction => { /* conflict, retry */ }
        ErrorKind::Closed(CloseReason::Fenced) => { /* another writer took over */ }
        ErrorKind::Unavailable => { /* network/storage error */ }
        ErrorKind::Data => { /* corruption */ }
        _ => { /* other */ }
    }
}
```

## Standalone Compactor

Run compaction in a separate process:

```rust
// Writer process: disable embedded compactor
let settings = Settings { compactor_options: None, ..Default::default() };

// Compactor process
use slatedb::CompactorBuilder;
let compactor = Arc::new(CompactorBuilder::new("path", object_store).build());
compactor.run().await?;
```

## Limits

- Max key size: 65 KiB
- Max value size: 4 GiB
