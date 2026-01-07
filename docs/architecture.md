# Architecture

portalbd is structured as a daemon that bridges two protocols: it speaks
[NBD][nbd] to Linux clients and uses [SlateDB][slatedb] for persistent
storage. The daemon translates block I/O operations into key-value
operations, where each 64 KiB block becomes a key-value pair in the LSM tree.

Snapshots fall out naturally: SlateDB checkpoints are block device snapshots.

## System Overview

```
                    ┌─────────────────────────────────────────┐
                    │        portalctl (CLI Client)           │
                    │  (snapshot create/list/delete/restore)  │
                    └──────────────────┬──────────────────────┘
                                       │ JSON over Unix socket
                                       ▼
              ┌────────────────────────────────────────────────┐
              │              portalbd Daemon                   │
              │   ┌────────────────────────────────────────┐   │
              │   │    Arc<RwLock<BlockStore>>            │   │
              │   │    - Read lock: I/O + snapshots       │   │
              │   │    - Write lock: restore (exclusive)  │   │
              │   └────────────────────────────────────────┘   │
              └────────────────┬─────────────────┬─────────────┘
                               │                 │
           ┌───────────────────▼───┐    ┌────────▼────────────┐
           │   Control Handler     │    │    NBD Server       │
           │   (Unix socket)       │    │    (TCP)            │
           │   - Snapshot ops      │    │    - Read/Write     │
           │   - Status queries    │    │    - Trim/Flush     │
           └───────────────────────┘    └──────────┬──────────┘
                                                   │
                        ┌──────────────────────────▼───────────┐
                        │           SlateDB                    │
                        │   (LSM-tree key-value store)         │
                        └──────────────────────────┬───────────┘
                                                   │
                        ┌──────────────────────────▼───────────┐
                        │     Object Storage Backend           │
                        │  ├─ file://  → LocalFileSystem       │
                        │  ├─ s3://    → AmazonS3              │
                        │  └─ (none)   → InMemory              │
                        └──────────────────────────────────────┘
```

## Block Storage Model

portalbd exposes a fixed-size block device divided into 64 KiB blocks. Each
block is stored as a key-value pair in SlateDB, where the key is the block
index (8-byte big-endian for lexicographic ordering) and the value is the
full block data.

**Sparse storage:** Unwritten blocks return zeros without consuming storage.
The daemon maintains a single reference-counted zero block that is cloned
(O(1) cost) for any read of unwritten data. This enables declaring an
exabyte-sized device while only paying for blocks actually written.

**Block alignment:** All storage operations work with complete blocks. When
the NBD client issues a partial block write, the daemon reads the existing
block, merges the new data, and writes the complete block back.

## Snapshot Semantics

Snapshots are implemented as SlateDB checkpoints, which are atomic
point-in-time captures of the database state. Creating a snapshot is an
O(1) metadata operation that records a pointer to the current manifest.

```
Timeline:
─────────────────────────────────────────────────────────────────►
     │         │              │                │
   Write    Create         Write           Restore
   block 0  snapshot       block 0          to snap1
   = 0xAA   "snap1"        = 0xBB
     │         │              │                │
     ▼         ▼              ▼                ▼
   [0xAA]   [0xAA]         [0xBB]           [0xAA]
```

**Copy-on-write behavior:** When a snapshot exists and new data is written,
SlateDB creates new SST files for the changes while the original data
remains immutable. The snapshot continues to reference the original files.

**Restore operation:** Restoring a snapshot requires exclusive access to the
database. The daemon acquires a write lock (blocking all I/O), closes the
database, calls `restore_checkpoint()` to reopen at the target state, then
releases the lock. The filesystem sees the device contents revert to the
snapshot state.

## NBD Protocol

The [Network Block Device protocol][nbd-protocol] enables a userspace server
to expose a block device to the kernel. portalbd implements the newstyle
fixed negotiation protocol with the following phases:

1. **Handshake:** Server sends magic numbers and flags
2. **Option negotiation:** Client requests export information (device name, size)
3. **Transmission:** I/O command loop until disconnect

**Supported commands:**

| Command | Description |
|---------|-------------|
| `READ` | Read data from device |
| `WRITE` | Write data to device |
| `FLUSH` | Ensure durability (triggers SlateDB flush) |
| `TRIM` | Discard data (deletes blocks, subsequent reads return zeros) |
| `WRITE_ZEROES` | Write zeros efficiently (same as TRIM) |
| `DISCONNECT` | Graceful connection close |

## Handler Middleware

The NBD server uses a middleware pattern for I/O handling. The
`TransmissionHandler` trait defines the interface:

```rust
pub trait TransmissionHandler: Send + Sync {
    async fn read(&self, offset: u64, length: usize) -> Result<Bytes>;
    async fn write(&self, offset: u64, data: Bytes) -> Result<()>;
    async fn trim(&self, offset: u64, length: u64) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}
```

Handlers can wrap other handlers, enabling composition:

- **BlockStoreHandler:** Core I/O operations against SlateDB
- **RecordingHandler:** Wraps any handler, captures operation traces for
  replay and benchmarking

This pattern makes it easy to add metrics, caching, fault injection, or
other cross-cutting concerns without modifying core logic.

## Flush Coalescing

Multiple concurrent flush requests are coalesced into a single SlateDB flush
via the `FlushQueue`. When 10 clients call flush simultaneously, only one
database flush occurs and all clients receive the result.

This is important for workloads like PostgreSQL that issue frequent fsync
calls. Without coalescing, each sync would trigger a separate flush,
degrading performance.

## Control Protocol

The CLI (`portalctl`) communicates with the daemon over a Unix domain socket
using line-delimited JSON. This is simpler than gRPC for the limited command
set and enables easy debugging with tools like `nc` or `socat`.

**Commands:**
- `snapshot create <name>` - Create a named snapshot
- `snapshot list` - List all snapshots
- `snapshot get <name>` - Get snapshot details
- `snapshot delete <name>` - Delete a snapshot
- `snapshot restore <name>` - Restore to a snapshot (requires unmounted device)
- `status` - Get daemon status

## Storage Backends

The storage URL determines which [object_store][object-store] backend is used:

| URL | Backend | Use Case |
|-----|---------|----------|
| (none) | InMemory | Testing, ephemeral workloads |
| `file://path` | LocalFileSystem | Development, single-node deployment |
| `s3://bucket/path` | AmazonS3 | Production cloud deployment |

S3 storage uses conditional puts for consistency, ensuring atomic operations
even with concurrent writers.

## LSM Configuration

SlateDB is configured for write-heavy block device workloads:

- **Manual flush:** `flush_interval_ms = 0` disables automatic flushing.
  The daemon controls durability via explicit flush calls, enabling group
  commit patterns.

- **Large L0 SSTs:** 256 MiB L0 files reduce flush frequency and improve
  write batching.

- **LZ4 compression:** Reduces storage and network I/O with minimal CPU
  overhead.

- **64 KiB SST blocks:** Matches the storage block size for alignment.

## Known Limitations

**SlateDB restore constraints:** Two workarounds exist for current SlateDB
limitations:

1. Restore requires deleting checkpoints created after the target. Snapshot
   history after the restore point is lost.

2. The compactor is disabled after restore due to an epoch mismatch bug.
   This will be addressed once SlateDB PR #1072 is merged.

**Connection limits:** Maximum 64 concurrent NBD connections per daemon
instance. Each daemon serves a single block device.

[nbd]: https://github.com/NetworkBlockDevice/nbd
[nbd-protocol]: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
[object-store]: https://github.com/apache/arrow-rs/tree/master/object_store
[slatedb]: https://github.com/slatedb/slatedb
