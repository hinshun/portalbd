# portalbd

**First read [README.md](README.md)** for usage and development workflow, then
**[docs/architecture.md](docs/architecture.md)** for system design.

## Codebase

**Stack**: Rust 2024, async (tokio), SlateDB for storage, NBD protocol

```
nbd/                    # NBD protocol library
├── lib.rs              # Public exports
├── protocol.rs         # Wire format, commands, flags
├── client.rs           # NBD client for connecting to servers
└── device.rs           # Linux kernel NBD device

portalbd/               # Main library and binaries
├── src/
│   ├── lib.rs          # Public API exports
│   ├── types.rs        # BlockIndex, IoLength, SnapshotInfo
│   ├── error.rs        # StoreError, NbdError, ConfigError
│   ├── config.rs       # TOML configuration, object store factory
│   ├── control.rs      # Unix socket protocol (daemon <-> CLI)
│   ├── daemon.rs       # Daemon orchestration
│   ├── store/          # BlockStore: SlateDB wrapper with snapshots
│   ├── nbd/            # NBD server and handler middleware
│   └── record/         # Operation recording for trace replay
└── src/bin/
    ├── portalbd.rs     # Daemon binary
    └── portalctl.rs    # CLI client

portalbd-csi/           # CSI driver for Kubernetes
├── main.rs             # gRPC server entrypoint
├── driver/             # Driver state and lifecycle
├── identity.rs         # CSI Identity service
├── controller.rs       # CSI Controller service (volume create/delete)
├── node.rs             # CSI Node service (mount/unmount)
├── volume.rs           # Volume instance management
└── types.rs            # NBD address parsing

portalbd-dst/           # Deterministic simulation testing
├── lib.rs              # Test harness exports
├── simulation.rs       # Deterministic runtime, fault injection
└── harness/            # BlockStore and NBD test oracles

portalbd-trace/         # Trace replay benchmarks
├── lib.rs              # Re-exports from portalbd
├── replay.rs           # NbdReplayer for trace playback
└── bin/
    ├── trace_bench.rs  # Benchmark runner with profiling
    └── trace_replay.rs # Single trace replay utility
```

## Coding Conventions

Follow `docs/tigerstyle.md`. Key points:
- Newtypes for semantic safety (BlockIndex, IoLength)
- Bounded resources with explicit limits
- No recursion, no unbounded allocations
- Explicit error handling with context

**Determinism guards** in `clippy.toml` forbid non-deterministic APIs
(`thread_rng`, `SystemTime::now`, `tokio::time::sleep`). Use seeded PRNGs
and injected clocks for testability.

## Related Docs

- `docs/architecture.md` — System design and component overview
- `docs/tigerstyle.md` — Coding conventions
- `docs/slatedb.md` — SlateDB API reference
- `docs/slatedb-checkpoint.md` — Checkpoint semantics
