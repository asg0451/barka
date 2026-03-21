# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Barka

Kafka-like distributed log with an S3 backend. Ephemeral nodes coordinate partition leadership via S3-based leases (conditional writes with version tokens and heartbeats). Uses Cap'n Proto for zero-copy RPC and serialization. Includes Jepsen integration for correctness testing.

## Build Commands

```bash
cargo build                # build (requires `capnp` compiler installed: apt install capnproto)
cargo check                # type-check only
RUST_LOG=info cargo run    # start a node (capnp-rpc on :9292, control API on :9293)
```

### Jepsen Tests

```bash
cd jepsen/barka
CLASSPATH= lein run test --barka-bin /path/to/target/debug/barka
```

Jepsen runs locally (no SSH/VMs needed) — starts barka as a subprocess, sends produce/consume ops via the JSON control API, then checks queue ordering invariants. Currently expected to report `:valid? false` since the storage backend is a stub.

## Architecture

Two communication layers:
- **Cap'n Proto RPC** (port 9292): The real client protocol. Schema in `schema/barka.capnp`, compiled by `build.rs` with `default_parent_module(["rpc"])` so generated code lives at `crate::rpc::barka_capnp`. capnp-rpc is `!Send`, so the RPC server runs on a dedicated thread with its own single-threaded tokio runtime and `LocalSet`.
- **JSON control API** (port 9293): Newline-delimited JSON over TCP. Exists for Jepsen to drive the system. Protocol: `{"op":"produce","topic":"...","partition":0,"value":"..."}` / `{"op":"consume","topic":"...","partition":0,"offset":0,"max":10}`.

### Key design decisions
- **Storage is a black box**: `src/storage.rs` is an opaque trait — intentionally has no implementation details.
- **Lease is a stub**: `src/lease.rs` defines the S3 lease interface (`acquire`/`heartbeat`/`release`) with `todo!()` bodies.
- **Partitions are keyed by `(topic, partition_id)`**: The `Node.partitions` map uses `(String, u32)` as the key. Topics are created on first write.
- **No Raft**: Leadership is S3-based leases, not consensus protocol.
- **No wrapper structs for capnp-rpc**: `Node` implements `barka_svc::Server` directly. It holds `Arc<Mutex<...>>` internally so it's cheap to clone. `serve_rpc` takes a `Node` by value, creates one capnp client, and clones it per connection (same pattern as [queueber](https://github.com/asg0451/queueber)). Don't introduce intermediate types to bridge capnp-rpc and shared state.

### Jepsen test structure (`jepsen/barka/`)
- `db.clj` — starts/stops barka as a local process, waits for control port
- `client.clj` — TCP/JSON client that talks to the control API
- `core.clj` — test definition with produce/consume generator and `log-checker` that validates ordering and completeness
