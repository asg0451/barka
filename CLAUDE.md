# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Barka

Kafka-like distributed log with an S3 backend. Ephemeral nodes coordinate partition leadership via S3-based leases (conditional writes with version tokens and heartbeats). Uses Cap'n Proto for zero-copy RPC and serialization. Includes Jepsen integration for correctness testing.

## Build Commands

```bash
cargo build                # build (requires `capnp` compiler installed: apt install capnproto)
cargo check                # type-check only
RUST_LOG=info cargo run    # start a node (capnp-rpc on :9292, Jepsen gateway on :9293)
```

### Jepsen Tests

```bash
cd jepsen/barka
CLASSPATH= lein run test --barka-bin /path/to/target/debug/barka
```

Jepsen runs locally (no SSH/VMs needed) — starts barka as a subprocess, sends produce/consume ops via the **Jepsen API gateway** (newline JSON over TCP), then checks queue ordering invariants. Currently expected to report `:valid? false` since the storage backend is a stub.

## Architecture

Two communication layers (same backend; the gateway is a frontend only):
- **Cap'n Proto RPC** (port 9292): The real client protocol. Schema in `schema/barka.capnp`, compiled by `build.rs` with `default_parent_module(["rpc"])` so generated code lives at `crate::rpc::barka_capnp`. capnp-rpc is `!Send`, so the RPC server runs on a dedicated thread with its own single-threaded tokio runtime and `LocalSet`.
- **Jepsen API gateway** (port 9293, env `BARKA_JEPSEN_GATEWAY_PORT`): Newline-delimited JSON over TCP for Jepsen. Implementation in `src/jepsen_gateway.rs`: translates each line to [`BarkaClient`](src/rpc/client.rs) Cap'n Proto calls to the node's RPC address (retries until the RPC listener is up). Runs on its own dedicated `current_thread` runtime + `LocalSet` for the same `!Send` reasons as the RPC server. Protocol: `{"op":"produce","topic":"...","partition":0,"value":"..."}` / `{"op":"consume","topic":"...","partition":0,"offset":0,"max":10}`.

### Key design decisions
- **Storage is a black box**: `src/storage.rs` is an opaque trait — intentionally has no implementation details.
- **Lease is a stub**: `src/lease.rs` defines the S3 lease interface (`acquire`/`heartbeat`/`release`) with `todo!()` bodies.
- **Partitions are keyed by `(topic, partition_id)`**: The `Node.partitions` map uses `(String, u32)` as the key. Topics are created on first write.
- **No Raft**: Leadership is S3-based leases, not consensus protocol.
- **No wrapper structs for capnp-rpc**: `Node` implements `barka_svc::Server` directly. It holds `Arc<Mutex<...>>` internally so it's cheap to clone. `serve_rpc` takes a `Node` by value, creates one capnp client, and clones it per connection (same pattern as [queueber](https://github.com/asg0451/queueber)). Don't introduce intermediate types to bridge capnp-rpc and shared state.

### Leader election (`src/leader_election.rs`)
- Based on [S3 conditional writes](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/): list lock files, check newest, `put_if_absent` with `if-none-match: *` to claim the next epoch.
- Lock files are scoped per namespace — listing must use `self.prefix` (`lock/{namespace}/`), not the global `lock/` prefix, otherwise namespaces interfere.
- When `try_become_leader` finds a valid lock belonging to the caller's own `node_id`, it returns `Leader` (not `NotLeader`), so callers can safely poll without losing track of their leadership.
- After winning an election, old epoch lock files are deleted via a fire-and-forget `tokio::spawn` (best-effort cleanup; failures are logged as warnings).

### Running tests
- Unit tests in `leader_election` require **LocalStack** running locally with S3 enabled: `docker run -d -p 4566:4566 localstack/localstack`
- Tests use unique bucket names (timestamp + atomic counter) for isolation when running in parallel. The counter matters because macOS clock resolution can cause nanosecond-timestamp collisions.
- Race-condition tests use `#[tokio::test(flavor = "multi_thread")]` with `tokio::sync::Barrier` to synchronize concurrent `try_become_leader` calls.

### Jepsen test structure (`jepsen/barka/`)
- `db.clj` — starts/stops barka as a local process, waits for the Jepsen gateway port
- `client.clj` — TCP/JSON client for the Jepsen gateway
- `core.clj` — test definition with produce/consume generator and `log-checker` that validates ordering and completeness
