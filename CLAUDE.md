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
- **Reads don't require leadership**: Only produce (write) checks the leadership lease. Consume reads segments directly from S3 and can be served by any node.
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

---

## Working style (Claude Code agents)

### Strategy

1. **Plan mode default** — Enter plan mode for any non-trivial task (roughly three or more steps, or architectural choices). If execution goes sideways, stop and re-plan instead of pushing through. Use planning for verification as well as implementation. Write a clear spec up front to cut ambiguity.

2. **Subagent strategy** — Use subagents liberally to keep the main context clean. Offload research, exploration, and parallel analysis to subagents. For hard problems, use more parallel subagent work. One focused task per subagent.

3. **Self-improvement loop** — When a tool call is rejected, the user says "don't do X", or pushes back on an approach: **stop implementation immediately**, update `tasks/lessons.md` with the pattern, then resume. This is not a post-task activity — it is an interrupt handler. The next tool call after a correction must be an edit to `tasks/lessons.md`.

4. **Verification before done** — Do not mark work complete without evidence it works. When useful, compare behavior against main (or baseline) and your branch. Ask whether a staff engineer would accept the change. Run tests, inspect logs, and show correctness.

5. **Demand elegance (balanced)** — For non-trivial changes, pause and ask if there is a cleaner approach. If a fix feels hacky, redo it with full context—prefer the elegant path. Skip this for small, obvious fixes; avoid over-engineering. Critique your own work before handing it off.

6. **Autonomous bug fixing** — On bug reports: fix them without asking for hand-holding. Use logs, errors, and failing tests as ground truth. Prefer zero back-and-forth from the user; repair failing CI without waiting for step-by-step instructions.

### Task management

0. **Read lessons** — Before starting work, read `tasks/lessons.md` (if it exists). Apply anything relevant to the current task.
1. **Plan first** — Write the plan to `tasks/todo.md` with checkable items (create `tasks/` if it does not exist yet).
2. **Verify plan** — Sanity-check the plan before coding.
3. **Track progress** — Check items off as you finish them.
4. **Explain changes** — Give a short high-level summary at each meaningful step.
5. **Document results** — Add a brief review section to `tasks/todo.md` when wrapping up.
6. **Clean up** — Once the user confirms the solution is good, delete `tasks/todo.md`.
7. **Capture lessons** — After user corrections, update `tasks/lessons.md`.

### Core principles

- **Simplicity first** — Make each change as small and direct as possible; touch only what the task requires.
- **No laziness** — Find root causes; avoid temporary or cosmetic fixes. Hold work to senior-engineer standards.
