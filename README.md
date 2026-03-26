# Barka

A Kafka-style distributed append log backed by **S3**. Ephemeral nodes coordinate **partition leadership** with S3 conditional writes and leases—no Raft. **Produce** goes through the leader; **consume** reads segment objects from S3 directly, so any node can serve reads. RPC uses **Cap'n Proto** (including a zero-copy path on the produce side).

This repository is a research / correctness-oriented system: it ships a Jepsen harness that runs locally and checks ordering invariants.

## Requirements

- **Rust** (toolchain in `rust-toolchain.toml` if present; otherwise stable)
- **[Cap'n Proto compiler](https://capnproto.org/)** — e.g. `apt install capnproto` on Debian/Ubuntu
- **AWS S3–compatible storage** — real AWS or LocalStack for development and tests
- **Leiningen** — for Jepsen tests under `jepsen/barka/`

## Build

```bash
cargo build
cargo check   # type-check only
```

CI expects a warning-free clippy run:

```bash
cargo clippy -- -D warnings
```

## Binaries

| Binary | Role |
|--------|------|
| `produce-node` | Leader election + produce RPC (Cap'n Proto `ProduceSvc`, default listen **:9292**) |
| `consume-node` | Consume RPC (Cap'n Proto `ConsumeSvc`, default listen **:9392**) |
| `jepsen-gateway` | Newline-delimited JSON over TCP (**:9293**), routes to produce/consume for Jepsen |
| `barka-cli` | Operational CLI (S3, consume, leader helpers, etc.) |
| `barka-load` | Produce load tester |
| `rebalancer` | Partition rebalancer (reads/writes routing metadata in S3) |
| `barka-segment` | Decode on-disk segment files to JSON |
| `barka-viz` | TUI cluster visualizer |

Example runs (set `RUST_LOG` as you prefer):

```bash
RUST_LOG=info cargo run --bin produce-node
RUST_LOG=info cargo run --bin consume-node
RUST_LOG=info cargo run --bin jepsen-gateway
```

Configure S3 via the usual AWS SDK environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, …) and optional endpoint overrides for LocalStack (e.g. `AWS_ENDPOINT_URL` or per-binary flags — see `--help` on each binary).

## Architecture (short)

- **Leadership** — S3 lock objects under a namespace prefix; `put` with `if-none-match: *` to claim epochs. See `src/leader_election.rs` and [this write-up on S3 leader election](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/).
- **No consensus protocol** for the log body: the authoritative store is S3; leases only gate writers.
- **Separate Cap'n Proto services** for produce vs. consume (`schema/barka.capnp`); produce uses a custom `BytesVatNetwork` for zero-copy payloads.

For more detail (RPC threading model, Jepsen layout, conventions for contributors), see **[`CLAUDE.md`](CLAUDE.md)**.

## Tests

Many unit tests (especially leader election) expect **LocalStack** with S3 on port **4566**:

```bash
make localstack
cargo test
```

Tear down when finished:

```bash
make localstack-down
```

## Jepsen

End-to-end checks run produce/consume through the gateway and validate log behavior:

```bash
make jepsen
```

This builds the Rust binaries, ensures LocalStack is up, then runs the Clojure test under `jepsen/barka/`. You can tune `--num-nodes` and `--num-partitions` via `NODES` and `PARTITIONS` make variables.

Manual equivalent (from `jepsen/barka`):

```bash
CLASSPATH= lein run test --bin-dir /absolute/path/to/target/debug
```

## Jepsen gateway protocol (sketch)

Newline-delimited JSON over TCP:

- Produce: `{"op":"produce","topic":"...","partition":0,"value":"..."}`
- Consume: `{"op":"consume","topic":"...","partition":0,"offset":0,"max":10}`

## Repository layout

| Path | Contents |
|------|----------|
| `src/` | Library and binaries |
| `schema/` | Cap'n Proto schema |
| `jepsen/barka/` | Jepsen test and local process orchestration |

## Contributing

Agent and editor notes live in [`AGENTS.md`](AGENTS.md) and [`CLAUDE.md`](CLAUDE.md). Prefer merge commits over rebasing on shared branches.
