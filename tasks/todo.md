# Multi-topic and multi-partition support

- [x] Add partition_data_prefix() and leader_namespace() helpers to src/node.rs
- [x] Refactor ProduceNode: TopicConfig, partition map, multi-partition new() and serve()
- [x] Refactor ConsumeNode to pass s3_config/prefix to RPC server
- [x] Update rpc/server.rs produce+consume handlers to route by (topic, partition)
- [x] Add --topics CLI arg to src/bin/produce-node.rs
- [x] Update rpc/server.rs tests for new multi-partition API
- [x] cargo build + clippy + run tests to verify

## Review

All items complete. Summary of changes:

### `src/node.rs`
- Added `partition_data_prefix(base, topic, partition)` and `leader_namespace(topic, partition)` helpers with tests.

### `src/produce_node.rs`
- Added `TopicConfig`, `PartitionProduceState`, `PartitionMap` types.
- `ProduceNodeConfig` gains a `topics: Vec<TopicConfig>` field (default: `default:1`).
- `ProduceNode` now holds a `PartitionMap` — a `HashMap<(String, u32), PartitionProduceState>` — instead of a single producer/leadership.
- `new()` creates one `PartitionProducer` + `LeadershipState` per configured (topic, partition).
- `serve()` spawns one leader election loop per partition.

### `src/consume_node.rs`
- Removed the single hardcoded `PartitionConsumer`.
- Now passes `S3Config` + base prefix to the RPC server for lazy consumer creation.

### `src/rpc/server.rs`
- `serve_produce_rpc` takes a `PartitionMap`; `PerConnectionProduceNode` looks up producer/leadership by (topic, partition) from each request.
- `serve_consume_rpc` takes `S3Config` + base prefix; lazily creates `PartitionConsumer` instances per (topic, partition) via `get_or_create_consumer`.
- Shared consumer map uses `Rc<RefCell<HashMap>>` (single-thread `LocalSet`).

### `src/bin/produce-node.rs`
- Added `--topics` CLI arg (env `BARKA_TOPICS`, default `default:1`), parsed as `TOPIC:NUM_PARTITIONS[,...]`.

### Tests
- All 47 tests pass. Test helpers updated to build partition maps matching the topics used in each test.
