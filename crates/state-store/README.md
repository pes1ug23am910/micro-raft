# Applied-state storage

`state-store` provides atomic batches of namespaced byte keys, values/tombstones, and one applied-index watermark. The LSM implementation and the redb 4.3.0 adapter share the same commit, replay, snapshot-install, lookup and scan interface. A caller uses one batch for application values, retained retry records and progress. The consensus log remains a separate recovery component.

Ordinary commits advance the watermark by exactly one. `commit_range(first, last, cells)` atomically advances a contiguous range of at most 256 entries; its first index must follow the durable watermark. The caller evaluates every entry in order before coalescing final cells, preserving each request's original reply. Repeating the latest range and identical encoded cells returns `Replay`; the digest binds both ends. Changed payloads, gaps and older indices are rejected before durable mutation. A batch has distinct nonempty keys, at most 65536 records and 16 MiB of logical key/value bytes. Individual keys are at most 4096 bytes and values at most 1 MiB. A separately fenced snapshot install can replace all cells at an authoritative boundary, with a 128 MiB/one-million-cell input bound. Materialized scans stop at 256 MiB. File-count and compaction limits can reject workloads earlier; these are explicit capacity boundaries.

Both engines finish their content durability barrier before returning commit success. A failure after durable work starts leaves the outcome unknown and makes the handle unusable until reopen. Admission errors use `InvalidInput`; corruption uses `InvalidData`. Diagnostic counters remain available after a failed mutation. A directory lock prevents two adapter writers from opening the same state directory.

A versioned `ENGINE` identity is durably published before first payload creation. Reopening with another backend, a missing selected database/manifest, malformed or missing identity, or interrupted initialization fails closed. An incomplete first creation requires operator review and a fresh directory; open does not erase or silently migrate the ambiguous directory. Application group identity remains the caller's additional responsibility.

## LSM layout and recovery

The memtable holds the newest record for each key. Each WAL frame contains a complete atomic batch, a length, format marker and SHA-256 checksum. A separate fixed-header checksum validates the length before recovery interprets a short payload as an incomplete tail. Recovery requires contiguous ranges. It trims only an incomplete final frame; a complete frame with invalid contents fails open without silently removing it.

SSTables store sorted, versioned records in approximately 16 KiB blocks; a single larger value occupies its own block. Each block and the footer have checksums. The footer contains exact block ranges/offsets and a serialized Bloom filter with seven probes and ten bits per key, rounded to whole bytes with a 64-bit minimum. Open validates the entire table, its index, sequence bounds and all inserted keys against the filter. A lookup then reads only the candidate block after range/filter checks. Merge iteration retains each input run's identity and resolves equal keys by sequence; contradictory equal-sequence values fail as corruption.

Level zero may overlap. Levels one and two are non-overlapping. When level zero reaches its trigger, a compaction drains its selected runs together with all overlapping level-one runs. Otherwise an over-limit level one moves its oldest run and overlapping level-two runs. Draining level zero together prevents the pre-publication memtable flush from replacing each removed run and perpetuating the trigger. Output streams into bounded tables. A tombstone remains whenever an unselected run could contain an older value at that key. A maintenance call runs at most one bounded job; pending work is visible in statistics. The worker includes this synchronous maintenance in the mutation response interval.

Default thresholds are a 1 MiB memtable, 512 KiB output tables, four level-zero runs and 8 MiB at level one. One compaction reads at most 64 MiB of input by default; configuration admits bounds up to 256 MiB. At most 256 tables are selected at once. Iteration buffers one checked block per participating run plus one output table; scans explicitly materialize their result. Recovery reads at most 64 MiB of WAL. These bounds describe this implementation's supported operating range.

A flush or compaction writes new immutable tables and an empty replacement WAL, synchronizes their content and directory entries, then replaces the checksummed `CURRENT` manifest and synchronizes its directory. Only afterward can old owned tables/WALs be removed. Reopen validates the selected generation before reclaiming unselected reserved filenames. A missing or corrupt selected file fails recovery. A directory-sync failure after rename is uncertain publication; the still-open handle cannot continue. Linux and Windows use actual directory barriers and propagate unsupported/filesystem errors.

## redb adapter

Cells and versioned progress live in two tables within the same `Durability::Immediate` transaction. Snapshot installation clears and replaces the cells in that transaction. The adapter uses an 8 MiB redb cache and delegates file locking to redb's `FileBackend`, while a wrapper counts successful backend writes and sync calls. Redb's persistence and repair implementation remains the upstream library's responsibility. Its [durability API](https://docs.rs/redb/4.3.0/redb/enum.Durability.html) describes the selected commit contract.

## Engine worker and measurements

Build with `cargo build --release -p state-store --bin state-worker --locked`. Start `state-worker --engine lsm --directory ./data/state` (or `--engine redb`). It emits a versioned ready record, then accepts one bounded JSON request per line and produces one response per request. For example:

```json
{"op":"commit","index":1,"changes":[{"key":[107],"value":[118]},{"key":[114],"value":[49]}]}
{"op":"get","key":[107]}
{"op":"statistics"}
{"op":"exit"}
```

Operations are `commit`, `commit_range`, `get`, `scan`, `install`, `flush`, `maintain`, `statistics` and `exit`. Responses distinguish `ok`, `absent`, `rejected`, `unknown` and `error`. A missing mutation response is unknown to the observer. The worker flushes each complete response; `exit` performs durable finalization. The process-kill tests deliberately bypass that exit path after receiving a complete commit ACK.

Counters separate logical key/value bytes, WAL writes, flush-table writes, compaction writes, manifest bytes, redb backend writes, content syncs and directory syncs. Bloom counters count in-range table probes, positives and false positives; a logical lookup can probe several tables. The false-positive denominator is `probes - (positives - false_positives)`. Zero denominators remain undefined. `disk_bytes` sums logical file lengths. Counters report bytes submitted through the engine and successful sync calls; allocated blocks, filesystem amplification and physical-device writes require separate instrumentation.

Paired comparison uses one executable and identical synthetic value/retry-cell batches, seeds, durability policy and filesystem, alternating engine order. Raw samples preserve observer IPC latency, engine operation latency (including synchronous maintenance), outcomes and failures. Engine-only closed-loop rates include request/response and recording overhead; they do not describe replicated service throughput or open-loop tail latency. Setup, workload and finalization counters are recorded separately, and pending compaction remains visible.

Run `cargo test -p state-store --locked` for reference-map/reopen, table corruption, incomplete WAL, equal-key merge, Bloom serialization, publication interruptions, sync failures, retained tombstones, directory-lock and real worker termination tests. Syscall failure injection and process termination establish their stated failure models; physical power interruption is a separate experiment.
