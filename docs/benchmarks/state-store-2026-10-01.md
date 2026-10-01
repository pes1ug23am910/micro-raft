# State-store comparison — 2026-10-01

Eight alternating-order runs of the same 1,200-operation workload passed for each revision. Every final cell and applied watermark matched the reference map; all owned workers exited successfully. The initial implementation exposed a maintenance feedback loop. Its results are retained below alongside the corrected implementation.

These are small engine-only, single-client measurements on Windows/NTFS (Intel Core Ultra 7 155H, about 16 GiB RAM). They do not measure replicated HTTP throughput, physical-device writes, power-loss behavior or a general engine ranking. Project builds were paused during timing; unrelated operating-system background work was not controlled.

## Workload and results

Each pair uses one seed (20261001–20261004), 128 keys and 256-byte put values. Seeded operation choices target 20% reads, 20% deletes and 60% puts; every mutation atomically includes a synthetic retained-retry cell and progress. Both adapters use synchronous commits. LSM thresholds are deliberately small to exercise compaction: 16 KiB memtable, 8 KiB output table, 64 KiB level-one threshold. Redb 4.3.0 uses an 8 MiB cache. This is a custom workload, not an official YCSB run.

| Pair | Engine | Operations/sec | p95 ms | p99 ms | Maximum ms | Issued-write amplification | Compactions |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | lsm | 1149.9 | 0.969 | 8.249 | 29.547 | 3.60 | 8 |
| 1 | redb | 1026.8 | 1.571 | 2.095 | 2.839 | 97.32 | 0 |
| 2 | redb | 946.7 | 1.795 | 2.354 | 3.123 | 97.73 | 0 |
| 2 | lsm | 1109.9 | 1.040 | 8.446 | 30.795 | 3.70 | 8 |
| 3 | lsm | 1157.2 | 0.977 | 8.013 | 32.280 | 3.62 | 8 |
| 3 | redb | 905.1 | 1.853 | 2.177 | 2.676 | 96.99 | 0 |
| 4 | redb | 956.8 | 1.699 | 2.031 | 2.860 | 96.58 | 0 |
| 4 | lsm | 1082.6 | 1.044 | 8.770 | 33.285 | 3.65 | 8 |

The corrected runs finished the workload with no pending compaction. Each LSM run performed eight compactions. Across its four runs, Bloom filters made 1054 in-range table probes and returned 1 false positive out of 425 known-negative probes. This is a small denominator, not a general false-positive-rate estimate. Filters use seven probes and ten bits/key, with byte rounding and a 64-bit minimum.

Latency quantiles use successful reads and mutations together, with nearest-rank percentiles. The maximum includes occasional synchronous maintenance that p95 can miss. Rates include observer IPC and recording overhead. Amplification divides WAL, flush-table, compaction and metadata writes (or redb backend writes) by logical changed key/value bytes; it excludes setup and reports finalization separately in the [machine-readable results](state-store-2026-10-01.json). File sizes are logical lengths, not allocated storage.

## Initial result and correction

The initial LSM performed 834–859 compactions per run, retained pending maintenance and achieved 76–79 operations/sec; redb achieved 938–1,140. A compaction flushed the active memtable into level zero, then removed only one level-zero run. Continued writes could keep that level at its trigger and prevent work at the next level.

A regression executed against that implementation reproduced the problem. The corrected job drains the selected level-zero set and every level-one run overlapping its full key span, retaining existing input limits. A second regression checks that continued writes permit level-one promotion to level two. The comparison was then repeated with identical workload hashes and a separately built executable. The corrected measurements support this local implementation change; four small pairs do not establish statistical significance or behavior under other workloads.

## Reproduction

```powershell
cargo build --release -p state-store --bin state-worker --locked
python scripts/engine_benchmark.py --worker target/release/state-worker.exe --out data/engine-comparison --operations 1200 --pairs 4 --seed 20261001 --keys 128 --value-bytes 256
```

On Linux, use `target/release/state-worker`. The output directory must be new. The runner retains workload inputs, all per-operation outcomes/intervals, setup/workload/finalization counters, final-state checks and worker cleanup results. Raw data and pinned binaries for this dated observation are retained in the private evidence archive; the JSON report records source, binary, workload and sample hashes. The public runner regenerates the same deterministic workload.

Engine tests cover seeded reference maps, reopen, atomic ranges and retry cells, compaction, missing/corrupt selected files, independently checked WAL lengths, partial tails, directory identity, injected publication/sync errors and actual child termination after a complete ACK. Service read-cache behavior and Raft durability are separate integration contracts.
