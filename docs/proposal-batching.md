# Proposal batching and WAL group commit

The writer collects at most 16 proposals and 1 MiB of canonical JSON command
bytes per batch. The default collection deadline is 2 ms from the first request.
`--batch-records` accepts 1–16 and `--batch-delay-ms` accepts 0–10. The HTTP
proposal channel holds 256 requests; one request that exceeds the remaining
batch byte budget is retained for the next batch in FIFO order. A command that
alone exceeds the byte limit is rejected before Raft admission. HTTP body/key
limits still apply independently.

The core admits an entire batch atomically and emits one ordered WAL append
before its individual proposal acceptance effects. Storage writes all CRC
frames, calls `sync_all`, and only then permits later effects. Each request
keeps its own log index and response; HTTP success still requires quorum commit
and application. Followers independently persist the replication entries they
accept. Batching does not remove hard-state or directory durability barriers.

Canceled requests are skipped before admission. Cancellation after admission
only removes the response waiter: it cannot undo committed application. Session
retries within one batch keep the original cached result and do not repeat a
mutation, including when an independent write occurs between the original and
its retry. If a WAL operation fails, all affected admitted waiters receive an
unknown outcome and the writer terminates. A partially written operation is
never reported as definitely rejected.

After every batch the writer refreshes its logical clock and services up to one
queued peer input and one queued checked read before another proposal batch.
It also refreshes the clock immediately before batch admission. Sparse traffic
flushes at the configured deadline; busy traffic flushes at the record/byte
bound. Even canceled requests count toward a finite 16-request collection scan,
so a producer of canceled work cannot monopolize the writer. A shutdown request
interrupts collection, rejects work not yet admitted, and drains accepted work
under the existing five-second quorum-wait budget. A synchronous filesystem
call already in progress cannot be preempted by that asynchronous budget.

`GET /status` exposes `batching` settings and counters: admitted batches/records,
encoded command bytes, observed maxima, sampled proposal queue peak, canceled
requests and oversized rejections. `storage.wal_append_sync_attempts` and
`wal_append_sync_completed` count actual append-path `sync_all` calls. Append
bytes/records count completed writes before that barrier. These counters restart
at process startup and deliberately exclude conflict rewrites, snapshot writes,
hard-state files, directory barriers and the final shutdown flush. They are not
physical-device write counters or a count of every filesystem sync.

## Reproduce a paired local measurement

Build once, then use the same executable for every sample:

```sh
cargo build --release -p kv-node --locked
python scripts/benchmark_batching.py --binary target/release/kv-node \
  --output /tmp/micro-raft-batching-fresh --pairs 3 --operations 256 \
  --concurrency 32 --sparse 8
```

Use the actual target path when `CARGO_TARGET_DIR` is configured; Windows binaries
end in `.exe`. The output directory must not exist. The script starts fresh local
three-process clusters and alternates single-request mode (1 record, 0 ms) with
batch mode (16 records, 2 ms) in AB/BA/AB order. Each sample has a sequential sparse
phase and a concurrent busy phase. Automatic snapshots are disabled in both
modes so compaction does not contaminate append-sync counts. The same unique-key
workload, replication factor and acknowledgment durability apply to both modes.

Artifacts retain every response and latency, errors, node logs and data, launch
arguments, executable/workload hashes, status/queue metrics, sync deltas, elapsed
time and acknowledgment throughput. Each write must return a strict valid ACK;
all replicas must then apply through the acknowledged boundary. A checked read
establishes a fence before local value readback, bracketed by stable leader/term
checks. A failed request, incomplete pair, invalid acknowledgment, readback
mismatch or cleanup error prevents PASS. PUT phases have a 60-second request
admission deadline and readback a 30-second deadline; individual HTTP operations
are also bounded. Startup and replication catch-up have separate finite budgets.

Cleanup deliberately hard-terminates only the child handles created by the
measurement and retains their data directories. It is not a graceful-shutdown
or physical-power-loss test. Results describe one host, its filesystem, selected
build and chosen workload. Keep raw samples and report both latency and
throughput, including regressions; fewer WAL barriers alone is not a general
performance result.
