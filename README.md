# micro-raft

A Rust implementation of the Raft consensus core, with a deterministic fault-injection simulator, a TCP election driver and standalone storage components.

The project explores leader election, replicated-log consistency and commitment under network failures. The consensus core and simulator implement log replication and application histories. The runnable node currently demonstrates elections over TCP; its persistence effects and client key-value API are not wired into the driver.

## Architecture

| Crate | Responsibility |
|---|---|
| `raft-core` | I/O-free state machine for leader election, log replication and commitment. Inputs include messages, ticks and client proposals; outputs are ordered effects. |
| `kv-node` | Tokio/TCP transport, command-line configuration and election driver. Storage helpers implement atomic hard-state replacement, CRC32 records and torn-tail recovery, independently of the driver. |
| `sim` | Seeded virtual time and networking, message loss, partitions, application histories and safety checks. |

## Implemented mechanisms

- **Leader election:** randomized timeouts, term advancement, vote eligibility based on log freshness, split-vote recovery and higher-term step-down.
- **Replication:** previous-index/term checks, conflict-only suffix repair and per-follower replication tracking.
- **Commitment:** a strict majority must replicate a current-term entry before the leader advances its commit index. Older entries commit through that current-term entry, as required by Raft's Figure 8 scenario.
- **Ordered effects:** the core emits persistence before dependent replies and applies committed entries in index order. The simulator executes these effects; the TCP driver does not yet execute storage or application effects.
- **Storage components:** atomic hard-state replacement and checksum-based log recovery, covered by isolated storage tests.

## Tests

The workspace contains 32 automated tests, including seven election scenarios and seven replication scenarios. Tests cover divergent-log repair, the Figure 8 commit rule, transport framing/reconnection, CRC32 and storage recovery.

The simulator checks election safety, term monotonicity, log matching and agreement between applied histories. A replication soak runs 20 seeds with three nodes and 10% message loss, followed by fault-free convergence checks. Failures include the seed for reproduction.

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## TCP election demo

Run each command in a separate terminal:

```sh
cargo run -p kv-node -- --id 1 --peers 2@127.0.0.1:7102,3@127.0.0.1:7103 --data-dir data/n1 --http-port 8101 --raft-port 7101
cargo run -p kv-node -- --id 2 --peers 1@127.0.0.1:7101,3@127.0.0.1:7103 --data-dir data/n2 --http-port 8102 --raft-port 7102
cargo run -p kv-node -- --id 3 --peers 1@127.0.0.1:7101,2@127.0.0.1:7102 --data-dir data/n3 --http-port 8103 --raft-port 7103
```

These nodes exchange Raft messages and elect a leader. `--data-dir` and `--http-port` are required configuration fields but are not connected to storage or an HTTP service yet. The driver logs unhandled persistence and application effects; it is not a durable key-value service.

## Scope

Runtime persistence/recovery, client proposal handling, key-value application and HTTP responses remain unfinished. Simulator crash/restart schedules, snapshots, membership changes and client-session deduplication are also outside the implemented scope.

Handwritten notes in [`Evidence/`](Evidence/) cover RequestVote decisions, log freshness and split-vote timing.

## License

MIT — see [LICENSE](LICENSE).
