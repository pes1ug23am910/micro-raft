# micro-raft

A distributed key–value store in Rust implementing the **Raft consensus algorithm from scratch** — no consensus libraries of any kind — running as a 3-node cluster over TCP, and verified by a deterministic fault-injection simulator.

Written by **Yash Verma**. Every commit in this repository is mine, and the design was defended in written re-derivations at each milestone gate before the next one was allowed to start (see [Authorship and gates](#authorship-and-gates)).

## Why it exists

Most Raft demos stop at "a leader gets elected." The interesting part is everything after: what happens when a follower's log diverges, when a message is dropped mid-replication, when a node crashes with a half-written record on disk. This implementation targets those cases specifically, and proves them with a simulator that can replay any failure deterministically from a seed.

## Architecture

Three crates in a Cargo workspace:

| Crate | Responsibility |
|---|---|
| **`raft-core`** | Pure consensus state machine — election, replication, message types, deterministic RNG. No I/O, no clock, no sockets. Every transition is a function of `(state, message)`. |
| **`kv-node`** | The runnable node — TCP transport with a length-framed codec, durable storage, CRC32 integrity, and the driver that wires the core to the outside world. |
| **`sim`** | Deterministic virtual-time and virtual-network simulator: injects partitions, message loss and reordering, then asserts safety invariants after every step. |

The core is deliberately I/O-free. That is what makes the simulator possible: the same `raft-core` that runs against real TCP sockets in `kv-node` runs against a virtual network in `sim`, with a virtual clock, so a failing run is reproducible from its seed rather than from luck.

## What's implemented

**Leader election** — randomised timeouts, term advancement, vote granting with the up-to-date log check, split-vote recovery, and the step-down paths on discovering a higher term.

**Log replication and commitment** — `AppendEntries` with the previous-index/term consistency check, conflict-only log repair (a follower truncates only where it genuinely diverges), commit-index advancement via match-index quorum, and the apply loop feeding the state machine.

**The Figure 8 rule** — a leader may only advance the commit index on an entry from its *own* term. This is the subtle one that makes naive implementations unsafe, and it's implemented as a pure `commit_advance` function so it can be tested in isolation and compared against a hand-derivation.

**Durability** — hard state (term, vote) is swapped atomically, so a crash mid-write leaves either the old value or the new one, never a torn mix. The log recovers from a torn tail: a partially written trailing record is detected by CRC and discarded rather than being replayed as truth.

## Verification

The simulator asserts these invariants after **every** step, not just at the end:

- **Election Safety** — at most one leader per term
- **Log Matching** — if two logs contain an entry with the same index and term, all preceding entries are identical
- **State Machine Safety** — no two nodes ever apply different commands at the same log index

On top of that, named acceptance suites: **7 election scenarios** and **7 replication scenarios**, plus a multi-seed message-loss soak. Unit tests cover storage recovery, the transport frame codec, CRC32 against reference vectors, and the deterministic RNG.

Failures print their seed, so any red run replays exactly.

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Running a cluster

```bash
cargo run -p kv-node -- --id 1 --peers 127.0.0.1:5002,127.0.0.1:5003 --listen 127.0.0.1:5001
```

Start three nodes on different ports and they will elect a leader and replicate among themselves.

## Authorship and gates

This was built as a learning-by-construction project under a rule I set for myself: **no milestone could begin until I had re-derived the previous one in writing, by hand, without reference to the code.** Those written gate submissions are preserved in [`Evidence/`](Evidence/) — they are the record that the consensus logic here is understood rather than merely present.

`Evidence/Section-A micro-raft.pdf` and `Evidence/Decide-Vote-rs-2nd-Attemp.pdf` are my handwritten derivations of the vote-decision and replication paths.

## Status

Leader election, log replication and commitment, durable storage, and the deterministic simulator are complete and green. Snapshotting, membership changes, and client-session deduplication are not implemented — the project's goal was correctness depth on the core protocol, not feature completeness.

## Licence

MIT — see [LICENSE](LICENSE).
