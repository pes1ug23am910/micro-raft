# Conflict-term backtracking

A rejected AppendEntries consistency check can return an optional `conflict`
object with the rejected `prev_log_index`, the follower's term at that index,
and the first available index of that term. When the follower is too short,
the term is `null` and the first index is its last log index plus one.

The leader uses the last local index of the reported term plus one when that
term exists locally. Otherwise it retries at the follower's first index. This
implements the optional log-repair optimization in
[the Raft extended paper, section 5.3](https://raft.github.io/raft.pdf).
The in-memory log has monotonic terms, so both term-boundary lookups use binary
search. No additional persistent term index is required.

## Safety and delayed messages

- The hint must refer to the leader's current retry anchor. Delayed or duplicate
  rejections for an older anchor are ignored.
- A rejection changes only `next_index`. It never advances the peer's
  `match_index`, commits an entry, or establishes a checked-read result.
- The retry cursor moves backwards, bounded below by the peer's confirmed
  `match_index + 1`. Replies without a hint obey the same lower bound.
- Invalid hints, including inconsistent bounds, impossible terms, or a hint on
  a success reply, produce no progress or quorum-contact credit. Ordinary
  higher-term step-down still applies.
- Malformed payloads and committed-prefix/configuration rejections carry no
  conflict hint. The existing rules preventing committed-log truncation remain.

The optimization reduces failed probes across long term runs; it does not
change durability, commit quorums, heartbeat cadence or the maximum entries per
replication batch. No general latency or throughput gain is asserted.

## Snapshots

A follower with a compacted prefix reports only an available term boundary:
its snapshot boundary can stand in for the first retained entry of a term.
It does not invent the original start of a compacted-away term run. Requests
anchored before that boundary still use the existing `CompactedPrefix` path.

The leader can find a term at its own snapshot boundary. If the selected retry
cursor falls inside the leader's compacted prefix, the existing snapshot
transfer path handles the next replication attempt. The leader's global commit
index is deliberately not the cursor's lower bound: a lagging follower may
still need committed entries or a snapshot.

## Wire format and regression coverage

`AppendEntriesReply.match_index` keeps its earlier meaning: a proven match on
success, the follower's last log index on failure. The optional `conflict`
field defaults to absent and is omitted on serialization when unused. Replies
without it retain last-index/decrement repair. Both scoped and legacy JSON
transports carry the hint; disk formats are unchanged. This additive field is
not a claim that arbitrary older releases support mixed-version clusters.

Core regressions cover invalid/stale replies, snapshot boundaries, confirmed
progress floors and JSON compatibility. Explicit two-node repair schedules
compare the same divergent logs with hints enabled and with the optional field
removed. For suffix lengths 1, 64 and 257, a single conflicting term requires
one rejected probe with hints, versus one per conflicting entry with fallback.
Both paths must reach identical logs and committed state, and retain the same
number of successful bounded append batches. These are deterministic regression
schedules, not network-performance measurements.
