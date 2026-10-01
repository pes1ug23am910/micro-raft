# Snapshots and recovery

This page describes `kv-node`. The shard service uses the same durable
snapshot/WAL publication primitives with a separate controller/data-state
carrier; its actor settings and HTTP status schema are described in
[sharding](sharding.md).

`--snapshot-threshold N` attempts a local snapshot after N additional applied
entries. The default is 256. Zero disables automatic capture while preserving
snapshot recovery and transfer. The boundary is the application's actual applied
index and its corresponding log term, rather than a scheduled apply position.

An image includes values, session registrations, closed sessions, retained
per-key retry responses and configuration metadata. Its framing contains a
version, declared length and SHA-256 digest. Decoding validates the complete
application and boundary before the image can become active. Snapshotting does
not expire sessions or reset sequence numbers.

Legacy fixed-configuration V1 images remain readable. Membership-aware V2
images use a distinct `MRFTSN02` header and carry the complete validated
membership history, including the immutable group/genesis identity and
retained administrative results. An image can predate a learner's admission;
the retained log supplies later configuration changes. The receiver validates
the sender and current group authority separately from the image boundary.
See [membership recovery](membership.md) for explicit bootstrap and migration.


## Publication order

The durable generation consists of an immutable snapshot and its retained WAL
suffix. A checksummed `CURRENT` record selects that pair.

1. Write and sync the new snapshot and suffix files, then sync their directory.
2. Write and sync the replacement `CURRENT`, rename it into place, and sync the
   directory again.
3. Reclaim obsolete generations and sync their directory, then notify the core
   that publication completed.

Recovery follows the selected pair. A missing or corrupt selected file is an
error; an older surviving snapshot cannot silently replace it. Unselected files
are reclaimed after storage-layer snapshot and WAL validation. Remaining
consensus and engine recovery must also succeed before service. A publication
or reclamation I/O failure stops further mutation through that storage handle.

Recovery also reconciles the selected image with the durable membership
watermark and retained log. If publication selected a newer valid image before
a crash, recovery persists the corresponding membership authority while
preserving term and vote, before engine recovery or service. Incompatible
histories fail closed; an older snapshot cannot override later authority.


Before the first snapshot, nodes retain the legacy `log.jsonl` layout. After
publication, `CURRENT` selects the generation WAL. Recovery checks contiguous
indices, term ordering and every complete CRC-framed WAL record. Only an
unterminated final record is treated as a torn tail. File and directory barriers
are implemented on both Windows and Linux; process tests do not establish
survival through every physical power-loss scenario.

## Follower catch-up

A leader sends an active image when a follower needs entries already compacted
away. Transfer uses 64 KiB chunks and one disk-backed incoming staging slot.
The descriptor binds the boundary, complete byte length and image hash; the
transfer identity also binds the leader, term and transfer sequence. Duplicate
chunks are checked, and stale or inconsistent chunks cannot advance the slot.
After a receiver restart, transfer resumes from offset zero with a fresh transfer
sequence rather than treating the old temporary file as committed state.

The receiver validates the complete staged image, publishes its snapshot/WAL
generation, installs the application state, and only then sends the final
successful acknowledgement. When an additional applied-state engine is enabled,
its snapshot install also finishes before the application and ACK are exposed.
An interrupted publication can recover the old or new selected generation;
partial incoming bytes never become usable application state.

## Bounds and observability

| Item | Bound or behavior |
|---|---|
| Encoded snapshot image | 64 MiB |
| Chunk | 64 KiB |
| Retained suffix in one publication | 64 MiB |
| Incoming staging slots | One for the node's Raft group |
| New generation plus incoming stage | At most 192 MiB, excluding the old generation and active WAL growth |
| In-memory application | Complete state, including when LSM or redb is enabled |

These are operation bounds, not a total disk quota. A snapshot that exceeds its
capacity is refused while the existing WAL is retained. `snapshot_error` records
the failure, and automatic capture waits for another threshold of applied entries
before retrying. Continued writes can therefore grow the retained log.

`GET /status` reports `snapshot_index`, `snapshot_term`, `retained_log_entries`,
`snapshot_threshold` and `snapshot_error`. A nonzero snapshot index establishes
the local compaction boundary; it does not establish remote catch-up by itself.

## Verification

```sh
cargo test -p kv-node --lib snapshot
cargo test -p kv-node --lib storage
cargo test -p kv-node --test live_driver
cargo test -p kv-node --test engine_binary
cargo test -p sim --test snapshots
```

The suites cover publication/reopen boundaries, selected-file corruption and
absence, duplicate and interrupted transfers, lagging-follower catch-up, and
session retry/delete/close preservation. The live catch-up fixture uses an image
larger than one chunk and requires the recovered follower's application boundary
and data to agree with the committed state. The engine process fixture repeats
catch-up and abrupt all-process restart with both persistent backends. The
membership process tests add snapshots produced before learner admission,
removed-node catch-up and final-configuration recovery under TCP-gated faults.
