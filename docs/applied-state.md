# Durable applied state

`kv-node --state-backend memory|lsm|redb` selects the application-state backend.
`memory` is the default. All modes retain Raft's durable consensus log and
snapshot generations. The LSM and redb modes additionally commit application
cells and an applied watermark before publishing those cells to HTTP readers or
sending their successful proposal responses.

Choose a persistent backend when creating a fresh node directory. A durable
`application-identity.json` binds the chosen backend and immutable group/genesis
identity. Reopening with a different backend, group or genesis fails. Existing
unmarked legacy directories can adopt `memory`; conversion of an initialized
directory to LSM/redb is not an implicit migration. Adopting a legacy group's
identity uses the explicit [membership migration procedure](membership.md);
it does not change the backend. Keep the entire node directory together when
backing up or restoring it.

## Atomic application

Values, registration nonces, session headers, retained retry records, counters,
and the applied index/term share one engine transaction. Adjacent committed
`Apply` effects are evaluated in order and coalesced into an atomic prefix of
at most 256 entries and 16 MiB of changed encoded cells. Every non-`Apply` effect
ends that prefix. Repeated updates to one cell contribute its final bytes to
the limit. Each operation retains its own response, including a session retry's
original result index.

The application write lock covers preparation and the durable engine commit.
An undo journal records the old values, registration mapping, session metadata,
retry records, counters, and watermark of each touched entry. A failed commit
restores that whole prefix before releasing the lock. The writer then stops,
and admitted requests with unresolved responses have unknown outcomes. A crash
can occur after the durable engine transaction and before the response; retry a
protected operation with the same session/key/sequence/payload.

HTTP reads use a complete in-memory application cache in every mode. They do
not exercise SSTable reads or Bloom filters. This implementation is not an
out-of-core serving architecture: cache memory grows with application state,
and snapshots remain bounded by their existing 64 MiB image limit. Engine-only
read/compaction measurements have a separate workload and must not be presented
as HTTP measurements.

## Recovery and snapshot installation

Named groups first restore consensus identity and reconcile durable membership
across hard state, the selected snapshot and retained log. Required membership
refreshes persist before engine recovery. After restoring the engine watermark,
the writer replays any remaining known committed configuration prefix before
opening listeners. A wrong group or incompatible history fails closed.

Before listeners start, recovery validates canonical engine cells, identity,
counters, cached responses, and applied index/term. It reconstructs the expected
application from the selected Raft snapshot and retained WAL through that index,
then requires exact equality with the engine state. The validated watermark is
restored into the core so already visible entries are not reapplied. An engine
index outside the retained Raft history or state that disagrees with replay is a
startup error, rather than a fallback to empty state.

Remote snapshot publication first durably selects its Raft snapshot/WAL
generation. The application engine then installs the complete validated image,
after which the shared application changes and the final transfer acknowledgement
may be sent. If a process crashes between the Raft publication and engine install,
the selected newer image repairs the older engine on reopen. An install error
stops the writer without exposing the new application or sending the final ACK.

At normal loop boundaries the writer performs at most one engine maintenance
job before returning to input processing. Shutdown attempts both application and
consensus-store flushes. Errors propagate to a nonzero binary exit; filesystem
calls themselves cannot be preempted by the logical quorum-drain deadline.

## Status and checks

`GET /status` includes `applied_store.backend`, `applied_store.applied_index`,
`applied_store.durable`, and the engine's cumulative statistics since open.
The `durable` field describes the additional applied-state engine; `memory`
continues to rely on durable Raft log/snapshot recovery. Engine write-byte and
sync counters describe submitted operations, not physical device writes or
power-loss survival. `--check-config` reports the backend without opening data.

The targeted checks are:

```sh
cargo test -p kv-node --lib applied_store
cargo test -p kv-node --lib engine_
cargo test -p kv-node --test engine_binary
```

The process tests use fresh three-node clusters for each persistent backend,
stop a follower, require a multi-chunk snapshot to catch it up, abruptly terminate
all owned children, reopen them, and verify protected retries keep their original
response while an independent later value remains visible. These are process
crash and OS durability-contract checks; they do not simulate a device losing
power. Unit counterexamples cover corrupt/mismatched application cells, backend
identity changes, failed range commits, byte-budget splits, snapshot installation
ordering, and final flush errors.
