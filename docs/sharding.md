# Replicated shard service

`shard-service` runs a controller Raft group and independently replicated data
groups in each service process. A committed controller route selects the owner
and epoch of each shard. Data groups reject stale epochs when applying writes;
checked reads require ReadIndex and an applied-state fence. This service has no
cross-shard transactions.

Each actor has its own durable membership. Listing another routable host in the
topology does not add a voter: a new host starts passive until admitted through
that group's replicated membership log. A controller maintenance ticket
serializes membership changes with shard handoffs across the service.

## Local three-process example

Build `cargo build -p shard-service`. Generate a topology in an empty working
directory using Python 3:

```python
import json
from pathlib import Path

topology = {
    "version": 1,
    "cluster": "local-shards",
    "groups": [1, 2],
    "owners": [1, 2, 1, 2],
    "genesis_voters": {str(g): [1, 2, 3] for g in (0, 1, 2)},
    "nodes": [
        {"id": n, "http": f"127.0.0.1:{8000+n}",
         "raft": {str(g): f"127.0.0.1:{9000+n*10+g}" for g in (0, 1, 2)}}
        for n in (1, 2, 3)
    ],
}
Path("topology.json").write_text(json.dumps(topology), encoding="utf-8")
```

Use an empty data directory for each process, and run each command in its own
terminal. `--check-config` validates topology before opening storage and prints
the immutable cluster fingerprint needed for HTTP requests.

```sh
target/debug/shard-service --topology topology.json --id 1 --data-dir data-1 --seed 1 --check-config
target/debug/shard-service --topology topology.json --id 1 --data-dir data-1 --seed 1
target/debug/shard-service --topology topology.json --id 2 --data-dir data-2 --seed 2
target/debug/shard-service --topology topology.json --id 3 --data-dir data-3 --seed 3
```

On Windows the executable ends in `.exe`. Each process opens a separate TCP
listener and durable directory for every group, plus one HTTP listener. The
background coordinator bootstraps each group through its replicated log. Wait
for a checked `configuration` response with a non-null `initialized_at` for
groups 0, 1 and 2 before client operations.

All POST bodies have the envelope `{"cluster":"FINGERPRINT","body":...}`.
For example, replace `FINGERPRINT` with the value printed by `--check-config`:

```sh
curl -sS http://127.0.0.1:8001/v1/checked -H 'Content-Type: application/json' \
  -d '{"cluster":"FINGERPRINT","body":{"group":0,"query":{"query":"configuration"}}}'
curl -sS http://127.0.0.1:8001/v1/checked -H 'Content-Type: application/json' \
  -d '{"cluster":"FINGERPRINT","body":{"group":0,"query":{"query":"route","shard":0}}}'
```

These examples use shell single-quote syntax; Windows callers can use
`curl.exe` with JSON from a file or `Invoke-RestMethod`. `/status` is diagnostic
and does not prove read authority.

## Writes, reads and retries

Keys map to shards by interpreting the first eight bytes of SHA-256 as a
big-endian integer and taking the remainder modulo the number of shards. For
this four-shard example, find a key for shard 0 with:

```python
import hashlib
print(next(f"key-{n}" for n in range(1000)
           if int.from_bytes(hashlib.sha256(f"key-{n}".encode()).digest()[:8], "big") % 4 == 0))
```

Send `register` to the current data owner through `/v1/dispatch`:

```json
{"cluster":"FINGERPRINT","body":{"group":1,"action":{"action":"register","shard":0,"epoch":1,"nonce":"client-a"}}}
```

Retain the returned `session` object, containing its original `group` and
`index`. A write uses that exact session and a per-key increasing sequence:

```json
{"cluster":"FINGERPRINT","body":{"group":1,"action":{"action":"mutate","shard":0,"epoch":1,"session":{"group":1,"index":3},"key":"KEY_FOR_SHARD_ZERO","sequence":1,"value":"hello"}}}
```

The example session index `3` is a placeholder: use the actual registration
receipt. A `null` value creates a retained deletion tombstone. A checked read
uses `/v1/checked` with
`{"group":1,"query":{"query":"read","shard":0,"epoch":1,"key":"KEY_FOR_SHARD_ZERO"}}`.

An HTTP response is successful only when the typed outcome confirms application
and contains the expected receipt. A timeout or `unknown` outcome can represent
an already committed operation. Retry the same session/key/sequence/payload;
after a move, first obtain the new route and use its owner and epoch. The
returned receipt keeps its original group and index even when the destination
log has unrelated indices. A changed payload for the same sequence is rejected.
Closing a session is permanent and its tombstone moves with the shard.

## Handoff and recovery

Request a move through `/v1/dispatch` on controller group 0:

```json
{"cluster":"FINGERPRINT","body":{"group":0,"action":{"action":"begin_move","request_id":"move-0-to-2","shard":0,"epoch":1,"destination":2}}}
```

The normal background coordinator advances this replicated intent. Obtain its
checked progress from `/v1/checked` with
`{"group":0,"query":{"query":"transfer","request_id":"move-0-to-2"}}`.
An accepted BeginMove response does not mean the move is complete.

The phase order is source fence, destination install, controller ownership,
destination activation, source cleanup, and completion. The source fence stops
both writes and checked reads. Destination installation pulls the exact frozen
image in bounded, digest-checked chunks and includes values, deletion
tombstones, sessions, closed-session state and cached retry receipts. An
incomplete destination never serves the shard. The interval from fencing until
destination activation is deliberately unavailable.

Each cross-group transition fetches fresh checked controller/source/destination
state and verifies the exact transfer, epoch, digest and namespaced indices.
The HTTP API accepts intentions rather than caller-supplied commit proofs.
Source data is retained until destination activation and controller ownership
are durably established.

Before ownership changes, an abort preserves the old route. Both endpoints
verify the committed abort, restore their prior usable state or discard staging,
and retain permanent exact-transfer tombstones. The controller releases the
groups for another move only after both recovery receipts. Ownership and abort
are mutually exclusive decisions in the controller log. After ownership changes,
recovery must finish the move forward. Reusing a transfer identity is rejected.

Capacity refusal when fencing or beginning installation triggers this abort
path. The application reserves space for phase completion and abort receipts;
unrelated writes cannot consume the bytes required for an already admitted
post-ownership activation. A quorum outage can still prevent progress: restart
the same nodes with their intact directories, then let the coordinator reconcile
durable phase state. Do not remove data directories to bypass a failed startup.

`--manual-handoff` disables automatic handoff and membership-ticket advancement
for fault experiments.
`POST /v1/reconcile` with `{"cluster":"FINGERPRINT","body":{"request_id":"move-0-to-2"}}`
performs one checked transition. Explicit recovery uses `/v1/dispatch` group 0
with `{"action":"advance","request_id":"move-0-to-2","step":"abort"}`;
the abort is rejected after ownership. Reconcile subsequently drives endpoint
recovery.

## Per-group replica changes

Keep the original `genesis_voters` unchanged when adding or removing hosts. Add
node 4 to the topology's sorted `nodes` list with HTTP `127.0.0.1:8004` and
Raft addresses `127.0.0.1:9040`, `127.0.0.1:9041`, `127.0.0.1:9042` for groups
0, 1 and 2 respectively. Start it with `--id 4 --data-dir data-4 --seed 4` and
the same immutable bootstrap fields. Its actors cannot campaign or vote merely
because their listeners are reachable.

Request a learner for data group 1 through controller group 0:

```json
{"cluster":"FINGERPRINT","body":{"group":0,"action":{"action":"begin_maintenance","request_id":"group1-add4","target":1,"operation":{"kind":"add_learner","id":4,"endpoints":{"raft":"127.0.0.1:9041","http":"127.0.0.1:8004"}}}}}
```

The automatic coordinator applies the exact ticket to the target group and
releases it only after a checked, committed membership record is applied.
Read its progress through `/v1/checked`:

```json
{"cluster":"FINGERPRINT","body":{"group":0,"query":{"query":"maintenance","request_id":"group1-add4"}}}
```

Wait for a non-null `ticket.completion`. An accepted ticket or
`membership_applied` response alone does not establish controller release.
Inspect the target through `{"group":1,"query":{"query":"membership","request_id":null}}`.
Once the learner has caught up, a new ticket may use
`{"kind":"set_voters","voters":[2,3,4]}` to replace the three-voter set through
joint consensus. That group needs separate majorities of old and new voters
during its joint phase. The other data groups and controller retain their own
memberships. `{"kind":"remove","id":4}` removes a learner directly or a voter
through joint consensus; removing the last voter is refused. Retired node
identities cannot be reused.

During manual experiments, dispatch controller group 0 action
`{"action":"reconcile_maintenance","request_id":"group1-add4"}` repeatedly.
One step may return an unknown result after target application. Retry the same
request ID and exact operation. Unknown, absent or merely pending target results
never release the ticket, including after a controller restart. There is no
timeout cancellation: restore the target's quorum and reconcile its retained
outcome. A stalled ticket deliberately blocks new handoffs and new membership
tickets. An incomplete aborted handoff likewise blocks ticket acquisition until
both endpoint recovery receipts are committed.

The controller checks a generation from the idle view when admitting a new
ticket, preventing stale endpoint prevalidation from crossing another completed
membership change. Target actors independently check their membership revision
before accepting a fresh learner operation. Exact retained retries preserve the
original outcome without requiring a fresh DNS lookup.

## Limits and durability

The topology supports up to 15 seed hosts, 16 data groups and 64 shards.
Seed addresses are numeric TCP endpoints with explicit ports. Membership
advertisements can use validated hostnames. Immutable identity
includes cluster name, group IDs, bootstrap ownership and each group's explicit
genesis voters. Route-address changes do not change that identity or grant votes.
Historical genesis identities remain unchanged after their routes are retired.
TCP scope and the core group envelope reject another group before adopting its
term or message. HTTP routing uses accepted candidate addresses and one shared
request deadline. Admission/publication checks reject cross-group Raft aliases
and cross-node HTTP aliases; one node's HTTP service is shared across its groups.
Transport reconnect validation is per group. A later DNS rebinding can attempt
another group's socket before the registry refreshes, but exact scope rejects
that connection's messages before they gain authority.

Each actor is bounded to 16 MiB of serialized application state; a shard image
is bounded to 4 MiB with 32 KiB pull chunks. Keys are at most 1 KiB and values
64 KiB. There are bounded retained session, retry and transfer histories; closed
sessions and abort identities are retained rather than evicted. At those limits,
new operations can receive `capacity` and require an operator-managed new
deployment or a separately designed retention protocol.
The controller retains at most 1,024 maintenance tickets, and each core retains
at most 1,024 administrative outcomes. These histories are not silently evicted.

Snapshots use the existing durable generation publisher, with a versioned,
chunked application carrier whose group, exact key set, length and digest are
validated before restore. Disk identity and an exclusive directory lock prevent
mixing nodes or bootstraps. Missing initialized payloads fail closed. A partially
created initial directory requires inspection and a new empty directory; it is
not treated as fresh automatically.
Snapshots carry the exact committed membership, while effective membership
controls replication routes. Recovery executes durable configuration replay
before ordinary inputs. The service uses its own versioned application carrier;
there is no automatic migration from earlier fixed-membership experimental
directories. Restore fails closed rather than falling back to older state.
Generic storage recovery can reclaim unselected files before actor-specific
payload validation, so semantic rejection does not guarantee old generations
remain available.

Shutdown stops admission and new accepts, drains accepted proposals for up to
five seconds of quorum wait, then performs the actual storage flush. Synchronous
filesystem durability barriers have no hard wall-clock deadline. This is a
trusted-network crash-fault service; it does not provide authentication, TLS,
Byzantine proofs or a physical power-loss guarantee.

The [placement comparison](benchmarks/shard-placement-2026-10-01.md) measures
Ketama versus modulo placement separately. Those moved-key and imbalance results
do not measure handoff throughput or prove balanced placement for every workload.

## Reproducible process experiments

Python 3.11 or later and the pinned Rust toolchain are required. Build the
experiment binary from a fresh source copy outside the repository:

```sh
python3 -B scripts/seal_shard_build.py --source-root "$PWD" --out /tmp/shard-build --jobs 1
python3 -B scripts/membership_handoff_experiment.py \
  --binary /tmp/shard-build/bin/shard-service --client deploy/docker/client/client.py \
  --handoff-helper scripts/handoff_experiment.py \
  --source-binding /tmp/shard-build/source-binding.json \
  --out /tmp/membership-handoff --seed 20261002 --case-seconds 600
python3 -B scripts/handoff_experiment.py \
  --binary /tmp/shard-build/bin/shard-service --client deploy/docker/client/client.py \
  --source-binding /tmp/shard-build/source-binding.json \
  --out /tmp/handoff-phases --seed 20261002 --case-seconds 240
```

Use new output paths for each invocation. The seal helper accepts an explicit
versioned build-input manifest, rejects unsafe or missing inputs, builds with
`--locked`, checks source bytes before and after, and pins the copied binary.
Its `--target-dir` can reuse a Cargo cache; do not build concurrently into that
same target directory. The process observers preserve unknowns, offered and
unattempted counts, phase proofs, per-process logs and retained data directories.
They terminate and reap only their own children. `--cases normal
--automatic-normal` on the handoff observer separately verifies background
advancement without manual reconciliation.

Run focused script controls with `python3 -B -m unittest discover -s scripts
-p 'test_*handoff*.py' -v` and `python3 -B -m unittest discover -s scripts
-p test_seal_shard_build.py -v`. See the
[verification report](benchmarks/shard-handoff-2026-10-01.md) for the tested
source and dependency scope, failures, unknown control outcomes and limits.
