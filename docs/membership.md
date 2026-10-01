# Membership administration

A node's immutable group identity and genesis voters are separate from its current network routes. The Raft core uses the newest durable logged configuration for elections, replication, CheckQuorum and ReadIndex. A joint configuration requires a majority of both its old and new voters. Its final configuration is logged only after joint commitment and immediately uses the new majority. An excluded leader continues replication without counting itself until that final entry commits, then steps down.

An administrative success is stricter than log admission: the completed record must be durably committed and applied. Status exposes `effective_membership` and `committed_membership` separately. Learners and retired processes do not become voters merely because they are reachable.

## Bootstrap and routes

Start each new group with the same explicit identity and sorted, unique genesis list:

```sh
kv-node --id 1 --group-id orders-a --genesis-voters 1,2,3   --raft-port 7001 --http-port 8001   --peers 2@127.0.0.1:7002,3@127.0.0.1:7003 --data-dir data/n1
```

Explicit groups recover their authoritative identity before resolving startup routes. Local advertisements must resolve and pass policy before listeners open. Active peer routes share one five-second startup budget; unavailable or invalid peer DNS leaves visible degraded routing that is retried. Retired seed hostnames do not block reopening, and a stale offline voter can listen for a valid new-leader advertisement even if an old seed has disappeared. Partial routes grant no votes or read/write authority. Legacy fixed-group startup and `--check-config` keep strict complete resolution before storage.

The legacy port flags support loopback development and are independent of group identity. For remote deployment use all four listen/advertise endpoint flags described in the README. `--peers` supplies seed Raft routes, not a membership change. A fresh node 4 uses the same `--group-id` and `--genesis-voters 1,2,3`, its own ports/directory and routes to the existing group. It remains passive before replicated learner admission. Its initial snapshot may predate its addition; the retained suffix supplies the later membership change.

An explicit group id is case-sensitive printable ASCII, 1–128 bytes, without whitespace. `legacy` and `legacy-default` are reserved. Starting an existing explicit directory requires matching group/genesis flags. Legacy fixed groups still work without them, but cannot administer membership. The legacy default does not isolate independent clusters with the same node ids. All explicit-group messages are bound to group and genesis before term adoption; the transport and core derive the same identity from durable consensus state.

## HTTP operations and retries

Use `POST /admin/membership` with JSON and a caller-retained request id:

```json
{"request_id":"add-4","operation":{"kind":"add_learner","id":4,"endpoints":{"raft":"127.0.0.1:7004","http":"127.0.0.1:8004"}}}
```

After catch-up, promote through `{"request_id":"promote-4","operation":{"kind":"set_voters","voters":[1,2,3,4]}}`. The requested list must be sorted and contain existing voters/learners. Promotion requires the learner to reach a captured log fence and respond in the current contact round. A `learner_not_caught_up` rejection can be retried with the same id and payload. `{"request_id":"remove-2","operation":{"kind":"remove","id":2}}` removes a learner directly or a voter through joint consensus. Removing the last voter is forbidden.

A 200 response has `outcome:"completed"`, the original `request_id`, and a `record` with original operation, first index/term, joint flag, and final index/term. A 503 unknown outcome or a lost connection does not establish whether the change committed. Retry the identical id and payload on the current leader; completed retries return the original record. Reusing an id with a changed payload is rejected. Only one admitted change may be outstanding. A local pending joint record is not a completed result.

`GET /admin/membership/{request_id}` reports a locally known committed record, with `read_mode:"local"`, membership index and application watermark; it is not a linearizable absence check. `GET /status` exposes both membership states, node role, engine state and any routing error. Existing checked key reads continue using a current-term quorum and applied fence. No lease read is introduced.

Endpoint validation resolves new Raft and HTTP destinations, local advertisements and the current membership's declared destinations. It rejects wildcard, multicast, broadcast, self and known active-service aliases under the configured endpoint policy. Successful DNS validation is bound to a SHA-256 digest of the complete canonical effective membership; the writer rejects a stale validation before appending. An exact retained retry needs no fresh endpoint validation. Genesis HTTP addresses absent from the declared endpoint map are not invented from port conventions.

## Durability and migration

Membership-aware hard state uses a version2 outer JSON envelope with checksum. An older binary cannot decode that envelope as the old raw term/vote object. Snapshot V2 uses a distinct `MRFTSN02` header and includes the complete validated membership history; V1 fixed snapshots remain readable. Recovery checks exact history extensions, selected snapshot/WAL coverage and persisted terms. If CURRENT selected a newer valid snapshot just before a crash, the corresponding membership authority is durably refreshed without changing the preserved term/vote before engine recovery or service.

An existing fixed-group directory is adopted deliberately with matching genesis, explicit group and `--migrate-legacy-group`. The operation is allowed only before any administrative membership history. It validates the existing application backend/binding, persists core identity, atomically rewrites engine identity at the same verified application boundary, and publishes the matching application marker. A crash between these steps fails closed without the migration flag or resumes only the exact requested migration. The established application label `legacy` maps explicitly to the core's `legacy-default`; current routes never determine identity. Remove the migration flag after adoption. Backend migration is a separate unsupported operation.

A fresh explicit bootstrap records a bounded initialization intent before publishing core identity. It can resume only the same backend/group/genesis at index zero, before any ordinary log, snapshot or election. Startup restores the engine watermark, replays any remaining known committed configuration prefix, and completes required persistence before opening listeners.

## Bounds, routing and shutdown

Node ids remain u8; retired ids are permanent tombstones and cannot be reused. This limits a group's lifetime to 256 distinct ids. At most 1024 administrative records are retained; no silent eviction permits an old request id to be replayed as new. Administrative request ids are bounded to 128 bytes and endpoints to 512 bytes. The complete serialized membership history is capped at 3 MiB; frame-cap checks refuse a change before its configuration or advertisement can exceed the transport limit. The channel holds 32 requests and the writer holds at most 64 response waiters. A request has a seven-second HTTP budget including validation and queueing; DNS has the existing two-second lookup/five-second snapshot bounds. OS lookup cancellation remains cooperative at the caller, with at most four residual system workers.

Routes are updated outside the single writer. Each accepted configuration is validated before publication, removed/repointed links are cancelled, and unchanged connections survive. In-flight DNS results carry a generation and cannot restore an obsolete route. Unchanged peers' accepted addresses do not require unrelated DNS to remain available. Durable effective endpoints take precedence over advertisement hints; hints expire by term or later authoritative membership. A bounded, group-bound advertisement can provide a newly elected leader's reply route to a stale node, but does not install its configuration or certify commitment. This is a trusted crash-fault protocol, not a cryptographic membership certificate.

Retired routes may briefly remain for removal notification, but do not reserve sockets against new ids. Colliding retired routes are suppressed; retired ids themselves remain forbidden. Route errors are visible in status and retried. An unexpected routing task failure stops the node visibly. The administrative endpoint shares the service's existing network trust boundary; no authentication or public Internet hardening is added here.

Shutdown closes new administrative admission, preserves each started durable effect batch, and lets accepted waiters resolve until the common drain deadline. Unresolved changes return unknown and remain resolvable by durable request id. The five-second drain deadline bounds asynchronous quorum waiting, not a filesystem sync blocked in the OS. Engine and consensus flush failures remain fatal.

## Validation

Deterministic core/simulator tests cover both joint majorities, old/new partitions, interrupted promotion, final-configuration activation, learner vote admission, removed restarts, stale/group-mismatched advertisements, bounded frames and snapshot authority gaps. An executed faulty union-majority branch reaches a false commit that the independent simulator quorum witness rejects.

Runtime tests exercise actual application fences and idempotent administration, endpoint alias/routing-lag and validation-admission races, dynamic route replacement and stale DNS generations, plus real-directory migration and selected-snapshot crash gaps. Real isolated process scenarios run with both LSM and redb: passive joins, WAL-only learner catch-up, refusal of promotion while behind, multi-chunk snapshots made before learner admission, leader removal, a formerly unregistered new leader, offline removed-voter catch-up, all-directory reopen, exact administrative/session retry and checked reads preserving acknowledged data. A separate process scenario sends mismatched group and genesis frames at both transport and core binding layers and proves an accepted pending request in the WAL before observing unknown outcome and retrying it. A separate real TCP-gate matrix persists joint/final phases before interruption: old-only and new-only joint components cannot acquire authority after reopening, while a durably logged final configuration recovers using its new majority after all old voters crash. These local process tests are not physical-host or device power-loss evidence. Private run artifacts retain exact revisions, commands and failed development attempts.
