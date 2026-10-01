# Shard handoff and membership verification — 2026-10-01

These local native Windows experiments ran real service processes, each with a
controller Raft actor and two independent data-group actors. They checked
committed state through ReadIndex, application fences and exact original
receipt identities. They are correctness observations for bounded schedules,
not throughput measurements or physical power-loss tests.

## Tested implementation and failure retained

The corrected integration binary was built with a locked dependency graph from
94 archived source inputs. Its source archive SHA-256 is
`8c6468ef3ec3cbc79e07e69a501fe43f6116c4eeafc0114e26051c5d5f3895e8`;
the Windows debug binary SHA-256 is
`08546df33fb50873e16c1f47a81151cbc020ee97f5b4e4421ffce006f023368e`.
Observers copied and verified the binary, client and helper before starting
children. A single observer process clock ordered each experiment's requests.

The first two-case integration invocation failed both cases. An AddLearner
operation had committed, but remote checked membership responses could not be
decoded once their numeric node-ID endpoint map became nonempty. The nested
tagged HTTP enums exposed a map-key deserialization mismatch. A full HTTP
`Wire<Response>` regression reproduced `string "4", expected u8`; a narrow
membership response decoder correction fixed it without changing persisted core
formats. The failing invocation and its unattempted later stages remain counted.

The public workspace preserves its existing dependency pins rather than
silently adopting the experiment workspace's newer compatible versions. For
example, the recorded integration build used Tokio 1.53.1, Serde 1.0.229 and
serde_json 1.0.151; the public promotion retained Tokio 1.52.3, Serde 1.0.228
and serde_json 1.0.150. The archive hashes above identify the tested graph.
The portable build helper and CI rerun the experiments against the actual public
lockfile; these recorded candidate results are not a claim that every subsequent
dependency combination passed.

## Actual process outcomes

| Invocation | Planned / attempted / passed cases | Client operations offered / planned | Receipt observations | Explicit logical rejections | Unknown control responses |
|---|---:|---:|---:|---:|---:|
| Initial composition | 2 / 2 / 0 | 7 / 20 | 7 | 4 | 1 |
| Corrected composition | 2 / 2 / 2 | 20 / 20 | 17 | 8 | 4 |
| Final nine-phase handoff regression | 9 / 9 / 9 | 144 / 144 | 117 | 27 | 0 |
| Final automatic handoff | 1 / 1 / 1 | 15 / 15 | 13 | 2 | 0 |

Receipt observations include exact retries; they are not counts of unique
mutations. The corrected composition's eight logical rejections comprise three
client retries after permanent session closure and five coordination `busy`
responses. Its four unknown responses were controller reconciliation requests
with HTTP 200 and typed outcome `unknown`. No client mutation had an unknown
outcome in that corrected invocation. An unknown response was never treated as
failure to commit or as permission to release a maintenance ticket.

The corrected replacement case completed all ten planned stages. It started
five processes with genesis voters 1, 2 and 3, verified passive hosts 4 and 5,
exercised abort/maintenance interlocks, killed and reopened the controller
leader with a retained ticket, and changed each group's voters through joint
consensus while excluding its prior leader. With every old host 1, 2 and 3
stopped, the new pair 4 and 5 completed a protected data write. It then completed
an actual shard move, reopened the surviving processes, and moved the shard
back. A 74,101-byte image required multiple chunks. Value, deletion, closed
session state and original retry receipts survived both group changes and
handoffs.

The separate outage case completed all five stages. It changed only controller
membership, acquired a data-group ticket, then removed the target group's
quorum while leaving the controller available. Unavailable/unknown target
observations did not release the ticket. The exact ticket survived a controller
restart and completed after target quorum recovery. This case does not establish
that the unavailable target had admitted the operation before recovery.

The phase matrix used fresh three-process clusters for normal completion and
eight interruption points: started, fenced, partial installation, installed,
ownership committed, activated, source cleaned and complete. Each interrupted
case killed a process selected through a checked leader observation, then
verified recovery. The observer also issued real reads and protected writes to
the frozen source and premature destination where applicable; an unexpected
acknowledgement failed the case. After restart it verified the deleted key was
still absent and its exact deletion receipt remained retrievable. Images were
73,768 bytes, transferred in three chunks of at most 32 KiB.

The final automatic case omitted `--manual-handoff` and never invoked manual
reconciliation. The background coordinator completed the move; a full cluster
reopen preserved the checked data and exact retry receipts.

Every owned child was reaped, with no cleanup failure in these invocations.
The corrected composition recorded 11 starts/reaps in the replacement case
and 10 in the outage case. Windows process termination models abrupt process
loss, not a machine power cut. Leader selection was checked before termination;
there is no claim of atomic leadership attribution at the precise kill instant.

## Controls and reproducibility

Deterministic controls cover exact maintenance-generation admission, transfer
versus membership exclusion, retained completion ordering, snapshot corruption,
capacity headroom after controller ownership, and future applied-watermark
growth. Isolated changes removing the generation check, transfer gate, phase
headroom and watermark reserve each reached their intended failure. The routing
control likewise failed with the old policy that could starve healthy peers
behind repeatedly unresolved hints.

Observer controls reject boolean IDs masquerading as integers, absent deletion
fields, malformed receipts, wrong retained operations, unapplied records and
unavailability masquerading as an interlock. Cleanup controls reproduce a child
exit race and require subsequent children to be visited and reaped. Build-seal
controls reject unsafe/missing inputs, escaped Cargo paths and both original
source and copied-context mutation; failed builds cannot emit a success binding.

See [sharding](../sharding.md) for runnable topology, client, handoff,
membership and experiment commands. Each invocation requires a fresh output
directory and preserves its plan, offered/unattempted counts, unknown responses,
source/binary hashes, request journal, phase proofs and process logs. The scripts
do not erase retained data directories after cleanup.

The evidence does not cover every interleaving, Byzantine participants,
authenticated transport, independent physical hosts, cross-shard transactions,
unbounded histories or general performance. A maintenance ticket can remain
blocked while its target quorum is unavailable; this is an explicit safety
choice, not an implicit timeout cancellation.
