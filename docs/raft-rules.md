# Raft rule index

The `R1`-`R20` labels in this repository are local traceability identifiers.
They are not rule numbers from the Raft paper. Their design basis is Diego
Ongaro and John Ousterhout's [*In Search of an Understandable Consensus
Algorithm (Extended Version)*][raft-paper], especially Figure 2 and Sections
5.2-5.4.2. Section 8 supplies the client-completion guidance.

The paper describes state transitions and stable-storage requirements. This
implementation makes their ordering observable: `raft-core` emits an ordered
list of effects, and the node driver completes each persistence effect before
executing a later send or client-visible effect.

## State and common message handling

| Rule | Repository contract | Paper basis | Primary implementation |
|---|---|---|---|
| **R1** | Persist terms, votes, the retained log, and explicit-group membership authority. A selected snapshot supplies its applied/configuration boundary. Recovery validates these together and reconstructs a follower; a validated engine watermark may restore a later applied prefix. | Figure 2, "State"; §§5.2-5.3 | [`RaftNode::restore`](../crates/raft-core/src/lib.rs), [`Storage`](../crates/kv-node/src/storage.rs) |
| **R2** | Validate group identity and message structure before term handling. An admissible higher actual term clears the vote, steps down and persists hard state. A PreVote prospective term is a probe, not permission to advance the receiver term. | Figure 2, "Rules for Servers" | [`RaftNode::on_message`](../crates/raft-core/src/lib.rs) |
| **R3** | Reject stale requests with the current term. Ignore stale replies and replies that no longer match the receiver's role. | Figure 2, AppendEntries and RequestVote receiver rules; candidate and leader rules | [`RaftNode::on_message`](../crates/raft-core/src/lib.rs), [`on_request_vote_reply`](../crates/raft-core/src/election.rs), [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R4** | Apply committed entries strictly in increasing index order. Recovery restores validated snapshot/engine application state and replays the remaining known committed prefix. The driver publishes a result only after actual application and any required engine transaction. | Figure 2, "Rules for Servers"; §5.3 | [`RaftNode::apply_committed`](../crates/raft-core/src/lib.rs) |

## Election timing and voting

| Rule | Repository contract | Paper basis | Primary implementation |
|---|---|---|---|
| **R5** | An eligible voter starts a PreVote round at its election deadline. Only a successful prospective quorum starts the term-changing election. Learners and retired replicas cannot campaign; a leader instead checks fresh quorum contact. | Figure 2, follower and candidate rules; §5.2 | [`RaftNode::on_tick`](../crates/raft-core/src/lib.rs) |
| **R6** | Reset the election deadline on initial boot, campaigning, quorum-loss stepdown, granting a real vote, or valid leader contact (including an accepted snapshot chunk). Rejected votes and incoming PreVote requests do not reset it. | Figure 2, follower and candidate rules; §5.2 | [`reset_election_deadline`](../crates/raft-core/src/lib.rs) |
| **R6a** | Granting a vote resets the randomized election deadline. | Figure 2, RequestVote receiver rule; §5.2 | [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R6b** | A non-stale AppendEntries identifies the current leader and resets the election deadline, even when the log-consistency check rejects that RPC. A rejected RequestVote does not reset it. | Figure 2, AppendEntries receiver rule; §5.2 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R6c** | Starting a prospective campaign or a term-changing election draws a new randomized deadline. | Figure 2, candidate rules; §5.2 | [`start_election`](../crates/raft-core/src/election.rs), [`reset_election_deadline`](../crates/raft-core/src/lib.rs) |
| **R7** | Becoming a follower discards candidate-only or leader-only volatile state. | Figure 2, server states and term transition; §5.2 | [`RaftNode::become_follower`](../crates/raft-core/src/lib.rs) |
| **R8** | Starting an election increments the term, votes for self, creates a deduplicated self-vote set, persists hard state, and then requests votes with the candidate's last-log index and term. | Figure 2, candidate rules; §5.2 | [`start_election`](../crates/raft-core/src/election.rs) |
| **R9** | Grant RequestVote only to an eligible candidate under validated group/configuration authority, with both the per-term vote condition (**R9a**) and log up-to-dateness condition (**R9b**). A learner may respond to a validated candidate history that promotes it without campaigning or installing that advertised history. | Figure 2, RequestVote receiver rule; §§5.2 and 5.4.1 | [`decide_vote`](../crates/raft-core/src/election.rs), [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R9a** | Grant at most one candidate per term, while permitting an idempotent repeat from the same candidate. Persist a newly granted vote before replying. | Figure 2, RequestVote receiver rule; §5.2 | [`decide_vote`](../crates/raft-core/src/election.rs), [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R9b** | Grant only when the candidate's log is at least as up to date: compare last term first, then last index. | Figure 2, RequestVote receiver rule; §5.4.1 | [`decide_vote`](../crates/raft-core/src/election.rs) |
| **R10** | A current-term candidate becomes leader only with the effective configuration quorum: one majority while stable, both old and new majorities while joint. Ineligible senders and duplicate replies do not count. | Figure 2, candidate rules; §5.2 | [`on_request_vote_reply`](../crates/raft-core/src/election.rs) |
| **R11** | A candidate that receives non-stale AppendEntries steps down and processes the RPC as a follower. | Figure 2, candidate rules; §5.2 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |

## Log replication and commitment

| Rule | Repository contract | Paper basis | Primary implementation |
|---|---|---|---|
| **R12** | Reject AppendEntries when its suffix indices are not contiguous, its term sequence is invalid, or the entry at `prev_log_index` is absent or has a different term. Return the local last-index hint, or a validated compacted-prefix boundary hint when the anchor precedes the retained log, without treating a structurally valid sender as stale. | Figure 2, AppendEntries receiver rule; §5.3. Structural payload validation is a local defensive check. | [`validate_append_entries`](../crates/raft-core/src/replication.rs), [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R13** | After a successful prefix check, keep every matching entry. At the first same-index/different-term conflict, truncate that suffix and append missing incoming entries. Persist the mutation before the success reply. | Figure 2, AppendEntries receiver rule; §5.3 | [`on_append_entries`](../crates/raft-core/src/replication.rs), [`Storage::append_entries`](../crates/kv-node/src/storage.rs) |
| **R14** | A follower advances `commit_index` no farther than both the leader's commit index and the last index proven by this AppendEntries, then applies in order. | Figure 2, AppendEntries receiver rule; §5.3 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R15** | A leader only appends to its own log; conflict truncation is a follower operation. | Figure 3, Leader Append-Only Property; §5.3 | [`on_client_propose`](../crates/raft-core/src/replication.rs), [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R16** | On election, initialize per-peer replication indexes, append and persist a current-term no-op, announce the role change, and immediately send replication RPCs. The no-op gives the new leader a current-term entry through which earlier entries can become committed safely. | Figure 2, leader initialization; §§5.2 and 5.4.2 | [`become_leader`](../crates/raft-core/src/election.rs) |
| **R17** | On each due heartbeat, replicate from each participant's `next_index`, including the preceding index and term. AppendEntries carries at most 16 entries. A follower behind the retained prefix receives bounded snapshot chunks instead; ordinary proposals ride the next 50 ms heartbeat. | Figure 2, leader rules; §5.3. The batch bound and cadence are local implementation choices. | [`send_append_entries`](../crates/raft-core/src/replication.rs) |
| **R18** | A valid current-term success monotonically raises that participant's replication progress, then attempts commitment. Failure backs off using the follower's last-index hint. A compacted-prefix hint is checked against a retained boundary term before advancing progress. | Figure 2, leader response rules; §5.3 | [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R19** | Choose the greatest index satisfying both the majority condition (**R19a**) and current-term condition (**R19b**), advance `commit_index`, and apply in order. | Figure 2, leader commit rule; §5.4.2 and Figure 8 | [`quorum_commit_candidate`](../crates/raft-core/src/membership_protocol.rs), [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R19a** | Choose an index durably replicated on the effective configuration quorum. Joint requires both old and new majorities; final uses the new majority only after joint commitment permitted its creation. An excluded leader's local log does not count toward that quorum. | Figure 2, leader commit rule; §§5.3-5.4 | [`quorum_commit_candidate`](../crates/raft-core/src/membership_protocol.rs) |
| **R19b** | The counted entry must be from the leader's current term. Prior-term entries become committed indirectly beneath a committed current-term entry. | Figure 8; §5.4.2 | [`quorum_commit_candidate`](../crates/raft-core/src/membership_protocol.rs), [`figure8_prior_term_not_committed_by_count`](../crates/sim/tests/replication.rs) |
| **R20** | An eligible leader appends a client command in its current term, persists it, and reports its log index to the driver; a non-leader rejects with its best leader hint. The HTTP layer returns success only after the corresponding actual application and, when selected, its durable applied-state transaction. | Figure 2, leader client rule; §§5.3 and 8 | [`on_client_propose`](../crates/raft-core/src/replication.rs), [`execute_in_order`](../crates/kv-node/src/driver.rs) |

## Extension contracts

The original R-labels remain stable for source traceability. The implemented
extensions add the following obligations to that baseline:

| Mechanism | Additional contract | Entry points |
|---|---|---|
| PreVote and CheckQuorum | Prospective probes preserve terms and votes; a leader loses authority without fresh effective-quorum contact. Neither creates a read lease. | [election](../crates/raft-core/src/election.rs), [core timers](../crates/raft-core/src/lib.rs) |
| ReadIndex | Require a committed current-term entry, matching fresh context/term replies and a quorum; the shell then waits for the actual applied watermark. | [read protocol](../crates/raft-core/src/read.rs), [driver](../crates/kv-node/src/driver.rs) |
| Sessions | Exact latest retries preserve their original result; changed payloads, stale/gap sequences and exhausted retention are explicit outcomes. Closed identities do not reopen. | [application state](../crates/kv-node/src/application.rs), [client contract](client-semantics.md) |
| Snapshots | Select the validated snapshot/WAL generation durably before reclaiming older files. Final install acknowledgement follows application installation. | [core transfer](../crates/raft-core/src/snapshot.rs), [storage](../crates/kv-node/src/storage.rs), [snapshot contract](snapshots.md) |
| Membership | Latest durable logged configuration controls authority; only committed and applied retained records complete administration. Immutable group/genesis binds transport and consensus messages. | [quorum arithmetic](../crates/raft-core/src/membership.rs), [membership protocol](../crates/raft-core/src/membership_protocol.rs), [administration](membership.md) |
| Proposal batching | One WAL barrier may cover a bounded ordered batch; per-request results, cancellation and shutdown preserve the durable acknowledgement boundary. | [driver](../crates/kv-node/src/driver.rs), [batching contract](proposal-batching.md) |

Membership and snapshots extend the paper basis to sections 6 and 7; checked
reads and duplicate suppression use section 8. PreVote, CheckQuorum and the
repository's bounded queues/retention are implementation choices beyond
Figure 2. The current implementation has no time-based read leases,
authenticated peer transport, Byzantine tolerance or optional conflict-term
fast backtracking. See the [architecture guide](architecture.md) and
[README](../README.md) for the complete service boundaries.

[raft-paper]: https://raft.github.io/raft.pdf
