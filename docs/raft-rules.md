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
| **R1** | Persistent Raft state is exactly `current_term`, `voted_for`, and the log. A restart reconstructs a follower with fresh volatile election, commit, and apply state. | Figure 2, "State"; §§5.2-5.3 | [`RaftNode::restore`](../crates/raft-core/src/lib.rs), [`Storage`](../crates/kv-node/src/storage.rs) |
| **R2** | Before interpreting a structurally valid message with a higher term, adopt that term, clear the vote, become a follower, and persist the new hard state. Malformed AppendEntries is rejected before it can change term or other state. | Figure 2, "Rules for Servers" | [`RaftNode::on_message`](../crates/raft-core/src/lib.rs) |
| **R3** | Reject stale requests with the current term. Ignore stale replies and replies that no longer match the receiver's role. | Figure 2, AppendEntries and RequestVote receiver rules; candidate and leader rules | [`RaftNode::on_message`](../crates/raft-core/src/lib.rs), [`on_request_vote_reply`](../crates/raft-core/src/election.rs), [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R4** | Apply committed entries strictly in increasing index order. Each index is applied once during a boot; after a restart, application is replayed as commitment is relearned. | Figure 2, "Rules for Servers"; §5.3 | [`RaftNode::apply_committed`](../crates/raft-core/src/lib.rs) |

## Election timing and voting

| Rule | Repository contract | Paper basis | Primary implementation |
|---|---|---|---|
| **R5** | A follower or candidate starts an election when its election deadline expires. A leader does not start an election from that timer. | Figure 2, follower and candidate rules; §5.2 | [`RaftNode::on_tick`](../crates/raft-core/src/lib.rs) |
| **R6** | Reset the randomized election deadline only in the three cases below. In particular, receiving a rejected RequestVote is not a reset condition. | Figure 2, follower and candidate rules; §5.2 | [`reset_election_deadline`](../crates/raft-core/src/lib.rs) |
| **R6a** | Granting a vote resets the randomized election deadline. | Figure 2, RequestVote receiver rule; §5.2 | [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R6b** | A non-stale AppendEntries identifies the current leader and resets the election deadline, even when the log-consistency check rejects that RPC. A rejected RequestVote does not reset it. | Figure 2, AppendEntries receiver rule; §5.2 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R6c** | Starting an election draws a new randomized deadline. | Figure 2, candidate rules; §5.2 | [`start_election`](../crates/raft-core/src/election.rs), [`reset_election_deadline`](../crates/raft-core/src/lib.rs) |
| **R7** | Becoming a follower discards candidate-only or leader-only volatile state. | Figure 2, server states and term transition; §5.2 | [`RaftNode::become_follower`](../crates/raft-core/src/lib.rs) |
| **R8** | Starting an election increments the term, votes for self, creates a deduplicated self-vote set, persists hard state, and then requests votes with the candidate's last-log index and term. | Figure 2, candidate rules; §5.2 | [`start_election`](../crates/raft-core/src/election.rs) |
| **R9** | Grant RequestVote only when both the per-term vote condition (**R9a**) and log up-to-dateness condition (**R9b**) hold. | Figure 2, RequestVote receiver rule; §§5.2 and 5.4.1 | [`decide_vote`](../crates/raft-core/src/election.rs), [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R9a** | Grant at most one candidate per term, while permitting an idempotent repeat from the same candidate. Persist a newly granted vote before replying. | Figure 2, RequestVote receiver rule; §5.2 | [`decide_vote`](../crates/raft-core/src/election.rs), [`on_request_vote`](../crates/raft-core/src/election.rs) |
| **R9b** | Grant only when the candidate's log is at least as up to date: compare last term first, then last index. | Figure 2, RequestVote receiver rule; §5.4.1 | [`decide_vote`](../crates/raft-core/src/election.rs) |
| **R10** | A current-term candidate becomes leader only after votes from a strict cluster majority. Unknown senders and duplicate vote replies do not count. | Figure 2, candidate rules; §5.2 | [`on_request_vote_reply`](../crates/raft-core/src/election.rs) |
| **R11** | A candidate that receives non-stale AppendEntries steps down and processes the RPC as a follower. | Figure 2, candidate rules; §5.2 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |

## Log replication and commitment

| Rule | Repository contract | Paper basis | Primary implementation |
|---|---|---|---|
| **R12** | Reject AppendEntries when its suffix indices are not contiguous, its term sequence is invalid, or the entry at `prev_log_index` is absent or has a different term. Return the local last index as a backoff hint without treating a structurally valid sender as stale. | Figure 2, AppendEntries receiver rule; §5.3. Structural payload validation is a local defensive check. | [`validate_append_entries`](../crates/raft-core/src/replication.rs), [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R13** | After a successful prefix check, keep every matching entry. At the first same-index/different-term conflict, truncate that suffix and append missing incoming entries. Persist the mutation before the success reply. | Figure 2, AppendEntries receiver rule; §5.3 | [`on_append_entries`](../crates/raft-core/src/replication.rs), [`Storage::append_entries`](../crates/kv-node/src/storage.rs) |
| **R14** | A follower advances `commit_index` no farther than both the leader's commit index and the last index proven by this AppendEntries, then applies in order. | Figure 2, AppendEntries receiver rule; §5.3 | [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R15** | A leader only appends to its own log; conflict truncation is a follower operation. | Figure 3, Leader Append-Only Property; §5.3 | [`on_client_propose`](../crates/raft-core/src/replication.rs), [`on_append_entries`](../crates/raft-core/src/replication.rs) |
| **R16** | On election, initialize per-peer replication indexes, append and persist a current-term no-op, announce the role change, and immediately send replication RPCs. The no-op gives the new leader a current-term entry through which earlier entries can become committed safely. | Figure 2, leader initialization; §§5.2 and 5.4.2 | [`become_leader`](../crates/raft-core/src/election.rs) |
| **R17** | On each due heartbeat, send every peer an AppendEntries beginning at that peer's `next_index`, including the preceding index and term. This implementation sends at most 16 entries per RPC and lets proposals ride the next 50 ms heartbeat. | Figure 2, leader rules; §5.3. The batch bound and cadence are local implementation choices. | [`send_append_entries`](../crates/raft-core/src/replication.rs) |
| **R18** | A successful current-term reply monotonically raises that peer's `match_index` and `next_index`, then attempts commitment. A failure backs `next_index` toward one, using the follower's last-index hint. | Figure 2, leader response rules; §5.3 | [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R19** | Choose the greatest index satisfying both the majority condition (**R19a**) and current-term condition (**R19b**), advance `commit_index`, and apply in order. | Figure 2, leader commit rule; §5.4.2 and Figure 8 | [`commit_advance`](../crates/raft-core/src/replication.rs), [`on_append_entries_reply`](../crates/raft-core/src/replication.rs) |
| **R19a** | A leader advances commitment only to an index durably replicated on a strict majority, counting its own log. Choose the greatest qualifying index. | Figure 2, leader commit rule; §§5.3-5.4 | [`commit_advance`](../crates/raft-core/src/replication.rs) |
| **R19b** | The counted entry must be from the leader's current term. Prior-term entries become committed indirectly beneath a committed current-term entry. | Figure 8; §5.4.2 | [`commit_advance`](../crates/raft-core/src/replication.rs), [`figure8_prior_term_not_committed_by_count`](../crates/sim/tests/replication.rs) |
| **R20** | A leader appends a client command in its current term, persists it, and reports its log index to the driver; a non-leader rejects with its best leader hint. The HTTP layer returns success only after the corresponding Apply effect. | Figure 2, leader client rule; §§5.3 and 8 | [`on_client_propose`](../crates/raft-core/src/replication.rs), [`execute_in_order`](../crates/kv-node/src/driver.rs) |

## Deliberate scope boundaries

The rule index describes the implemented, fixed-membership, non-Byzantine
protocol. It does not add PreVote, CheckQuorum, dynamic membership, snapshots,
ReadIndex, leases, authenticated transport, or the optional conflict-term fast
backtracking optimization. Those omissions are documented in the main
[README](../README.md).

[raft-paper]: https://raft.github.io/raft.pdf
