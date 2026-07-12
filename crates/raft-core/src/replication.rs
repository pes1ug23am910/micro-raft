//! Log replication & commitment — lands in M4.
//!
//! M4: AppendEntries construction per peer (R17), the follower consistency
//! check (R12), conflict truncation + append (R13), follower commit update
//! (R14), leader bookkeeping and backoff (R18), the pure `commit_advance`
//! function (R19 — kept pure for the Gate 2 hand-diff), client proposals (R20),
//! and the leader NoOp completing R16.
