//! # raft-core — a pure, deterministic Raft state machine (sans-I/O)
//!
//! No networking, no disk access, no clocks, no threads, no async, and no
//! dependencies beyond `serde`. Everything the node
//! ever learns arrives as an [`Input`]; everything it ever wants done comes
//! back as an [`Effect`]. Two interchangeable shells execute those effects:
//! the real driver (`kv-node`: tokio, TCP, disk) and the simulator (`sim`:
//! virtual clock, virtual network, virtual disk).
//!
//! Timing is core-owned (decision D-000): election/heartbeat deadlines are
//! computed from the `now_ms` carried by [`Input::Tick`]. There is no timer
//! effect, so driver and core can never disagree about time — and the sim
//! controls time trivially.

pub mod election;
pub mod message;
pub mod replication;
pub mod rng;
pub mod types;

pub use message::{Effect, Input, RaftMessage};
pub use types::{Command, Entry, HardState, LogIndex, NodeId, Role, Term};

use rng::Pcg32;

/// Drivers deliver `Input::Tick` every this-many logical milliseconds.
pub const TICK_MS: u64 = 10;
/// Leader heartbeat cadence. Must stay ≪ `ELECTION_MIN_MS` (Gate 1 probe 5).
pub const HEARTBEAT_MS: u64 = 50;
/// Election timeout lower bound.
pub const ELECTION_MIN_MS: u64 = 150;
/// Election timeout upper bound: deadline = now + rng.range_inclusive(150, 300).
pub const ELECTION_MAX_MS: u64 = 300;

/// One Raft node as a pure state machine; drivers execute its returned effects.
#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    /// The OTHER nodes (len 2 in a 3-node cluster).
    pub peers: Vec<NodeId>,
    pub hard: HardState,
    /// In-core copy; durability happens via `Persist*` effects.
    pub log: Vec<Entry>,
    /// Volatile (R1: rebuilt as 0 on every boot).
    pub commit_index: LogIndex,
    /// Volatile (R1: rebuilt as 0 on every boot).
    pub last_applied: LogIndex,
    pub role: Role,
    // -- timing, all in logical milliseconds fed via Tick (core-owned; D-000) --
    // M3: these four fields are written at construction but only read once
    // election timing lands; the `allow`s disappear with the M3 diff.
    #[allow(dead_code)]
    now_ms: u64,
    /// When to start/restart an election.
    #[allow(dead_code)]
    election_deadline_ms: u64,
    /// Leader only: when to send the next heartbeats.
    #[allow(dead_code)]
    heartbeat_due_ms: u64,
    /// Seeded at construction; the ONLY randomness in the core.
    #[allow(dead_code)]
    rng: Pcg32,
}

impl RaftNode {
    /// A fresh, never-booted node: empty log, term 0, `Follower` (R1).
    /// M5: recovered `HardState` + log from storage become parameters here.
    pub fn new(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Self {
        RaftNode {
            id,
            peers,
            hard: HardState::default(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            now_ms: 0,
            // M3: real deadlines are drawn from `rng` on the first Tick.
            election_deadline_ms: 0,
            heartbeat_due_ms: 0,
            rng: Pcg32::new(seed),
        }
    }

    /// Feed one input; get back an ordered list of effects the driver must
    /// execute — in the exact order emitted (§2.4 contract).
    ///
    /// M2 stub: returns no effects so the real driver loop can run (§4/M2
    /// deliverable 4). M3: elections & timing (R2–R3, R5–R11). M4:
    /// replication & commit (R12–R20).
    pub fn step(&mut self, _input: Input) -> Vec<Effect> {
        Vec::new()
    }
}
