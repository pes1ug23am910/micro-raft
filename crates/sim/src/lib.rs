//! Deterministic fault-injection simulator for the micro-raft core.
//!
//! This in-memory driver feeds the same [`raft_core::Input`]s from a
//! virtual clock and an in-memory message bus, executes [`raft_core::Effect`]s
//! against in-memory models of disk and network, and asserts safety
//! invariants after every single step. Nothing here sleeps, ever; given the
//! same seed, every run is byte-identical.
//!
//! M3: virtual clock/net, `FaultConfig` (drops + partitions), virtual
//! persistence, ElectionSafety + TermMonotonicity. M4: applied histories,
//! LogMatching, StateMachineSafety, CommittedRegistry. M5: crash/restart.

pub mod invariants;

use std::collections::{BTreeMap, BTreeSet};

use raft_core::rng::Pcg32;
use raft_core::{
    Command, Effect, Entry, HardState, Input, LogIndex, NodeId, RaftMessage, RaftNode, Role, Term,
    TICK_MS,
};

use invariants::{CommittedRegistry, Invariants};

/// Multiplier for deriving per-node seeds from the master seed (§4/M3).
/// The spec's `seed ^ node_id * 0x9E3779B97F4A7C15` requires wrapping
/// semantics — the product overflows u64 for every node_id ≥ 2.
const NODE_SEED_MULT: u64 = 0x9E37_79B9_7F4A_7C15;

/// Fault injection knobs. A message is dropped if the seeded RNG says so or
/// if sender and receiver sit in different partition cells; otherwise it is
/// delivered after a seeded delay drawn from the band.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub drop_prob: f64,
    /// Complete partition spec: nodes in different cells cannot talk. A node
    /// listed in no cell is implicitly in one shared "rest" cell.
    pub partitions: Vec<BTreeSet<NodeId>>,
    /// Per-message delay band in virtual ms (min is clamped to ≥ 1 so a
    /// message can never be delivered inside the step that sent it).
    pub delay_min_ms: u64,
    pub delay_max_ms: u64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        FaultConfig {
            drop_prob: 0.0,
            partitions: Vec::new(),
            delay_min_ms: 1,
            delay_max_ms: 20,
        }
    }
}

struct SimNode {
    core: RaftNode,
    /// M5: crash/restart. In M3–M4 every node stays alive.
    alive: bool,
    /// Virtual disk — updated ONLY when a `Persist*` effect executes.
    disk_hard: HardState,
    disk_log: Vec<Entry>,
    /// For TermMonotonicity.
    last_term_seen: Term,
    /// Applied-command history (M4): every `Effect::Apply` this node ever
    /// executed, in order — the raw material for StateMachineSafety.
    applied: Vec<(LogIndex, Command)>,
    /// High-water mark of what this node has recorded into the
    /// CommittedRegistry while Leader (M4).
    registered_commit: LogIndex,
}

struct Envelope {
    from: NodeId,
    to: NodeId,
    msg: RaftMessage,
}

pub struct Sim {
    pub seed: u64,
    now_ms: u64,
    nodes: BTreeMap<NodeId, SimNode>,
    /// Virtual network ordered by `(deliver_at_ms, seq)`; `seq` breaks equal-time
    /// delivery ties deterministically.
    net: BTreeMap<(u64, u64), Envelope>,
    next_seq: u64,
    /// The sim's own RNG (drops, delays) — separate from the cores'.
    rng: Pcg32,
    pub faults: FaultConfig,
    invariants: Invariants,
    /// Every entry ever committed by a leader in its own term (M4) — the
    /// ground truth LeaderCompleteness/CommittedDurability checks against.
    committed: CommittedRegistry,
}

impl Sim {
    pub fn new(n_nodes: u8, seed: u64) -> Sim {
        let mut nodes = BTreeMap::new();
        for id in 1..=n_nodes {
            let peers: Vec<NodeId> = (1..=n_nodes).filter(|&p| p != id).collect();
            let node_seed = seed ^ u64::from(id).wrapping_mul(NODE_SEED_MULT);
            nodes.insert(
                id,
                SimNode {
                    core: RaftNode::new(id, peers, node_seed),
                    alive: true,
                    disk_hard: HardState::default(),
                    disk_log: Vec::new(),
                    last_term_seen: 0,
                    applied: Vec::new(),
                    registered_commit: 0,
                },
            );
        }
        Sim {
            seed,
            now_ms: 0,
            nodes,
            net: BTreeMap::new(),
            next_seq: 0,
            rng: Pcg32::new(seed),
            faults: FaultConfig::default(),
            invariants: Invariants::default(),
            committed: CommittedRegistry::default(),
        }
    }

    /// One TICK_MS step (§4/M3): deliver every due message (each delivery is
    /// one `step()` on its target), then tick every alive node, executing
    /// effects into the virtual world and checking invariants throughout.
    pub fn step_once(&mut self) {
        self.step_once_ticking(None);
    }

    fn step_once_ticking(&mut self, only: Option<&[NodeId]>) {
        let due: Vec<(u64, u64)> = self
            .net
            .range(..=(self.now_ms, u64::MAX))
            .map(|(&k, _)| k)
            .collect();
        for key in due {
            let env = self.net.remove(&key).expect("due key just collected");
            let Some(node) = self.nodes.get_mut(&env.to) else {
                continue;
            };
            if !node.alive {
                continue; // M5: mail to a crashed node is dropped
            }
            let effects = node.core.step(Input::Message {
                from: env.from,
                msg: env.msg,
            });
            self.execute_effects(env.to, effects);
            self.check_node_invariants(env.to);
        }
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            if let Some(filter) = only {
                if !filter.contains(&id) {
                    continue;
                }
            }
            let node = self.nodes.get_mut(&id).expect("known id");
            if !node.alive {
                continue;
            }
            let effects = node.core.step(Input::Tick { now_ms: self.now_ms });
            self.execute_effects(id, effects);
            self.check_node_invariants(id);
        }
        self.now_ms += TICK_MS;
    }

    fn execute_effects(&mut self, origin: NodeId, effects: Vec<Effect>) {
        let mut log_changed = false;
        for effect in effects {
            match effect {
                Effect::PersistHardState(hs) => {
                    self.nodes.get_mut(&origin).expect("origin exists").disk_hard = hs;
                }
                Effect::PersistLogEntries {
                    truncate_from,
                    entries,
                } => {
                    let disk = &mut self.nodes.get_mut(&origin).expect("origin exists").disk_log;
                    if let Some(from) = truncate_from {
                        disk.retain(|e| e.index < from);
                    }
                    disk.extend(entries);
                    log_changed = true;
                }
                Effect::Send { to, msg } => self.enqueue(origin, to, msg),
                Effect::RoleChanged { role_name, term } => {
                    if role_name == "Leader" {
                        self.invariants.on_leader_elected(self.seed, term, origin);
                        // LeaderCompleteness/CommittedDurability v1: every
                        // entry ever committed must already be in the log of
                        // every subsequent leader — checked the moment one
                        // is elected.
                        let log = &self.nodes.get(&origin).expect("origin exists").core.log;
                        self.committed
                            .assert_all_in_leader_log(self.seed, origin, term, log);
                    }
                }
                Effect::Apply(entry) => self.on_apply(origin, entry),
                // Client acks/rejections have no world to act on here; tests
                // inspect them via the effect copies `propose` returns.
                Effect::ProposeAccepted { .. } | Effect::ProposeRejected { .. } => {}
            }
        }
        // LogMatching is asserted every step; a step that changed no log
        // cannot newly violate it, so the check runs exactly when the
        // origin's log changed (every core mutation emits PersistLogEntries).
        if log_changed {
            let origin_log = &self.nodes.get(&origin).expect("origin exists").core.log;
            for (&other, node) in &self.nodes {
                if other != origin {
                    invariants::check_log_matching(
                        self.seed,
                        origin,
                        origin_log,
                        other,
                        &node.core.log,
                    );
                }
            }
        }
    }

    /// Execute one `Apply` into the virtual world: record it in the node's
    /// history (asserting R4's strict ordering) and hold the histories of
    /// every pair of nodes to StateMachineSafety.
    fn on_apply(&mut self, origin: NodeId, entry: Entry) {
        let node = self.nodes.get_mut(&origin).expect("origin exists");
        let expected = node.applied.last().map_or(1, |&(i, _)| i + 1);
        assert_eq!(
            entry.index, expected,
            "seed={}: R4 violated — n{origin} applied index {} but expected {expected}",
            self.seed, entry.index
        );
        node.applied.push((entry.index, entry.command));
        let origin_applied = &self.nodes.get(&origin).expect("origin exists").applied;
        for (&other, node) in &self.nodes {
            if other != origin {
                invariants::check_applied_agreement(
                    self.seed,
                    origin,
                    origin_applied,
                    other,
                    &node.applied,
                );
            }
        }
    }

    fn enqueue(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) {
        if self.faults.drop_prob > 0.0 {
            // roll ∈ [0, 1): drop_prob = 1.0 drops everything.
            let roll = f64::from(self.rng.next_u32()) / (f64::from(u32::MAX) + 1.0);
            if roll < self.faults.drop_prob {
                return;
            }
        }
        if self.partitioned(from, to) {
            return;
        }
        let delay = self
            .rng
            .range_inclusive(self.faults.delay_min_ms.max(1), self.faults.delay_max_ms.max(1));
        let key = (self.now_ms + delay, self.next_seq);
        self.next_seq += 1;
        self.net.insert(key, Envelope { from, to, msg });
    }

    fn partitioned(&self, a: NodeId, b: NodeId) -> bool {
        if self.faults.partitions.is_empty() {
            return false;
        }
        let cell_of = |n: NodeId| self.faults.partitions.iter().position(|c| c.contains(&n));
        cell_of(a) != cell_of(b)
    }

    fn check_node_invariants(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).expect("known id");
        Invariants::check_term_monotonic(
            self.seed,
            id,
            node.last_term_seen,
            node.core.hard.current_term,
        );
        node.last_term_seen = node.core.hard.current_term;
        // CommittedRegistry: commitment is DEFINED where R19 runs — on a
        // leader advancing commit_index in its own term. Record the newly
        // covered entries the moment it happens (this runs right after
        // every step of every node).
        if node.core.is_leader() && node.core.commit_index > node.registered_commit {
            for i in (node.registered_commit + 1)..=node.core.commit_index {
                let entry = node
                    .core
                    .log
                    .get(usize::try_from(i - 1).expect("log index fits usize"))
                    .expect("commit_index never passes the log end (R19)");
                self.committed.record(self.seed, entry);
            }
            node.registered_commit = node.core.commit_index;
        }
    }

    // ---- accessors & test seams -------------------------------------------

    pub fn now(&self) -> u64 {
        self.now_ms
    }

    /// Advance the virtual clock by `ms`.
    pub fn run_ms(&mut self, ms: u64) {
        for _ in 0..ms.div_ceil(TICK_MS) {
            self.step_once();
        }
    }

    /// Advance until `pred` holds or `max_ms` elapses; returns whether it held.
    pub fn run_until(&mut self, pred: impl Fn(&Sim) -> bool, max_ms: u64) -> bool {
        let deadline = self.now_ms + max_ms;
        while self.now_ms < deadline {
            if pred(self) {
                return true;
            }
            self.step_once();
        }
        pred(self)
    }

    /// Freeze every clock except `ids` (frozen nodes still receive mail) —
    /// the deterministic way to force a specific node to campaign first.
    pub fn run_ms_ticking_only(&mut self, ids: &[NodeId], ms: u64) {
        for _ in 0..ms.div_ceil(TICK_MS) {
            self.step_once_ticking(Some(ids));
        }
    }

    /// The current leader: the alive Leader-role node at the highest term.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.alive && matches!(n.core.role, Role::Leader { .. }))
            .max_by_key(|(_, n)| n.core.hard.current_term)
            .map(|(&id, _)| id)
    }

    /// Every (term → leader) pair ever observed by ElectionSafety.
    pub fn leaders_by_term(&self) -> &BTreeMap<Term, NodeId> {
        self.invariants.leaders_by_term()
    }

    pub fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes.get(&id).expect("known node id").core
    }

    /// Test seam: hand-construct a node's log (core + virtual disk together,
    /// as if it had been legitimately persisted before the scenario starts).
    pub fn seed_log(&mut self, id: NodeId, entries: Vec<Entry>) {
        let node = self.nodes.get_mut(&id).expect("known node id");
        node.core.log = entries.clone();
        node.disk_log = entries;
    }

    /// Test seam: hand-construct a node's hard state (core + virtual disk).
    pub fn seed_hard_state(&mut self, id: NodeId, hs: HardState) {
        let node = self.nodes.get_mut(&id).expect("known node id");
        node.last_term_seen = hs.current_term;
        node.core.hard = hs.clone();
        node.disk_hard = hs;
    }

    /// A client PUT delivered to `node` as a `ClientPropose` input (§4/M4).
    /// Returns a copy of the emitted effects so tests can inspect the
    /// `ProposeAccepted` / `ProposeRejected` outcome.
    pub fn client_put(&mut self, node: NodeId, key: &str, value: &str) -> Vec<Effect> {
        self.propose(
            node,
            Command::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
        )
    }

    /// A client DELETE delivered to `node` as a `ClientPropose` input (§4/M4).
    pub fn client_delete(&mut self, node: NodeId, key: &str) -> Vec<Effect> {
        self.propose(node, Command::Delete { key: key.to_string() })
    }

    fn propose(&mut self, id: NodeId, command: Command) -> Vec<Effect> {
        let node = self.nodes.get_mut(&id).expect("known node id");
        if !node.alive {
            return Vec::new(); // M5: proposals to a crashed node go nowhere
        }
        let effects = node.core.step(Input::ClientPropose { command });
        let copy = effects.clone();
        self.execute_effects(id, effects);
        self.check_node_invariants(id);
        copy
    }

    /// This node's applied-command history, in application order (M4).
    pub fn applied(&self, id: NodeId) -> &[(LogIndex, Command)] {
        &self.nodes.get(&id).expect("known node id").applied
    }

    /// The committed-entry registry (M4) — every entry a leader ever
    /// committed in its own term.
    pub fn registry(&self) -> &CommittedRegistry {
        &self.committed
    }

    /// Test seam: deliver a message immediately, bypassing the virtual
    /// network; returns a copy of the emitted effects for inspection.
    pub fn deliver_now(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) -> Vec<Effect> {
        let node = self.nodes.get_mut(&to).expect("known node id");
        let effects = node.core.step(Input::Message { from, msg });
        let copy = effects.clone();
        self.execute_effects(to, effects);
        self.check_node_invariants(to);
        copy
    }
}
