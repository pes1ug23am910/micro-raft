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
use raft_core::{Effect, Entry, HardState, Input, NodeId, RaftMessage, RaftNode, Role, Term, TICK_MS};

use invariants::Invariants;

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
    /// M5: crash/restart. In M3 every node stays alive.
    alive: bool,
    /// Virtual disk — updated ONLY when a `Persist*` effect executes.
    disk_hard: HardState,
    disk_log: Vec<Entry>,
    /// For TermMonotonicity.
    last_term_seen: Term,
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
                }
                Effect::Send { to, msg } => self.enqueue(origin, to, msg),
                Effect::RoleChanged { role_name, term } => {
                    if role_name == "Leader" {
                        self.invariants.on_leader_elected(self.seed, term, origin);
                    }
                }
                // M4: Apply → per-node applied histories (StateMachineSafety);
                // ProposeAccepted/Rejected → client bookkeeping.
                Effect::Apply(_) | Effect::ProposeAccepted { .. } | Effect::ProposeRejected { .. } => {}
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
