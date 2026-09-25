//! Deterministic fault-injection simulator for the micro-raft core.
//!
//! The second, fake driver: it feeds the same [`raft_core::Input`]s from a
//! virtual clock and an in-memory message bus, executes [`raft_core::Effect`]s
//! against in-memory models of disk and network, and asserts safety
//! invariants after every single step. Nothing here sleeps, ever; given the
//! same seed, every run is byte-identical.
//!
//! It models virtual time, network delay/loss/partitions, durable storage,
//! crash/restart, applied histories, and the Raft safety invariants.

pub mod invariants;

use std::collections::{BTreeMap, BTreeSet};

use raft_core::rng::Pcg32;
use raft_core::{
    Command, Effect, Entry, HardState, Input, LogIndex, NodeId, RaftMessage, RaftNode, Role, Term,
    TICK_MS,
};

use invariants::{CommittedRegistry, Invariants};

/// Multiplier for deriving per-node seeds from the master seed.
/// The spec's `seed ^ node_id * 0x9E3779B97F4A7C15` requires wrapping
/// semantics — the product overflows u64 for every node_id ≥ 2.
const NODE_SEED_MULT: u64 = 0x9E37_79B9_7F4A_7C15;
/// A separate odd mixer keeps each reboot deterministic without replaying the
/// exact same timeout sequence after every crash.
const BOOT_SEED_MULT: u64 = 0xD1B5_4A32_D192_ED03;

fn node_seed(master: u64, id: NodeId, boot: u64) -> u64 {
    master ^ u64::from(id).wrapping_mul(NODE_SEED_MULT) ^ boot.wrapping_mul(BOOT_SEED_MULT)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PersistenceMode {
    #[default]
    Durable,
    /// Test-only negative control: emitted persistence effects are discarded.
    Negligent,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FaultStats {
    pub messages_attempted: u64,
    pub messages_delivered: u64,
    pub messages_dropped_randomly: u64,
    pub messages_dropped_by_partition: u64,
    pub messages_dropped_to_crashed_nodes: u64,
    pub partitions_imposed: u64,
    pub partitions_healed: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub effect_batches_audited: u64,
    pub external_effects_checked: u64,
}

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
    alive: bool,
    /// Virtual disk — updated ONLY when a `Persist*` effect executes.
    disk_hard: HardState,
    disk_log: Vec<Entry>,
    /// For TermMonotonicity.
    last_term_seen: Term,
    /// Applied-command history: every `Effect::Apply` this boot has
    /// executed, in order — the raw material for StateMachineSafety.
    applied: Vec<(LogIndex, Command)>,
    /// High-water mark of what this node has recorded into the
    /// committed registry while Leader.
    registered_commit: LogIndex,
    boot_count: u64,
    observed_core_log: Vec<Entry>,
    observed_disk_log: Vec<Entry>,
    observed_applied_len: usize,
    observed_leader_term: Option<Term>,
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
    /// Virtual network: ordered by (deliver_at_ms, seq) — deterministic
    /// delivery order, with no hash iteration anywhere.
    net: BTreeMap<(u64, u64), Envelope>,
    next_seq: u64,
    /// The sim's own RNG (drops, delays) — separate from the cores'.
    rng: Pcg32,
    pub faults: FaultConfig,
    invariants: Invariants,
    /// Every entry ever committed by a leader in its own term — the
    /// ground truth LeaderCompleteness/CommittedDurability checks against.
    committed: CommittedRegistry,
    persistence_mode: PersistenceMode,
    stats: FaultStats,
}

/// Syntactic half of the persist-before-observe contract.
///
/// A batch may legitimately acknowledge an idempotent vote or heartbeat
/// without writing anything. This audit rejects persistence that occurs too
/// late; the simulator separately compares the complete durable and core
/// state at every externally visible effect.
pub fn audit_persist_before_send(batch: &[Effect]) -> Result<(), String> {
    let mut saw_send = false;
    for effect in batch {
        match effect {
            Effect::PersistHardState(_) => {
                if saw_send {
                    return Err("PersistHardState appears after a Send in one effect batch".into());
                }
            }
            Effect::PersistLogEntries { .. } => {
                if saw_send {
                    return Err("PersistLogEntries appears after a Send in one effect batch".into());
                }
            }
            Effect::Send { .. } => {
                saw_send = true;
            }
            _ => {}
        }
    }
    Ok(())
}

impl Sim {
    pub fn new(n_nodes: u8, seed: u64) -> Sim {
        let mut nodes = BTreeMap::new();
        for id in 1..=n_nodes {
            let peers: Vec<NodeId> = (1..=n_nodes).filter(|&p| p != id).collect();
            let node_seed = node_seed(seed, id, 0);
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
                    boot_count: 0,
                    observed_core_log: Vec::new(),
                    observed_disk_log: Vec::new(),
                    observed_applied_len: 0,
                    observed_leader_term: None,
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
            persistence_mode: PersistenceMode::Durable,
            stats: FaultStats::default(),
        }
    }

    /// One TICK_MS step: deliver every due message (each delivery is
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
            let Some(target) = self.nodes.get(&env.to) else {
                continue;
            };
            if !target.alive {
                self.stats.messages_dropped_to_crashed_nodes += 1;
                continue;
            }
            // A partition imposed after enqueue must still block an in-flight
            // message when its delivery time arrives.
            if self.partitioned(env.from, env.to) {
                self.stats.messages_dropped_by_partition += 1;
                continue;
            }
            self.stats.messages_delivered += 1;
            let effects = self
                .nodes
                .get_mut(&env.to)
                .expect("target exists")
                .core
                .step(Input::Message {
                    from: env.from,
                    msg: env.msg,
                });
            self.execute_effects(env.to, effects);
            self.check_all_invariants();
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
            let effects = node.core.step(Input::Tick {
                now_ms: self.now_ms,
            });
            self.execute_effects(id, effects);
            self.check_all_invariants();
        }
        self.now_ms += TICK_MS;
    }

    fn execute_effects(&mut self, origin: NodeId, effects: Vec<Effect>) {
        if let Err(error) = audit_persist_before_send(&effects) {
            panic!(
                "seed={}: n{origin} persistence ordering violation: {error}",
                self.seed
            );
        }
        self.stats.effect_batches_audited += 1;
        for effect in effects {
            match effect {
                Effect::PersistHardState(hs) => {
                    if self.persistence_mode == PersistenceMode::Durable {
                        self.nodes
                            .get_mut(&origin)
                            .expect("origin exists")
                            .disk_hard = hs;
                    }
                }
                Effect::PersistLogEntries {
                    truncate_from,
                    entries,
                } => {
                    if self.persistence_mode == PersistenceMode::Durable {
                        let disk =
                            &mut self.nodes.get_mut(&origin).expect("origin exists").disk_log;
                        if let Some(from) = truncate_from {
                            disk.retain(|e| e.index < from);
                        }
                        disk.extend(entries);
                    }
                }
                Effect::Send { to, msg } => {
                    self.assert_fully_durable(origin, "Send");
                    self.enqueue(origin, to, msg);
                }
                Effect::RoleChanged {
                    role_name: "Follower",
                    ..
                } => {
                    // A step-down can precede unrelated log persistence in the
                    // same batch, but its adopted term must already be durable.
                    self.assert_term_durable(origin, "RoleChanged(Follower)");
                }
                Effect::RoleChanged { role_name, .. } => {
                    self.assert_fully_durable(origin, role_name);
                }
                Effect::Apply(entry) => {
                    self.assert_fully_durable(origin, "Apply");
                    self.on_apply(origin, entry);
                }
                // Client acks/rejections have no world to act on here; tests
                // inspect them via the effect copies `propose` returns.
                Effect::ProposeAccepted { .. } => {
                    self.assert_fully_durable(origin, "ProposeAccepted");
                }
                Effect::ProposeRejected { .. } => {
                    self.assert_fully_durable(origin, "ProposeRejected");
                }
            }
        }
        self.assert_fully_durable(origin, "effect-batch completion");
    }

    fn assert_term_durable(&mut self, origin: NodeId, context: &str) {
        if self.persistence_mode == PersistenceMode::Negligent {
            return;
        }
        self.stats.external_effects_checked += 1;
        let node = self.nodes.get(&origin).expect("origin exists");
        assert_eq!(
            node.disk_hard.current_term, node.core.hard.current_term,
            "seed={}: n{origin} exposed {context} before current_term was durable",
            self.seed
        );
    }

    fn assert_fully_durable(&mut self, origin: NodeId, context: &str) {
        if self.persistence_mode == PersistenceMode::Negligent {
            return;
        }
        self.stats.external_effects_checked += 1;
        let node = self.nodes.get(&origin).expect("origin exists");
        assert_eq!(
            node.disk_hard, node.core.hard,
            "seed={}: n{origin} exposed {context} before hard state was durable",
            self.seed
        );
        assert_eq!(
            node.disk_log, node.core.log,
            "seed={}: n{origin} exposed {context} before its log was durable",
            self.seed
        );
    }

    /// Execute one `Apply` into the virtual world: record it in the node's
    /// history, asserting R4's strict ordering. Cross-node agreement is
    /// checked unconditionally after the complete core step.
    fn on_apply(&mut self, origin: NodeId, entry: Entry) {
        let node = self.nodes.get_mut(&origin).expect("origin exists");
        let expected = node.applied.last().map_or(1, |&(i, _)| i + 1);
        assert_eq!(
            entry.index, expected,
            "seed={}: R4 violated — n{origin} applied index {} but expected {expected}",
            self.seed, entry.index
        );
        node.applied.push((entry.index, entry.command));
    }

    fn enqueue(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) {
        self.stats.messages_attempted += 1;
        if self.nodes.get(&to).is_some_and(|node| !node.alive) {
            self.stats.messages_dropped_to_crashed_nodes += 1;
            return;
        }
        assert!(
            (0.0..=1.0).contains(&self.faults.drop_prob),
            "seed={}: drop_prob must be in [0, 1]",
            self.seed
        );
        if self.faults.drop_prob > 0.0 {
            // roll ∈ [0, 1): drop_prob = 1.0 drops everything.
            let roll = f64::from(self.rng.next_u32()) / (f64::from(u32::MAX) + 1.0);
            if roll < self.faults.drop_prob {
                self.stats.messages_dropped_randomly += 1;
                return;
            }
        }
        if self.partitioned(from, to) {
            self.stats.messages_dropped_by_partition += 1;
            return;
        }
        let delay_min = self.faults.delay_min_ms.max(1);
        let delay_max = self.faults.delay_max_ms.max(1);
        assert!(
            delay_min <= delay_max,
            "seed={}: invalid delay band {delay_min}..={delay_max}",
            self.seed
        );
        let delay = self.rng.range_inclusive(delay_min, delay_max);
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

    fn check_all_invariants(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let mut newly_committed = Vec::new();
        let mut new_leaders = Vec::new();
        let mut core_log_changed = false;
        let mut disk_log_changed = false;
        let mut applied_changed = false;

        for &id in &ids {
            let node = self.nodes.get_mut(&id).expect("known id");
            if node.core.log != node.observed_core_log {
                if node.alive {
                    invariants::check_log_structure(self.seed, id, "core", &node.core.log);
                }
                node.observed_core_log.clone_from(&node.core.log);
                core_log_changed = true;
            }
            if node.disk_log != node.observed_disk_log {
                if self.persistence_mode == PersistenceMode::Durable {
                    invariants::check_log_structure(self.seed, id, "durable", &node.disk_log);
                }
                node.observed_disk_log.clone_from(&node.disk_log);
                disk_log_changed = true;
            }
            if node.applied.len() != node.observed_applied_len {
                node.observed_applied_len = node.applied.len();
                applied_changed = true;
            }

            let leader_term = node
                .alive
                .then(|| node.core.is_leader().then_some(node.core.hard.current_term))
                .flatten();
            if leader_term != node.observed_leader_term {
                node.observed_leader_term = leader_term;
                if let Some(term) = leader_term {
                    new_leaders.push((id, term, node.core.log.clone()));
                }
            }

            if !node.alive {
                continue;
            }
            Invariants::check_term_monotonic(
                self.seed,
                id,
                node.last_term_seen,
                node.core.hard.current_term,
            );
            node.last_term_seen = node.core.hard.current_term;

            let last_index = node.core.log.last().map_or(0, |entry| entry.index);
            assert!(
                node.core.last_applied <= node.core.commit_index
                    && node.core.commit_index <= last_index,
                "seed={}: n{id} progress bounds violated: applied={} commit={} log_last={last_index}",
                self.seed,
                node.core.last_applied,
                node.core.commit_index
            );
            assert_eq!(
                node.applied.len() as LogIndex,
                node.core.last_applied,
                "seed={}: n{id} emitted Apply history disagrees with last_applied",
                self.seed
            );
            if node.applied.len() == node.observed_applied_len && applied_changed {
                for (offset, (index, command)) in node.applied.iter().enumerate() {
                    let expected = offset as LogIndex + 1;
                    assert_eq!(
                        *index, expected,
                        "seed={}: n{id} applied history skipped or duplicated an index",
                        self.seed
                    );
                    assert_eq!(
                        node.core.log[offset].command, *command,
                        "seed={}: n{id} applied command differs from its log at index {expected}",
                        self.seed
                    );
                }
            }

            if node.core.is_leader() && node.core.commit_index > node.registered_commit {
                for index in (node.registered_commit + 1)..=node.core.commit_index {
                    newly_committed.push(
                        node.core.log[usize::try_from(index - 1).expect("log index fits usize")]
                            .clone(),
                    );
                }
                node.registered_commit = node.core.commit_index;
            }
        }

        let commitment_changed = !newly_committed.is_empty();
        for entry in newly_committed {
            self.committed.record(self.seed, &entry);
        }

        if self.persistence_mode == PersistenceMode::Durable {
            for (&id, node) in &self.nodes {
                if node.alive {
                    assert_eq!(
                        node.disk_hard, node.core.hard,
                        "seed={}: n{id} hard state diverged from its virtual disk after a step",
                        self.seed
                    );
                    if core_log_changed || disk_log_changed {
                        assert_eq!(
                            node.disk_log, node.core.log,
                            "seed={}: n{id} log diverged from its virtual disk after a step",
                            self.seed
                        );
                    }
                }
            }
        }

        if core_log_changed || disk_log_changed || applied_changed {
            for left in 0..ids.len() {
                for right in (left + 1)..ids.len() {
                    let a_id = ids[left];
                    let b_id = ids[right];
                    let a = self.nodes.get(&a_id).expect("known id");
                    let b = self.nodes.get(&b_id).expect("known id");
                    if core_log_changed && a.alive && b.alive {
                        invariants::check_log_matching(
                            self.seed,
                            a_id,
                            &a.core.log,
                            b_id,
                            &b.core.log,
                        );
                    }
                    if applied_changed {
                        invariants::check_applied_agreement(
                            self.seed, a_id, &a.applied, b_id, &b.applied,
                        );
                    }
                    if disk_log_changed && self.persistence_mode == PersistenceMode::Durable {
                        invariants::check_log_matching(
                            self.seed,
                            a_id,
                            &a.disk_log,
                            b_id,
                            &b.disk_log,
                        );
                    }
                }
            }
        }

        for (id, term, log) in new_leaders {
            self.invariants.on_leader_elected(self.seed, term, id);
            self.committed
                .assert_all_in_leader_log(self.seed, id, term, &log);
        }

        if self.persistence_mode == PersistenceMode::Durable
            && (disk_log_changed || commitment_changed)
        {
            let majority = self.nodes.len() / 2 + 1;
            for (&index, entry) in self.committed.iter() {
                let durable_copies = self
                    .nodes
                    .values()
                    .filter(|node| {
                        node.disk_log
                            .get(usize::try_from(index - 1).expect("log index fits usize"))
                            == Some(entry)
                    })
                    .count();
                assert!(
                    durable_copies >= majority,
                    "seed={}: CommittedDurability violated — entry {entry:?} has only \
                     {durable_copies}/{} durable copies (majority {majority})",
                    self.seed,
                    self.nodes.len()
                );
            }
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

    pub fn is_alive(&self, id: NodeId) -> bool {
        self.nodes.get(&id).expect("known node id").alive
    }

    pub fn durable_hard_state(&self, id: NodeId) -> &HardState {
        &self.nodes.get(&id).expect("known node id").disk_hard
    }

    pub fn durable_log(&self, id: NodeId) -> &[Entry] {
        &self.nodes.get(&id).expect("known node id").disk_log
    }

    pub fn persistence_mode(&self) -> PersistenceMode {
        self.persistence_mode
    }

    pub fn set_persistence_mode(&mut self, mode: PersistenceMode) {
        self.persistence_mode = mode;
    }

    pub fn stats(&self) -> &FaultStats {
        &self.stats
    }

    pub fn reset_stats(&mut self) {
        self.stats = FaultStats::default();
    }

    pub fn set_partitions(&mut self, partitions: Vec<BTreeSet<NodeId>>) {
        let mut seen = BTreeSet::new();
        for cell in &partitions {
            for &id in cell {
                assert!(
                    self.nodes.contains_key(&id),
                    "seed={}: partition names unknown n{id}",
                    self.seed
                );
                assert!(
                    seen.insert(id),
                    "seed={}: n{id} appears in more than one partition cell",
                    self.seed
                );
            }
        }
        if !partitions.is_empty() {
            self.stats.partitions_imposed += 1;
        }
        self.faults.partitions = partitions;
    }

    pub fn heal_partitions(&mut self) {
        if !self.faults.partitions.is_empty() {
            self.stats.partitions_healed += 1;
            self.faults.partitions.clear();
        }
    }

    /// Crash a node: volatile consensus/application state disappears, while
    /// its virtual disk and historical invariant evidence remain intact.
    pub fn crash(&mut self, id: NodeId) -> bool {
        let node = self.nodes.get_mut(&id).expect("known node id");
        if !node.alive {
            return false;
        }
        let peers = node.core.peers.clone();
        node.core = RaftNode::new(id, peers, node_seed(self.seed, id, node.boot_count));
        node.alive = false;
        node.applied.clear();
        self.stats.crashes += 1;

        let before = self.net.len();
        self.net.retain(|_, envelope| envelope.to != id);
        self.stats.messages_dropped_to_crashed_nodes += (before - self.net.len()) as u64;
        self.check_all_invariants();
        true
    }

    /// Restart from only the state represented by the virtual disk. A logical
    /// tick at the current virtual time initializes a fresh election deadline
    /// before any queued message can arrive.
    pub fn restart(&mut self, id: NodeId) -> bool {
        let (peers, hard, log, boot_count) = {
            let node = self.nodes.get_mut(&id).expect("known node id");
            if node.alive {
                return false;
            }
            node.boot_count += 1;
            (
                node.core.peers.clone(),
                node.disk_hard.clone(),
                node.disk_log.clone(),
                node.boot_count,
            )
        };
        let mut core =
            RaftNode::restore(id, peers, node_seed(self.seed, id, boot_count), hard, log)
                .expect("virtual disk always contains valid recovered state");
        let effects = core.step(Input::Tick {
            now_ms: self.now_ms,
        });
        let node = self.nodes.get_mut(&id).expect("known node id");
        node.core = core;
        node.alive = true;
        node.applied.clear();
        self.stats.restarts += 1;
        self.execute_effects(id, effects);
        self.check_all_invariants();
        true
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

    /// A client PUT delivered to `node` as a `ClientPropose` input.
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

    /// A client DELETE delivered to `node` as a `ClientPropose` input.
    pub fn client_delete(&mut self, node: NodeId, key: &str) -> Vec<Effect> {
        self.propose(
            node,
            Command::Delete {
                key: key.to_string(),
            },
        )
    }

    fn propose(&mut self, id: NodeId, command: Command) -> Vec<Effect> {
        let node = self.nodes.get_mut(&id).expect("known node id");
        if !node.alive {
            return Vec::new(); // Proposals to a crashed node go nowhere.
        }
        let effects = node.core.step(Input::ClientPropose { command });
        let copy = effects.clone();
        self.execute_effects(id, effects);
        self.check_all_invariants();
        copy
    }

    /// This node's applied-command history, in application order.
    pub fn applied(&self, id: NodeId) -> &[(LogIndex, Command)] {
        &self.nodes.get(&id).expect("known node id").applied
    }

    /// The committed-entry registry — every entry a leader ever
    /// committed in its own term.
    pub fn registry(&self) -> &CommittedRegistry {
        &self.committed
    }

    /// Test seam: deliver a message immediately, bypassing the virtual
    /// network; returns a copy of the emitted effects for inspection.
    pub fn deliver_now(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) -> Vec<Effect> {
        self.stats.messages_attempted += 1;
        let node = self.nodes.get_mut(&to).expect("known node id");
        if !node.alive {
            self.stats.messages_dropped_to_crashed_nodes += 1;
            return Vec::new();
        }
        self.stats.messages_delivered += 1;
        let effects = node.core.step(Input::Message { from, msg });
        let copy = effects.clone();
        self.execute_effects(to, effects);
        self.check_all_invariants();
        copy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vote_request(term: Term) -> RaftMessage {
        RaftMessage::RequestVote {
            term,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    #[test]
    fn partition_blocks_a_message_already_in_flight() {
        let mut sim = Sim::new(3, 17);
        sim.faults.delay_min_ms = 100;
        sim.faults.delay_max_ms = 100;
        sim.enqueue(1, 2, vote_request(9));
        sim.set_partitions(vec![BTreeSet::from([1])]);

        sim.run_ms(110);

        assert_eq!(sim.node(2).hard.current_term, 0);
        assert!(sim.stats().messages_dropped_by_partition >= 1);
    }

    #[test]
    fn crashed_node_rejects_immediate_and_queued_delivery() {
        let mut sim = Sim::new(3, 23);
        sim.faults.delay_min_ms = 100;
        sim.faults.delay_max_ms = 100;
        sim.enqueue(1, 2, vote_request(8));
        assert!(sim.crash(2));

        assert!(sim.deliver_now(1, 2, vote_request(9)).is_empty());
        sim.run_ms(110);

        assert_eq!(sim.durable_hard_state(2).current_term, 0);
        assert!(sim.stats().messages_dropped_to_crashed_nodes >= 2);
    }
}
