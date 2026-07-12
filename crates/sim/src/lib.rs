//! Deterministic fault-injection simulator for the micro-raft core.
//!
//! This crate deliberately depends on `raft-core` and nothing else. It is an
//! in-memory driver that feeds the same
//! [`raft_core::Input`]s from a virtual clock and an in-memory message bus,
//! executes [`raft_core::Effect`]s against in-memory models of disk and
//! network, and asserts Raft's safety invariants after every single step.
//!
//! M3: `Sim`, virtual clock/net, `FaultConfig`, invariants v1 (ElectionSafety,
//! TermMonotonicity). M4: LogMatching, StateMachineSafety, CommittedRegistry.
//! M5: crash/restart with a virtual disk honoring the `Persist*` boundary.
