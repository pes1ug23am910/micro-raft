//! Leader election — lands in M3.
//!
//! M3: election start (R8), the pure `decide_vote` function (R9 — kept pure so
//! Gate 1 can diff a hand-written version against it), reply counting (R10),
//! candidate step-down (R11), deadline management (R5–R6), term overtake (R2),
//! stale rejection (R3). This module remains empty until its M3 implementation.
