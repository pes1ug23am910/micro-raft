//! The real driver: a single-task event loop around
//! the pure core — deliberately single-threaded for simplicity over
//! throughput. It feeds the core `Input`s and executes the returned `Effect`s
//! in the exact order emitted.
//!
//! M3: the core is live — real elections run over real sockets. M5:
//! `Persist*` effects execute against `storage` with fsync completing before
//! any later `Send`. M7: the propose channel and `Apply`/client-response
//! effects join the loop.

use std::time::Duration;

use raft_core::{Effect, Input, NodeId, RaftMessage, RaftNode, TICK_MS};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::transport::Transport;

/// Runs forever. The loop never awaits a network send (§4/M2 failure mode):
/// `Transport::send` only pushes onto a channel.
pub async fn run_driver(
    mut node: RaftNode,
    transport: impl Transport,
    mut inbound_rx: mpsc::UnboundedReceiver<(NodeId, RaftMessage)>,
) {
    let started = tokio::time::Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut inputs: Vec<Input> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                inputs.push(Input::Tick { now_ms: started.elapsed().as_millis() as u64 });
            }
            Some((from, msg)) = inbound_rx.recv() => {
                debug!(from, ?msg, "message received");
                inputs.push(Input::Message { from, msg });
            }
            // M7: propose_rx (client commands) joins this select.
        }
        for input in inputs.drain(..) {
            let effects = node.step(input);
            execute_in_order(&transport, effects);
        }
    }
}

/// §2.4 contract: effects run strictly in emitted order. (The fsync-before-
/// dependent-Send half of the contract becomes real in M5 when `Persist*`
/// executes against storage.)
fn execute_in_order(transport: &impl Transport, effects: Vec<Effect>) {
    for effect in effects {
        match effect {
            Effect::Send { to, msg } => {
                debug!(to, ?msg, "sending");
                transport.send(to, msg);
            }
            Effect::RoleChanged { role_name, term } => {
                info!(role = role_name, term, "role changed");
            }
            // M5: PersistHardState / PersistLogEntries → storage.rs (fsync
            //     completes before any later Send in the same batch).
            // M7: Apply / ProposeAccepted / ProposeRejected → KV state machine
            //     and pending client oneshots.
            other => warn!(?other, "effect not executable until M5/M7 wiring"),
        }
    }
}
