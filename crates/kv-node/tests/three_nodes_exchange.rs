//! Three in-process nodes on real localhost sockets exchange
//! frames; killing one node's tasks and restarting them on the same port
//! resumes flow — the per-peer dial/backoff loop reconnects on its own.

use std::collections::BTreeSet;
use std::net::{SocketAddr, TcpListener as PortReservation};
use std::time::Duration;

use kv_node::transport::{spawn_listener, TcpTransport, Transport, INBOUND_QUEUE_CAPACITY};
use raft_core::{NodeId, RaftMessage};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

type Nodes = [(NodeId, u16); 3];

struct TestNode {
    inbound: mpsc::Receiver<(NodeId, RaftMessage)>,
    listener: tokio::task::JoinHandle<()>,
    pinger: tokio::task::JoinHandle<()>,
}

/// Listener, per-peer transports, and a 10 Hz test-message task for one node.
async fn start_node(id: NodeId, nodes: &Nodes) -> TestNode {
    let port = nodes.iter().find(|(n, _)| *n == id).unwrap().1;
    let peers: Vec<(NodeId, SocketAddr)> = nodes
        .iter()
        .filter(|(n, _)| *n != id)
        .map(|&(n, port)| (n, SocketAddr::from(([127, 0, 0, 1], port))))
        .collect();
    let (tx, inbound) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let listener = spawn_listener(port, tx).await.expect("bind test port");
    let transport = TcpTransport::spawn(id, &peers);
    let peer_ids: Vec<NodeId> = peers.iter().map(|&(n, _)| n).collect();
    let pinger = tokio::spawn(async move {
        loop {
            for &p in &peer_ids {
                transport.send(
                    p,
                    RaftMessage::RequestVoteReply {
                        term: 0,
                        vote_granted: false,
                    },
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
    });
    TestNode {
        inbound,
        listener,
        pinger,
    }
}

fn reserve_node_ports() -> (Nodes, [PortReservation; 3]) {
    let reservations = std::array::from_fn(|_| {
        PortReservation::bind(("127.0.0.1", 0)).expect("reserve ephemeral test port")
    });
    let nodes = std::array::from_fn(|index| {
        let port = reservations[index]
            .local_addr()
            .expect("read reserved test address")
            .port();
        ((index + 1) as NodeId, port)
    });
    (nodes, reservations)
}

async fn start_reserved_node(id: NodeId, nodes: &Nodes, reservation: PortReservation) -> TestNode {
    let port = nodes.iter().find(|(n, _)| *n == id).unwrap().1;
    assert_eq!(reservation.local_addr().unwrap().port(), port);
    drop(reservation);
    start_node(id, nodes).await
}

impl TestNode {
    /// Abort this node's tasks; dropping the struct closes its channels, so
    /// its transport tasks and inbound connections wind down too.
    fn kill(self) {
        self.listener.abort();
        self.pinger.abort();
    }

    fn drain(&mut self) {
        while self.inbound.try_recv().is_ok() {}
    }

    /// Wait up to five seconds for traffic from every node in `expect`.
    async fn expect_traffic_from(&mut self, me: NodeId, expect: &[NodeId]) {
        let want: BTreeSet<NodeId> = expect.iter().copied().collect();
        let mut seen: BTreeSet<NodeId> = BTreeSet::new();
        let ok = timeout(Duration::from_secs(5), async {
            while !seen.is_superset(&want) {
                match self.inbound.recv().await {
                    Some((from, _)) => {
                        seen.insert(from);
                    }
                    None => break,
                }
            }
        })
        .await
        .is_ok();
        assert!(
            ok && seen.is_superset(&want),
            "node {me}: expected traffic from {want:?} within 5s, saw only {seen:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_exchange() {
    let (nodes, reservations) = reserve_node_ports();
    let [port1, port2, port3] = reservations;
    let mut n1 = start_reserved_node(1, &nodes, port1).await;
    let mut n2 = start_reserved_node(2, &nodes, port2).await;
    let mut n3 = start_reserved_node(3, &nodes, port3).await;

    // Phase 1: within 5 s every node has received messages from both peers.
    n1.expect_traffic_from(1, &[2, 3]).await;
    n2.expect_traffic_from(2, &[1, 3]).await;
    n3.expect_traffic_from(3, &[1, 2]).await;

    // Phase 2: kill node 3 entirely (tasks aborted, channels dropped).
    n3.kill();
    // Let in-flight loopback traffic settle, then discard anything node 3
    // sent before it died so phase 3 only counts fresh arrivals.
    sleep(Duration::from_millis(500)).await;
    n1.drain();
    n2.drain();

    // Phase 3: restart node 3 on the same port; the survivors' dial/backoff
    // loops must reconnect and traffic must flow in both directions again.
    let mut n3 = start_node(3, &nodes).await;
    n3.expect_traffic_from(3, &[1, 2]).await;
    n1.expect_traffic_from(1, &[3]).await;
    n2.expect_traffic_from(2, &[3]).await;

    n1.kill();
    n2.kill();
    n3.kill();
}
