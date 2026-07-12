//! TCP transport: length-prefixed JSON frames,
//! fire-and-forget peer links, reconnect-on-failure.
//!
//! What this layer is ALLOWED to be bad at is the point: Raft assumes an
//! asynchronous, unreliable network — messages may be lost, delayed,
//! reordered, or duplicated — and stays correct anyway. So `send` never
//! blocks, never retries a message, and never correlates request/response:
//! a dropped connection just means silence until the next heartbeat (R17).

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use raft_core::{NodeId, RaftMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Frames larger than this are a protocol error: the connection is dropped.
pub const MAX_FRAME_BYTES: u32 = 8 * 1024 * 1024;

/// Wire format: `u32` big-endian payload length, then `serde_json` bytes of
/// `(from: NodeId, msg: RaftMessage)`. TCP is a byte stream, not a message
/// stream — the prefix answers "where does one message end?".
#[derive(Debug)]
pub enum FrameError {
    Oversize { len: u32 },
    Json(serde_json::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Oversize { len } => {
                write!(f, "frame length {len} exceeds max {MAX_FRAME_BYTES}")
            }
            FrameError::Json(e) => write!(f, "frame json error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame(from: NodeId, msg: &RaftMessage) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(&(from, msg)).map_err(FrameError::Json)?;
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::Oversize { len: u32::MAX })?;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::Oversize { len });
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Incremental frame decoder — pure, so `frame_partial_read` can test it
/// against arbitrary chunking without sockets.
#[derive(Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn push_bytes(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// `Ok(Some)` = one complete frame; `Ok(None)` = need more bytes;
    /// `Err` = protocol error — the caller must drop the connection.
    pub fn next_frame(&mut self) -> Result<Option<(NodeId, RaftMessage)>, FrameError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        if len > MAX_FRAME_BYTES {
            return Err(FrameError::Oversize { len });
        }
        let total = 4 + len as usize;
        if self.buf.len() < total {
            return Ok(None);
        }
        let payload: Vec<u8> = self.buf.drain(..total).skip(4).collect();
        let parsed = serde_json::from_slice(&payload).map_err(FrameError::Json)?;
        Ok(Some(parsed))
    }
}

/// Fire-and-forget by design (§4/M2): no acknowledgments, no resend queues,
/// no exactly-once machinery — correctness reasoning lives in the state
/// machine, so the network layer stays cheap.
pub trait Transport {
    fn send(&self, to: NodeId, msg: RaftMessage);
}

/// Per-peer outbound tasks own one connection each: dial on demand, retry
/// with 200 ms backoff, silently drop messages while disconnected.
#[derive(Clone)]
pub struct TcpTransport {
    self_id: NodeId,
    outbound: BTreeMap<NodeId, mpsc::UnboundedSender<RaftMessage>>,
}

impl TcpTransport {
    /// Must be called inside a tokio runtime: spawns one task per peer.
    pub fn spawn(self_id: NodeId, peers: &[(NodeId, SocketAddr)]) -> TcpTransport {
        let mut outbound = BTreeMap::new();
        for &(peer, addr) in peers {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(peer_task(self_id, peer, addr, rx));
            outbound.insert(peer, tx);
        }
        TcpTransport { self_id, outbound }
    }
}

impl Transport for TcpTransport {
    fn send(&self, to: NodeId, msg: RaftMessage) {
        if let Some(tx) = self.outbound.get(&to) {
            // A closed channel means the peer task is gone (shutdown); loss
            // is tolerated by design.
            let _ = tx.send(msg);
        } else {
            warn!(self_id = self.self_id, to, "send to unknown peer dropped");
        }
    }
}

async fn peer_task(
    self_id: NodeId,
    peer: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::UnboundedReceiver<RaftMessage>,
) {
    loop {
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                debug!(peer, %addr, "peer connection established");
                loop {
                    match rx.recv().await {
                        // Channel closed: the transport was dropped — shut down.
                        None => return,
                        Some(msg) => {
                            let frame = match encode_frame(self_id, &msg) {
                                Ok(f) => f,
                                Err(e) => {
                                    warn!(peer, error = %e, "unencodable message dropped");
                                    continue;
                                }
                            };
                            if let Err(e) = stream.write_all(&frame).await {
                                // The message is lost; Raft tolerates it (R17
                                // retries via the heartbeat cadence).
                                debug!(peer, error = %e, "write failed; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!(peer, %addr, error = %e, "dial failed; retrying in 200ms");
                let backoff = tokio::time::sleep(Duration::from_millis(200));
                tokio::pin!(backoff);
                loop {
                    tokio::select! {
                        () = &mut backoff => break,
                        m = rx.recv() => match m {
                            // Disconnected: drop silently at debug (§4/M2).
                            Some(_) => debug!(peer, "message dropped while disconnected"),
                            None => return,
                        },
                    }
                }
            }
        }
    }
}

/// Binds `127.0.0.1:<port>` (§9: never 0.0.0.0) and feeds every decoded
/// inbound frame into `tx`. Returns the accept-loop task handle.
pub async fn spawn_listener(
    port: u16,
    tx: mpsc::UnboundedSender<(NodeId, RaftMessage)>,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, remote)) => {
                    tokio::spawn(conn_task(stream, remote, tx.clone()));
                }
                Err(e) => warn!(error = %e, "accept failed"),
            }
        }
    }))
}

async fn conn_task(
    mut stream: TcpStream,
    remote: SocketAddr,
    tx: mpsc::UnboundedSender<(NodeId, RaftMessage)>,
) {
    let mut dec = FrameDecoder::default();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        match stream.read(&mut chunk).await {
            Ok(0) => {
                debug!(%remote, "peer closed connection");
                return;
            }
            Ok(n) => {
                dec.push_bytes(&chunk[..n]);
                loop {
                    match dec.next_frame() {
                        Ok(Some(frame)) => {
                            if tx.send(frame).is_err() {
                                return; // driver gone — shut down
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            warn!(%remote, error = %e, "protocol error; dropping connection");
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                debug!(%remote, error = %e, "read failed; dropping connection");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use raft_core::{Command, Entry};

    use super::*;

    /// One message per RaftMessage variant, with entries covering every
    /// Command variant.
    fn all_variants() -> Vec<RaftMessage> {
        vec![
            RaftMessage::RequestVote {
                term: 3,
                candidate_id: 1,
                last_log_index: 7,
                last_log_term: 2,
            },
            RaftMessage::RequestVoteReply {
                term: 3,
                vote_granted: true,
            },
            RaftMessage::AppendEntries {
                term: 4,
                leader_id: 2,
                prev_log_index: 9,
                prev_log_term: 3,
                entries: vec![
                    Entry {
                        index: 10,
                        term: 4,
                        command: Command::Put {
                            key: "k".into(),
                            value: "v".into(),
                        },
                    },
                    Entry {
                        index: 11,
                        term: 4,
                        command: Command::Delete { key: "k".into() },
                    },
                    Entry {
                        index: 12,
                        term: 4,
                        command: Command::NoOp,
                    },
                ],
                leader_commit: 9,
            },
            RaftMessage::AppendEntriesReply {
                term: 4,
                success: false,
                match_index: 5,
            },
        ]
    }

    #[test]
    fn frame_roundtrip() {
        for (i, msg) in all_variants().into_iter().enumerate() {
            let from = u8::try_from(i + 1).unwrap();
            let bytes = encode_frame(from, &msg).unwrap();
            let mut dec = FrameDecoder::default();
            dec.push_bytes(&bytes);
            let (f, m) = dec.next_frame().unwrap().expect("one complete frame");
            assert_eq!((f, &m), (from, &msg));
            assert_eq!(
                encode_frame(f, &m).unwrap(),
                bytes,
                "re-encoding the decoded message reproduces the frame byte-exactly"
            );
            assert!(dec.next_frame().unwrap().is_none(), "no residue left over");
        }
    }

    #[test]
    fn frame_partial_read() {
        let msgs = all_variants();
        let mut wire = Vec::new();
        for m in &msgs {
            wire.extend_from_slice(&encode_frame(9, m).unwrap());
        }
        let mut dec = FrameDecoder::default();
        let mut got = Vec::new();
        for &b in &wire {
            dec.push_bytes(&[b]);
            while let Some((from, m)) = dec.next_frame().unwrap() {
                assert_eq!(from, 9);
                got.push(m);
            }
        }
        assert_eq!(got, msgs, "1-byte chunking decodes every frame correctly");
    }

    #[test]
    fn frame_oversize_rejected() {
        let mut dec = FrameDecoder::default();
        dec.push_bytes(&(MAX_FRAME_BYTES + 1).to_be_bytes());
        match dec.next_frame() {
            Err(FrameError::Oversize { len }) => assert_eq!(len, MAX_FRAME_BYTES + 1),
            other => panic!("expected Oversize protocol error, got {other:?}"),
        }
    }
}
