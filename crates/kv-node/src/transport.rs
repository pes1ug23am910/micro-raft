//! TCP transport using length-prefixed JSON frames,
//! fire-and-forget peer links, reconnect-on-failure.
//!
//! What this layer is ALLOWED to be bad at is the point: Raft assumes an
//! asynchronous, unreliable network — messages may be lost, delayed,
//! reordered, or duplicated — and stays correct anyway. So `send` never
//! blocks, never retries a message, and never correlates request/response:
//! a dropped connection just means silence until the next heartbeat.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raft_core::{NodeId, RaftMessage};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle, JoinSet};
use tokio::time::{timeout, Instant};
use tracing::{debug, warn};

use crate::config::{Endpoint, PeerRefreshError, ResolvedConfig, Resolver, ValidatedConfig};
use crate::shutdown::ShutdownRx;

/// Frames larger than this are a protocol error: the connection is dropped.
pub const MAX_FRAME_BYTES: u32 = 8 * 1024 * 1024;

/// Maximum number of messages waiting for a single peer. With frames allowed
/// to approach [`MAX_FRAME_BYTES`], this deliberately small queue keeps the
/// per-peer memory bound practical. A saturated link is treated like packet
/// loss; a later heartbeat will retry replication.
pub const OUTBOUND_QUEUE_CAPACITY: usize = 4;

/// Maximum number of decoded frames waiting for the single-writer driver.
/// Awaiting a full queue propagates TCP backpressure to each peer connection.
pub const INBOUND_QUEUE_CAPACITY: usize = 8;

/// Bound accepted sockets, tasks, and retained decoder allocations. This service
/// targets small trusted clusters; surplus connections evict the oldest stream.
pub const MAX_INBOUND_CONNECTIONS: usize = 16;

const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
#[cfg(any(windows, target_os = "linux"))]
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Cluster/group isolation for explicit multi-group transports. This is a wire
/// boundary, not authentication. Callers derive it from durable group authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportScope {
    group_id: String,
    genesis_voters: Vec<NodeId>,
}
impl TransportScope {
    pub fn new(group_id: String, genesis_voters: Vec<NodeId>) -> io::Result<Self> {
        let scope = Self {
            group_id,
            genesis_voters,
        };
        scope.validate()?;
        Ok(scope)
    }
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
    pub fn genesis_voters(&self) -> &[NodeId] {
        &self.genesis_voters
    }
    fn validate(&self) -> io::Result<()> {
        if self.group_id.is_empty()
            || self.group_id.len() > 128
            || self.group_id == "legacy-default"
            || !self.group_id.bytes().all(|b| b.is_ascii_graphic())
            || self.genesis_voters.is_empty()
            || self
                .genesis_voters
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid explicit transport scope",
            ));
        }
        Ok(())
    }
}
#[derive(Serialize)]
struct ScopedFrameRef<'a> {
    scope: &'a TransportScope,
    from: NodeId,
    msg: &'a RaftMessage,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedFrame {
    scope: TransportScope,
    from: NodeId,
    msg: RaftMessage,
}

/// Wire format: `u32` big-endian payload length, then `serde_json` bytes of
/// `(from: NodeId, msg: RaftMessage)`. TCP is a byte stream, not a message
/// stream — the prefix answers "where does one message end?".
#[derive(Debug)]
pub enum FrameError {
    Oversize { len: u32 },
    Json(serde_json::Error),
    ScopeMismatch,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Oversize { len } => {
                write!(f, "frame length {len} exceeds max {MAX_FRAME_BYTES}")
            }
            FrameError::Json(e) => write!(f, "frame json error: {e}"),
            FrameError::ScopeMismatch => write!(f, "frame group scope mismatch"),
        }
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame(from: NodeId, msg: &RaftMessage) -> Result<Vec<u8>, FrameError> {
    encode_payload(serde_json::to_vec(&(from, msg)).map_err(FrameError::Json)?)
}

pub fn encode_scoped_frame(
    from: NodeId,
    msg: &RaftMessage,
    scope: &TransportScope,
) -> Result<Vec<u8>, FrameError> {
    scope.validate().map_err(|_| FrameError::ScopeMismatch)?;
    encode_payload(
        serde_json::to_vec(&ScopedFrameRef { scope, from, msg }).map_err(FrameError::Json)?,
    )
}
fn encode_payload(payload: Vec<u8>) -> Result<Vec<u8>, FrameError> {
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
    scope: Option<TransportScope>,
}

impl FrameDecoder {
    pub fn scoped(scope: TransportScope) -> io::Result<Self> {
        scope.validate()?;
        Ok(Self {
            buf: Vec::new(),
            scope: Some(scope),
        })
    }
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
        match &self.scope {
            Some(expected) => {
                let frame: ScopedFrame =
                    serde_json::from_slice(&payload).map_err(FrameError::Json)?;
                if frame.scope != *expected {
                    return Err(FrameError::ScopeMismatch);
                }
                Ok(Some((frame.from, frame.msg)))
            }
            None => Ok(Some(
                serde_json::from_slice(&payload).map_err(FrameError::Json)?,
            )),
        }
    }
}

/// Fire-and-forget by design: no acknowledgments, no resend queues,
/// no exactly-once machinery — correctness reasoning lives in the state
/// machine, so the network layer stays cheap.
pub trait Transport {
    fn send(&self, to: NodeId, msg: RaftMessage);
}

/// Per-peer links resolve and validate destinations again whenever they reconnect.
/// Dropping the final transport clone cancels every link, including pending I/O.
#[derive(Clone)]
pub struct TcpTransport {
    self_id: NodeId,
    outbound: Arc<Mutex<BTreeMap<NodeId, mpsc::Sender<RaftMessage>>>>,
    _tasks: Arc<PeerTasks>,
    routing: Option<ConfiguredRouting>,
}

#[derive(Default)]
struct PeerTasks(Vec<JoinHandle<()>>);

impl Drop for PeerTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

#[derive(Clone)]
enum PeerDestination {
    Numeric(SocketAddr),
    Configured(Arc<Mutex<RouteAuthority>>),
}
struct RouteAuthority {
    config: Arc<ValidatedConfig>,
    resolver: Arc<dyn Resolver>,
    cache: ResolvedConfig,
    generation: u64,
    scope: Option<TransportScope>,
}
#[derive(Clone)]
struct ConfiguredRouting {
    authority: Arc<Mutex<RouteAuthority>>,
    changed: watch::Sender<u64>,
}

const CANDIDATE_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_PROGRESS_TIMEOUT: Duration = Duration::from_secs(2);
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

impl TcpTransport {
    /// Numeric-address compatibility entry point for same-host fixtures.
    /// Must be called inside a Tokio runtime.
    pub fn spawn(self_id: NodeId, peers: &[(NodeId, SocketAddr)]) -> TcpTransport {
        Self::spawn_links(
            self_id,
            peers
                .iter()
                .map(|&(peer, addr)| (peer, PeerDestination::Numeric(addr))),
            None,
        )
    }

    /// Explicitly scoped numeric routes for independently isolated Raft groups.
    pub fn spawn_scoped(
        self_id: NodeId,
        peers: &[(NodeId, SocketAddr)],
        scope: TransportScope,
    ) -> io::Result<TcpTransport> {
        scope.validate()?;
        Ok(Self::spawn_links(
            self_id,
            peers
                .iter()
                .map(|&(id, addr)| (id, PeerDestination::Numeric(addr))),
            Some(scope),
        ))
    }

    /// Refresh this peer and local advertisements before each reconnect.
    /// Validate against other peers' last accepted candidates without requiring
    /// their DNS lookups to succeed. The initial snapshot is validated at startup.
    pub fn spawn_configured(
        config: Arc<ValidatedConfig>,
        resolver: Arc<dyn Resolver>,
        resolved: ResolvedConfig,
    ) -> TcpTransport {
        Self::spawn_configured_inner(config, resolver, resolved, None)
    }

    pub fn spawn_configured_scoped(
        config: Arc<ValidatedConfig>,
        resolver: Arc<dyn Resolver>,
        resolved: ResolvedConfig,
        scope: TransportScope,
    ) -> io::Result<TcpTransport> {
        scope.validate()?;
        if config.group_id.as_deref() != Some(scope.group_id())
            || config.genesis_voters != scope.genesis_voters()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configured scope differs from group authority",
            ));
        }
        config
            .validate_snapshot(&resolved, true)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(Self::spawn_configured_inner(
            config,
            resolver,
            resolved,
            Some(scope),
        ))
    }
    fn spawn_configured_inner(
        config: Arc<ValidatedConfig>,
        resolver: Arc<dyn Resolver>,
        cache: ResolvedConfig,
        scope: Option<TransportScope>,
    ) -> TcpTransport {
        let self_id = config.id;
        let authority = Arc::new(Mutex::new(RouteAuthority {
            config,
            resolver,
            cache,
            generation: 0,
            scope,
        }));
        let (changed, updates) = watch::channel(0);
        let outbound = Arc::new(Mutex::new(BTreeMap::new()));
        let task = tokio::spawn(route_supervisor(
            self_id,
            Arc::clone(&authority),
            Arc::clone(&outbound),
            updates,
        ));
        TcpTransport {
            self_id,
            outbound,
            _tasks: Arc::new(PeerTasks(vec![task])),
            routing: Some(ConfiguredRouting { authority, changed }),
        }
    }

    /// Publish only a complete accepted route set. DNS runs in the caller; this
    /// method validates again before changing authority. Updates coalesce while
    /// a bounded supervisor joins cancelled links. Established unchanged links
    /// remain connected and all reconnects use the newest accepted route set.
    pub fn replace_configured_routes(
        &self,
        config: Arc<ValidatedConfig>,
        resolved: ResolvedConfig,
    ) -> io::Result<()> {
        let routing = self.routing.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "numeric transport has no configured route authority",
            )
        })?;
        config
            .validate_snapshot(&resolved, true)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if config.peers.len() > 255 || config.peers.iter().any(|p| p.id == self.self_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many peer routes",
            ));
        }
        let mut accepted = routing
            .authority
            .lock()
            .map_err(|_| io::Error::other("route authority poisoned"))?;
        let old = &accepted.config;
        if config.id != self.self_id
            || config.group_id != old.group_id
            || config.genesis_voters != old.genesis_voters
            || config.raft_listen != old.raft_listen
            || config.http_listen != old.http_listen
            || config.raft_advertise != old.raft_advertise
            || config.http_advertise != old.http_advertise
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "route update changed immutable local/group identity",
            ));
        }
        let generation = accepted
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("route generation exhausted"))?;
        // Close admission for removed/repointed links immediately; the supervisor
        // cancels and joins their task before admitting replacement connections.
        self.outbound
            .lock()
            .map_err(|_| io::Error::other("outbound map poisoned"))?
            .retain(|id, _| {
                old.peers
                    .iter()
                    .find(|p| p.id == *id)
                    .zip(config.peers.iter().find(|p| p.id == *id))
                    .is_some_and(|(before, after)| before.endpoint == after.endpoint)
            });
        accepted.config = config;
        accepted.cache = resolved;
        accepted.generation = generation;
        routing.changed.send_replace(generation);
        Ok(())
    }

    fn spawn_links(
        self_id: NodeId,
        peers: impl IntoIterator<Item = (NodeId, PeerDestination)>,
        scope: Option<TransportScope>,
    ) -> TcpTransport {
        let mut outbound = BTreeMap::new();
        let mut tasks = PeerTasks::default();
        for (peer, destination) in peers {
            let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
            tasks.0.push(tokio::spawn(peer_task(
                self_id,
                peer,
                destination,
                rx,
                scope.clone(),
            )));
            outbound.insert(peer, tx);
        }
        TcpTransport {
            self_id,
            outbound: Arc::new(Mutex::new(outbound)),
            _tasks: Arc::new(tasks),
            routing: None,
        }
    }
}

impl Transport for TcpTransport {
    fn send(&self, to: NodeId, msg: RaftMessage) {
        let tx = self
            .outbound
            .lock()
            .ok()
            .and_then(|links| links.get(&to).cloned());
        if let Some(tx) = tx {
            match tx.try_send(msg) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    debug!(
                        self_id = self.self_id,
                        to, "outbound queue full; message dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!(
                        self_id = self.self_id,
                        to, "peer task stopped; message dropped"
                    );
                }
            }
        } else {
            warn!(self_id = self.self_id, to, "send to unknown peer dropped");
        }
    }
}

struct SupervisedLink {
    endpoint: Endpoint,
    task: AbortHandle,
}
async fn route_supervisor(
    self_id: NodeId,
    authority: Arc<Mutex<RouteAuthority>>,
    outbound: Arc<Mutex<BTreeMap<NodeId, mpsc::Sender<RaftMessage>>>>,
    mut changed: watch::Receiver<u64>,
) {
    let mut tasks = JoinSet::new();
    let mut links: BTreeMap<NodeId, SupervisedLink> = BTreeMap::new();
    loop {
        changed.borrow_and_update();
        let (config, scope, generation) = {
            let Ok(state) = authority.lock() else { return };
            (
                Arc::clone(&state.config),
                state.scope.clone(),
                state.generation,
            )
        };
        let removed: Vec<_> = links
            .iter()
            .filter(|(id, link)| {
                !config
                    .peers
                    .iter()
                    .any(|p| p.id == **id && p.endpoint == link.endpoint)
            })
            .map(|(id, _)| *id)
            .collect();
        let mut stopping = std::collections::BTreeSet::new();
        for id in removed {
            if let Some(link) = links.remove(&id) {
                stopping.insert(link.task.id());
                link.task.abort();
            }
            if let Ok(mut map) = outbound.lock() {
                map.remove(&id);
            } else {
                return;
            }
        }
        while !stopping.is_empty() {
            let Some(result) = tasks.join_next_with_id().await else {
                break;
            };
            let id = match result {
                Ok((id, ())) => id,
                Err(error) => error.id(),
            };
            stopping.remove(&id);
            let ended: Vec<_> = links
                .iter()
                .filter(|(_, link)| link.task.id() == id)
                .map(|(peer, _)| *peer)
                .collect();
            for peer in ended {
                links.remove(&peer);
                if let Ok(mut map) = outbound.lock() {
                    map.remove(&peer);
                } else {
                    return;
                }
            }
        }
        {
            let Ok(state) = authority.lock() else { return };
            if state.generation != generation {
                continue;
            }
            let Ok(mut map) = outbound.lock() else { return };
            for peer in &config.peers {
                if links.contains_key(&peer.id) {
                    continue;
                }
                let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
                let task = tasks.spawn(peer_task(
                    self_id,
                    peer.id,
                    PeerDestination::Configured(Arc::clone(&authority)),
                    rx,
                    scope.clone(),
                ));
                links.insert(
                    peer.id,
                    SupervisedLink {
                        endpoint: peer.endpoint.clone(),
                        task,
                    },
                );
                map.insert(peer.id, tx);
            }
        }
        tokio::select! {
            result=changed.changed()=>{if result.is_err(){return;}}
            result=tasks.join_next_with_id(),if !tasks.is_empty()=>{
                if let Some(result)=result {
                    let id=match result{Ok((id,()))=>id,Err(error)=>{warn!(%error,"configured peer task exited");error.id()}};
                    let ended:Vec<_>=links.iter().filter(|(_,link)|link.task.id()==id).map(|(peer,_)|*peer).collect();
                    for peer in ended {links.remove(&peer);if let Ok(mut map)=outbound.lock(){map.remove(&peer);}else{return;}}
                }
            }
        }
    }
}

async fn connect_peer(
    peer: NodeId,
    destination: &PeerDestination,
    candidate_offset: usize,
) -> io::Result<(TcpStream, SocketAddr)> {
    let addresses = match destination {
        PeerDestination::Numeric(addr) => vec![*addr],
        PeerDestination::Configured(authority) => {
            let (config, resolver, generation) = {
                let accepted = authority
                    .lock()
                    .map_err(|_| io::Error::other("route authority poisoned"))?;
                (
                    Arc::clone(&accepted.config),
                    Arc::clone(&accepted.resolver),
                    accepted.generation,
                )
            };
            let fresh = config
                .resolve_peer(peer, resolver.as_ref())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let accepted = {
                let mut accepted = authority
                    .lock()
                    .map_err(|_| io::Error::other("route authority poisoned"))?;
                if accepted.generation != generation {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "route changed during DNS refresh",
                    ));
                }
                match config.accept_peer_refresh(&mut accepted.cache, fresh) {
                    Ok(addresses) => Ok(addresses),
                    Err(PeerRefreshError::CachedConflict(error)) => {
                        debug!(peer,%error,"cached address conflict; resolving joint snapshot");
                        Err(accepted.cache.clone())
                    }
                    Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
                }
            };
            match accepted {
                Ok(addresses) => addresses,
                Err(expected) => {
                    let fresh = config
                        .resolve(resolver.as_ref())
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    let mut accepted = authority
                        .lock()
                        .map_err(|_| io::Error::other("route authority poisoned"))?;
                    if accepted.generation != generation {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "route changed during joint DNS refresh",
                        ));
                    }
                    config
                        .accept_snapshot_refresh(&mut accepted.cache, &expected, fresh, peer)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                }
            }
        }
    };
    connect_candidates(&addresses, candidate_offset).await
}

async fn connect_candidates(
    addresses: &[SocketAddr],
    candidate_offset: usize,
) -> io::Result<(TcpStream, SocketAddr)> {
    let deadline = Instant::now() + DIAL_TIMEOUT;
    let mut last_error = io::Error::new(io::ErrorKind::InvalidInput, "peer has no addresses");
    // Rotate the first candidate on each retry. With more blackholed addresses
    // than fit in one dial budget, a later reachable address must not starve.
    for index in 0..addresses.len() {
        let addr = addresses[(candidate_offset % addresses.len() + index) % addresses.len()];
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "peer dial deadline elapsed",
            ));
        }
        match timeout(
            remaining.min(CANDIDATE_CONNECT_TIMEOUT),
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(stream)) => return Ok((stream, addr)),
            Ok(Err(error)) => last_error = error,
            Err(_) => {
                last_error =
                    io::Error::new(io::ErrorKind::TimedOut, "peer candidate dial timed out");
            }
        }
        debug!(%addr, error = %last_error, "peer candidate unavailable; trying next address");
    }
    Err(last_error)
}

async fn write_frame<W: AsyncWrite + Unpin>(
    stream: &mut W,
    frame: &[u8],
    within: Duration,
) -> io::Result<()> {
    let mut written = 0;
    while written < frame.len() {
        let count = timeout(within, stream.write(&frame[written..]))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "peer frame write made no progress")
            })??;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "peer frame write returned zero",
            ));
        }
        written += count;
    }
    Ok(())
}

async fn peer_task(
    self_id: NodeId,
    peer: NodeId,
    destination: PeerDestination,
    mut rx: mpsc::Receiver<RaftMessage>,
    scope: Option<TransportScope>,
) {
    let mut backoff = INITIAL_BACKOFF;
    let mut candidate_offset = 0usize;
    loop {
        // Drop disconnected messages without restarting an in-progress lookup/dial.
        let connect = connect_peer(peer, &destination, candidate_offset);
        candidate_offset = candidate_offset.wrapping_add(1);
        tokio::pin!(connect);
        let result = loop {
            tokio::select! {
                result = &mut connect => break result,
                message = rx.recv() => match message {
                    Some(_) => debug!(peer, "message dropped while connecting"),
                    None => return,
                },
            }
        };
        match result {
            Ok((mut stream, addr)) => {
                backoff = INITIAL_BACKOFF;
                if let Err(error) = stream.set_nodelay(true) {
                    warn!(peer, %addr, %error, "failed to enable TCP_NODELAY on outbound peer connection");
                }
                debug!(peer, %addr, "peer connection established");
                loop {
                    tokio::select! {
                        message = rx.recv() => {
                            let Some(message) = message else { return };
                            let encoded = match &scope {
                                Some(scope) => encode_scoped_frame(self_id, &message, scope),
                                None => encode_frame(self_id, &message),
                            };
                            let frame = match encoded {
                                Ok(frame) => frame,
                                Err(error) => {
                                    warn!(peer, %error, "unencodable message dropped");
                                    continue;
                                }
                            };
                            if let Err(error) = write_frame(&mut stream, &frame, WRITE_PROGRESS_TIMEOUT).await {
                                // A partial frame cannot be resumed on a new connection.
                                // The heartbeat cadence retries replication at the Raft layer.
                                debug!(peer, %addr, %error, "write failed; reconnecting");
                                break;
                            }
                        }
                        readiness = stream.readable() => {
                            if let Err(error) = readiness {
                                debug!(peer, %addr, %error, "peer read readiness failed");
                                break;
                            }
                            // Links are unidirectional. Observe EOF promptly even while
                            // idle; replies arrive on the peer's separate outbound link.
                            match stream.try_read(&mut [0u8; 1]) {
                                Ok(0) => break,
                                Ok(_) => {
                                    warn!(peer, %addr, "unexpected bytes on outbound peer link");
                                    break;
                                }
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                                Err(error) => {
                                    debug!(peer, %addr, %error, "peer closed outbound link");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => {
                if error.kind() == io::ErrorKind::InvalidInput {
                    warn!(peer, %error, "peer resolution rejected; no candidate dialed");
                } else {
                    debug!(peer, %error, "peer dial failed");
                }
            }
        }
        debug!(
            peer,
            delay_ms = backoff.as_millis(),
            "peer reconnect backoff"
        );
        let delay = tokio::time::sleep(backoff);
        tokio::pin!(delay);
        loop {
            tokio::select! {
                () = &mut delay => break,
                message = rx.recv() => match message {
                    Some(_) => debug!(peer, "message dropped while disconnected"),
                    None => return,
                },
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Same-host compatibility wrapper for the loopback Raft listener.
pub async fn spawn_listener(
    port: u16,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
) -> io::Result<JoinHandle<()>> {
    spawn_listener_at(SocketAddr::from(([127, 0, 0, 1], port)), tx).await
}

/// Bind the configured local interface and own all accepted connection tasks.
/// Aborting the returned accept loop also cancels its accepted connections.
pub async fn spawn_listener_at(
    addr: SocketAddr,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(addr).await?;
    Ok(spawn_bound_listener(listener, tx))
}

/// Stop accepting peers on shutdown, but keep existing peer streams available
/// while the single writer drains. Closing its input receiver ends the task and
/// cancels the remaining connections. A shutdown request alone is not completion.
pub async fn spawn_listener_with_shutdown(
    addr: SocketAddr,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
    shutdown: ShutdownRx,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(addr).await?;
    Ok(spawn_bound_listener_controlled(
        listener,
        tx,
        Some(shutdown),
        None,
    ))
}

/// An explicit listener rejects legacy/unscoped and other-group frames before
/// forwarding any message to the Raft actor. Shutdown preserves accepted drains.
pub async fn spawn_listener_scoped_with_shutdown(
    addr: SocketAddr,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
    shutdown: ShutdownRx,
    scope: TransportScope,
) -> io::Result<JoinHandle<()>> {
    scope.validate()?;
    let listener = TcpListener::bind(addr).await?;
    Ok(spawn_bound_listener_controlled(
        listener,
        tx,
        Some(shutdown),
        Some(scope),
    ))
}

fn spawn_bound_listener(
    listener: TcpListener,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
) -> JoinHandle<()> {
    spawn_bound_listener_controlled(listener, tx, None, None)
}

fn enable_peer_keepalive(stream: &TcpStream) -> io::Result<()> {
    let keepalive = TcpKeepalive::new().with_time(TCP_KEEPALIVE_IDLE);
    #[cfg(any(windows, target_os = "linux"))]
    let keepalive = keepalive.with_interval(TCP_KEEPALIVE_INTERVAL);
    SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

fn reap_connection(
    result: Result<(Id, ()), JoinError>,
    order: &mut VecDeque<(Id, AbortHandle)>,
) -> Id {
    let finished = match result {
        Ok((id, ())) => id,
        Err(error) => {
            if !error.is_cancelled() {
                warn!(%error, "accepted peer task failed");
            }
            error.id()
        }
    };
    order.retain(|(id, _)| *id != finished);
    finished
}

async fn evict_oldest_connection(
    connections: &mut JoinSet<()>,
    order: &mut VecDeque<(Id, AbortHandle)>,
) {
    let (oldest, handle) = order.front().expect("accepted task has an eviction record");
    let oldest = *oldest;
    handle.abort();
    // Complete cancellation before admitting a replacement, keeping even the
    // actual live task/socket count bounded rather than just the handle list.
    while let Some(result) = connections.join_next_with_id().await {
        if reap_connection(result, order) == oldest {
            break;
        }
    }
    warn!(task_id = ?oldest, limit = MAX_INBOUND_CONNECTIONS, "peer connection limit reached; oldest connection closed");
}

fn spawn_bound_listener_controlled(
    listener: TcpListener,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
    mut shutdown: Option<ShutdownRx>,
    scope: Option<TransportScope>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut listener = Some(listener);
        let mut connections = JoinSet::new();
        let mut connection_order = VecDeque::new();
        let mut accept_ready = Instant::now();
        loop {
            tokio::select! {
                // Once the writer is gone no connection can make progress. A
                // shutdown already requested also takes precedence over accepts.
                biased;
                () = tx.closed() => return,
                () = async {
                    match shutdown.as_mut() {
                        Some(shutdown) => { shutdown.requested().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if listener.is_some() => {
                    drop(listener.take());
                    debug!("peer listener closed; existing connections remain for writer drain");
                }
                result = connections.join_next_with_id(), if !connections.is_empty() => {
                    if let Some(result) = result {
                        reap_connection(result, &mut connection_order);
                    }
                }
                accepted = async {
                    tokio::time::sleep_until(accept_ready).await;
                    listener.as_ref().expect("accept branch requires listener").accept().await
                }, if listener.is_some() => match accepted {
                    Ok((stream, remote)) => {
                        if shutdown.as_ref().is_some_and(ShutdownRx::is_requested) {
                            drop(listener.take());
                            continue;
                        }
                        if let Err(error) = stream.set_nodelay(true) {
                            warn!(%remote, %error, "failed to enable TCP_NODELAY on accepted peer connection");
                        }
                        if let Err(error) = enable_peer_keepalive(&stream) {
                            warn!(%remote, %error, "failed to configure peer keepalive; connection closed");
                            continue;
                        }
                        if connections.len() >= MAX_INBOUND_CONNECTIONS {
                            evict_oldest_connection(&mut connections, &mut connection_order).await;
                        }
                        if shutdown.as_ref().is_some_and(ShutdownRx::is_requested) {
                            drop(listener.take());
                            continue;
                        }
                        let handle = connections.spawn(conn_task(stream, remote, tx.clone(), scope.clone()));
                        connection_order.push_back((handle.id(), handle));
                    }
                    Err(error) => {
                        warn!(%error, "peer accept failed");
                        accept_ready = Instant::now() + INITIAL_BACKOFF;
                    }
                }
            }
        }
    })
}

async fn conn_task(
    mut stream: TcpStream,
    remote: SocketAddr,
    tx: mpsc::Sender<(NodeId, RaftMessage)>,
    scope: Option<TransportScope>,
) {
    let mut dec = FrameDecoder {
        buf: Vec::new(),
        scope,
    };
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
                            if tx.send(frame).await.is_err() {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use clap::Parser;
    use raft_core::{Command, Entry, MAX_APPEND_ENTRIES};
    use tokio::sync::Notify;

    use crate::config::{validate, Config, ResolveFuture};

    use super::*;
    use crate::http::{MAX_KEY_BYTES, MAX_VALUE_BYTES};

    fn test_scope(name: &str) -> TransportScope {
        TransportScope::new(name.into(), vec![1, 2, 3]).unwrap()
    }

    #[test]
    fn scoped_frames_require_exact_group_and_genesis_and_reject_legacy() {
        let message = all_variants()[0].clone();
        let expected = test_scope("cluster-a/controller");
        let frame = encode_scoped_frame(1, &message, &expected).unwrap();
        let mut decoder = FrameDecoder::scoped(expected.clone()).unwrap();
        decoder.push_bytes(&frame);
        assert_eq!(decoder.next_frame().unwrap(), Some((1, message.clone())));
        for wrong in [
            test_scope("cluster-a/data-1"),
            TransportScope::new("cluster-a/controller".into(), vec![1, 2, 4]).unwrap(),
        ] {
            let mut decoder = FrameDecoder::scoped(wrong).unwrap();
            decoder.push_bytes(&frame);
            assert!(matches!(
                decoder.next_frame(),
                Err(FrameError::ScopeMismatch)
            ));
        }
        let mut legacy = FrameDecoder::default();
        legacy.push_bytes(&frame);
        assert!(
            legacy.next_frame().is_err(),
            "legacy listeners cannot admit scoped frames"
        );
        let mut scoped = FrameDecoder::scoped(expected).unwrap();
        scoped.push_bytes(&encode_frame(1, &message).unwrap());
        assert!(
            scoped.next_frame().is_err(),
            "scoped listeners cannot admit legacy frames"
        );
        for invalid in ["", "bad group", "legacy-default"] {
            assert!(TransportScope::new(invalid.into(), vec![1]).is_err());
        }
        assert!(TransportScope::new("g".into(), vec![2, 1]).is_err());
    }

    #[test]
    fn scoped_frame_limit_includes_scope_bytes() {
        let mut message = all_variants()[2].clone();
        if let RaftMessage::AppendEntries { entries, .. } = &mut message {
            entries.truncate(1);
            entries[0].command = Command::Put {
                key: "k".into(),
                value: String::new(),
            };
        }
        let empty = encode_frame(1, &message).unwrap().len() - 4;
        if let RaftMessage::AppendEntries { entries, .. } = &mut message {
            entries[0].command = Command::Put {
                key: "k".into(),
                value: "x".repeat(MAX_FRAME_BYTES as usize - empty),
            };
        }
        assert_eq!(
            encode_frame(1, &message).unwrap().len(),
            MAX_FRAME_BYTES as usize + 4
        );
        assert!(matches!(
            encode_scoped_frame(1, &message, &test_scope("g")),
            Err(FrameError::Oversize { .. })
        ));
    }

    #[tokio::test]
    async fn scoped_listener_rejects_cross_wired_and_unscoped_real_streams() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let task =
            spawn_bound_listener_controlled(listener, tx, None, Some(test_scope("controller")));
        let message = all_variants()[0].clone();
        for frame in [
            encode_scoped_frame(1, &message, &test_scope("shard-1")).unwrap(),
            encode_frame(1, &message).unwrap(),
        ] {
            let mut wrong = TcpStream::connect(addr).await.unwrap();
            wrong.write_all(&frame).await.unwrap();
            let closed = timeout(Duration::from_secs(2), wrong.read_u8())
                .await
                .expect("invalid stream must be closed");
            assert!(
                matches!(closed, Err(ref e) if matches!(e.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset))
            );
            assert!(
                rx.try_recv().is_err(),
                "cross-wired frame reached Raft input"
            );
        }
        let mut correct = TcpStream::connect(addr).await.unwrap();
        correct
            .write_all(&encode_scoped_frame(2, &message, &test_scope("controller")).unwrap())
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), rx.recv()).await.unwrap(),
            Some((2, message))
        );
        drop(rx);
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

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
                    Entry {
                        index: 13,
                        term: 4,
                        command: Command::RegisterSession {
                            nonce: "client".into(),
                        },
                    },
                    Entry {
                        index: 14,
                        term: 4,
                        command: Command::CloseSession { session_id: 1 },
                    },
                    Entry {
                        index: 15,
                        term: 4,
                        command: Command::SessionPut {
                            session_id: 1,
                            key: "k".into(),
                            sequence: 1,
                            value: "v".into(),
                        },
                    },
                    Entry {
                        index: 16,
                        term: 4,
                        command: Command::SessionDelete {
                            session_id: 1,
                            key: "k".into(),
                            sequence: 2,
                        },
                    },
                ],
                leader_commit: 9,
                contact_round: 0,
            },
            RaftMessage::AppendEntriesReply {
                term: 4,
                success: false,
                match_index: 5,
                contact_round: 0,
            },
            RaftMessage::PreVote {
                prospective_term: 5,
                campaign_id: raft_core::CampaignId {
                    incarnation: 42,
                    sequence: 7,
                },
                candidate_id: 2,
                last_log_index: 12,
                last_log_term: 4,
            },
            RaftMessage::PreVoteReply {
                term: 4,
                prospective_term: 5,
                campaign_id: raft_core::CampaignId {
                    incarnation: 42,
                    sequence: 7,
                },
                vote_granted: true,
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

    #[test]
    fn maximum_http_commands_fit_one_replication_frame() {
        let key = "\0".repeat(MAX_KEY_BYTES);
        let value = "\0".repeat(MAX_VALUE_BYTES);
        let entries = (1..=MAX_APPEND_ENTRIES)
            .map(|index| Entry {
                index: index as u64,
                term: 1,
                command: Command::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect();
        let message = RaftMessage::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
            contact_round: 0,
        };

        let frame = encode_frame(1, &message).expect("maximum API-originated batch must fit");
        assert!(frame.len() - 4 <= MAX_FRAME_BYTES as usize);
    }

    #[test]
    fn outbound_queue_drops_on_saturation_and_closed_peer() {
        let (tx, mut rx) = mpsc::channel(1);
        let first = all_variants().remove(0);
        let second = all_variants().remove(1);
        let transport = TcpTransport {
            self_id: 1,
            outbound: Arc::new(Mutex::new(BTreeMap::from([(2, tx)]))),
            _tasks: Arc::new(PeerTasks::default()),
            routing: None,
        };

        transport.send(2, first.clone());
        transport.send(2, second);
        assert_eq!(
            rx.try_recv().unwrap(),
            first,
            "the queued message is retained"
        );
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "the message sent to a full queue is dropped"
        );

        drop(rx);
        transport.send(2, all_variants().remove(0));
    }

    fn numeric_config(peers: &[(NodeId, SocketAddr)]) -> Arc<ValidatedConfig> {
        let mut args = vec![
            "kv-node".to_string(),
            "--id".into(),
            "1".into(),
            "--raft-port".into(),
            "7101".into(),
            "--http-port".into(),
            "8101".into(),
            "--data-dir".into(),
            "unused-dynamic-fixture".into(),
        ];
        for (id, addr) in peers {
            args.push("--peers".into());
            args.push(format!("{id}@{addr}"));
        }
        Arc::new(validate(&Config::try_parse_from(args).unwrap()).unwrap())
    }
    async fn expect_stream_closed(stream: &mut TcpStream) {
        timeout(Duration::from_secs(2), async {
            let mut bytes = [0u8; 1024];
            loop {
                match stream.read(&mut bytes).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => break,
                    Err(e) => panic!("unexpected terminal stream error: {e}"),
                }
            }
        })
        .await
        .expect("cancelled peer stream must close");
    }

    #[tokio::test]
    async fn dynamic_routes_preserve_unchanged_connection_and_reuse_retired_address() {
        let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = a.local_addr().unwrap();
        let addr_b = b.local_addr().unwrap();
        let addr_c = c.local_addr().unwrap();
        let config = numeric_config(&[(2, addr_a), (3, addr_b)]);
        let resolver: Arc<dyn Resolver> = Arc::new(crate::config::SystemResolver);
        let transport = start_configured(config, Arc::clone(&resolver)).await;
        let mut old_a = accept_peer(&a).await;
        let mut stable_b = accept_peer(&b).await;
        let stable_tx = transport.outbound.lock().unwrap().get(&3).unwrap().clone();
        let replaced = numeric_config(&[(3, addr_b), (4, addr_a)]);
        let snapshot = replaced.resolve(resolver.as_ref()).await.unwrap();
        transport
            .replace_configured_routes(replaced, snapshot)
            .unwrap();
        assert!(!transport.outbound.lock().unwrap().contains_key(&2));
        expect_stream_closed(&mut old_a).await;
        let mut new_a = accept_peer(&a).await;
        assert!(stable_tx.same_channel(transport.outbound.lock().unwrap().get(&3).unwrap()));
        let message = all_variants()[0].clone();
        expect_frame_for(&mut new_a, &transport, 4, &message).await;
        expect_frame_for(&mut stable_b, &transport, 3, &message).await;
        let moved = numeric_config(&[(3, addr_b), (4, addr_c)]);
        let snapshot = moved.resolve(resolver.as_ref()).await.unwrap();
        transport
            .replace_configured_routes(moved, snapshot)
            .unwrap();
        expect_stream_closed(&mut new_a).await;
        let mut new_c = accept_peer(&c).await;
        expect_frame_for(&mut new_c, &transport, 4, &message).await;
        assert!(stable_tx.same_channel(transport.outbound.lock().unwrap().get(&3).unwrap()));
        assert!(
            timeout(Duration::from_millis(50), b.accept())
                .await
                .is_err(),
            "unchanged route must not reconnect"
        );
        drop(transport);
        drop(stable_tx);
        expect_stream_closed(&mut new_c).await;
        expect_stream_closed(&mut stable_b).await;
    }

    #[tokio::test]
    async fn dynamic_route_rejection_preserves_accepted_authority_and_scope() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = numeric_config(&[(2, listener.local_addr().unwrap())]);
        let resolver: Arc<dyn Resolver> = Arc::new(crate::config::SystemResolver);
        let snapshot = config.resolve(resolver.as_ref()).await.unwrap();
        let transport =
            TcpTransport::spawn_configured(Arc::clone(&config), resolver, snapshot.clone());
        let _stream = accept_peer(&listener).await;
        let before = transport.outbound.lock().unwrap().get(&2).unwrap().clone();
        let mut changed = (*config).clone();
        changed.group_id = Some("different-group".into());
        assert!(transport
            .replace_configured_routes(Arc::new(changed), snapshot.clone())
            .is_err());
        let mut invalid = snapshot.clone();
        invalid.peer_addrs[0].1 = config
            .resolve(&crate::config::SystemResolver)
            .await
            .unwrap()
            .raft_advertise;
        assert!(transport
            .replace_configured_routes(config, invalid)
            .is_err());
        assert!(before.same_channel(transport.outbound.lock().unwrap().get(&2).unwrap()));
        let accepted = transport
            .routing
            .as_ref()
            .unwrap()
            .authority
            .lock()
            .unwrap();
        assert_eq!(accepted.generation, 0);
        assert_eq!(accepted.cache, snapshot);
    }

    struct HeldResolver {
        entered: Notify,
        release: Notify,
        address: SocketAddr,
    }
    impl Resolver for HeldResolver {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> ResolveFuture<'a> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(vec![self.address])
            })
        }
    }
    #[tokio::test]
    async fn dynamic_generation_rejects_old_inflight_dns_before_dial() {
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = old.local_addr().unwrap();
        let config = hostname_config(addr.port());
        let held = Arc::new(HeldResolver {
            entered: Notify::new(),
            release: Notify::new(),
            address: addr,
        });
        let initial = config
            .resolve(&MutableResolver::new(vec![addr]))
            .await
            .unwrap();
        let authority = Arc::new(Mutex::new(RouteAuthority {
            config,
            resolver: held.clone(),
            cache: initial,
            generation: 0,
            scope: None,
        }));
        let destination = PeerDestination::Configured(Arc::clone(&authority));
        let pending = tokio::spawn(async move { connect_peer(2, &destination, 0).await });
        timeout(Duration::from_secs(2), held.entered.notified())
            .await
            .unwrap();
        let current = numeric_config(&[(3, addr)]);
        let cache = current
            .resolve(&crate::config::SystemResolver)
            .await
            .unwrap();
        {
            let mut accepted = authority.lock().unwrap();
            accepted.config = current;
            accepted.cache = cache.clone();
            accepted.generation = 1;
        }
        held.release.notify_one();
        let error = timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(authority.lock().unwrap().cache, cache);
        assert!(
            timeout(Duration::from_millis(50), old.accept())
                .await
                .is_err(),
            "stale lookup cannot dial retired endpoint"
        );
    }

    struct MutableResolver {
        addresses: Mutex<Vec<SocketAddr>>,
        calls: AtomicUsize,
    }

    impl MutableResolver {
        fn new(addresses: Vec<SocketAddr>) -> Self {
            Self {
                addresses: Mutex::new(addresses),
                calls: AtomicUsize::new(0),
            }
        }

        fn replace(&self, addresses: Vec<SocketAddr>) {
            *self.addresses.lock().unwrap() = addresses;
        }

        async fn wait_for_calls(&self, expected: usize) {
            timeout(Duration::from_secs(5), async {
                while self.calls.load(Ordering::SeqCst) < expected {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("expected resolver call before deadline");
        }
    }

    impl Resolver for MutableResolver {
        fn resolve<'a>(&'a self, host: &'a str, _port: u16) -> ResolveFuture<'a> {
            assert_eq!(host, "changing.test");
            self.calls.fetch_add(1, Ordering::SeqCst);
            let addresses = self.addresses.lock().unwrap().clone();
            Box::pin(async move { Ok(addresses) })
        }
    }

    fn hostname_config(port: u16) -> Arc<ValidatedConfig> {
        let peer = format!("2@changing.test:{port}");
        let config = Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--peers",
            &peer,
            "--data-dir",
            "unused-transport-fixture",
            "--raft-port",
            "7101",
            "--http-port",
            "8101",
        ])
        .unwrap();
        Arc::new(validate(&config).unwrap())
    }

    async fn start_configured(
        config: Arc<ValidatedConfig>,
        resolver: Arc<dyn Resolver>,
    ) -> TcpTransport {
        let resolved = config.resolve(resolver.as_ref()).await.unwrap();
        TcpTransport::spawn_configured(config, resolver, resolved)
    }

    async fn accept_peer(listener: &TcpListener) -> TcpStream {
        timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("peer must connect before deadline")
            .expect("accept peer")
            .0
    }

    async fn expect_frame(stream: &mut TcpStream, transport: &TcpTransport, message: &RaftMessage) {
        expect_frame_for(stream, transport, 2, message).await;
    }

    async fn expect_frame_for(
        stream: &mut TcpStream,
        transport: &TcpTransport,
        to: NodeId,
        message: &RaftMessage,
    ) {
        let read = async {
            let length = stream.read_u32().await.unwrap();
            let mut payload = vec![0; length as usize];
            stream.read_exact(&mut payload).await.unwrap();
            serde_json::from_slice::<(NodeId, RaftMessage)>(&payload).unwrap()
        };
        tokio::pin!(read);
        let (from, received) = timeout(Duration::from_secs(5), async {
            let mut heartbeat = tokio::time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    received = &mut read => break received,
                    _ = heartbeat.tick() => transport.send(to, message.clone()),
                }
            }
        })
        .await
        .expect("peer frame received before deadline");
        assert_eq!(from, 1);
        assert_eq!(&received, message);
    }

    #[tokio::test]
    async fn configured_peer_refreshes_dns_after_idle_disconnect() {
        let old_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let old_addr = old_listener.local_addr().unwrap();
        let new_listener = TcpListener::bind(("127.0.0.2", old_addr.port()))
            .await
            .unwrap();
        let new_addr = new_listener.local_addr().unwrap();
        let resolver = Arc::new(MutableResolver::new(vec![old_addr]));
        let transport = start_configured(hostname_config(old_addr.port()), resolver.clone()).await;
        let mut old_stream = accept_peer(&old_listener).await;
        let message = all_variants().remove(0);
        transport.send(2, message.clone());
        expect_frame(&mut old_stream, &transport, &message).await;

        let previous_calls = resolver.calls.load(Ordering::SeqCst);
        resolver.replace(vec![new_addr]);
        old_stream.shutdown().await.unwrap();
        drop(old_stream);
        let mut new_stream = accept_peer(&new_listener).await;
        assert!(resolver.calls.load(Ordering::SeqCst) > previous_calls);
        transport.send(2, message.clone());
        expect_frame(&mut new_stream, &transport, &message).await;

        drop(transport);
        timeout(
            Duration::from_secs(1),
            new_stream.read_to_end(&mut Vec::new()),
        )
        .await
        .expect("dropping transport closes its connected peer after buffered frames")
        .unwrap();
    }

    #[tokio::test]
    async fn forbidden_dns_refresh_dials_no_candidate_and_can_recover() {
        let old_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let old_addr = old_listener.local_addr().unwrap();
        let new_listener = TcpListener::bind(("127.0.0.2", old_addr.port()))
            .await
            .unwrap();
        let new_addr = new_listener.local_addr().unwrap();
        let resolver = Arc::new(MutableResolver::new(vec![old_addr]));
        let transport = start_configured(hostname_config(old_addr.port()), resolver.clone()).await;
        let mut old_stream = accept_peer(&old_listener).await;
        let previous_calls = resolver.calls.load(Ordering::SeqCst);
        resolver.replace(vec![
            new_addr,
            SocketAddr::from(([0, 0, 0, 0], old_addr.port())),
        ]);
        old_stream.shutdown().await.unwrap();
        drop(old_stream);
        resolver.wait_for_calls(previous_calls + 1).await;
        assert!(
            timeout(Duration::from_millis(250), new_listener.accept())
                .await
                .is_err(),
            "a mixed valid/forbidden answer must reject the entire dial"
        );

        resolver.replace(vec![new_addr]);
        let mut new_stream = accept_peer(&new_listener).await;
        let message = all_variants().remove(1);
        transport.send(2, message.clone());
        expect_frame(&mut new_stream, &transport, &message).await;
    }

    #[tokio::test]
    async fn peer_dial_falls_back_from_ipv6_to_ipv4_candidate() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let unavailable = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, address.port()));
        let resolver = Arc::new(MutableResolver::new(vec![unavailable, address]));
        let transport = start_configured(hostname_config(address.port()), resolver).await;
        let mut stream = accept_peer(&listener).await;
        let message = all_variants().remove(2);
        transport.send(2, message.clone());
        expect_frame(&mut stream, &transport, &message).await;
    }

    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    struct PendingResolver {
        started: Notify,
        cancelled: Arc<Notify>,
    }

    impl Resolver for PendingResolver {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> ResolveFuture<'a> {
            Box::pin(async move {
                let _guard = NotifyOnDrop(Arc::clone(&self.cancelled));
                self.started.notify_one();
                std::future::pending().await
            })
        }
    }

    #[tokio::test]
    async fn final_transport_drop_cancels_pending_resolution() {
        let resolver = Arc::new(PendingResolver {
            started: Notify::new(),
            cancelled: Arc::new(Notify::new()),
        });
        let config = hostname_config(7102);
        let initial = config
            .resolve(&MutableResolver::new(vec![SocketAddr::from((
                [127, 0, 0, 1],
                7102,
            ))]))
            .await
            .unwrap();
        let transport = TcpTransport::spawn_configured(config, resolver.clone(), initial);
        timeout(Duration::from_secs(1), resolver.started.notified())
            .await
            .unwrap();
        let survivor = transport.clone();
        drop(transport);
        assert!(
            timeout(Duration::from_millis(30), resolver.cancelled.notified())
                .await
                .is_err(),
            "a live transport clone must retain its peer task"
        );
        drop(survivor);
        timeout(Duration::from_secs(1), resolver.cancelled.notified())
            .await
            .expect("last transport owner cancels the pending DNS future");
    }

    #[tokio::test]
    async fn stalled_frame_write_has_a_deadline() {
        let (mut writer, _unread_peer) = tokio::io::duplex(16);
        let error = write_frame(&mut writer, &[0; 1024], Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn wildcard_listener_reaches_other_loopback_address_and_owns_connections() {
        let listener = TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, mut rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let listener_task = spawn_bound_listener(listener, tx);
        let mut client = TcpStream::connect(("127.0.0.2", port)).await.unwrap();
        let message = all_variants().remove(0);
        client
            .write_all(&encode_frame(2, &message).unwrap())
            .await
            .unwrap();
        let received = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, (2, message));
        listener_task.abort();
        let _ = listener_task.await;
        let eof = timeout(Duration::from_secs(1), client.read_u8())
            .await
            .expect("aborting listener closes its accepted connections")
            .unwrap_err();
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn ipv6_listener_exchanges_frames() {
        let listener = TcpListener::bind(("::1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let listener_task = spawn_bound_listener(listener, tx);
        let mut client = TcpStream::connect(address).await.unwrap();
        let message = all_variants().remove(1);
        client
            .write_all(&encode_frame(2, &message).unwrap())
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            (2, message)
        );
        drop(rx);
        timeout(Duration::from_secs(1), listener_task)
            .await
            .expect("closing driver input ends listener and connections")
            .unwrap();
    }

    #[tokio::test]
    async fn retry_rotates_first_candidate_so_late_addresses_do_not_starve() {
        let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let second = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addresses = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        // Both listeners can connect. Selecting the later candidate on the
        // next attempt avoids repeatedly spending a whole budget on a prefix.
        for attempt in 0..4 {
            let (stream, address) = connect_candidates(&addresses, attempt).await.unwrap();
            assert_eq!(address, addresses[attempt % addresses.len()]);
            assert_eq!(stream.peer_addr().unwrap(), address);
        }
    }

    struct PeerMapResolver {
        answers: Mutex<BTreeMap<String, Result<Vec<SocketAddr>, String>>>,
        calls: Mutex<BTreeMap<String, usize>>,
    }

    impl PeerMapResolver {
        fn new(first: SocketAddr, second: SocketAddr) -> Self {
            Self {
                answers: Mutex::new(BTreeMap::from([
                    ("changing.test".into(), Ok(vec![first])),
                    ("unrelated.test".into(), Ok(vec![second])),
                ])),
                calls: Mutex::new(BTreeMap::new()),
            }
        }

        fn answer(&self, host: &str, answer: Result<Vec<SocketAddr>, String>) {
            self.answers.lock().unwrap().insert(host.into(), answer);
        }

        fn calls(&self, host: &str) -> usize {
            self.calls.lock().unwrap().get(host).copied().unwrap_or(0)
        }

        async fn wait_for_calls(&self, host: &str, expected: usize) {
            timeout(Duration::from_secs(5), async {
                while self.calls(host) < expected {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("peer DNS refresh was exercised");
        }
    }

    impl Resolver for PeerMapResolver {
        fn resolve<'a>(&'a self, host: &'a str, _port: u16) -> ResolveFuture<'a> {
            *self.calls.lock().unwrap().entry(host.into()).or_default() += 1;
            let answer = self
                .answers
                .lock()
                .unwrap()
                .get(host)
                .expect("known fixture hostname")
                .clone();
            Box::pin(async move { answer })
        }
    }

    fn two_peer_config(port: u16) -> Arc<ValidatedConfig> {
        let peers = format!("2@changing.test:{port},3@unrelated.test:{port}");
        let config = Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--peers",
            &peers,
            "--data-dir",
            "unused-transport-fixture",
            "--raft-port",
            "7101",
            "--http-port",
            "8101",
        ])
        .unwrap();
        Arc::new(validate(&config).unwrap())
    }

    #[tokio::test]
    async fn unrelated_dns_failure_does_not_block_healthy_peer_reconnect() {
        let healthy = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let first = healthy.local_addr().unwrap();
        let unrelated = TcpListener::bind(("127.0.0.2", first.port()))
            .await
            .unwrap();
        let second = unrelated.local_addr().unwrap();
        let resolver = Arc::new(PeerMapResolver::new(first, second));
        let transport = start_configured(two_peer_config(first.port()), resolver.clone()).await;
        let mut healthy_stream = accept_peer(&healthy).await;
        let mut failed_stream = accept_peer(&unrelated).await;

        let before_failure = resolver.calls("unrelated.test");
        resolver.answer("unrelated.test", Err("injected DNS outage".into()));
        failed_stream.shutdown().await.unwrap();
        drop(failed_stream);
        resolver
            .wait_for_calls("unrelated.test", before_failure + 1)
            .await;

        let before_healthy = resolver.calls("changing.test");
        healthy_stream.shutdown().await.unwrap();
        drop(healthy_stream);
        let mut reconnected = accept_peer(&healthy).await;
        assert!(resolver.calls("changing.test") > before_healthy);
        expect_frame(&mut reconnected, &transport, &all_variants().remove(0)).await;
        assert!(
            timeout(Duration::from_millis(250), unrelated.accept())
                .await
                .is_err(),
            "the unresolvable peer remains disconnected while healthy traffic resumes"
        );
    }

    #[tokio::test]
    async fn dns_refresh_collision_with_cached_peer_is_rejected() {
        let first_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let first = first_listener.local_addr().unwrap();
        let other_listener = TcpListener::bind(("127.0.0.2", first.port()))
            .await
            .unwrap();
        let other = other_listener.local_addr().unwrap();
        let moved_listener = TcpListener::bind(("127.0.0.3", first.port()))
            .await
            .unwrap();
        let moved = moved_listener.local_addr().unwrap();
        let resolver = Arc::new(PeerMapResolver::new(first, other));
        let transport = start_configured(two_peer_config(first.port()), resolver.clone()).await;
        let mut old_stream = accept_peer(&first_listener).await;
        let _other_stream = accept_peer(&other_listener).await;

        let before = resolver.calls("changing.test");
        resolver.answer("changing.test", Ok(vec![other]));
        old_stream.shutdown().await.unwrap();
        drop(old_stream);
        resolver.wait_for_calls("changing.test", before + 1).await;
        assert!(
            timeout(Duration::from_millis(250), other_listener.accept())
                .await
                .is_err(),
            "a refresh must not dial another peer's cached destination"
        );

        resolver.answer("changing.test", Ok(vec![moved]));
        let mut moved_stream = accept_peer(&moved_listener).await;
        expect_frame(&mut moved_stream, &transport, &all_variants().remove(1)).await;
    }

    #[tokio::test]
    async fn requested_listener_drain_refuses_new_peers_but_keeps_established_frames() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (shutdown, requested) = crate::shutdown::channel();
        let listener_task = spawn_bound_listener_controlled(listener, tx, Some(requested), None);
        let mut peer = TcpStream::connect(address).await.unwrap();
        let before_shutdown = all_variants().remove(0);
        peer.write_all(&encode_frame(2, &before_shutdown).unwrap())
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            (2, before_shutdown),
            "the first stream must be accepted before requesting shutdown"
        );

        shutdown.request();
        // Winsock reported refusal about two seconds after a sub-millisecond
        // socket close in the diagnostic run. Require an actual refusal, not a
        // timeout, without confusing OS connect completion with listener drain.
        timeout(Duration::from_secs(5), async {
            loop {
                match TcpStream::connect(address).await {
                    Ok(stream) => drop(stream),
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
                    // A connect racing the close can enter Linux's pending
                    // accept queue and be reset when the listener is dropped.
                    // Retry: only a subsequent refusal proves accepts stopped.
                    Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
                    Err(error) => panic!("unexpected post-shutdown connection error: {error}"),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("shutdown drops the listener socket");
        assert!(
            !listener_task.is_finished(),
            "a shutdown request must not report listener completion while the writer drains"
        );

        let during_drain = all_variants().remove(1);
        peer.write_all(&encode_frame(2, &during_drain).unwrap())
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            (2, during_drain),
            "established peers must still deliver during writer drain"
        );

        drop(rx);
        timeout(Duration::from_secs(1), listener_task)
            .await
            .expect("driver input closure completes listener drain")
            .unwrap();
        let eof = timeout(Duration::from_secs(1), peer.read_u8())
            .await
            .expect("driver completion closes established peer streams")
            .unwrap_err();
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn frame_write_preserves_slow_continuous_progress() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let payload = vec![0x42; 1024];
        let expected = payload.clone();
        let drain = tokio::spawn(async move {
            let mut observed = Vec::new();
            let mut chunk = [0; 64];
            while observed.len() < expected.len() {
                tokio::time::sleep(Duration::from_millis(30)).await;
                let count = reader.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0, "writer closed before delivering its entire frame");
                observed.extend_from_slice(&chunk[..count]);
            }
            assert_eq!(observed, expected);
        });
        let within = Duration::from_millis(200);
        let started = Instant::now();
        write_frame(&mut writer, &payload, within).await.unwrap();
        assert!(
            started.elapsed() > within,
            "fixture must exceed a whole-frame deadline while making steady progress"
        );
        drain.await.unwrap();
    }

    #[tokio::test]
    async fn swapped_peer_dns_addresses_reconnect_with_atomic_joint_refresh() {
        let first_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let first = first_listener.local_addr().unwrap();
        let second_listener = TcpListener::bind(("127.0.0.2", first.port()))
            .await
            .unwrap();
        let second = second_listener.local_addr().unwrap();
        let resolver = Arc::new(PeerMapResolver::new(first, second));
        let transport = start_configured(two_peer_config(first.port()), resolver.clone()).await;
        let mut old_first = accept_peer(&first_listener).await;
        let mut old_second = accept_peer(&second_listener).await;
        resolver.answer("changing.test", Ok(vec![second]));
        resolver.answer("unrelated.test", Ok(vec![first]));
        old_first.shutdown().await.unwrap();
        old_second.shutdown().await.unwrap();
        drop((old_first, old_second));

        let mut peer_three = accept_peer(&first_listener).await;
        let mut peer_two = accept_peer(&second_listener).await;
        let message_two = RaftMessage::RequestVoteReply {
            term: 102,
            vote_granted: false,
        };
        let message_three = RaftMessage::RequestVoteReply {
            term: 103,
            vote_granted: false,
        };
        expect_frame_for(&mut peer_two, &transport, 2, &message_two).await;
        expect_frame_for(&mut peer_three, &transport, 3, &message_three).await;
    }

    #[tokio::test]
    async fn accepted_connection_limit_evicts_oldest_without_an_idle_read_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let listener_task = spawn_bound_listener(listener, tx);
        let mut peers = Vec::new();
        for sequence in 0..=MAX_INBOUND_CONNECTIONS {
            let mut peer = TcpStream::connect(address).await.unwrap();
            let message = RaftMessage::RequestVoteReply {
                term: sequence as u64,
                vote_granted: false,
            };
            peer.write_all(&encode_frame(2, &message).unwrap())
                .await
                .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                (2, message),
                "frame proves the new connection was accepted"
            );
            peers.push(peer);
        }
        timeout(
            Duration::from_secs(1),
            peers[0].read_to_end(&mut Vec::new()),
        )
        .await
        .expect("oldest accepted stream is closed at capacity")
        .unwrap();
        // The next-oldest idle stream stays usable; no application idle timeout
        // is used because follower-to-follower links can legitimately be quiet.
        let message = RaftMessage::RequestVoteReply {
            term: 1000,
            vote_granted: false,
        };
        peers[1]
            .write_all(&encode_frame(2, &message).unwrap())
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            (2, message)
        );
        drop(rx);
        timeout(Duration::from_secs(1), listener_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn peer_keepalive_is_enabled_without_requiring_application_traffic() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        enable_peer_keepalive(&server).unwrap();
        assert!(SockRef::from(&server).keepalive().unwrap());
        drop(client);
    }
}
