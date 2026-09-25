//! Command-line entry point for a persistent micro-raft node.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use raft_core::NodeId;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::EnvFilter;

use kv_node::driver::{run_driver, PROPOSAL_CHANNEL_CAPACITY};
use kv_node::http::ApiState;
use kv_node::kv::SharedReadState;
use kv_node::storage::Storage;
use kv_node::transport::{spawn_listener, TcpTransport, INBOUND_QUEUE_CAPACITY};

/// One peer endpoint from `--peers`, for example `2@127.0.0.1:7102`.
#[derive(Clone, Debug)]
struct PeerSpec {
    id: NodeId,
    addr: SocketAddr,
}

fn parse_peer(s: &str) -> Result<PeerSpec, String> {
    let (id, addr) = s
        .split_once('@')
        .ok_or_else(|| format!("peer `{s}` must be `id@host:port`"))?;
    let id: NodeId = id.parse().map_err(|e| format!("peer id `{id}`: {e}"))?;
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| format!("peer addr `{addr}`: {e}"))?;
    Ok(PeerSpec { id, addr })
}

/// One member of a Raft-replicated key-value store.
#[derive(Debug, Parser)]
#[command(name = "kv-node", version)]
struct Config {
    /// This node's id.
    #[arg(long)]
    id: NodeId,

    /// Other cluster members, comma-separated: `id@host:port,...`.
    #[arg(long, value_delimiter = ',', value_parser = parse_peer, required = true)]
    peers: Vec<PeerSpec>,

    /// Directory containing this node's durable state.
    #[arg(long)]
    data_dir: PathBuf,

    /// Local client HTTP API port.
    #[arg(long)]
    http_port: u16,

    /// Local Raft peer-to-peer TCP port.
    #[arg(long)]
    raft_port: u16,
}

fn validate(cfg: &Config) -> Result<(), String> {
    if cfg.peers.iter().any(|peer| peer.id == cfg.id) {
        return Err(format!(
            "--peers must list only other nodes, but contains this node's id {}",
            cfg.id
        ));
    }
    let mut ids: Vec<NodeId> = cfg.peers.iter().map(|peer| peer.id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != cfg.peers.len() {
        return Err("--peers contains duplicate node ids".into());
    }
    Ok(())
}

/// Prefixes each log event so interleaved node output remains attributable.
struct NodePrefix<E> {
    prefix: String,
    inner: E,
}

impl<S, N, E> FormatEvent<S, N> for NodePrefix<E>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    E: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(writer, "{} ", self.prefix)?;
        self.inner.format_event(ctx, writer, event)
    }
}

fn init_tracing(id: NodeId) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(NodePrefix {
            prefix: format!("[n{id}]"),
            inner: tracing_subscriber::fmt::format(),
        })
        .init();
}

#[tokio::main]
async fn main() {
    let cfg = Config::parse();
    init_tracing(cfg.id);
    if let Err(error) = validate(&cfg) {
        tracing::error!("invalid config: {error}");
        std::process::exit(2);
    }
    if let Err(error) = run(cfg).await {
        tracing::error!(%error, "node stopped");
        std::process::exit(1);
    }
}

async fn run(cfg: Config) -> io::Result<()> {
    // Recovery is deliberately completed before either public listener binds.
    let (storage, hard, log) = Storage::open(&cfg.data_dir)?;
    info!(
        id = cfg.id,
        term = hard.current_term,
        recovered_entries = log.len(),
        data_dir = %cfg.data_dir.display(),
        "durable state recovered"
    );

    let peer_addrs: Vec<(NodeId, SocketAddr)> =
        cfg.peers.iter().map(|peer| (peer.id, peer.addr)).collect();
    let peer_ids: Vec<NodeId> = cfg.peers.iter().map(|peer| peer.id).collect();
    for peer in &cfg.peers {
        info!(peer_id = peer.id, peer_addr = %peer.addr, "peer configured");
    }

    let seed: u64 = rand::random();
    let node = raft_core::RaftNode::restore(cfg.id, peer_ids, seed, hard, log)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let shared = SharedReadState::from_node(&node);
    let (proposal_tx, proposal_rx) = tokio::sync::mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);

    let http_listener = TcpListener::bind(("127.0.0.1", cfg.http_port)).await?;
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let mut raft_listener = spawn_listener(cfg.raft_port, inbound_tx).await?;
    let transport = TcpTransport::spawn(cfg.id, &peer_addrs);

    info!(
        seed,
        raft_port = cfg.raft_port,
        http_port = cfg.http_port,
        "node started"
    );

    let api = ApiState::new(shared.clone(), proposal_tx);
    let mut http_task = tokio::spawn(kv_node::http::serve(http_listener, api));
    let driver = run_driver(node, storage, transport, inbound_rx, proposal_rx, shared);
    tokio::pin!(driver);

    tokio::select! {
        result = &mut driver => {
            raft_listener.abort();
            http_task.abort();
            result
        }
        result = &mut raft_listener => {
            http_task.abort();
            match result {
                Ok(()) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Raft listener stopped")),
                Err(error) => Err(io::Error::other(format!("Raft listener failed: {error}"))),
            }
        }
        result = &mut http_task => {
            raft_listener.abort();
            match result {
                Ok(Ok(())) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "HTTP server stopped")),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(io::Error::other(format!("HTTP server failed: {error}"))),
            }
        }
    }
}
