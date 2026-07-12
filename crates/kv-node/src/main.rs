//! micro-raft node binary — M0: parse config, log it, idle.
//!
//! M2: the driver loop (tick → step → execute effects) and TCP transport.
//! M7: the axum HTTP client API.

// M5: storage (and its CRC framing) is wired into the driver's effect
// executor; until then these modules are exercised only by their tests.
#[allow(dead_code)]
mod crc;
#[allow(dead_code)]
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use raft_core::NodeId;
use tracing::info;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::EnvFilter;

/// One peer endpoint from `--peers`, e.g. `2@127.0.0.1:7102`.
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
    let addr: SocketAddr = addr.parse().map_err(|e| format!("peer addr `{addr}`: {e}"))?;
    Ok(PeerSpec { id, addr })
}

/// micro-raft node: one member of a Raft-replicated key-value store.
#[derive(Debug, Parser)]
#[command(name = "kv-node", version)]
struct Config {
    /// This node's id (e.g. 1).
    #[arg(long)]
    id: NodeId,

    /// The OTHER cluster members, comma-separated: `id@host:port,...`.
    #[arg(long, value_delimiter = ',', value_parser = parse_peer, required = true)]
    peers: Vec<PeerSpec>,

    /// Per-node data directory, e.g. `data/n1` (created by storage in M1).
    #[arg(long)]
    data_dir: PathBuf,

    /// Client HTTP API port (served from M7).
    #[arg(long)]
    http_port: u16,

    /// Raft peer-to-peer TCP port (listening from M2).
    #[arg(long)]
    raft_port: u16,
}

fn validate(cfg: &Config) -> Result<(), String> {
    if cfg.peers.iter().any(|p| p.id == cfg.id) {
        return Err(format!(
            "--peers must list only OTHER nodes, but contains this node's id {}",
            cfg.id
        ));
    }
    let mut ids: Vec<NodeId> = cfg.peers.iter().map(|p| p.id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != cfg.peers.len() {
        return Err("--peers contains duplicate node ids".into());
    }
    Ok(())
}

/// Prefixes every log line with `[n<id>]`, then delegates to the default
/// format — three interleaved demo windows stay attributable (§4/M0).
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
    if let Err(e) = validate(&cfg) {
        tracing::error!("invalid config: {e}");
        std::process::exit(2);
    }
    info!(
        id = cfg.id,
        raft_port = cfg.raft_port,
        http_port = cfg.http_port,
        data_dir = %cfg.data_dir.display(),
        "kv-node config parsed"
    );
    for p in &cfg.peers {
        info!(peer_id = p.id, peer_addr = %p.addr, "peer configured");
    }
    // M2: the driver loop replaces this idle.
    info!("scaffold node idling (M0); press Ctrl+C to exit");
    std::future::pending::<()>().await;
}
