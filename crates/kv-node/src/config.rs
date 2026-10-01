//! Validated bind/advertised endpoints and bounded, injectable DNS resolution.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use clap::builder::TypedValueParser;
use clap::Parser;
use raft_core::NodeId;
use tokio::sync::{oneshot, Semaphore};

pub const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
pub const RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const STARTUP_DNS_RETRY_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_DNS_ADDRESSES: usize = 16;
pub const MAX_DNS_WORKERS: usize = 4;

/// Raw numeric bind address; validated before storage/listener initialization.
#[derive(Clone, Debug)]
pub struct ListenEndpoint(pub String);

/// Raw reachable endpoint, kept separate from the bind address.
#[derive(Clone, Debug)]
pub struct AdvertisedEndpoint(pub String);

/// A numeric IP or canonical ASCII hostname and nonzero port.
///
/// Hostnames remain names after resolution so reconnects can observe DNS changes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.contains('%') {
            return Err(format!(
                "endpoint {value:?}: IPv6 scope identifiers are unsupported"
            ));
        }
        if let Ok(address) = value.parse::<SocketAddr>() {
            check_port(address.port())?;
            return Ok(Self {
                host: canonical_ip(address.ip()).to_string(),
                port: address.port(),
            });
        }
        let (host, port) = value
            .rsplit_once(':')
            .ok_or_else(|| format!("endpoint {value:?} must be host:port (bracket IPv6)"))?;
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("endpoint {value:?} has an invalid port"));
        }
        let port = port
            .parse::<u16>()
            .map_err(|error| format!("endpoint {value:?} port: {error}"))?;
        check_port(port)?;
        // A terminal dot requests an absolute DNS name: preserve it for lookup.
        let labels = host.strip_suffix('.').unwrap_or(host);
        // Some OS resolvers accept short, octal, hex, or integer IPv4 forms
        // rejected by Rust. Never reinterpret an ambiguous numeric spelling as DNS.
        let decimal = |label: &str| !label.is_empty() && label.bytes().all(|b| b.is_ascii_digit());
        let numeric_label = |label: &str| {
            decimal(label)
                || label
                    .strip_prefix("0x")
                    .or_else(|| label.strip_prefix("0X"))
                    .is_some_and(|digits| {
                        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_hexdigit())
                    })
        };
        if labels.rsplit('.').next().is_some_and(decimal)
            || (!labels.is_empty() && labels.split('.').all(numeric_label))
        {
            return Err(format!(
                "endpoint {value:?}: use canonical dotted-decimal IPv4, not numeric-looking DNS"
            ));
        }
        if labels.is_empty()
            || labels.len() > 253
            || labels.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || !label.as_bytes()[0].is_ascii_alphanumeric()
                    || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            return Err(format!(
                "endpoint {value:?} needs an ASCII hostname or numeric IP (bracket IPv6)"
            ));
        }
        Ok(Self {
            host: host.to_ascii_lowercase(),
            port,
        })
    }

    fn numeric(&self) -> Option<SocketAddr> {
        self.host
            .parse::<IpAddr>()
            .ok()
            .map(|ip| SocketAddr::new(canonical_ip(ip), self.port))
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSpec {
    pub id: NodeId,
    pub endpoint: Endpoint,
}

pub fn parse_peer(value: &str) -> Result<PeerSpec, String> {
    let (id, endpoint) = value
        .split_once('@')
        .ok_or_else(|| format!("peer {value:?} must be id@host:port"))?;
    let id: NodeId = id
        .parse()
        .map_err(|error| format!("peer id {id:?}: {error}"))?;
    Ok(PeerSpec {
        id,
        endpoint: Endpoint::parse(endpoint)?,
    })
}

/// One member of a Raft-replicated key-value store.
#[derive(Debug, Parser)]
#[command(name = "kv-node", version)]
pub struct Config {
    /// This node's id.
    #[arg(long)]
    pub id: NodeId,

    /// Immutable explicit Raft group identity; required for membership changes.
    #[arg(long)]
    pub group_id: Option<String>,
    /// Canonical initial voters, independent of current network routes.
    #[arg(long, value_delimiter = ',')]
    pub genesis_voters: Vec<NodeId>,
    /// Deliberately adopt an existing fixed legacy directory into this group.
    #[arg(long)]
    pub migrate_legacy_group: bool,

    /// Other cluster members, comma-separated: id@host:port,...
    #[arg(long, value_delimiter = ',', value_parser = parse_peer, required_unless_present = "group_id")]
    pub peers: Vec<PeerSpec>,

    /// Directory containing this node's durable state.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Legacy local client HTTP port; requires --raft-port.
    #[arg(long, required_unless_present_any = ["raft_listen", "raft_advertise", "http_listen", "http_advertise"])]
    pub http_port: Option<u16>,

    /// Legacy local Raft TCP port; requires --http-port.
    #[arg(long, required_unless_present_any = ["raft_listen", "raft_advertise", "http_listen", "http_advertise"])]
    pub raft_port: Option<u16>,

    /// Numeric peer bind IP:port (IPv6 must be bracketed).
    #[arg(long, value_parser = clap::builder::StringValueParser::new().map(ListenEndpoint))]
    pub raft_listen: Option<ListenEndpoint>,

    /// Reachable peer hostname/IP:port; never a wildcard or loopback.
    #[arg(long, value_parser = clap::builder::StringValueParser::new().map(AdvertisedEndpoint))]
    pub raft_advertise: Option<AdvertisedEndpoint>,

    /// Numeric HTTP bind IP:port (IPv6 must be bracketed).
    #[arg(long, value_parser = clap::builder::StringValueParser::new().map(ListenEndpoint))]
    pub http_listen: Option<ListenEndpoint>,

    /// Reachable HTTP hostname/IP:port; never a wildcard or loopback.
    #[arg(long, value_parser = clap::builder::StringValueParser::new().map(AdvertisedEndpoint))]
    pub http_advertise: Option<AdvertisedEndpoint>,

    /// Print the resolved configuration and exit before opening durable state.
    #[arg(long)]
    pub check_config: bool,

    /// Applied entries between local snapshots; zero disables automatic capture.
    #[arg(long, default_value_t = 256)]
    pub snapshot_threshold: u64,
    /// Maximum proposals per WAL append barrier (1..=16).
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u8).range(1..=16))]
    pub batch_records: u8,
    /// Maximum wait for a sparse proposal batch, in milliseconds (0..=10).
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(0..=10))]
    pub batch_delay_ms: u64,
    /// Applied-state backend; persistent backends require a fresh node directory.
    #[arg(long, value_enum, default_value = "memory")]
    pub state_backend: crate::applied_store::StateBackend,
}

#[derive(Clone, Debug)]
pub struct ValidatedConfig {
    pub id: NodeId,
    pub group_id: Option<String>,
    pub genesis_voters: Vec<NodeId>,
    pub migrate_legacy_group: bool,
    pub data_dir: PathBuf,
    pub raft_listen: SocketAddr,
    pub http_listen: SocketAddr,
    pub raft_advertise: Endpoint,
    pub http_advertise: Endpoint,
    pub peers: Vec<PeerSpec>,
    pub check_config: bool,
    pub snapshot_threshold: u64,
    pub batch_records: u8,
    pub batch_delay_ms: u64,
    pub state_backend: crate::applied_store::StateBackend,
    pub legacy: bool,
}

/// All candidates passed policy and alias checks in the same DNS snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedConfig {
    pub peer_addrs: Vec<(NodeId, Vec<SocketAddr>)>,
    pub raft_advertise: Vec<SocketAddr>,
    pub http_advertise: Vec<SocketAddr>,
}

/// Only a collision with accepted cached addresses permits a wider DNS refresh.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerRefreshError {
    Invalid(String),
    CachedConflict(String),
    StaleSnapshot,
}

impl fmt::Display for PeerRefreshError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::CachedConflict(message) => {
                write!(formatter, "cached address conflict: {message}")
            }
            Self::StaleSnapshot => formatter.write_str(
                "accepted endpoint snapshot changed during DNS fallback; retry required",
            ),
        }
    }
}

impl std::error::Error for PeerRefreshError {}

fn check_port(port: u16) -> Result<(), String> {
    if port == 0 {
        Err("endpoint port must be nonzero".into())
    } else {
        Ok(())
    }
}

fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        IpAddr::V4(ip) => IpAddr::V4(ip),
    }
}

fn canonical_address(address: SocketAddr) -> SocketAddr {
    SocketAddr::new(canonical_ip(address.ip()), address.port())
}

fn check_destination(address: SocketAddr, legacy: bool, label: &str) -> Result<(), String> {
    if matches!(address, SocketAddr::V6(ip) if ip.scope_id() != 0) {
        return Err(format!("{label}: scoped IPv6 candidates are unsupported"));
    }
    check_port(address.port())?;
    let ip = canonical_ip(address.ip());
    if ip.is_unspecified() || ip.is_multicast() || ip == IpAddr::V4(Ipv4Addr::BROADCAST) {
        return Err(format!("{label}: {address} is not a unicast destination"));
    }
    if !legacy && ip.is_loopback() {
        return Err(format!(
            "{label}: loopback destination {address} is forbidden in explicit endpoint mode"
        ));
    }
    Ok(())
}

fn parse_listen(value: &str, label: &str) -> Result<SocketAddr, String> {
    if value.contains('%') {
        return Err(format!("{label}: IPv6 scope identifiers are unsupported"));
    }
    let address: SocketAddr = value
        .parse()
        .map_err(|error| format!("{label} requires numeric IP:port (bracket IPv6): {error}"))?;
    check_port(address.port())?;
    let ip = canonical_ip(address.ip());
    if ip.is_multicast() || ip == IpAddr::V4(Ipv4Addr::BROADCAST) {
        return Err(format!(
            "{label}: {address} is not a valid unicast/wildcard bind"
        ));
    }
    Ok(canonical_address(address))
}

fn listen_overlap(left: SocketAddr, right: SocketAddr) -> bool {
    let left = canonical_address(left);
    let right = canonical_address(right);
    left.port() == right.port()
        && (left.ip() == right.ip()
            || (left.is_ipv6() && left.ip().is_unspecified())
            || (right.is_ipv6() && right.ip().is_unspecified())
            || (left.is_ipv4() == right.is_ipv4()
                && (left.ip().is_unspecified() || right.ip().is_unspecified())))
}

pub fn validate(cfg: &Config) -> Result<ValidatedConfig, String> {
    if cfg.batch_records == 0 || cfg.batch_records > 16 || cfg.batch_delay_ms > 10 {
        return Err("batch records must be 1..=16 and delay at most 10ms".into());
    }
    let explicit = cfg.raft_listen.is_some()
        || cfg.raft_advertise.is_some()
        || cfg.http_listen.is_some()
        || cfg.http_advertise.is_some();
    let (raft_listen, http_listen, raft_advertise, http_advertise) = if explicit {
        if cfg.raft_port.is_some() || cfg.http_port.is_some() {
            return Err(
                "cannot mix legacy --raft-port/--http-port with explicit endpoint flags".into(),
            );
        }
        let (Some(raft_listen), Some(raft_advertise), Some(http_listen), Some(http_advertise)) = (
            &cfg.raft_listen,
            &cfg.raft_advertise,
            &cfg.http_listen,
            &cfg.http_advertise,
        ) else {
            return Err(
                "explicit mode requires all four --raft-listen, --raft-advertise, --http-listen, --http-advertise flags"
                    .into(),
            );
        };
        (
            parse_listen(&raft_listen.0, "--raft-listen")?,
            parse_listen(&http_listen.0, "--http-listen")?,
            Endpoint::parse(&raft_advertise.0)?,
            Endpoint::parse(&http_advertise.0)?,
        )
    } else {
        let (Some(raft_port), Some(http_port)) = (cfg.raft_port, cfg.http_port) else {
            return Err("legacy configuration requires --http-port and --raft-port".into());
        };
        check_port(raft_port)?;
        check_port(http_port)?;
        let raft_listen = SocketAddr::from(([127, 0, 0, 1], raft_port));
        let http_listen = SocketAddr::from(([127, 0, 0, 1], http_port));
        (
            raft_listen,
            http_listen,
            Endpoint::parse(&raft_listen.to_string())?,
            Endpoint::parse(&http_listen.to_string())?,
        )
    };
    if listen_overlap(raft_listen, http_listen) {
        return Err("Raft and HTTP listen endpoints overlap".into());
    }
    if cfg.peers.is_empty() && cfg.group_id.is_none() {
        return Err("--peers must contain at least one other node".into());
    }
    if cfg.group_id.is_some() == cfg.genesis_voters.is_empty() {
        return Err("--group-id and --genesis-voters must be supplied together".into());
    }
    if cfg.migrate_legacy_group && cfg.group_id.is_none() {
        return Err("--migrate-legacy-group requires an explicit group identity".into());
    }
    if let Some(group) = &cfg.group_id {
        if group == raft_core::membership::LEGACY_GROUP_ID || group == "legacy" {
            return Err("explicit group cannot use a reserved legacy identity".into());
        }
        raft_core::membership::CommittedMembership::bootstrap_with_group(
            group.clone(),
            cfg.genesis_voters.clone(),
        )?;
    }
    let validated = ValidatedConfig {
        id: cfg.id,
        group_id: cfg.group_id.clone(),
        genesis_voters: cfg.genesis_voters.clone(),
        migrate_legacy_group: cfg.migrate_legacy_group,
        data_dir: cfg.data_dir.clone(),
        raft_listen,
        http_listen,
        raft_advertise,
        http_advertise,
        peers: cfg.peers.clone(),
        check_config: cfg.check_config,
        snapshot_threshold: cfg.snapshot_threshold,
        batch_records: cfg.batch_records,
        batch_delay_ms: cfg.batch_delay_ms,
        state_backend: cfg.state_backend,
        legacy: !explicit,
    };
    if validated.raft_advertise == validated.http_advertise {
        return Err("Raft and HTTP advertisements overlap".into());
    }
    validated.check_service_collisions(
        &validated
            .raft_advertise
            .numeric()
            .into_iter()
            .collect::<Vec<_>>(),
        &validated
            .http_advertise
            .numeric()
            .into_iter()
            .collect::<Vec<_>>(),
    )?;
    let mut ids = HashSet::new();
    let mut endpoints = HashSet::new();
    let mut numeric_peers = HashSet::new();
    let mut local = validated.concrete_listeners();
    for (label, endpoint) in [
        ("--raft-advertise", &validated.raft_advertise),
        ("--http-advertise", &validated.http_advertise),
    ] {
        if let Some(address) = endpoint.numeric() {
            check_destination(address, validated.legacy, label)?;
            local.insert(canonical_address(address));
        }
    }
    for peer in &validated.peers {
        if peer.id == validated.id {
            return Err(format!("--peers contains this node's id {}", peer.id));
        }
        if !ids.insert(peer.id) {
            return Err(format!("--peers contains duplicate node id {}", peer.id));
        }
        if !endpoints.insert(peer.endpoint.clone()) {
            return Err(format!(
                "peer {} has duplicate destination {}",
                peer.id, peer.endpoint
            ));
        }
        if peer.endpoint == validated.raft_advertise || peer.endpoint == validated.http_advertise {
            return Err(format!(
                "peer {} destination {} aliases this node",
                peer.id, peer.endpoint
            ));
        }
        if let Some(address) = peer.endpoint.numeric() {
            check_destination(address, validated.legacy, &format!("peer {}", peer.id))?;
            let address = canonical_address(address);
            if local.contains(&address) {
                return Err(format!(
                    "peer {} destination {address} aliases this node",
                    peer.id
                ));
            }
            if !numeric_peers.insert(address) {
                return Err(format!(
                    "peer {} has duplicate destination {address}",
                    peer.id
                ));
            }
        }
    }
    Ok(validated)
}

pub type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, String>> + Send + 'a>>;

/// Implementations return every candidate; policy filtering is not a resolver job.
pub trait Resolver: Send + Sync {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a>;
}

/// Retry lookup failures only. A successful answer is returned immediately so
/// destination, alias, and result-count policies remain fail-fast at the caller.
/// The outer snapshot deadline includes every attempt and retry sleep.
struct StartupResolver<'a> {
    inner: &'a dyn Resolver,
    lookup_timeout: Duration,
    retry_interval: Duration,
}

impl Resolver for StartupResolver<'_> {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        Box::pin(async move {
            loop {
                let failure =
                    match tokio::time::timeout(self.lookup_timeout, self.inner.resolve(host, port))
                        .await
                    {
                        Ok(Ok(addresses)) => return Ok(addresses),
                        Ok(Err(error)) => error,
                        Err(_) => "DNS lookup timed out".into(),
                    };
                tracing::debug!(host, port, error = %failure, "startup DNS retry within snapshot deadline");
                tokio::time::sleep(self.retry_interval).await;
            }
        })
    }
}

#[derive(Debug, Default)]
pub struct SystemResolver;

static DNS_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static DNS_IN_FLIGHT: OnceLock<Arc<Mutex<HashSet<String>>>> = OnceLock::new();

struct LookupFlight {
    host: String,
    active: Arc<Mutex<HashSet<String>>>,
}

impl Drop for LookupFlight {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.host);
    }
}

/// A canceled caller leaves its hostname reserved until actual OS work finishes.
/// Cancellation while waiting for capacity instead drops the unstarted lookup.
async fn singleflight_lookup<F>(
    slots: Arc<Semaphore>,
    active: Arc<Mutex<HashSet<String>>>,
    host: String,
    lookup: F,
) -> Result<Vec<SocketAddr>, String>
where
    F: FnOnce() -> Result<Vec<SocketAddr>, String> + Send + 'static,
{
    let host = host.to_ascii_lowercase();
    if !active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(host.clone())
    {
        return Err(format!("DNS lookup for {host} is already in progress"));
    }
    let flight = LookupFlight { host, active };
    threaded_lookup(slots, move || {
        let result = lookup();
        // Release the name before publishing the result so sequential queries
        // for different ports on the same host cannot observe a completed flight.
        drop(flight);
        result
    })
    .await
}

/// The permit belongs to the actual OS lookup, even after its caller times out.
/// Dedicated threads avoid making Tokio runtime shutdown wait for uncancelable DNS.
async fn threaded_lookup<F>(slots: Arc<Semaphore>, lookup: F) -> Result<Vec<SocketAddr>, String>
where
    F: FnOnce() -> Result<Vec<SocketAddr>, String> + Send + 'static,
{
    let permit = slots
        .acquire_owned()
        .await
        .map_err(|_| "DNS worker pool is closed".to_string())?;
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name("endpoint-dns".into())
        .spawn(move || {
            let _permit = permit;
            let result = lookup();
            let _ = sender.send(result);
        })
        .map_err(|error| format!("cannot start DNS worker: {error}"))?;
    receiver
        .await
        .map_err(|_| "DNS worker stopped without a result".to_string())?
}

impl Resolver for SystemResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        Box::pin(async move {
            let host = host.to_owned();
            let slots = DNS_SLOTS
                .get_or_init(|| Arc::new(Semaphore::new(MAX_DNS_WORKERS)))
                .clone();
            let active = DNS_IN_FLIGHT
                .get_or_init(|| Arc::new(Mutex::new(HashSet::new())))
                .clone();
            singleflight_lookup(slots, active, host.clone(), move || {
                let addresses = (host.as_str(), port)
                    .to_socket_addrs()
                    .map_err(|error| format!("DNS lookup for {host}:{port} failed: {error}"))?;
                // Keep one overflow marker as a successful answer. Candidate
                // count is a policy error, never a transient lookup to retry.
                Ok(addresses.take(MAX_DNS_ADDRESSES + 1).collect())
            })
            .await
        })
    }
}

impl ValidatedConfig {
    // Advertisements may map ports through a separate network namespace, but
    // cannot knowingly point at this node's other protocol endpoint.
    fn check_service_collisions(
        &self,
        raft_advertise: &[SocketAddr],
        http_advertise: &[SocketAddr],
    ) -> Result<(), String> {
        let raft: HashSet<_> = raft_advertise
            .iter()
            .copied()
            .map(canonical_address)
            .collect();
        let http: HashSet<_> = http_advertise
            .iter()
            .copied()
            .map(canonical_address)
            .collect();
        if !raft.is_disjoint(&http) {
            return Err("Raft and HTTP advertisements overlap".into());
        }
        if (!canonical_ip(self.http_listen.ip()).is_unspecified()
            && raft.contains(&canonical_address(self.http_listen)))
            || (!canonical_ip(self.raft_listen.ip()).is_unspecified()
                && http.contains(&canonical_address(self.raft_listen)))
        {
            return Err("advertisement aliases this node's other protocol listener".into());
        }
        Ok(())
    }

    fn concrete_listeners(&self) -> HashSet<SocketAddr> {
        [self.raft_listen, self.http_listen]
            .into_iter()
            .filter(|address| !canonical_ip(address.ip()).is_unspecified())
            .map(canonical_address)
            .collect()
    }

    /// Administrative endpoints are validated before a configuration proposal.
    /// DNS failures remain explicit rejection; they never grant voting authority.
    pub async fn validate_member_endpoints(
        &self,
        id: NodeId,
        endpoints: &raft_core::membership::MemberEndpoints,
        resolver: &dyn Resolver,
        accepted: &ResolvedConfig,
        membership: &raft_core::membership::MembershipState,
    ) -> Result<(), String> {
        endpoints.validate()?;
        if id == self.id {
            return Err("learner endpoint aliases this node id".into());
        }
        let raft = Endpoint::parse(&endpoints.raft)?;
        let http = Endpoint::parse(&endpoints.http)?;
        if raft == http {
            return Err("learner Raft/HTTP endpoints overlap".into());
        }
        let deadline = tokio::time::Instant::now() + RESOLUTION_TIMEOUT;
        let raft_addresses = self
            .resolve_endpoint(
                &raft,
                "learner Raft",
                resolver,
                DNS_LOOKUP_TIMEOUT,
                deadline,
            )
            .await?;
        let http_addresses = self
            .resolve_endpoint(
                &http,
                "learner HTTP",
                resolver,
                DNS_LOOKUP_TIMEOUT,
                deadline,
            )
            .await?;
        let mut local = self.concrete_listeners();
        local.extend(
            self.resolve_endpoint(
                &self.raft_advertise,
                "local Raft",
                resolver,
                DNS_LOOKUP_TIMEOUT,
                deadline,
            )
            .await?,
        );
        local.extend(
            self.resolve_endpoint(
                &self.http_advertise,
                "local HTTP",
                resolver,
                DNS_LOOKUP_TIMEOUT,
                deadline,
            )
            .await?,
        );
        let active: HashSet<_> = accepted
            .peer_addrs
            .iter()
            .filter(|(peer, _)| *peer != id)
            .flat_map(|(_, addresses)| addresses.iter().copied())
            .collect();
        let mut service_addresses = active;
        for (&peer, endpoints) in &membership.endpoints {
            if peer == id || membership.retired.contains(&peer) {
                continue;
            }
            for (label, endpoint) in [
                ("active member Raft", &endpoints.raft),
                ("active member HTTP", &endpoints.http),
            ] {
                let endpoint = Endpoint::parse(endpoint)?;
                service_addresses.extend(
                    self.resolve_endpoint(&endpoint, label, resolver, DNS_LOOKUP_TIMEOUT, deadline)
                        .await?,
                );
            }
        }
        let mut seen = HashSet::new();
        for address in raft_addresses.into_iter().chain(http_addresses) {
            if local.contains(&address)
                || service_addresses.contains(&address)
                || !seen.insert(address)
            {
                return Err("learner destination aliases a service or active peer".into());
            }
        }
        Ok(())
    }

    /// Resolve a complete snapshot once, including a conflict-refresh fallback.
    /// Ordinary reconnect behavior does not include startup DNS retries.
    pub async fn resolve(&self, resolver: &dyn Resolver) -> Result<ResolvedConfig, String> {
        self.resolve_with_limits(resolver, DNS_LOOKUP_TIMEOUT, RESOLUTION_TIMEOUT)
            .await
    }

    /// Resolve startup before storage/listeners, tolerating transient DNS failure.
    /// Every actual lookup remains bounded by two seconds; retries and all
    /// endpoints share the existing five-second complete-snapshot budget.
    pub async fn resolve_startup(&self, resolver: &dyn Resolver) -> Result<ResolvedConfig, String> {
        self.resolve_startup_with_limits(
            resolver,
            DNS_LOOKUP_TIMEOUT,
            RESOLUTION_TIMEOUT,
            STARTUP_DNS_RETRY_INTERVAL,
        )
        .await
    }

    async fn resolve_startup_with_limits(
        &self,
        resolver: &dyn Resolver,
        lookup_timeout: Duration,
        total_timeout: Duration,
        retry_interval: Duration,
    ) -> Result<ResolvedConfig, String> {
        let startup = StartupResolver {
            inner: resolver,
            lookup_timeout,
            retry_interval,
        };
        // The adapter bounds individual attempts. The endpoint wrapper uses
        // the total budget so it does not cut a retry sequence off after 2s.
        self.resolve_with_limits(&startup, total_timeout, total_timeout)
            .await
    }

    /// Refresh this node's advertisements and one reconnecting peer only.
    ///
    /// The result must pass accept_peer_refresh against the shared accepted
    /// snapshot before dialing. Unrelated failed peer DNS is not consulted.
    pub async fn resolve_peer(
        &self,
        peer_id: NodeId,
        resolver: &dyn Resolver,
    ) -> Result<ResolvedConfig, String> {
        self.resolve_selected(
            Some(peer_id),
            resolver,
            DNS_LOOKUP_TIMEOUT,
            RESOLUTION_TIMEOUT,
        )
        .await
    }

    /// Validate and atomically replace a single peer in the accepted snapshot.
    /// Callers serialize this operation, but never hold that lock across DNS.
    pub fn accept_peer_refresh(
        &self,
        cache: &mut ResolvedConfig,
        fresh: ResolvedConfig,
    ) -> Result<Vec<SocketAddr>, PeerRefreshError> {
        if fresh.peer_addrs.len() != 1 {
            return Err(PeerRefreshError::Invalid(
                "a peer refresh must contain exactly one configured peer".into(),
            ));
        }
        self.validate_snapshot(&fresh, false)
            .map_err(PeerRefreshError::Invalid)?;
        self.validate_snapshot(cache, true)
            .map_err(PeerRefreshError::Invalid)?;
        let (peer_id, addresses) = &fresh.peer_addrs[0];
        let mut merged = cache.clone();
        let (_, previous) = merged
            .peer_addrs
            .iter_mut()
            .find(|(id, _)| id == peer_id)
            .ok_or_else(|| {
                PeerRefreshError::Invalid(format!(
                    "peer {peer_id} is absent from the accepted snapshot"
                ))
            })?;
        *previous = addresses.clone();
        merged.raft_advertise = fresh.raft_advertise;
        merged.http_advertise = fresh.http_advertise;
        // Both inputs are valid individually. Only an interaction with cached
        // peers can invalidate their merge and justify the joint-refresh fallback.
        self.validate_snapshot(&merged, true)
            .map_err(PeerRefreshError::CachedConflict)?;
        let accepted = addresses.clone();
        *cache = merged;
        Ok(accepted)
    }

    /// Publish a complete conflict-resolution snapshot only if no concurrent
    /// accepted address change has occurred since the caller started resolving.
    pub fn accept_snapshot_refresh(
        &self,
        cache: &mut ResolvedConfig,
        expected: &ResolvedConfig,
        fresh: ResolvedConfig,
        peer_id: NodeId,
    ) -> Result<Vec<SocketAddr>, PeerRefreshError> {
        self.validate_snapshot(&fresh, true)
            .map_err(PeerRefreshError::Invalid)?;
        if cache != expected {
            return Err(PeerRefreshError::StaleSnapshot);
        }
        let addresses = fresh
            .peer_addrs
            .iter()
            .find(|(id, _)| *id == peer_id)
            .map(|(_, addresses)| addresses.clone())
            .ok_or_else(|| {
                PeerRefreshError::Invalid(format!("peer {peer_id} is not configured"))
            })?;
        *cache = fresh;
        Ok(addresses)
    }
    async fn resolve_with_limits(
        &self,
        resolver: &dyn Resolver,
        lookup_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<ResolvedConfig, String> {
        self.resolve_selected(None, resolver, lookup_timeout, total_timeout)
            .await
    }

    async fn resolve_selected(
        &self,
        selected_peer: Option<NodeId>,
        resolver: &dyn Resolver,
        lookup_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<ResolvedConfig, String> {
        if let Some(id) = selected_peer {
            if !self.peers.iter().any(|peer| peer.id == id) {
                return Err(format!("peer {id} is not configured"));
            }
        }
        let deadline = tokio::time::Instant::now() + total_timeout;
        let raft_advertise = self
            .resolve_endpoint(
                &self.raft_advertise,
                "--raft-advertise",
                resolver,
                lookup_timeout,
                deadline,
            )
            .await?;
        let http_advertise = self
            .resolve_endpoint(
                &self.http_advertise,
                "--http-advertise",
                resolver,
                lookup_timeout,
                deadline,
            )
            .await?;
        let mut peer_addrs = Vec::new();
        for peer in self
            .peers
            .iter()
            .filter(|peer| selected_peer.is_none_or(|id| peer.id == id))
        {
            let label = format!("peer {} ({})", peer.id, peer.endpoint);
            let addresses = self
                .resolve_endpoint(&peer.endpoint, &label, resolver, lookup_timeout, deadline)
                .await?;
            peer_addrs.push((peer.id, addresses));
        }
        let snapshot = ResolvedConfig {
            peer_addrs,
            raft_advertise,
            http_advertise,
        };
        self.validate_snapshot(&snapshot, selected_peer.is_none())?;
        Ok(snapshot)
    }

    pub(crate) fn validate_snapshot(
        &self,
        snapshot: &ResolvedConfig,
        complete: bool,
    ) -> Result<(), String> {
        self.check_candidates(
            &self.raft_advertise,
            "--raft-advertise",
            &snapshot.raft_advertise,
        )?;
        self.check_candidates(
            &self.http_advertise,
            "--http-advertise",
            &snapshot.http_advertise,
        )?;
        self.check_service_collisions(&snapshot.raft_advertise, &snapshot.http_advertise)?;
        let mut local = self.concrete_listeners();
        local.extend(
            snapshot
                .raft_advertise
                .iter()
                .copied()
                .map(canonical_address),
        );
        local.extend(
            snapshot
                .http_advertise
                .iter()
                .copied()
                .map(canonical_address),
        );
        let mut ids = HashSet::new();
        let mut destinations = HashSet::new();
        for (id, addresses) in &snapshot.peer_addrs {
            let peer = self
                .peers
                .iter()
                .find(|peer| peer.id == *id)
                .ok_or_else(|| format!("peer {id} is not configured"))?;
            if !ids.insert(*id) {
                return Err(format!("duplicate peer {id} in resolved snapshot"));
            }
            let label = format!("peer {id} ({})", peer.endpoint);
            for address in self.check_candidates(&peer.endpoint, &label, addresses)? {
                let address = canonical_address(address);
                if local.contains(&address) {
                    return Err(format!(
                        "{label}: resolved destination {address} aliases this node"
                    ));
                }
                if !destinations.insert(address) {
                    return Err(format!(
                        "{label}: duplicate resolved peer destination {address}"
                    ));
                }
            }
        }
        if complete && ids.len() != self.peers.len() {
            return Err("accepted snapshot does not contain every configured peer".into());
        }
        Ok(())
    }
    async fn resolve_endpoint(
        &self,
        endpoint: &Endpoint,
        label: &str,
        resolver: &dyn Resolver,
        lookup_timeout: Duration,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<SocketAddr>, String> {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{label}: whole endpoint resolution snapshot timed out"
            ));
        }
        let addresses = if let Some(address) = endpoint.numeric() {
            vec![address]
        } else {
            let lookup_deadline = tokio::time::Instant::now() + lookup_timeout;
            let limited_by_total = deadline <= lookup_deadline;
            let query_deadline = deadline.min(lookup_deadline);
            tokio::time::timeout_at(
                query_deadline,
                resolver.resolve(&endpoint.host, endpoint.port),
            )
            .await
            .map_err(|_| {
                if limited_by_total {
                    format!("{label}: whole endpoint resolution snapshot timed out")
                } else {
                    format!("{label}: DNS lookup timed out")
                }
            })?
            .map_err(|error| format!("{label}: {error}"))?
        };
        self.check_candidates(endpoint, label, &addresses)
    }

    fn check_candidates(
        &self,
        endpoint: &Endpoint,
        label: &str,
        addresses: &[SocketAddr],
    ) -> Result<Vec<SocketAddr>, String> {
        if addresses.is_empty() || addresses.len() > MAX_DNS_ADDRESSES {
            return Err(format!(
                "{label}: resolution returned {} candidates; expected 1..={MAX_DNS_ADDRESSES}",
                addresses.len()
            ));
        }
        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        for &address in addresses {
            if address.port() != endpoint.port {
                return Err(format!(
                    "{label}: resolver returned a different port: {address}"
                ));
            }
            check_destination(address, self.legacy, label)?;
            let address = canonical_address(address);
            if seen.insert(address) {
                unique.push(address);
            }
        }
        Ok(unique)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn legacy(peers: &str) -> Config {
        Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--peers",
            peers,
            "--data-dir",
            "unused",
            "--raft-port",
            "7101",
            "--http-port",
            "8101",
        ])
        .expect("legacy syntax")
    }

    fn explicit(peers: &str) -> Config {
        Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--peers",
            peers,
            "--data-dir",
            "unused",
            "--raft-listen",
            "0.0.0.0:7100",
            "--raft-advertise",
            "10.0.0.1:7100",
            "--http-listen",
            "[::]:8100",
            "--http-advertise",
            "[2001:db8::1]:8100",
        ])
        .expect("explicit syntax")
    }

    struct Answers(BTreeMap<&'static str, Vec<IpAddr>>);

    impl Resolver for Answers {
        fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
            Box::pin(async move {
                self.0
                    .get(host)
                    .ok_or_else(|| format!("no fixture for {host}"))
                    .map(|addresses| {
                        addresses
                            .iter()
                            .map(|ip| SocketAddr::new(*ip, port))
                            .collect()
                    })
            })
        }
    }

    fn answers(entries: &[(&'static str, &[&str])]) -> Answers {
        Answers(
            entries
                .iter()
                .map(|(host, ips)| (*host, ips.iter().map(|ip| ip.parse().unwrap()).collect()))
                .collect(),
        )
    }

    struct NeverResolver;

    impl Resolver for NeverResolver {
        fn resolve<'a>(&'a self, _: &'a str, _: u16) -> ResolveFuture<'a> {
            panic!("numeric endpoints must not invoke DNS")
        }
    }

    struct PendingResolver;

    impl Resolver for PendingResolver {
        fn resolve<'a>(&'a self, _: &'a str, _: u16) -> ResolveFuture<'a> {
            Box::pin(std::future::pending())
        }
    }

    struct StartupAnswers {
        calls: std::sync::atomic::AtomicUsize,
        failures: usize,
        first_attempt_pending: bool,
        candidates: Vec<IpAddr>,
    }

    impl Resolver for StartupAnswers {
        fn resolve<'a>(&'a self, _: &'a str, port: u16) -> ResolveFuture<'a> {
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if self.first_attempt_pending && call == 0 {
                    return std::future::pending().await;
                }
                if call < self.failures {
                    return Err("temporary name-resolution failure".into());
                }
                Ok(self
                    .candidates
                    .iter()
                    .map(|ip| SocketAddr::new(*ip, port))
                    .collect())
            })
        }
    }

    fn startup_answers(failures: usize, candidates: &[&str]) -> StartupAnswers {
        StartupAnswers {
            calls: std::sync::atomic::AtomicUsize::new(0),
            failures,
            first_attempt_pending: false,
            candidates: candidates.iter().map(|ip| ip.parse().unwrap()).collect(),
        }
    }

    #[tokio::test]
    async fn startup_retries_transient_dns_then_validates_the_complete_snapshot() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        let resolver = startup_answers(2, &["10.0.0.2"]);
        let snapshot = cfg
            .resolve_startup_with_limits(
                &resolver,
                Duration::from_millis(10),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            snapshot.peer_addrs,
            vec![(2, vec!["10.0.0.2:7100".parse().unwrap()])]
        );
    }

    #[tokio::test]
    async fn startup_retry_after_lookup_timeout_keeps_the_original_total_budget() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        let mut resolver = startup_answers(0, &["10.0.0.2"]);
        resolver.first_attempt_pending = true;
        cfg.resolve_startup_with_limits(
            &resolver,
            Duration::from_millis(5),
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn startup_permanent_lookup_failure_is_bounded_by_the_whole_snapshot() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        let resolver = startup_answers(usize::MAX, &[]);
        let started = tokio::time::Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            cfg.resolve_startup_with_limits(
                &resolver,
                Duration::from_millis(5),
                Duration::from_millis(25),
                Duration::from_millis(2),
            ),
        )
        .await
        .expect("retry loop must remain inside the complete snapshot deadline")
        .unwrap_err();
        assert!(
            error.contains("whole endpoint resolution snapshot timed out"),
            "{error}"
        );
        assert!(started.elapsed() >= Duration::from_millis(25));
        assert!(resolver.calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn startup_successful_invalid_dns_answers_are_never_retried() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        for candidates in [
            vec!["10.0.0.2", "127.0.0.2"],
            vec!["10.0.0.1"],
            vec![],
            vec!["10.0.0.2"; MAX_DNS_ADDRESSES + 1],
        ] {
            let resolver = startup_answers(0, &candidates);
            assert!(cfg.resolve_startup(&resolver).await.is_err());
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn startup_numeric_configuration_bypasses_dns_retry_adapter() {
        let cfg = validate(&explicit("2@10.0.0.2:7100")).unwrap();
        cfg.resolve_startup(&NeverResolver).await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_does_not_inherit_startup_dns_retries() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        let resolver = startup_answers(1, &["10.0.0.2"]);
        let error = cfg.resolve_peer(2, &resolver).await.unwrap_err();
        assert!(error.contains("temporary name-resolution failure"));
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn legacy_numeric_ports_preserve_same_host_peers_without_dns() {
        let mut cfg = legacy("2@127.0.0.1:7102,3@[::1]:7103");
        cfg.id = 0;
        cfg.check_config = true;
        let cfg = validate(&cfg).unwrap();
        assert!(cfg.legacy && cfg.check_config);
        assert_eq!(cfg.raft_listen, "127.0.0.1:7101".parse().unwrap());
        assert_eq!(cfg.http_listen, "127.0.0.1:8101".parse().unwrap());
        let resolved = cfg.resolve(&NeverResolver).await.unwrap();
        assert_eq!(
            resolved.peer_addrs[0].1,
            ["127.0.0.1:7102".parse().unwrap()]
        );
        assert_eq!(resolved.peer_addrs[1].1, ["[::1]:7103".parse().unwrap()]);
    }

    #[test]
    fn flags_are_complete_exclusive_families() {
        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.http_port = Some(8100);
        assert!(validate(&cfg).unwrap_err().contains("cannot mix"));
        cfg.http_port = None;
        cfg.http_advertise = None;
        assert!(validate(&cfg).unwrap_err().contains("all four"));
        let mut cfg = legacy("2@127.0.0.1:7102");
        cfg.raft_port = None;
        assert!(validate(&cfg).unwrap_err().contains("requires"));
    }

    #[test]
    fn hostname_and_ipv6_syntax_is_unambiguous() {
        assert_eq!(
            Endpoint::parse("Node-2.Example.:7100").unwrap().to_string(),
            "node-2.example.:7100"
        );
        assert_eq!(
            Endpoint::parse("[2001:db8::2]:7100").unwrap().to_string(),
            "[2001:db8::2]:7100"
        );
        for value in [
            "2001:db8::2:7100",
            "[::1]",
            "host:0",
            "host:65536",
            "host:+2",
            "host name:7100",
            "a..b:7100",
            "-host:7100",
            "host/:7100",
            "user@host:7100",
            "[fe80::1%3]:7100",
        ] {
            assert!(Endpoint::parse(value).is_err(), "{value}");
        }
        assert!(parse_peer("256@127.0.0.1:7100").is_err());
        assert!(parse_peer("2@@host:7100").is_err());
    }

    #[test]
    fn only_listeners_accept_wildcards_and_bind_ports_do_not_overlap() {
        let cfg = explicit("2@10.0.0.2:7100");
        assert!(validate(&cfg).is_ok());
        for bind in ["host:7100", "0.0.0.0:0", "224.0.0.1:7100", "[ff02::1]:7100"] {
            let mut cfg = explicit("2@10.0.0.2:7100");
            cfg.raft_listen = Some(ListenEndpoint(bind.into()));
            assert!(validate(&cfg).is_err(), "{bind}");
        }
        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.http_listen = Some(ListenEndpoint("127.0.0.1:7100".into()));
        assert!(validate(&cfg).unwrap_err().contains("overlap"));
        let mut cfg = legacy("2@127.0.0.1:7102");
        cfg.raft_port = Some(0);
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn explicit_advertisements_and_peers_reject_nonremote_destinations() {
        for address in [
            "0.0.0.0:7100",
            "[::]:7100",
            "127.2.3.4:7100",
            "[::1]:7100",
            "[::ffff:127.0.0.1]:7100",
            "[::ffff:0.0.0.0]:7100",
            "224.0.0.1:7100",
            "[ff02::1]:7100",
            "255.255.255.255:7100",
        ] {
            let mut cfg = explicit("2@10.0.0.2:7100");
            cfg.raft_advertise = Some(AdvertisedEndpoint(address.into()));
            assert!(validate(&cfg).is_err(), "advertise {address}");
            assert!(
                validate(&explicit(&format!("2@{address}"))).is_err(),
                "peer {address}"
            );
        }
        assert!(validate(&legacy("2@0.0.0.0:7102")).is_err());
    }

    #[test]
    fn duplicate_ids_endpoints_and_self_aliases_fail_before_resolution() {
        for peers in [
            "1@127.0.0.1:7102",
            "2@127.0.0.1:7102,2@127.0.0.1:7103",
            "2@127.0.0.1:7102,3@[::ffff:127.0.0.1]:7102",
            "2@127.0.0.1:7101",
            "2@[::ffff:127.0.0.1]:8101",
            "2@Node.:7102,3@node.:7102",
        ] {
            assert!(validate(&legacy(peers)).is_err(), "{peers}");
        }
    }

    #[tokio::test]
    async fn all_dns_candidates_are_checked_including_mapped_loopback() {
        let cfg = validate(&explicit("2@node2:7100")).unwrap();
        for forbidden in ["127.0.0.1", "::ffff:127.0.0.1", "::", "ff02::1"] {
            let resolver = answers(&[("node2", &["10.0.0.2", forbidden])]);
            let error = cfg.resolve(&resolver).await.unwrap_err();
            assert!(error.contains("peer 2"), "{error}");
        }
        let good = answers(&[("node2", &["10.0.0.2", "2001:db8::2"])]);
        assert_eq!(cfg.resolve(&good).await.unwrap().peer_addrs[0].1.len(), 2);
    }

    #[tokio::test]
    async fn resolved_aliases_detect_self_and_peer_collisions() {
        let cfg = validate(&explicit("2@node2:7100,3@node3:7100")).unwrap();
        let duplicate = answers(&[("node2", &["10.0.0.2"]), ("node3", &["10.0.0.2"])]);
        assert!(cfg
            .resolve(&duplicate)
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        let self_alias = answers(&[("node2", &["10.0.0.1"]), ("node3", &["10.0.0.3"])]);
        assert!(cfg
            .resolve(&self_alias)
            .await
            .unwrap_err()
            .to_string()
            .contains("aliases this node"));

        let mut raw = explicit("2@other-name:7100");
        raw.raft_advertise = Some(AdvertisedEndpoint("my-name:7100".into()));
        let cfg = validate(&raw).unwrap();
        let resolver = answers(&[("my-name", &["10.0.0.8"]), ("other-name", &["10.0.0.8"])]);
        assert!(cfg
            .resolve(&resolver)
            .await
            .unwrap_err()
            .to_string()
            .contains("aliases this node"));
    }

    #[tokio::test]
    async fn reconnect_retains_hostname_and_revalidates_changed_results() {
        let cfg = validate(&legacy("2@node2:7102")).unwrap();
        let first = cfg
            .resolve(&answers(&[("node2", &["127.0.0.2"])]))
            .await
            .unwrap();
        let second = cfg
            .resolve(&answers(&[("node2", &["127.0.0.3"])]))
            .await
            .unwrap();
        assert_ne!(first.peer_addrs, second.peer_addrs);
        assert_eq!(cfg.peers[0].endpoint.host, "node2");
        let error = cfg
            .resolve(&answers(&[("node2", &["0.0.0.0"])]))
            .await
            .unwrap_err();
        assert!(error.contains("not a unicast"));
    }

    #[tokio::test]
    async fn duplicate_dns_records_are_deduplicated_but_results_are_bounded() {
        let cfg = validate(&legacy("2@node2:7102")).unwrap();
        let duplicate = answers(&[("node2", &["127.0.0.2", "::ffff:127.0.0.2"])]);
        assert_eq!(
            cfg.resolve(&duplicate).await.unwrap().peer_addrs[0].1.len(),
            1
        );
        let too_many = Answers(BTreeMap::from([(
            "node2",
            vec!["127.0.0.2".parse().unwrap(); MAX_DNS_ADDRESSES + 1],
        )]));
        assert!(cfg
            .resolve(&too_many)
            .await
            .unwrap_err()
            .contains("candidates"));
        let none = answers(&[("node2", &[])]);
        assert!(cfg
            .resolve(&none)
            .await
            .unwrap_err()
            .contains("0 candidates"));
        let error = cfg.resolve(&answers(&[])).await.unwrap_err();
        assert!(error.contains("peer 2") && error.contains("no fixture"));
    }

    #[tokio::test]
    async fn lookup_and_complete_snapshot_have_separate_deadlines() {
        let cfg = validate(&legacy("2@node2:7102")).unwrap();
        let error = cfg
            .resolve_with_limits(
                &PendingResolver,
                Duration::from_millis(10),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("peer 2") && error.contains("DNS lookup timed out"),
            "{error}"
        );
        let error = cfg
            .resolve_with_limits(
                &PendingResolver,
                Duration::from_secs(1),
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("peer 2") && error.contains("snapshot timed out"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn reconnect_does_not_require_unrelated_peer_dns_and_rejects_cached_collisions() {
        let cfg = validate(&legacy("2@node2:7102,3@node3:7102")).unwrap();
        let initial = answers(&[("node2", &["127.0.0.2"]), ("node3", &["127.0.0.3"])]);
        let mut cache = cfg.resolve(&initial).await.unwrap();
        // This resolver cannot resolve node3: a surviving link must still reconnect.
        let fresh = cfg
            .resolve_peer(2, &answers(&[("node2", &["127.0.0.4"])]))
            .await
            .unwrap();
        let accepted = cfg.accept_peer_refresh(&mut cache, fresh).unwrap();
        assert_eq!(
            accepted,
            vec!["127.0.0.4:7102".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            cache.peer_addrs[1].1,
            vec!["127.0.0.3:7102".parse::<SocketAddr>().unwrap()]
        );

        let before = cache.clone();
        let conflicting = cfg
            .resolve_peer(2, &answers(&[("node2", &["127.0.0.3"])]))
            .await
            .unwrap();
        assert!(cfg
            .accept_peer_refresh(&mut cache, conflicting)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        assert_eq!(cache.peer_addrs, before.peer_addrs);
        assert_eq!(cache.raft_advertise, before.raft_advertise);
        assert_eq!(cache.http_advertise, before.http_advertise);
        assert!(cfg
            .resolve_peer(9, &NeverResolver)
            .await
            .unwrap_err()
            .contains("not configured"));
    }

    #[tokio::test]
    async fn refreshed_own_advertisements_are_checked_against_all_cached_peers() {
        let mut raw = explicit("2@node2:7100,3@node3:7100");
        raw.raft_advertise = Some(AdvertisedEndpoint("self-name:7100".into()));
        let cfg = validate(&raw).unwrap();
        let initial = answers(&[
            ("self-name", &["10.0.0.1"]),
            ("node2", &["10.0.0.2"]),
            ("node3", &["10.0.0.3"]),
        ]);
        let mut cache = cfg.resolve(&initial).await.unwrap();
        let before = cache.clone();
        let fresh = cfg
            .resolve_peer(
                2,
                &answers(&[("self-name", &["10.0.0.3"]), ("node2", &["10.0.0.2"])]),
            )
            .await
            .unwrap();
        assert!(cfg
            .accept_peer_refresh(&mut cache, fresh)
            .unwrap_err()
            .to_string()
            .contains("aliases this node"));
        assert_eq!(cache.peer_addrs, before.peer_addrs);
        assert_eq!(cache.raft_advertise, before.raft_advertise);
    }
    #[tokio::test]
    async fn a_joint_snapshot_can_resolve_a_cached_peer_address_swap() {
        let cfg = validate(&legacy("2@node2:7102,3@node3:7102")).unwrap();
        let initial = answers(&[("node2", &["127.0.0.2"]), ("node3", &["127.0.0.3"])]);
        let swapped = answers(&[("node2", &["127.0.0.3"]), ("node3", &["127.0.0.2"])]);
        let mut cache = cfg.resolve(&initial).await.unwrap();
        let expected = cache.clone();
        let single = cfg.resolve_peer(2, &swapped).await.unwrap();
        assert!(matches!(
            cfg.accept_peer_refresh(&mut cache, single),
            Err(PeerRefreshError::CachedConflict(_))
        ));
        assert_eq!(cache, expected, "the rejected partial update must not leak");
        let joint = cfg.resolve(&swapped).await.unwrap();
        let peer = cfg
            .accept_snapshot_refresh(&mut cache, &expected, joint, 2)
            .unwrap();
        assert_eq!(peer, vec!["127.0.0.3:7102".parse::<SocketAddr>().unwrap()]);
        assert_eq!(
            cache.peer_addrs[1].1,
            vec!["127.0.0.2:7102".parse::<SocketAddr>().unwrap()]
        );
        // The other link can now reconnect independently with the swapped name.
        let single = cfg.resolve_peer(3, &swapped).await.unwrap();
        assert!(cfg.accept_peer_refresh(&mut cache, single).is_ok());
    }

    #[tokio::test]
    async fn a_joint_refresh_cannot_overwrite_a_concurrent_accepted_update() {
        let cfg = validate(&legacy("2@node2:7102,3@node3:7102")).unwrap();
        let initial = answers(&[("node2", &["127.0.0.2"]), ("node3", &["127.0.0.3"])]);
        let mut cache = cfg.resolve(&initial).await.unwrap();
        let expected = cache.clone();
        let joint = cfg
            .resolve(&answers(&[
                ("node2", &["127.0.0.3"]),
                ("node3", &["127.0.0.2"]),
            ]))
            .await
            .unwrap();
        let concurrent = cfg
            .resolve_peer(2, &answers(&[("node2", &["127.0.0.4"])]))
            .await
            .unwrap();
        cfg.accept_peer_refresh(&mut cache, concurrent).unwrap();
        let updated = cache.clone();
        assert_eq!(
            cfg.accept_snapshot_refresh(&mut cache, &expected, joint, 2)
                .unwrap_err(),
            PeerRefreshError::StaleSnapshot,
        );
        assert_eq!(cache, updated);
    }

    #[tokio::test]
    async fn invalid_fresh_results_are_not_classified_as_cached_conflicts() {
        let cfg = validate(&legacy("2@node2:7102,3@node3:7102")).unwrap();
        let resolver = answers(&[("node2", &["127.0.0.2"]), ("node3", &["127.0.0.3"])]);
        let mut cache = cfg.resolve(&resolver).await.unwrap();
        let expected = cache.clone();
        let mut single = cfg.resolve_peer(2, &resolver).await.unwrap();
        single.peer_addrs[0].1[0].set_port(1);
        assert!(matches!(
            cfg.accept_peer_refresh(&mut cache, single),
            Err(PeerRefreshError::Invalid(_))
        ));
        let mut joint = expected.clone();
        joint.peer_addrs[1].1 = joint.peer_addrs[0].1.clone();
        assert!(matches!(
            cfg.accept_snapshot_refresh(&mut cache, &expected, joint, 2),
            Err(PeerRefreshError::Invalid(_))
        ));
        assert_eq!(cache, expected);
    }

    #[tokio::test]
    async fn ambiguous_numeric_spelling_is_rejected_and_mapped_candidates_are_ipv4() {
        for endpoint in [
            "192.168.001.010:7100",
            "127.1:7100",
            "2130706433:7100",
            "0177.0.0.1:7100",
            "0x7f000001:7100",
            "127.0.0.0x1:7100",
            "0X7F.0x1:7100",
            "node.123:7100",
            "127.0.0.1.:7100",
        ] {
            assert!(Endpoint::parse(endpoint).is_err(), "{endpoint}");
        }
        assert!(Endpoint::parse("worker2.example:7100").is_ok());
        assert_eq!(
            Endpoint::parse("[::ffff:10.0.0.2]:7100")
                .unwrap()
                .to_string(),
            "10.0.0.2:7100",
        );
        let mut raw = explicit("2@node2:7100");
        raw.raft_listen = Some(ListenEndpoint("[::ffff:10.0.0.1]:7100".into()));
        let cfg = validate(&raw).unwrap();
        assert_eq!(cfg.raft_listen, "10.0.0.1:7100".parse().unwrap());
        let resolved = cfg
            .resolve(&answers(&[("node2", &["::ffff:10.0.0.2"])]))
            .await
            .unwrap();
        assert_eq!(
            resolved.peer_addrs[0].1,
            vec!["10.0.0.2:7100".parse::<SocketAddr>().unwrap()]
        );
        assert!(matches!(resolved.peer_addrs[0].1[0], SocketAddr::V4(_)));
    }

    #[tokio::test]
    async fn one_stalled_name_cannot_consume_all_os_lookup_slots_on_retries() {
        let slots = Arc::new(Semaphore::new(4));
        let active = Arc::new(Mutex::new(HashSet::new()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_calls = calls.clone();
        let first = tokio::spawn(singleflight_lookup(
            slots.clone(),
            active.clone(),
            "stalled".into(),
            move || {
                first_calls.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                release_rx.recv().map_err(|error| error.to_string())?;
                Ok(vec![])
            },
        ));
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 3);
        for _ in 0..5 {
            let attempted_calls = calls.clone();
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                singleflight_lookup(slots.clone(), active.clone(), "STALLED".into(), move || {
                    attempted_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![])
                }),
            )
            .await
            .expect("duplicate flight fails promptly")
            .unwrap_err();
            assert!(error.contains("already in progress"));
            assert_eq!(slots.available_permits(), 3);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(
            Duration::from_secs(1),
            singleflight_lookup(slots.clone(), active.clone(), "healthy".into(), || {
                Ok(vec![])
            }),
        )
        .await
        .expect("healthy name retains capacity")
        .unwrap();
        let free =
            tokio::time::timeout(Duration::from_secs(1), slots.clone().acquire_many_owned(3))
                .await
                .unwrap()
                .unwrap();
        drop(free);
        release_tx.send(()).unwrap();
        let all = tokio::time::timeout(Duration::from_secs(1), slots.clone().acquire_many_owned(4))
            .await
            .unwrap()
            .unwrap();
        assert!(active.lock().unwrap().is_empty());
        drop(all);
        assert!(
            singleflight_lookup(slots, active, "stalled".into(), || Ok(vec![]))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn canceled_capacity_wait_does_not_reserve_a_name_forever() {
        let slots = Arc::new(Semaphore::new(1));
        let active = Arc::new(Mutex::new(HashSet::new()));
        let held = slots.clone().acquire_owned().await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            singleflight_lookup(slots.clone(), active.clone(), "waiting".into(), || {
                Ok(vec![])
            }),
        )
        .await;
        assert!(result.is_err());
        assert!(active.lock().unwrap().is_empty());
        drop(held);
        assert!(
            singleflight_lookup(slots, active, "waiting".into(), || Ok(vec![]))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn finished_os_error_releases_singleflight_before_returning_to_the_caller() {
        let slots = Arc::new(Semaphore::new(1));
        let active = Arc::new(Mutex::new(HashSet::new()));
        let error = singleflight_lookup(slots.clone(), active.clone(), "same-name".into(), || {
            Err("injected OS lookup failure".into())
        })
        .await
        .unwrap_err();
        assert_eq!(error, "injected OS lookup failure");
        assert!(
            singleflight_lookup(slots, active, "same-name".into(), || Ok(vec![]))
                .await
                .is_ok()
        );
    }
    #[tokio::test]
    async fn absolute_dns_names_retain_the_root_dot_for_the_resolver() {
        let cfg = validate(&legacy("2@Node2.:7102")).unwrap();
        assert_eq!(cfg.peers[0].endpoint.host, "node2.");
        let resolver = answers(&[("node2.", &["127.0.0.2"])]);
        assert!(cfg.resolve(&resolver).await.is_ok());
    }

    #[test]
    fn dual_stack_wildcard_same_port_is_conservatively_rejected() {
        for (raft, http) in [
            ("[::]:7100", "0.0.0.0:7100"),
            ("10.0.0.1:7100", "[::]:7100"),
        ] {
            let mut cfg = explicit("2@10.0.0.2:7100");
            cfg.raft_listen = Some(ListenEndpoint(raft.into()));
            cfg.http_listen = Some(ListenEndpoint(http.into()));
            assert!(validate(&cfg).unwrap_err().contains("overlap"));
        }
    }

    #[tokio::test]
    async fn service_advertisements_cannot_collide_or_alias_the_other_listener() {
        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.http_advertise = Some(AdvertisedEndpoint("[::ffff:10.0.0.1]:7100".into()));
        assert!(validate(&cfg).unwrap_err().contains("overlap"));

        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.raft_listen = Some(ListenEndpoint("10.0.0.1:7100".into()));
        cfg.http_listen = Some(ListenEndpoint("10.0.0.1:8100".into()));
        cfg.raft_advertise = Some(AdvertisedEndpoint("10.0.0.1:8100".into()));
        assert!(validate(&cfg).unwrap_err().contains("other protocol"));

        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.raft_advertise = Some(AdvertisedEndpoint("raft-name:7100".into()));
        cfg.http_advertise = Some(AdvertisedEndpoint("http-name:7100".into()));
        let cfg = validate(&cfg).unwrap();
        let resolver = answers(&[("raft-name", &["10.0.0.1"]), ("http-name", &["10.0.0.1"])]);
        assert!(cfg
            .resolve(&resolver)
            .await
            .unwrap_err()
            .contains("overlap"));

        let mut cfg = explicit("2@10.0.0.2:7100");
        cfg.raft_listen = Some(ListenEndpoint("10.0.0.1:7100".into()));
        // Keep the advertised Raft address distinct to isolate the bind alias.
        cfg.raft_advertise = Some(AdvertisedEndpoint("10.0.0.9:7100".into()));
        cfg.http_advertise = Some(AdvertisedEndpoint("http-name:7100".into()));
        let cfg = validate(&cfg).unwrap();
        let resolver = answers(&[("http-name", &["10.0.0.1"])]);
        assert!(cfg
            .resolve(&resolver)
            .await
            .unwrap_err()
            .contains("other protocol"));
    }
    #[tokio::test]
    async fn canceling_a_caller_keeps_its_actual_dns_worker_slot_occupied() {
        let slots = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = tokio::spawn(threaded_lookup(slots.clone(), move || {
            let _ = started_tx.send(());
            release_rx.recv().map_err(|error| error.to_string())?;
            Ok(vec![])
        }));
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 0);

        let called = Arc::new(AtomicBool::new(false));
        let second_called = called.clone();
        let mut second = tokio::spawn(threaded_lookup(slots.clone(), move || {
            second_called.store(true, Ordering::SeqCst);
            Ok(vec![])
        }));
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err());
        assert!(!called.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(called.load(Ordering::SeqCst));
    }
}
