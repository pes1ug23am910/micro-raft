//! Group-bound routes and shared HTTP destinations, independent of voting authority.
use crate::topology::Topology;
use crate::types::GroupId;
use kv_node::config::{
    Endpoint, PeerSpec, ResolvedConfig, Resolver, ValidatedConfig, DNS_LOOKUP_TIMEOUT,
    MAX_DNS_ADDRESSES, RESOLUTION_TIMEOUT,
};
use kv_node::membership_runtime::RouteUpdate;
use kv_node::shutdown::ShutdownRx;
use kv_node::transport::TcpTransport;
use raft_core::membership::{AdminOperation, MemberEndpoints, MembershipState};
use raft_core::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn canonical(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    };
    SocketAddr::new(ip, address.port())
}
fn checked_address(address: SocketAddr) -> Result<SocketAddr, String> {
    if matches!(address,SocketAddr::V6(address) if address.scope_id()!=0 || address.flowinfo()!=0) {
        return Err("scoped IPv6 destination is unsupported".into());
    }
    let address = canonical(address);
    if address.port() == 0
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address.ip(),IpAddr::V4(ip) if ip.is_broadcast())
    {
        return Err("unreachable destination address".into());
    }
    Ok(address)
}
async fn resolve_endpoint(
    endpoint: &Endpoint,
    resolver: &dyn Resolver,
    deadline: Instant,
) -> Result<Vec<SocketAddr>, String> {
    if Instant::now() >= deadline {
        return Err("shared resolution budget expired".into());
    }
    let candidates = if let Ok(ip) = endpoint.host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, endpoint.port)]
    } else {
        tokio::time::timeout_at(
            deadline.min(Instant::now() + DNS_LOOKUP_TIMEOUT),
            resolver.resolve(&endpoint.host, endpoint.port),
        )
        .await
        .map_err(|_| "endpoint resolution deadline".to_owned())??
    };
    if candidates.is_empty() || candidates.len() > MAX_DNS_ADDRESSES {
        return Err("empty or excessive DNS candidates".into());
    }
    let mut result = BTreeSet::new();
    for address in candidates {
        if address.port() != endpoint.port {
            return Err("resolver returned another port".into());
        }
        result.insert(checked_address(address)?);
    }
    Ok(result.into_iter().collect())
}
fn endpoints(
    topology: &Topology,
    group: GroupId,
    id: NodeId,
    membership: &MembershipState,
) -> Option<MemberEndpoints> {
    membership.endpoints.get(&id).cloned().or_else(|| {
        topology
            .nodes
            .iter()
            .find(|node| node.id == id)
            .and_then(|node| {
                node.raft.get(&group).map(|raft| MemberEndpoints {
                    raft: raft.to_string(),
                    http: node.http.to_string(),
                })
            })
    })
}
fn same_update(left: &RouteUpdate, right: &RouteUpdate) -> bool {
    left.term == right.term && left.membership == right.membership && left.hints == right.hints
}
struct View {
    authority: watch::Receiver<RouteUpdate>,
    http: BTreeMap<NodeId, Vec<SocketAddr>>,
    http_endpoints: BTreeMap<NodeId, Endpoint>,
    raft: BTreeMap<NodeId, Vec<SocketAddr>>,
    reserved: BTreeSet<NodeId>,
    error: Option<String>,
    resolver: Arc<dyn Resolver>,
    local: NodeId,
}
pub struct RouteRegistry {
    topology: Arc<Topology>,
    views: RwLock<BTreeMap<GroupId, View>>,
}
impl RouteRegistry {
    pub fn new(topology: Arc<Topology>) -> Self {
        Self {
            topology,
            views: RwLock::new(BTreeMap::new()),
        }
    }
    /// Accepted candidate addresses only; callers share one overall RPC deadline.
    pub fn http_routes(&self, group: GroupId) -> Vec<(NodeId, SocketAddr)> {
        // Never hold registry locks while acquiring a watch lock: publication
        // deliberately takes the opposite direction to fence stale DNS.
        let Some((authority, http, accepted_names)) = ({
            let views = self
                .views
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            views.get(&group).map(|view| {
                (
                    view.authority.clone(),
                    view.http.clone(),
                    view.http_endpoints.clone(),
                )
            })
        }) else {
            return Vec::new();
        };
        let authority = authority.borrow();
        let active = authority.membership.participants();
        http.iter()
            .filter(|(id, _)| {
                let hint = authority.hints.get(id).filter(|hint| {
                    hint.term >= authority.term && !authority.membership.retired.contains(id)
                });
                if !active.contains(id) && hint.is_none() {
                    return false;
                }
                let expected = if active.contains(id) {
                    endpoints(&self.topology, group, **id, &authority.membership)
                } else {
                    authority.membership.endpoints.get(id).cloned()
                }
                .or_else(|| hint.map(|hint| hint.endpoints.clone()));
                expected
                    .and_then(|value| Endpoint::parse(&value.http).ok())
                    .as_ref()
                    == accepted_names.get(id)
            })
            .flat_map(|(id, addresses)| addresses.iter().map(move |address| (*id, *address)))
            .collect()
    }

    pub fn errors(&self) -> BTreeMap<GroupId, Option<String>> {
        self.views
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(group, view)| (*group, view.error.clone()))
            .collect()
    }
    /// Validation is a pre-admission proof. The actor must recheck the returned
    /// target revision and the replicated global maintenance ticket before append.
    pub async fn validate_operation(
        &self,
        group: GroupId,
        operation: &AdminOperation,
        membership: &MembershipState,
    ) -> Result<[u8; 32], String> {
        let revision = kv_node::admin::membership_revision(membership);
        let (resolver, local, receivers) = {
            let views = self
                .views
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let view = views.get(&group).ok_or("group routes unavailable")?;
            (
                view.resolver.clone(),
                view.local,
                views
                    .iter()
                    .map(|(group, view)| (*group, view.authority.clone()))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        let authorities: BTreeMap<_, _> = receivers
            .into_iter()
            .map(|(group, receive)| (group, receive.borrow().membership.clone()))
            .collect();
        if authorities.get(&group) != Some(membership) {
            return Err("stale_endpoint_validation".into());
        }
        let AdminOperation::AddLearner {
            id,
            endpoints: added,
        } = operation
        else {
            return Ok(revision);
        };
        added.validate()?;
        let deadline = Instant::now() + RESOLUTION_TIMEOUT;
        let new_raft = Endpoint::parse(&added.raft)?;
        let new_http = Endpoint::parse(&added.http)?;
        let mut cache = HashMap::new();
        let raft = resolve_endpoint(&new_raft, resolver.as_ref(), deadline).await?;
        let http = resolve_endpoint(&new_http, resolver.as_ref(), deadline).await?;
        cache.insert(new_raft, raft.clone());
        cache.insert(new_http, http.clone());
        if raft.iter().any(|address| http.contains(address)) {
            return Err("Raft and HTTP aliases".into());
        }
        for (other_group, state) in authorities {
            for other_id in state.participants() {
                if other_group == group && other_id == *id {
                    continue;
                }
                let declared = endpoints(&self.topology, other_group, other_id, &state)
                    .ok_or_else(|| {
                        format!("group {other_group} peer {other_id}: no declared endpoints")
                    })?;
                for (kind, text) in [("raft", declared.raft), ("http", declared.http)] {
                    let endpoint = Endpoint::parse(&text)?;
                    let addresses = if let Some(addresses) = cache.get(&endpoint) {
                        addresses.clone()
                    } else {
                        let addresses =
                            resolve_endpoint(&endpoint, resolver.as_ref(), deadline).await?;
                        cache.insert(endpoint, addresses.clone());
                        addresses
                    };
                    if raft.iter().any(|address| addresses.contains(address))
                        || ((kind == "raft" || other_id != *id)
                            && http.iter().any(|address| addresses.contains(address)))
                    {
                        return Err(format!(
                            "endpoint aliases group {other_group} node {other_id} {kind}"
                        ));
                    }
                }
            }
        }
        // Passive local listeners still occupy real sockets in every group.
        if let Some(owner) = self.topology.nodes.iter().find(|node| node.id == local) {
            let local_sockets: BTreeSet<_> = std::iter::once(owner.http)
                .chain(owner.raft.values().copied())
                .map(canonical)
                .collect();
            if *id == local {
                // The controller leader may still be passive in the target group.
                // Only its exact target listener and shared HTTP service can be
                // admitted under its own ID; DNS cannot add a second destination.
                let target = owner.raft.get(&group).copied().map(canonical);
                if raft.as_slice() != target.as_slice()
                    || http.as_slice() != [canonical(owner.http)]
                {
                    return Err("local admission must match the exact target listeners".into());
                }
            } else if raft
                .iter()
                .chain(&http)
                .any(|address| local_sockets.contains(address))
            {
                return Err("endpoint aliases a local service listener".into());
            }
        }
        Ok(revision)
    }
}

// This table is built and checked under one registry write lock. Cached other-
// group ownership can conservatively reject a route until that group refreshes;
// simultaneous DNS completions cannot each publish the same cross-ID socket.
fn validate_publication(
    views: &BTreeMap<GroupId, View>,
    group: GroupId,
    reserved: &BTreeSet<NodeId>,
    candidate_raft: &BTreeMap<NodeId, Vec<SocketAddr>>,
    candidate_http: &BTreeMap<NodeId, Vec<SocketAddr>>,
) -> Result<(), String> {
    let mut rafts = BTreeSet::new();
    let mut https = BTreeMap::new();
    let mut add = |active: &BTreeSet<NodeId>,
                   raft: &BTreeMap<NodeId, Vec<SocketAddr>>,
                   http: &BTreeMap<NodeId, Vec<SocketAddr>>|
     -> Result<(), String> {
        for (&id, addresses) in raft.iter().filter(|(id, _)| active.contains(id)) {
            for address in addresses {
                let address = canonical(*address);
                if https.contains_key(&address) || !rafts.insert(address) {
                    return Err(format!("node {id} Raft aliases another service"));
                }
            }
        }
        for (&id, addresses) in http.iter().filter(|(id, _)| active.contains(id)) {
            for address in addresses {
                let address = canonical(*address);
                if rafts.contains(&address) || https.get(&address).is_some_and(|owner| *owner != id)
                {
                    return Err(format!("node {id} HTTP aliases another service"));
                }
                https.insert(address, id);
            }
        }
        Ok(())
    };
    for (&other, view) in views {
        if other != group {
            add(&view.reserved, &view.raft, &view.http)?;
        }
    }
    add(reserved, candidate_raft, candidate_http)
}

pub struct PreparedRoutes {
    pub transport: TcpTransport,
    topology: Arc<Topology>,
    group: GroupId,
    seed: Arc<ValidatedConfig>,
    config: Arc<ValidatedConfig>,
    resolved: ResolvedConfig,
    resolver: Arc<dyn Resolver>,
}
/// Start with only local advertisements. Durable membership is recovered before
/// any seed or learned peer route can be published into this transport.
pub async fn prepare_routes(
    topology: Arc<Topology>,
    node: NodeId,
    group: GroupId,
    resolver: Arc<dyn Resolver>,
) -> io::Result<PreparedRoutes> {
    topology.validate()?;
    let scope = topology.scope(group)?;
    let owner = topology
        .nodes
        .iter()
        .find(|member| member.id == node)
        .ok_or_else(|| invalid("local service endpoints missing"))?;
    let raft = *owner
        .raft
        .get(&group)
        .ok_or_else(|| invalid("local group listener missing"))?;
    let seed = Arc::new(ValidatedConfig {
        id: node,
        group_id: Some(scope.group_id().to_owned()),
        genesis_voters: scope.genesis_voters().to_vec(),
        migrate_legacy_group: false,
        data_dir: PathBuf::new(),
        raft_listen: raft,
        http_listen: owner.http,
        raft_advertise: Endpoint::parse(&raft.to_string()).map_err(invalid)?,
        http_advertise: Endpoint::parse(&owner.http.to_string()).map_err(invalid)?,
        peers: topology
            .nodes
            .iter()
            .filter(|member| member.id != node)
            .filter_map(|member| {
                member.raft.get(&group).map(|address| PeerSpec {
                    id: member.id,
                    endpoint: Endpoint::parse(&address.to_string())
                        .expect("validated numeric topology"),
                })
            })
            .collect(),
        check_config: false,
        snapshot_threshold: 0,
        batch_records: 16,
        batch_delay_ms: 2,
        state_backend: kv_node::applied_store::StateBackend::Memory,
        // Service topology explicitly permits numeric loopback development.
        // This controls destination policy, never group or voter authority.
        legacy: true,
    });
    let mut local = (*seed).clone();
    local.peers.clear();
    let config = Arc::new(local);
    let resolved = config
        .resolve_startup(resolver.as_ref())
        .await
        .map_err(invalid)?;
    let transport = TcpTransport::spawn_configured_scoped(
        config.clone(),
        resolver.clone(),
        resolved.clone(),
        scope,
    )?;
    Ok(PreparedRoutes {
        transport,
        topology,
        group,
        seed,
        config,
        resolved,
        resolver,
    })
}
struct Accepted {
    config: Arc<ValidatedConfig>,
    resolved: ResolvedConfig,
    http: BTreeMap<NodeId, (Endpoint, Vec<SocketAddr>)>,
}
pub struct GroupRoutes {
    pub updates: watch::Sender<RouteUpdate>,
    pub status: watch::Receiver<Option<String>>,
    receive: watch::Receiver<RouteUpdate>,
    status_tx: watch::Sender<Option<String>>,
    prepared: PreparedRoutes,
    accepted: RwLock<Accepted>,
    registry: Arc<RouteRegistry>,
    rotation: AtomicUsize,
}
impl GroupRoutes {
    pub fn new(
        prepared: PreparedRoutes,
        initial: RouteUpdate,
        registry: Arc<RouteRegistry>,
    ) -> io::Result<Self> {
        if initial.membership.group_id != prepared.seed.group_id.as_deref().unwrap_or_default()
            || initial.membership.genesis_voters != prepared.seed.genesis_voters
            || registry.topology.fingerprint() != prepared.topology.fingerprint()
        {
            return Err(invalid(
                "routing authority differs from durable group bootstrap",
            ));
        }
        let mut reserved = initial.membership.participants();
        reserved.insert(prepared.seed.id);
        let (updates, receive) = watch::channel(initial);
        let (status_tx, status) = watch::channel(None);
        let http = BTreeMap::from([(
            prepared.seed.id,
            (
                prepared.seed.http_advertise.clone(),
                prepared.resolved.http_advertise.clone(),
            ),
        )]);
        let mut views = registry
            .views
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if views.contains_key(&prepared.group) {
            return Err(invalid("group routes already registered"));
        }
        if views.values().any(|view| view.local != prepared.seed.id) {
            return Err(invalid("registry cannot mix local process identities"));
        }
        views.insert(
            prepared.group,
            View {
                authority: receive.clone(),
                http: http
                    .iter()
                    .map(|(id, (_, addresses))| (*id, addresses.clone()))
                    .collect(),
                http_endpoints: http
                    .iter()
                    .map(|(id, (endpoint, _))| (*id, endpoint.clone()))
                    .collect(),
                raft: BTreeMap::from([(
                    prepared.seed.id,
                    prepared.resolved.raft_advertise.clone(),
                )]),
                reserved,
                error: None,
                resolver: prepared.resolver.clone(),
                local: prepared.seed.id,
            },
        );
        drop(views);
        let accepted = RwLock::new(Accepted {
            config: prepared.config.clone(),
            resolved: prepared.resolved.clone(),
            http,
        });
        Ok(Self {
            updates,
            status,
            receive,
            status_tx,
            prepared,
            accepted,
            registry,
            rotation: AtomicUsize::new(0),
        })
    }
    fn wanted(&self, update: &RouteUpdate) -> Result<BTreeMap<NodeId, MemberEndpoints>, String> {
        let mut wanted = BTreeMap::new();
        for id in update
            .membership
            .participants()
            .union(&update.membership.retired)
            .copied()
        {
            if id != self.prepared.seed.id {
                if let Some(endpoint) = endpoints(
                    &self.prepared.topology,
                    self.prepared.group,
                    id,
                    &update.membership,
                ) {
                    wanted.insert(id, endpoint);
                }
            }
        }
        for (&id, hint) in &update.hints {
            if id != self.prepared.seed.id
                && hint.term >= update.term
                && !update.membership.retired.contains(&id)
                && !update.membership.endpoints.contains_key(&id)
            {
                wanted.entry(id).or_insert_with(|| hint.endpoints.clone());
            }
        }
        for endpoint in wanted.values() {
            endpoint.validate()?;
            Endpoint::parse(&endpoint.raft)?;
            Endpoint::parse(&endpoint.http)?;
        }
        Ok(wanted)
    }
    fn publish(
        &self,
        update: &RouteUpdate,
        config: Arc<ValidatedConfig>,
        resolved: ResolvedConfig,
        http: BTreeMap<NodeId, (Endpoint, Vec<SocketAddr>)>,
    ) -> Result<(), String> {
        // Keep the watch read guard across this bounded synchronous publication:
        // a new actor authority cannot race a stale DNS result into the map.
        let current = self.receive.borrow();
        if !same_update(&current, update) {
            return Err("stale route authority".into());
        }
        let mut accepted = self
            .accepted
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut views = self
            .registry
            .views
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reserved = update.membership.participants();
        reserved.extend(
            update
                .hints
                .iter()
                .filter(|(id, hint)| {
                    hint.term >= update.term && !update.membership.retired.contains(id)
                })
                .map(|(id, _)| *id),
        );
        reserved.insert(config.id); // every live local listener still owns its socket
        let mut raft: BTreeMap<_, _> = resolved.peer_addrs.iter().cloned().collect();
        raft.insert(config.id, resolved.raft_advertise.clone());
        let http_addresses: BTreeMap<_, _> = http
            .iter()
            .map(|(id, (_, addresses))| (*id, addresses.clone()))
            .collect();
        validate_publication(
            &views,
            self.prepared.group,
            &reserved,
            &raft,
            &http_addresses,
        )?;
        if accepted.config.peers != config.peers || accepted.resolved != resolved {
            self.prepared
                .transport
                .replace_configured_routes(config.clone(), resolved.clone())
                .map_err(|error| error.to_string())?;
        }
        let view = views
            .get_mut(&self.prepared.group)
            .expect("registered group");
        view.http = http_addresses;
        view.http_endpoints = http
            .iter()
            .map(|(id, (endpoint, _))| (*id, endpoint.clone()))
            .collect();
        view.raft = raft;
        view.reserved = reserved;
        *accepted = Accepted {
            config,
            resolved,
            http,
        };
        Ok(())
    }
    async fn refresh(&self, update: &RouteUpdate) -> Result<(), String> {
        self.refresh_before(update, Instant::now() + RESOLUTION_TIMEOUT)
            .await
    }
    async fn refresh_before(&self, update: &RouteUpdate, deadline: Instant) -> Result<(), String> {
        let wanted = self.wanted(update)?;
        let (mut config, mut resolved, mut http) = {
            let current = self
                .accepted
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                (*current.config).clone(),
                current.resolved.clone(),
                current.http.clone(),
            )
        };
        let active_change = wanted.iter().any(|(id, endpoints)| {
            !update.membership.retired.contains(id)
                && !config.peers.iter().any(|peer| {
                    peer.id == *id
                        && Endpoint::parse(&endpoints.raft).ok().as_ref() == Some(&peer.endpoint)
                })
        });
        config.peers.retain(|peer| {
            wanted.get(&peer.id).is_some_and(|value| {
                Endpoint::parse(&value.raft).ok().as_ref() == Some(&peer.endpoint)
            }) && (!active_change || !update.membership.retired.contains(&peer.id))
        });
        resolved
            .peer_addrs
            .retain(|(id, _)| config.peers.iter().any(|peer| peer.id == *id));
        http.retain(|id, (endpoint, _)| {
            *id == config.id
                || wanted.get(id).is_some_and(|value| {
                    Endpoint::parse(&value.http).ok().as_ref() == Some(endpoint)
                })
        });
        self.publish(
            update,
            Arc::new(config.clone()),
            resolved.clone(),
            http.clone(),
        )?;
        let mut errors: Vec<_> = update
            .membership
            .participants()
            .into_iter()
            .filter(|id| *id != config.id && !wanted.contains_key(id))
            .map(|id| format!("peer {id}: no seed or durable endpoint"))
            .collect();
        let mut ordered: Vec<_> = wanted.into_iter().collect();
        ordered.sort_by_key(|(id, _)| {
            (
                update.membership.retired.contains(id),
                !update.hints.contains_key(id),
                *id,
            )
        });
        // Hints lead the first round, then every active destination rotates.
        // Several unavailable hints must not exhaust every shared budget before
        // an ordinary healthy peer ever receives a resolution attempt.
        let active_end = ordered.partition_point(|(id, _)| !update.membership.retired.contains(id));
        let rotating = &mut ordered[..active_end];
        if !rotating.is_empty() {
            let offset = self.rotation.fetch_add(1, Ordering::Relaxed) % rotating.len();
            rotating.rotate_left(offset);
        }
        for (id, endpoints) in ordered {
            if Instant::now() >= deadline {
                if !update.membership.retired.contains(&id) {
                    errors.push(format!("peer {id}: shared route resolution budget expired"));
                }
                continue;
            }
            let raft = Endpoint::parse(&endpoints.raft)?;
            if !config
                .peers
                .iter()
                .any(|peer| peer.id == id && peer.endpoint == raft)
            {
                let mut next = config.clone();
                next.peers.push(PeerSpec { id, endpoint: raft });
                next.peers.sort_by_key(|peer| peer.id);
                match tokio::time::timeout_at(
                    deadline,
                    next.resolve_peer(id, self.prepared.resolver.as_ref()),
                )
                .await
                .map_err(|_| "shared route resolution budget expired".to_owned())
                .and_then(|value| value)
                {
                    Ok(fresh) => {
                        let mut candidate = resolved.clone();
                        candidate.raft_advertise = fresh.raft_advertise;
                        candidate.http_advertise = fresh.http_advertise;
                        candidate.peer_addrs.extend(fresh.peer_addrs);
                        match self.publish(
                            update,
                            Arc::new(next.clone()),
                            candidate.clone(),
                            http.clone(),
                        ) {
                            Ok(()) => {
                                config = next;
                                resolved = candidate;
                            }
                            Err(error) => {
                                if !update.membership.retired.contains(&id) {
                                    errors.push(format!("peer {id}: {error}"));
                                }
                                continue;
                            }
                        }
                    }
                    Err(error) => {
                        if !update.membership.retired.contains(&id) {
                            errors.push(format!("peer {id}: {error}"));
                        }
                        continue;
                    }
                }
            }
            let endpoint = Endpoint::parse(&endpoints.http)?;
            match resolve_endpoint(&endpoint, self.prepared.resolver.as_ref(), deadline).await {
                Ok(addresses) => {
                    http.insert(id, (endpoint, addresses));
                }
                Err(error) => {
                    http.remove(&id);
                    if !update.membership.retired.contains(&id) {
                        errors.push(format!("peer {id} HTTP: {error}"));
                    }
                }
            }
            self.publish(
                update,
                Arc::new(config.clone()),
                resolved.clone(),
                http.clone(),
            )?;
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    pub async fn run(self, mut shutdown: ShutdownRx) -> io::Result<()> {
        let mut receive = self.receive.clone();
        loop {
            let update = receive.borrow_and_update().clone();
            let result = tokio::select! {
                biased;
                _=shutdown.requested()=>return Ok(()),
                changed=receive.changed()=>{changed.map_err(|_|invalid("routing authority closed before shutdown"))?;continue;},
                result=self.refresh(&update)=>result,
            };
            let error = result.err();
            self.status_tx.send_replace(error.clone());
            self.registry
                .views
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(&self.prepared.group)
                .expect("registered group")
                .error = error;
            tokio::select! {
                _=shutdown.requested()=>return Ok(()),
                changed=receive.changed()=>{changed.map_err(|_|invalid("routing authority closed before shutdown"))?;},
                _=tokio::time::sleep(Duration::from_millis(250))=>{},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Member;
    use kv_node::config::{ResolveFuture, SystemResolver};
    use kv_node::membership_runtime::RouteHint;
    use raft_core::membership::{CommittedMembership, ConfigurationEntry, ConfigurationPhase};
    use std::net::TcpListener;

    struct Fixture {
        topology: Arc<Topology>,
        _ports: Vec<TcpListener>,
    }
    impl Fixture {
        fn new() -> Self {
            let ports: Vec<_> = (0..16)
                .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
                .collect();
            let nodes = (0..4)
                .map(|offset| Member {
                    id: (offset + 1) as u8,
                    http: ports[offset * 4].local_addr().unwrap(),
                    raft: (0..3)
                        .map(|group| {
                            (
                                group as u16,
                                ports[offset * 4 + 1 + group].local_addr().unwrap(),
                            )
                        })
                        .collect(),
                })
                .collect();
            let topology = Arc::new(Topology {
                version: 1,
                cluster: "routes".into(),
                groups: vec![1, 2],
                owners: vec![1, 2],
                genesis_voters: BTreeMap::from([
                    (0, vec![1, 2, 3]),
                    (1, vec![1, 2, 3]),
                    (2, vec![1, 2, 3]),
                ]),
                nodes,
            });
            topology.validate().unwrap();
            Self {
                topology,
                _ports: ports,
            }
        }
        fn state(&self, group: GroupId) -> MembershipState {
            let scope = self.topology.scope(group).unwrap();
            CommittedMembership::bootstrap_with_group(
                scope.group_id().to_owned(),
                scope.genesis_voters().to_vec(),
            )
            .unwrap()
            .state
        }
        fn endpoint(&self, id: NodeId, group: GroupId) -> MemberEndpoints {
            let node = self
                .topology
                .nodes
                .iter()
                .find(|node| node.id == id)
                .unwrap();
            MemberEndpoints {
                raft: node.raft[&group].to_string(),
                http: node.http.to_string(),
            }
        }
        fn admitted(
            &self,
            group: GroupId,
            id: NodeId,
            endpoint: MemberEndpoints,
        ) -> MembershipState {
            CommittedMembership {
                index: 0,
                term: 0,
                state: self.state(group),
            }
            .advanced(
                1,
                1,
                &ConfigurationEntry {
                    request_id: format!("add-{id}"),
                    operation: AdminOperation::AddLearner {
                        id,
                        endpoints: endpoint,
                    },
                    phase: ConfigurationPhase::Apply,
                },
            )
            .unwrap()
            .state
        }
        async fn controller(
            &self,
            group: GroupId,
            registry: Arc<RouteRegistry>,
            resolver: Arc<dyn Resolver>,
        ) -> GroupRoutes {
            let prepared = prepare_routes(self.topology.clone(), 1, group, resolver)
                .await
                .unwrap();
            GroupRoutes::new(
                prepared,
                RouteUpdate {
                    term: 1,
                    membership: self.state(group),
                    hints: BTreeMap::new(),
                },
                registry,
            )
            .unwrap()
        }
    }
    fn socket() -> TcpListener {
        TcpListener::bind(("127.0.0.1", 0)).unwrap()
    }
    fn with_authority(routes: &GroupRoutes, state: MembershipState) -> RouteUpdate {
        routes
            .updates
            .send_modify(|update| update.update_authority(1, state));
        routes.receive.borrow().clone()
    }
    #[tokio::test]
    async fn admission_allows_shared_same_id_http_but_rejects_cross_group_service_aliases() {
        let fixture = Fixture::new();
        let registry = Arc::new(RouteRegistry::new(fixture.topology.clone()));
        let zero = fixture
            .controller(0, registry.clone(), Arc::new(SystemResolver))
            .await;
        let one = fixture
            .controller(1, registry.clone(), Arc::new(SystemResolver))
            .await;
        let two = fixture
            .controller(2, registry.clone(), Arc::new(SystemResolver))
            .await;
        let update = with_authority(&two, fixture.admitted(2, 4, fixture.endpoint(4, 2)));
        two.refresh(&update).await.unwrap();
        let operation = AdminOperation::AddLearner {
            id: 4,
            endpoints: fixture.endpoint(4, 1),
        };
        assert_eq!(
            registry
                .validate_operation(1, &operation, &fixture.state(1))
                .await
                .unwrap(),
            kv_node::admin::membership_revision(&fixture.state(1))
        );
        let free = socket();
        let collision = AdminOperation::AddLearner {
            id: 5,
            endpoints: MemberEndpoints {
                raft: free.local_addr().unwrap().to_string(),
                http: fixture.endpoint(4, 2).http,
            },
        };
        assert!(registry
            .validate_operation(1, &collision, &fixture.state(1))
            .await
            .unwrap_err()
            .contains("node 4 http"));
        let collision = AdminOperation::AddLearner {
            id: 4,
            endpoints: MemberEndpoints {
                raft: fixture.endpoint(2, 2).raft,
                http: fixture.endpoint(4, 1).http,
            },
        };
        assert!(registry
            .validate_operation(1, &collision, &fixture.state(1))
            .await
            .unwrap_err()
            .contains("raft"));
        let port = fixture.topology.nodes[1].http.port();
        let mapped = AdminOperation::AddLearner {
            id: 4,
            endpoints: MemberEndpoints {
                raft: fixture.endpoint(4, 1).raft,
                http: format!("[::ffff:127.0.0.1]:{port}"),
            },
        };
        assert!(registry
            .validate_operation(1, &mapped, &fixture.state(1))
            .await
            .is_err());
        with_authority(&one, fixture.admitted(1, 4, fixture.endpoint(4, 1)));
        assert_eq!(
            registry
                .validate_operation(1, &operation, &fixture.state(1))
                .await
                .unwrap_err(),
            "stale_endpoint_validation"
        );
        drop((zero, one, two));
    }
    #[tokio::test]
    async fn passive_local_host_can_join_target_group_only_at_its_exact_listeners() {
        let fixture = Fixture::new();
        let registry = Arc::new(RouteRegistry::new(fixture.topology.clone()));
        let mut controllers = Vec::new();
        for group in 0..3 {
            let prepared =
                prepare_routes(fixture.topology.clone(), 4, group, Arc::new(SystemResolver))
                    .await
                    .unwrap();
            let state = if group == 0 {
                fixture.admitted(0, 4, fixture.endpoint(4, 0))
            } else {
                fixture.state(group)
            };
            controllers.push(
                GroupRoutes::new(
                    prepared,
                    RouteUpdate {
                        term: 1,
                        membership: state,
                        hints: BTreeMap::new(),
                    },
                    registry.clone(),
                )
                .unwrap(),
            );
        }
        let operation = AdminOperation::AddLearner {
            id: 4,
            endpoints: fixture.endpoint(4, 1),
        };
        assert!(registry
            .validate_operation(1, &operation, &fixture.state(1))
            .await
            .is_ok());
        let wrong_group = AdminOperation::AddLearner {
            id: 4,
            endpoints: fixture.endpoint(4, 2),
        };
        assert!(registry
            .validate_operation(1, &wrong_group, &fixture.state(1))
            .await
            .is_err());
        let foreign = socket();
        let wrong_host = AdminOperation::AddLearner {
            id: 4,
            endpoints: MemberEndpoints {
                raft: foreign.local_addr().unwrap().to_string(),
                http: fixture.endpoint(4, 1).http,
            },
        };
        assert!(registry
            .validate_operation(1, &wrong_host, &fixture.state(1))
            .await
            .unwrap_err()
            .contains("exact target listeners"));
        let alias_id = AdminOperation::AddLearner {
            id: 5,
            endpoints: fixture.endpoint(4, 1),
        };
        assert!(registry
            .validate_operation(1, &alias_id, &fixture.state(1))
            .await
            .is_err());
        drop(controllers);
    }
    #[tokio::test]
    async fn hint_http_is_hidden_on_new_authority_then_replaced_with_durable_endpoint() {
        let fixture = Fixture::new();
        let registry = Arc::new(RouteRegistry::new(fixture.topology.clone()));
        let routes = fixture
            .controller(1, registry.clone(), Arc::new(SystemResolver))
            .await;
        let ports: Vec<_> = (0..4).map(|_| socket()).collect();
        let endpoint = |offset: usize| MemberEndpoints {
            raft: ports[offset].local_addr().unwrap().to_string(),
            http: ports[offset + 1].local_addr().unwrap().to_string(),
        };
        let old = endpoint(0);
        let durable = endpoint(2);
        routes.updates.send_modify(|update| {
            update.hints.insert(
                4,
                RouteHint {
                    term: 1,
                    endpoints: old.clone(),
                },
            );
        });
        let hinted = routes.receive.borrow().clone();
        routes.refresh(&hinted).await.unwrap();
        assert!(registry
            .http_routes(1)
            .contains(&(4, ports[1].local_addr().unwrap())));
        let previous = routes.receive.borrow().clone();
        let update = with_authority(&routes, fixture.admitted(1, 4, durable));
        assert!(
            !registry.http_routes(1).iter().any(|(id, _)| *id == 4),
            "old hint must disappear before replacement DNS completes"
        );
        let (config, resolved, http) = {
            let accepted = routes.accepted.read().unwrap();
            (
                accepted.config.clone(),
                accepted.resolved.clone(),
                accepted.http.clone(),
            )
        };
        assert_eq!(
            routes
                .publish(&previous, config, resolved, http)
                .unwrap_err(),
            "stale route authority"
        );
        routes.refresh(&update).await.unwrap();
        assert!(registry
            .http_routes(1)
            .contains(&(4, ports[3].local_addr().unwrap())));
        assert!(!registry
            .http_routes(1)
            .contains(&(4, ports[1].local_addr().unwrap())));
    }
    struct AliasDns(SocketAddr);
    impl Resolver for AliasDns {
        fn resolve<'a>(&'a self, _host: &'a str, port: u16) -> ResolveFuture<'a> {
            Box::pin(async move { Ok(vec![SocketAddr::new(self.0.ip(), port)]) })
        }
    }
    #[tokio::test]
    async fn dns_refresh_cannot_publish_http_alias_of_another_groups_raft_listener() {
        let fixture = Fixture::new();
        let registry = Arc::new(RouteRegistry::new(fixture.topology.clone()));
        let two = fixture
            .controller(2, registry.clone(), Arc::new(SystemResolver))
            .await;
        let initial = two.receive.borrow().clone();
        two.refresh(&initial).await.unwrap();
        let alias = fixture.topology.nodes[1].raft[&2];
        let one = fixture
            .controller(1, registry.clone(), Arc::new(AliasDns(alias)))
            .await;
        let bad = MemberEndpoints {
            raft: fixture.endpoint(4, 1).raft,
            http: format!("http-rebind.invalid:{}", alias.port()),
        };
        let update = with_authority(&one, fixture.admitted(1, 4, bad));
        let error = one.refresh(&update).await.unwrap_err();
        assert!(error.contains("aliases"), "{error}");
        assert!(!registry.http_routes(1).iter().any(|(id, _)| *id == 4));
        assert!(registry
            .http_routes(2)
            .contains(&(2, fixture.topology.nodes[1].http)));
    }
    #[tokio::test]
    async fn a_missing_historical_genesis_route_stays_visible_and_shutdown_cancels_refresh() {
        let fixture = Fixture::new();
        let mut topology = (*fixture.topology).clone();
        topology.nodes.retain(|node| node.id != 3);
        topology.validate().unwrap();
        let topology = Arc::new(topology);
        let registry = Arc::new(RouteRegistry::new(topology.clone()));
        let prepared = prepare_routes(topology, 1, 1, Arc::new(SystemResolver))
            .await
            .unwrap();
        assert!(
            prepared.config.peers.is_empty(),
            "transport begins before recovered authority without peer routes"
        );
        let routes = GroupRoutes::new(
            prepared,
            RouteUpdate {
                term: 1,
                membership: fixture.state(1),
                hints: BTreeMap::new(),
            },
            registry.clone(),
        )
        .unwrap();
        let mut status = routes.status.clone();
        let (stop, shutdown) = kv_node::shutdown::channel();
        let task = tokio::spawn(routes.run(shutdown));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if status
                    .borrow()
                    .as_ref()
                    .is_some_and(|error| error.contains("peer 3: no seed"))
                {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(registry.errors()[&1]
            .as_ref()
            .unwrap()
            .contains("peer 3: no seed"));
        stop.request();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    struct MixedDns;
    impl Resolver for MixedDns {
        fn resolve<'a>(&'a self, _host: &'a str, port: u16) -> ResolveFuture<'a> {
            Box::pin(async move {
                Ok(vec![
                    SocketAddr::from(([127, 0, 0, 1], port)),
                    SocketAddr::from(([0, 0, 0, 0], port)),
                ])
            })
        }
    }
    struct PendingDns;
    impl Resolver for PendingDns {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> ResolveFuture<'a> {
            Box::pin(std::future::pending())
        }
    }
    #[tokio::test]
    async fn unresolved_hints_cannot_starve_healthy_active_peers_across_rounds() {
        let fixture = Fixture::new();
        let registry = Arc::new(RouteRegistry::new(fixture.topology.clone()));
        let routes = fixture.controller(1, registry, Arc::new(PendingDns)).await;
        let ports: Vec<_> = (0..6).map(|_| socket()).collect();
        routes.updates.send_modify(|update| {
            for (offset, id) in (4..=6).enumerate() {
                update.hints.insert(
                    id,
                    RouteHint {
                        term: 1,
                        endpoints: MemberEndpoints {
                            raft: format!(
                                "held-{id}.invalid:{}",
                                ports[offset * 2].local_addr().unwrap().port()
                            ),
                            http: ports[offset * 2 + 1].local_addr().unwrap().to_string(),
                        },
                    },
                );
            }
        });
        let update = routes.receive.borrow().clone();
        // Same production refresh path and absolute-deadline checks, shortened
        // here so the deliberately pending DNS counterexample stays bounded.
        assert!(routes
            .refresh_before(&update, Instant::now() + Duration::from_millis(30))
            .await
            .is_err());
        assert!(
            routes.accepted.read().unwrap().config.peers.is_empty(),
            "the first prioritized hint must actually consume a complete round"
        );
        for _ in 1..5 {
            assert!(routes
                .refresh_before(&update, Instant::now() + Duration::from_millis(30))
                .await
                .is_err());
        }
        let accepted = routes.accepted.read().unwrap();
        assert!(
            accepted.config.peers.iter().any(|peer| peer.id == 2),
            "healthy peer 2 was starved by repeated pending hints"
        );
        assert!(
            accepted.config.peers.iter().any(|peer| peer.id == 3),
            "healthy peer 3 was starved by repeated pending hints"
        );
        assert!(!accepted.config.peers.iter().any(|peer| peer.id >= 4));
    }
    #[tokio::test]
    async fn every_dns_candidate_and_the_shared_operation_deadline_are_enforced() {
        let endpoint = Endpoint::parse("mixed.invalid:49000").unwrap();
        assert!(resolve_endpoint(
            &endpoint,
            &MixedDns,
            Instant::now() + Duration::from_secs(1)
        )
        .await
        .is_err());
        let started = Instant::now();
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            resolve_endpoint(&endpoint, &PendingDns, started + Duration::from_millis(20))
        )
        .await
        .unwrap()
        .is_err());
        assert!(resolve_endpoint(
            &Endpoint::parse("127.0.0.1:49000").unwrap(),
            &PendingDns,
            Instant::now()
        )
        .await
        .is_err());
    }
}
