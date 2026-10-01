//! Routing follows durable membership without turning reachability into votes.
use crate::{
    config::{Endpoint, PeerSpec, ResolvedConfig, Resolver, ValidatedConfig},
    kv::SharedReadState,
    shutdown::ShutdownRx,
    transport::TcpTransport,
};
use raft_core::{
    membership::{MemberEndpoints, MembershipState},
    NodeId,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteHint {
    pub term: u64,
    pub endpoints: MemberEndpoints,
}
#[derive(Clone, Debug)]
pub struct RouteUpdate {
    pub term: u64,
    pub membership: MembershipState,
    pub hints: BTreeMap<NodeId, RouteHint>,
}
impl RouteUpdate {
    pub fn update_authority(&mut self, term: u64, membership: MembershipState) {
        self.term = term;
        self.hints.retain(|id, hint| {
            hint.term >= term
                && !membership.retired.contains(id)
                && !membership.endpoints.contains_key(id)
        });
        self.membership = membership;
    }
}
#[derive(Clone)]
pub struct RouteValidator {
    accepted: Arc<RwLock<(Arc<ValidatedConfig>, ResolvedConfig)>>,
    resolver: Arc<dyn Resolver>,
}
impl RouteValidator {
    pub async fn validate(
        &self,
        id: NodeId,
        endpoints: &MemberEndpoints,
        membership: &MembershipState,
    ) -> Result<(), String> {
        let (config, mut resolved) = self
            .accepted
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // Retired routes are notification-only and never reserve an address for
        // a lifetime. The replicated node id tombstone still forbids id reuse.
        resolved
            .peer_addrs
            .retain(|(peer, _)| !membership.retired.contains(peer));
        config
            .validate_member_endpoints(id, endpoints, self.resolver.as_ref(), &resolved, membership)
            .await
    }
}

pub struct RouteController {
    pub updates: watch::Sender<RouteUpdate>,
    pub validator: RouteValidator,
    receiver: watch::Receiver<RouteUpdate>,
    seed: Arc<ValidatedConfig>,
}
impl RouteController {
    pub fn new(
        config: Arc<ValidatedConfig>,
        resolved: ResolvedConfig,
        resolver: Arc<dyn Resolver>,
        membership: MembershipState,
    ) -> Self {
        let (updates, receiver) = watch::channel(RouteUpdate {
            term: 0,
            membership,
            hints: BTreeMap::new(),
        });
        Self {
            updates,
            receiver,
            seed: config.clone(),
            validator: RouteValidator {
                accepted: Arc::new(RwLock::new((config, resolved))),
                resolver,
            },
        }
    }
    pub fn with_seed(mut self, seed: Arc<ValidatedConfig>) -> Self {
        self.seed = seed;
        self
    }
    pub async fn run(
        mut self,
        transport: TcpTransport,
        shared: SharedReadState,
        mut shutdown: ShutdownRx,
    ) {
        loop {
            let update = self.receiver.borrow_and_update().clone();
            let refresh = refresh_routes(&self.seed, &self.validator, &transport, &update);
            tokio::select! {
                biased;
                _ = shutdown.requested() => break,
                changed = self.receiver.changed() => { if changed.is_err() { break; } continue; },
                result = refresh => {
                    shared.route_error(result.err());
                }
            }
            tokio::select! {
                _ = shutdown.requested() => break,
                changed = self.receiver.changed() => if changed.is_err() { break; },
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }
}

fn desired_routes(
    seed: &ValidatedConfig,
    update: &RouteUpdate,
) -> Result<BTreeMap<NodeId, Endpoint>, String> {
    let participants = update.membership.participants();
    let mut wanted = BTreeMap::new();
    for peer in &seed.peers {
        if participants.contains(&peer.id) || update.membership.retired.contains(&peer.id) {
            wanted.insert(peer.id, peer.endpoint.clone());
        }
    }
    for (&id, endpoints) in &update.membership.endpoints {
        if id != seed.id {
            wanted.insert(id, Endpoint::parse(&endpoints.raft)?);
        }
    }
    for (&id, hint) in &update.hints {
        if id != seed.id
            && hint.term >= update.term
            && !update.membership.retired.contains(&id)
            && !update.membership.endpoints.contains_key(&id)
        {
            wanted
                .entry(id)
                .or_insert(Endpoint::parse(&hint.endpoints.raft)?);
        }
    }
    Ok(wanted)
}

pub struct InitialRoutes {
    pub config: Arc<ValidatedConfig>,
    pub resolved: ResolvedConfig,
    pub error: Option<String>,
}
/// Explicit groups may start degraded after durable identity recovery. Local
/// service advertisements remain mandatory; peer DNS never supplies votes.
pub async fn startup_routes(
    seed: &Arc<ValidatedConfig>,
    membership: &MembershipState,
    resolver: &dyn Resolver,
) -> Result<InitialRoutes, String> {
    if seed.group_id.as_deref() != Some(membership.group_id.as_str())
        || seed.genesis_voters != membership.genesis_voters
    {
        return Err("route startup identity differs from durable group".into());
    }
    let deadline = tokio::time::Instant::now() + crate::config::RESOLUTION_TIMEOUT;
    let mut config = (**seed).clone();
    config.peers.clear();
    let mut resolved = tokio::time::timeout_at(deadline, config.resolve_startup(resolver))
        .await
        .map_err(|_| "local advertisement resolution timed out".to_owned())??;
    let update = RouteUpdate {
        term: 0,
        membership: membership.clone(),
        hints: BTreeMap::new(),
    };
    let mut wanted = desired_routes(seed, &update)?;
    wanted.retain(|id, _| !membership.retired.contains(id));
    let mut errors = Vec::new();
    for id in membership
        .participants()
        .into_iter()
        .filter(|id| *id != seed.id)
    {
        if !wanted.contains_key(&id) {
            errors.push(format!("peer {id}: no seed or durable endpoint"));
        }
    }
    for (id, endpoint) in wanted {
        let mut next = config.clone();
        next.peers.push(PeerSpec { id, endpoint });
        next.peers.sort_by_key(|peer| peer.id);
        let fresh = match tokio::time::timeout_at(deadline, next.resolve_peer(id, resolver)).await {
            Ok(Ok(fresh)) => fresh,
            Ok(Err(error)) => {
                errors.push(format!("peer {id}: {error}"));
                continue;
            }
            Err(_) => {
                errors.push(format!(
                    "peer {id}: shared startup resolution budget expired"
                ));
                continue;
            }
        };
        let mut candidate = resolved.clone();
        candidate.raft_advertise = fresh.raft_advertise;
        candidate.http_advertise = fresh.http_advertise;
        candidate.peer_addrs.extend(fresh.peer_addrs);
        if let Err(error) = next.validate_snapshot(&candidate, true) {
            errors.push(format!("peer {id}: {error}"));
            continue;
        }
        config = next;
        resolved = candidate;
    }
    Ok(InitialRoutes {
        config: Arc::new(config),
        resolved,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    })
}

async fn refresh_routes(
    seed: &Arc<ValidatedConfig>,
    validator: &RouteValidator,
    transport: &TcpTransport,
    update: &RouteUpdate,
) -> Result<(), String> {
    let wanted = desired_routes(seed, update)?;
    let (old_config, old_resolved) = validator
        .accepted
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut config = (*old_config).clone();
    let mut resolved = old_resolved;
    // Remove stale and retired routes first. Retired notification links are
    // restored below only if they do not collide with an active destination.
    let active_change = wanted.iter().any(|(id, endpoint)| {
        !update.membership.retired.contains(id)
            && !config
                .peers
                .iter()
                .any(|peer| peer.id == *id && peer.endpoint == *endpoint)
    });
    config.peers.retain(|peer| {
        wanted.get(&peer.id) == Some(&peer.endpoint)
            && (!active_change || !update.membership.retired.contains(&peer.id))
    });
    resolved
        .peer_addrs
        .retain(|(id, _)| config.peers.iter().any(|peer| peer.id == *id));
    publish(validator, transport, config.clone(), resolved.clone())?;
    let mut errors: Vec<_> = update
        .membership
        .participants()
        .into_iter()
        .filter(|id| *id != seed.id && !wanted.contains_key(id))
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
    for (id, endpoint) in ordered {
        if config
            .peers
            .iter()
            .any(|peer| peer.id == id && peer.endpoint == endpoint)
        {
            continue;
        }
        let mut next = config.clone();
        next.peers.push(PeerSpec { id, endpoint });
        next.peers.sort_by_key(|peer| peer.id);
        let result = async {
            let fresh = next.resolve_peer(id, validator.resolver.as_ref()).await?;
            let mut candidate = resolved.clone();
            candidate.raft_advertise = fresh.raft_advertise;
            candidate.http_advertise = fresh.http_advertise;
            candidate.peer_addrs.extend(fresh.peer_addrs);
            next.validate_snapshot(&candidate, true)?;
            publish(validator, transport, next.clone(), candidate.clone())?;
            Ok::<_, String>(candidate)
        }
        .await;
        match result {
            Ok(candidate) => {
                config = next;
                resolved = candidate;
            }
            Err(error) if update.membership.retired.contains(&id) => {
                tracing::debug!(id, %error, "retired notification route suppressed");
            }
            Err(error) => errors.push(format!("peer {id}: {error}")),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
fn publish(
    validator: &RouteValidator,
    transport: &TcpTransport,
    config: ValidatedConfig,
    resolved: ResolvedConfig,
) -> Result<(), String> {
    let unchanged = {
        let current = validator
            .accepted
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.0.peers == config.peers && current.1 == resolved
    };
    if unchanged {
        return Ok(());
    }
    let config = Arc::new(config);
    transport
        .replace_configured_routes(config.clone(), resolved.clone())
        .map_err(|error| error.to_string())?;
    *validator
        .accepted
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = (config, resolved);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use raft_core::membership::{
        AdminOperation, CommittedMembership, ConfigurationEntry, ConfigurationPhase,
    };
    fn admission(endpoints: MemberEndpoints) -> MembershipState {
        CommittedMembership::bootstrap_with_group("route-test".into(), vec![1])
            .unwrap()
            .advanced(
                1,
                1,
                &ConfigurationEntry {
                    request_id: "add4".into(),
                    operation: AdminOperation::AddLearner { id: 4, endpoints },
                    phase: ConfigurationPhase::Apply,
                },
            )
            .unwrap()
            .state
    }
    #[tokio::test]
    async fn durable_endpoint_replaces_divergent_hint_and_old_terms_expire_without_losing_next_term_route(
    ) {
        let reservations: Vec<_> = (0..6)
            .map(|_| std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap())
            .collect();
        let ports: Vec<_> = reservations
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();
        let args = crate::config::Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--group-id",
            "route-test",
            "--genesis-voters",
            "1",
            "--raft-port",
            &ports[0].to_string(),
            "--http-port",
            &ports[1].to_string(),
            "--data-dir",
            "unused",
        ])
        .unwrap();
        let config = Arc::new(crate::config::validate(&args).unwrap());
        let resolver: Arc<dyn Resolver> = Arc::new(crate::config::SystemResolver);
        let resolved = config.resolve(resolver.as_ref()).await.unwrap();
        let bootstrap = CommittedMembership::bootstrap_with_group("route-test".into(), vec![1])
            .unwrap()
            .state;
        let controller = RouteController::new(
            config.clone(),
            resolved.clone(),
            resolver.clone(),
            bootstrap.clone(),
        );
        let transport = TcpTransport::spawn_configured_scoped(
            config.clone(),
            resolver,
            resolved,
            crate::transport::TransportScope::new("route-test".into(), vec![1]).unwrap(),
        )
        .unwrap();
        let endpoint = |a, b| MemberEndpoints {
            raft: format!("127.0.0.1:{a}"),
            http: format!("127.0.0.1:{b}"),
        };
        let stale = endpoint(ports[2], ports[3]);
        let durable = endpoint(ports[4], ports[5]);
        let mut update = RouteUpdate {
            term: 1,
            membership: bootstrap.clone(),
            hints: BTreeMap::from([(
                4,
                RouteHint {
                    term: 3,
                    endpoints: stale.clone(),
                },
            )]),
        };
        refresh_routes(&config, &controller.validator, &transport, &update)
            .await
            .unwrap();
        assert_eq!(
            controller.validator.accepted.read().unwrap().0.peers[0]
                .endpoint
                .to_string(),
            stale.raft
        );
        // Exercise the actual route publication while the stale hint is still
        // present: precedence must hold independently of the pruning helper.
        update.membership = admission(durable.clone());
        refresh_routes(&config, &controller.validator, &transport, &update)
            .await
            .unwrap();
        assert_eq!(
            controller.validator.accepted.read().unwrap().0.peers[0]
                .endpoint
                .to_string(),
            durable.raft
        );
        update.update_authority(3, update.membership.clone());
        assert!(update.hints.is_empty());
        update.hints.insert(
            5,
            RouteHint {
                term: 5,
                endpoints: stale,
            },
        );
        update.update_authority(4, bootstrap.clone());
        assert!(update.hints.contains_key(&5));
        update.update_authority(5, bootstrap.clone());
        assert!(update.hints.contains_key(&5));
        update.update_authority(6, bootstrap);
        assert!(update.hints.is_empty());
    }
    #[tokio::test]
    async fn administrative_endpoints_reject_self_http_peer_alias_and_forbidden_destinations() {
        let args = crate::config::Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--group-id",
            "route-test",
            "--genesis-voters",
            "1,2,3",
            "--raft-port",
            "29001",
            "--http-port",
            "28001",
            "--peers",
            "2@127.0.0.1:29002,3@127.0.0.1:29003",
            "--data-dir",
            "unused",
        ])
        .unwrap();
        let cfg = crate::config::validate(&args).unwrap();
        let resolver = crate::config::SystemResolver;
        let resolved = cfg.resolve(&resolver).await.unwrap();
        let membership =
            CommittedMembership::bootstrap_with_group("route-test".into(), vec![1, 2, 3])
                .unwrap()
                .state;
        for (raft, http) in [
            ("127.0.0.1:28001", "127.0.0.1:28004"),
            ("127.0.0.1:29004", "127.0.0.1:29002"),
            ("0.0.0.0:29004", "127.0.0.1:28004"),
            ("127.0.0.1:29004", "127.0.0.1:29004"),
        ] {
            assert!(cfg
                .validate_member_endpoints(
                    4,
                    &MemberEndpoints {
                        raft: raft.into(),
                        http: http.into()
                    },
                    &resolver,
                    &resolved,
                    &membership
                )
                .await
                .is_err());
        }
        cfg.validate_member_endpoints(
            4,
            &MemberEndpoints {
                raft: "127.0.0.1:29004".into(),
                http: "127.0.0.1:28004".into(),
            },
            &resolver,
            &resolved,
            &membership,
        )
        .await
        .unwrap();
        // The durable membership changed but route publication has not run.
        // Neither the accepted cache nor the static route list yet includes4.
        let lagged = CommittedMembership::bootstrap_with_group("route-test".into(), vec![1, 2, 3])
            .unwrap()
            .advanced(
                1,
                1,
                &ConfigurationEntry {
                    request_id: "add4".into(),
                    operation: AdminOperation::AddLearner {
                        id: 4,
                        endpoints: MemberEndpoints {
                            raft: "127.0.0.1:29004".into(),
                            http: "127.0.0.1:28004".into(),
                        },
                    },
                    phase: ConfigurationPhase::Apply,
                },
            )
            .unwrap()
            .state;
        assert!(!resolved.peer_addrs.iter().any(|(id, _)| *id == 4));
        assert!(cfg
            .validate_member_endpoints(
                5,
                &MemberEndpoints {
                    raft: "127.0.0.1:29004".into(),
                    http: "127.0.0.1:28005".into()
                },
                &resolver,
                &resolved,
                &lagged
            )
            .await
            .is_err());
    }
    #[tokio::test]
    async fn missing_active_route_remains_visible_after_background_refresh() {
        let args = crate::config::Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--group-id",
            "route-test",
            "--genesis-voters",
            "1,2",
            "--raft-port",
            "39001",
            "--http-port",
            "38001",
            "--data-dir",
            "unused",
        ])
        .unwrap();
        let config = Arc::new(crate::config::validate(&args).unwrap());
        let membership = CommittedMembership::bootstrap_with_group("route-test".into(), vec![1, 2])
            .unwrap()
            .state;
        let resolver: Arc<dyn Resolver> = Arc::new(crate::config::SystemResolver);
        let initial = startup_routes(&config, &membership, resolver.as_ref())
            .await
            .unwrap();
        assert!(initial
            .error
            .as_ref()
            .is_some_and(|error| error.contains("peer 2: no seed")));
        let controller = RouteController::new(
            initial.config.clone(),
            initial.resolved.clone(),
            resolver.clone(),
            membership.clone(),
        );
        let transport = TcpTransport::spawn_configured_scoped(
            initial.config,
            resolver,
            initial.resolved,
            crate::transport::TransportScope::new("route-test".into(), vec![1, 2]).unwrap(),
        )
        .unwrap();
        let update = RouteUpdate {
            term: 1,
            membership,
            hints: BTreeMap::new(),
        };
        for _ in 0..2 {
            let error = refresh_routes(&config, &controller.validator, &transport, &update)
                .await
                .unwrap_err();
            assert!(error.contains("peer 2: no seed or durable endpoint"));
            assert!(controller
                .validator
                .accepted
                .read()
                .unwrap()
                .0
                .peers
                .is_empty());
        }
    }

    #[tokio::test]
    async fn explicit_startup_skips_retired_dns_and_stale_voter_keeps_valid_partial_routes() {
        struct FailedDns(std::sync::Mutex<Vec<String>>);
        impl Resolver for FailedDns {
            fn resolve<'a>(
                &'a self,
                host: &'a str,
                _port: u16,
            ) -> crate::config::ResolveFuture<'a> {
                self.0.lock().unwrap().push(host.to_owned());
                Box::pin(async { Err("unavailable old hostname".into()) })
            }
        }
        let args = crate::config::Config::try_parse_from([
            "kv-node",
            "--id",
            "1",
            "--group-id",
            "route-test",
            "--genesis-voters",
            "1,2,3",
            "--raft-port",
            "39001",
            "--http-port",
            "38001",
            "--peers",
            "2@removed.invalid:39002,3@127.0.0.1:39003",
            "--data-dir",
            "unused",
        ])
        .unwrap();
        let cfg = Arc::new(crate::config::validate(&args).unwrap());
        let resolver = FailedDns(std::sync::Mutex::new(Vec::new()));
        let original =
            CommittedMembership::bootstrap_with_group("route-test".into(), vec![1, 2, 3]).unwrap();
        let change = |phase| ConfigurationEntry {
            request_id: "remove2".into(),
            operation: AdminOperation::Remove { id: 2 },
            phase,
        };
        let removed = original
            .advanced(1, 1, &change(ConfigurationPhase::Joint))
            .unwrap()
            .advanced(2, 1, &change(ConfigurationPhase::Final))
            .unwrap();
        let healthy = startup_routes(&cfg, &removed.state, &resolver)
            .await
            .unwrap();
        assert!(healthy.error.is_none());
        assert_eq!(
            healthy
                .config
                .peers
                .iter()
                .map(|peer| peer.id)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert!(
            resolver.0.lock().unwrap().is_empty(),
            "retired seed must not even trigger DNS"
        );
        let degraded = startup_routes(&cfg, &original.state, &resolver)
            .await
            .unwrap();
        assert!(degraded
            .error
            .as_ref()
            .is_some_and(|error| error.contains("peer 2")));
        assert_eq!(
            degraded
                .config
                .peers
                .iter()
                .map(|peer| peer.id)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(*resolver.0.lock().unwrap(), vec!["removed.invalid"]);
        assert!(
            cfg.resolve(&resolver).await.is_err(),
            "strict diagnostic resolution still rejects missing seed DNS"
        );
    }
}
