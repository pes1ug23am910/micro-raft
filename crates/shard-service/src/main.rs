use clap::Parser;
use kv_node::config::{Resolver, SystemResolver};
use kv_node::membership_runtime::RouteUpdate;
use kv_node::shutdown;
use kv_node::transport::{spawn_listener_scoped_with_shutdown, INBOUND_QUEUE_CAPACITY};
use shard_service::{
    routing::{prepare_routes, GroupRoutes, RouteRegistry},
    runtime::Actor,
    service::Service,
    topology::{DirectoryGuard, Topology},
};
use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};

#[derive(Parser)]
struct Arguments {
    #[arg(long)]
    topology: PathBuf,
    #[arg(long)]
    id: u8,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    seed: u64,
    #[arg(long, default_value_t = 128)]
    snapshot_threshold: u64,
    /// Advance handoffs only through explicit reconcile intents (fault experiments).
    #[arg(long)]
    manual_handoff: bool,
    #[arg(long)]
    check_config: bool,
}
struct ListenerTasks(Vec<JoinHandle<()>>);
impl Drop for ListenerTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Arguments::parse();
    let raw = std::fs::read(&args.topology)?;
    if raw.len() > 256 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "topology too large",
        ));
    }
    let topology: Topology = serde_json::from_slice(&raw)?;
    topology.validate()?;
    let member = topology
        .nodes
        .iter()
        .find(|n| n.id == args.id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "node absent from topology"))?
        .clone();
    if args.check_config {
        println!(
            "{}",
            serde_json::json!({"valid":true,"cluster":topology.fingerprint(),"node":args.id})
        );
        return Ok(());
    }
    let guard = DirectoryGuard::open(&args.data_dir, args.id, &topology)?;
    let topology = Arc::new(topology);
    let registry = Arc::new(RouteRegistry::new(Arc::clone(&topology)));
    let resolver: Arc<dyn Resolver> = Arc::new(SystemResolver);
    let mut signals = shutdown::install()?;
    let (request, rx) = shutdown::channel();
    let mut actors = Vec::new();
    let mut handles = BTreeMap::new();
    let mut listeners = ListenerTasks(Vec::new());
    let mut route_tasks = JoinSet::new();
    for group in topology.all_groups() {
        let prepared =
            prepare_routes(Arc::clone(&topology), args.id, group, Arc::clone(&resolver)).await?;
        let transport = prepared.transport.clone();
        let mut seed_material = topology.fingerprint().into_bytes();
        seed_material.extend(args.seed.to_be_bytes());
        seed_material.extend(guard.boot.to_be_bytes());
        seed_material.push(args.id);
        seed_material.extend(group.to_be_bytes());
        let seed = u64::from_be_bytes(
            shard_service::sha256(&seed_material)[..8]
                .try_into()
                .expect("fixed digest"),
        );
        let (actor, handle) = Actor::open(
            group,
            args.id,
            topology.genesis_voters[&group].clone(),
            topology.scope(group)?.group_id().to_owned(),
            seed,
            &args.data_dir.join(format!("group-{group}")),
            transport,
            args.snapshot_threshold,
        )?;
        let routes = GroupRoutes::new(
            prepared,
            RouteUpdate {
                term: 0,
                membership: actor.effective_membership().clone(),
                hints: BTreeMap::new(),
            },
            Arc::clone(&registry),
        )?;
        let actor = actor.with_routes(routes.updates.clone());
        let route_shutdown = rx.clone();
        route_tasks.spawn(async move { routes.run(route_shutdown).await });
        let (tx, inbound) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        listeners.0.push(
            spawn_listener_scoped_with_shutdown(
                member.raft[&group],
                tx,
                rx.clone(),
                topology.scope(group)?,
            )
            .await?,
        );
        actors.push((group, actor, inbound));
        handles.insert(group, handle);
        tracing::info!(
            group,
            node = args.id,
            boot = guard.boot,
            seed,
            "group actor opened"
        );
    }
    let http = TcpListener::bind(member.http).await?;
    guard.mark_ready(&args.data_dir)?;
    let service = Service::new(topology, args.id, handles, registry);
    let mut running = JoinSet::new();
    for (group, actor, inbound) in actors {
        let shutdown = rx.clone();
        running.spawn(async move { (group, actor.run(inbound, shutdown).await) });
    }
    let mut http_shutdown = rx.clone();
    let router = service.router();
    let mut server = tokio::spawn(async move {
        axum::serve(http, router)
            .with_graceful_shutdown(async move {
                http_shutdown.requested().await;
            })
            .await
    });
    let mut background = tokio::spawn(Arc::clone(&service).background(rx, !args.manual_handoff));
    tracing::info!(node=args.id,cluster=%service.topology.fingerprint(),http=%member.http,"shard service ready");
    let mut failure = None;
    let mut server_consumed = false;
    let mut background_consumed = false;
    tokio::select! {
        signal=signals.recv()=>{tracing::info!(?signal,"service shutdown requested");if let Err(error)=signal{failure=Some(error);}}
        result=running.join_next()=>{failure=Some(match result{Some(Ok((group,Err(error))))=>io::Error::other(format!("group {group} failed: {error}")),other=>io::Error::other(format!("group actor stopped unexpectedly: {other:?}"))});}
        result=&mut server=>{server_consumed=true;failure=Some(io::Error::other(format!("HTTP server stopped unexpectedly: {result:?}")));}
        result=&mut background=>{background_consumed=true;failure=Some(io::Error::other(format!("coordinator stopped unexpectedly: {result:?}")));}
        result=route_tasks.join_next()=>{failure=Some(io::Error::other(format!("route controller stopped unexpectedly: {result:?}")));}
    }
    request.request();
    // Accepted proposals drain on their actor, followed by actual storage sync.
    // Synchronous durability barriers have no promised wall-clock deadline.
    while let Some(result) = running.join_next().await {
        let error = match result {
            Ok((_, Ok(()))) => None,
            Ok((group, Err(e))) => {
                Some(io::Error::other(format!("group {group} drain failed: {e}")))
            }
            Err(e) => Some(io::Error::other(e)),
        };
        if failure.is_none() {
            failure = error;
        } else if let Some(error) = error {
            tracing::error!(%error,"additional drain failure");
        }
    }
    while !route_tasks.is_empty() {
        match tokio::time::timeout(Duration::from_millis(250), route_tasks.join_next()).await {
            Ok(Some(Ok(Ok(())))) => {}
            Ok(other) => {
                if failure.is_none() {
                    failure = Some(io::Error::other(format!("route cleanup failed: {other:?}")));
                }
            }
            Err(_) => {
                route_tasks.abort_all();
                break;
            }
        }
    }
    for task in &mut listeners.0 {
        match tokio::time::timeout(Duration::from_millis(250), &mut *task).await {
            Err(_) => task.abort(),
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(io::Error::other(error));
                }
            }
            Ok(Ok(())) => {}
        }
    }
    if !server_consumed {
        let result = tokio::time::timeout(Duration::from_millis(250), &mut server).await;
        let error = match result {
            Err(_) => {
                server.abort();
                None
            }
            Ok(Err(e)) => Some(io::Error::other(e)),
            Ok(Ok(Err(e))) => Some(e),
            Ok(Ok(Ok(()))) => None,
        };
        if failure.is_none() {
            failure = error;
        }
    }
    if !background_consumed {
        let result = tokio::time::timeout(Duration::from_millis(250), &mut background).await;
        let error = match result {
            Err(_) => {
                background.abort();
                None
            }
            Ok(Err(e)) => Some(io::Error::other(e)),
            Ok(Ok(())) => None,
        };
        if failure.is_none() {
            failure = error;
        }
    }
    drop(guard);
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
