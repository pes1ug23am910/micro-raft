//! Command-line entry point for a persistent micro-raft node.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use raft_core::NodeId;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::EnvFilter;

use kv_node::config::{
    validate, Config, ResolvedConfig, Resolver, SystemResolver, ValidatedConfig,
};
use kv_node::driver::{
    run_driver_with_snapshots, BatchOptions, SnapshotRuntime, PROPOSAL_CHANNEL_CAPACITY,
    READ_CHANNEL_CAPACITY,
};
use kv_node::http::ApiState;
use kv_node::kv::SharedReadState;
use kv_node::shutdown;
use kv_node::transport::{
    spawn_listener_scoped_with_shutdown, spawn_listener_with_shutdown, TcpTransport,
    TransportScope, INBOUND_QUEUE_CAPACITY,
};

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
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .event_format(NodePrefix {
            prefix: format!("[n{id}]"),
            inner: tracing_subscriber::fmt::format(),
        })
        .init();
}

#[tokio::main]
async fn main() {
    let args = Config::parse();
    init_tracing(args.id);
    let cfg = match validate(&args) {
        Ok(cfg) => Arc::new(cfg),
        Err(error) => {
            tracing::error!(%error, "invalid config");
            std::process::exit(2);
        }
    };
    let resolver: Arc<dyn Resolver> = Arc::new(SystemResolver);
    let resolved = if cfg.check_config || cfg.group_id.is_none() {
        match cfg.resolve_startup(resolver.as_ref()).await {
            Ok(resolved) => Some(resolved),
            Err(error) => {
                tracing::error!(%error, "endpoint resolution failed");
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    if cfg.check_config {
        let resolved = resolved
            .as_ref()
            .expect("diagnostics resolve strictly before storage");
        let peers: Vec<_> = cfg
            .peers
            .iter()
            .map(|peer| {
                let candidates = resolved
                    .peer_addrs
                    .iter()
                    .find(|(id, _)| *id == peer.id)
                    .map(|(_, addresses)| addresses);
                serde_json::json!({
                    "id": peer.id,
                    "endpoint": peer.endpoint.to_string(),
                    "candidates": candidates,
                })
            })
            .collect();
        let output = serde_json::json!({
            "schema_version": 1,
            "id": cfg.id,
            "group_id": cfg.group_id,
            "genesis_voters": cfg.genesis_voters,
            "migrate_legacy_group": cfg.migrate_legacy_group,
            "mode": if cfg.legacy { "legacy" } else { "explicit" },
            "snapshot_threshold": cfg.snapshot_threshold,
            "batch_records": cfg.batch_records,
            "batch_delay_ms": cfg.batch_delay_ms,
            "state_backend": cfg.state_backend,
            "raft_listen": cfg.raft_listen,
            "raft_advertise": cfg.raft_advertise.to_string(),
            "raft_candidates": resolved.raft_advertise,
            "http_listen": cfg.http_listen,
            "http_advertise": cfg.http_advertise.to_string(),
            "http_candidates": resolved.http_advertise,
            "peers": peers,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&output).expect("JSON values serialize")
        );
        return;
    }
    if let Err(error) = run(cfg, resolved, resolver).await {
        tracing::error!(%error, "node stopped");
        std::process::exit(1);
    }
}

async fn run(
    cfg: Arc<ValidatedConfig>,
    resolved: Option<ResolvedConfig>,
    resolver: Arc<dyn Resolver>,
) -> io::Result<()> {
    // Register before storage/listeners: an unavailable signal source is a
    // startup error rather than an apparently running node without a stop path.
    let mut signals = shutdown::install()?;
    let (shutdown_handle, shutdown_rx) = shutdown::channel();
    let seed: u64 = rand::random();
    let kv_node::bootstrap::Bootstrapped {
        node,
        storage,
        image,
        application_store,
        application,
    } = kv_node::bootstrap::recover(&cfg, seed)?;
    info!(id = cfg.id, term = node.hard.current_term, recovered_entries = node.log.len(),
        data_dir = %cfg.data_dir.display(), "durable state recovered");
    let seed_config = cfg.clone();
    let (cfg, resolved, route_error) = match resolved {
        Some(resolved) => (cfg, resolved, None),
        None => {
            let routes = kv_node::membership_runtime::startup_routes(
                &cfg,
                node.effective_membership(),
                resolver.as_ref(),
            )
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            (routes.config, routes.resolved, routes.error)
        }
    };
    if let Some(error) = &route_error {
        tracing::warn!(%error,"explicit group starts with degraded peer routing");
    }
    let shared =
        SharedReadState::from_application(&node, application)?.with_route_error(route_error);
    let scope =
        if node.committed_membership().state.group_id != raft_core::membership::LEGACY_GROUP_ID {
            Some(TransportScope::new(
                node.committed_membership().state.group_id.clone(),
                node.committed_membership().state.genesis_voters.clone(),
            )?)
        } else {
            None
        };
    let routes = kv_node::membership_runtime::RouteController::new(
        cfg.clone(),
        resolved.clone(),
        resolver.clone(),
        node.effective_membership().clone(),
    )
    .with_seed(seed_config);
    let (admin_tx, admin_rx) = tokio::sync::mpsc::channel(kv_node::admin::ADMIN_CHANNEL_CAPACITY);
    let snapshots = SnapshotRuntime::new(&cfg.data_dir, image, cfg.snapshot_threshold)?
        .with_batching(BatchOptions {
            records: cfg.batch_records as usize,
            delay: std::time::Duration::from_millis(cfg.batch_delay_ms),
        })?
        .with_application_store(application_store)
        .with_administration(admin_rx, routes.updates.clone());
    let (proposal_tx, proposal_rx) = tokio::sync::mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
    let (read_tx, read_rx) = tokio::sync::mpsc::channel(READ_CHANNEL_CAPACITY);
    let http_listener = TcpListener::bind(cfg.http_listen).await?;
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let mut raft_listener = match scope.clone() {
        Some(scope) => {
            spawn_listener_scoped_with_shutdown(
                cfg.raft_listen,
                inbound_tx,
                shutdown_rx.clone(),
                scope,
            )
            .await?
        }
        None => {
            spawn_listener_with_shutdown(cfg.raft_listen, inbound_tx, shutdown_rx.clone()).await?
        }
    };
    let transport = match scope {
        Some(scope) => {
            TcpTransport::spawn_configured_scoped(cfg.clone(), resolver, resolved, scope)?
        }
        None => TcpTransport::spawn_configured(cfg.clone(), resolver, resolved),
    };
    let validator = routes.validator.clone();
    let mut route_task =
        tokio::spawn(routes.run(transport.clone(), shared.clone(), shutdown_rx.clone()));
    info!(
        seed,
        raft_listen = %cfg.raft_listen,
        raft_advertise = %cfg.raft_advertise,
        http_listen = %cfg.http_listen,
        http_advertise = %cfg.http_advertise,
        "node started"
    );
    let api = ApiState::new(shared.clone(), proposal_tx)
        .with_reads(read_tx)
        .with_administration(admin_tx, validator);
    let mut http_task = tokio::spawn(kv_node::http::serve_with_shutdown(
        http_listener,
        api,
        shutdown_rx.clone(),
    ));
    let mut driver = tokio::spawn(run_driver_with_snapshots(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        read_rx,
        shared,
        shutdown_rx,
        snapshots,
    ));
    let mut driver_result = None;
    let mut first_error = None;
    let mut http_finished = false;
    let mut peer_finished = false;
    let mut routes_finished = false;
    tokio::select! {
        signal = signals.recv() => match signal {
            Ok(reason) => info!(?reason, "shutdown signal received"),
            Err(error) => first_error = Some(error),
        },
        result = &mut driver => driver_result = Some(result),
        result = &mut http_task => {
            http_finished = true;
            first_error = Some(match result {
                Ok(Ok(())) => io::Error::new(io::ErrorKind::UnexpectedEof, "HTTP server stopped"),
                Ok(Err(error)) => error,
                Err(error) => io::Error::other(format!("HTTP server failed: {error}")),
            });
        }
        result = &mut route_task => {
            routes_finished = true;
            first_error = Some(match result {
                Ok(()) => io::Error::new(io::ErrorKind::UnexpectedEof, "routing controller stopped before shutdown"),
                Err(error) => io::Error::other(format!("routing controller failed: {error}")),
            });
        }
        result = &mut raft_listener => {
            peer_finished = true;
            first_error = Some(match result {
                Ok(()) => io::Error::new(io::ErrorKind::UnexpectedEof, "Raft listener stopped"),
                Err(error) => io::Error::other(format!("Raft listener failed: {error}")),
            });
        }
    }
    let requested_at = shutdown_handle.request();
    // Never abort the writer: its effect batch and required final sync must
    // finish. Its own deadline bounds quorum waiting, not blocked filesystem I/O.
    let result = match driver_result {
        Some(result) => result,
        None => driver.await,
    };
    match result {
        Ok(Ok(report)) => info!(
            ?report,
            elapsed_ms = requested_at.elapsed().as_millis(),
            "durable writer stopped"
        ),
        Ok(Err(error)) => {
            tracing::error!(%error, "durable writer failed during shutdown");
            first_error.get_or_insert(error);
        }
        Err(error) => {
            first_error.get_or_insert_with(|| {
                io::Error::other(format!("durable writer task failed: {error}"))
            });
        }
    }
    // Axum can retain a slow request-body connection after its server future
    // is aborted. This binary finishes runtime teardown after this shared
    // cleanup budget; it does not promise reusable in-runtime HTTP task cleanup.
    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    if !routes_finished {
        match tokio::time::timeout_at(cleanup_deadline, &mut route_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert_with(|| {
                    io::Error::other(format!("routing controller cleanup failed: {error}"))
                });
            }
            Err(_) => {
                route_task.abort();
                let _ = route_task.await;
            }
        }
    }
    if !peer_finished {
        match tokio::time::timeout_at(cleanup_deadline, &mut raft_listener).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert_with(|| {
                    io::Error::other(format!("peer listener cleanup failed: {error}"))
                });
            }
            Err(_) => {
                raft_listener.abort();
                let _ = raft_listener.await;
            }
        }
    }
    if !http_finished {
        match tokio::time::timeout_at(cleanup_deadline, &mut http_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                first_error.get_or_insert(error);
            }
            Ok(Err(error)) => {
                first_error.get_or_insert_with(|| {
                    io::Error::other(format!("HTTP cleanup failed: {error}"))
                });
            }
            Err(_) => {
                http_task.abort();
                let _ = http_task.await;
            }
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}
