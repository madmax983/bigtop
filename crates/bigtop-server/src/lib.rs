//! `BigTop` server: `REST` API, state store, scheduler, and persistence.

mod api;
mod auth;
mod ipam;
mod journal;
mod metrics;
mod scheduler;
mod state;

pub use api::root_routes;
pub use ipam::{Ipam, IpamError};
pub use journal::{DirLock, JournalOp, JournalWriter, StoreError};
pub use scheduler::tick;
pub use state::{service_endpoints, AppState, StateInner};

use autumn_web::auth::RequireApiToken;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

/// Errors from running the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The network CIDR was not a usable `/16` base.
    #[error("bad network CIDR: {0}")]
    Network(String),
    /// The data directory could not be opened, locked, or replayed.
    #[error("persistence error: {0}")]
    Persistence(String),
    /// The API token store could not be provisioned.
    #[error("auth setup error: {0}")]
    Auth(String),
}

/// Server configuration: bind address, scheduler tick, task-network base,
/// data directory, and the control-plane bearer token.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Interface to bind, e.g. `"127.0.0.1"`.
    pub bind_host: String,
    /// Port to bind, e.g. `4667`.
    pub bind_port: u16,
    /// Bearer token for the control plane. `None` issues one at startup
    /// and prints it once for the operator to distribute.
    pub api_token: Option<String>,
    /// How often the scheduler ticks.
    pub tick_interval: Duration,
    /// Task network in CIDR form, e.g. `"172.28.0.0/16"` (or plain
    /// `"172.28.0.0"`); the IPAM takes the address before the `/`.
    pub network_cidr: String,
    /// Data directory for the journal and snapshots (v0.4). `None` runs
    /// purely in-memory, as in v0.3 and earlier.
    pub data_dir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 4667,
            api_token: None,
            tick_interval: Duration::from_millis(500),
            network_cidr: "172.28.0.0/16".to_string(),
            data_dir: None,
        }
    }
}

/// Run the server with the default 500 ms scheduler tick, bound to the
/// default `127.0.0.1:4667` (see [`ServerConfig::default`]).
///
/// # Errors
///
/// Returns [`ServerError`] when the configuration, auth setup, or serving
/// fails.
pub async fn serve() -> Result<(), ServerError> {
    serve_with_tick(Duration::from_millis(500)).await
}

/// Run the server with the default bind address, ticking the scheduler
/// every `tick_interval`.
///
/// # Errors
///
/// Returns [`ServerError`] when the configuration, auth setup, or serving
/// fails.
pub async fn serve_with_tick(tick_interval: Duration) -> Result<(), ServerError> {
    serve_config(ServerConfig {
        tick_interval,
        ..ServerConfig::default()
    })
    .await
}

/// Run the server with `config`.
///
/// The `network_cidr` seeds the task-network IPAM; anything after the `/`
/// is ignored, so plain `"172.28.0.0"` works too. When `config.data_dir`
/// Open the persistence data dir (v0.4): take the exclusive lock, load
/// the snapshot, replay the journal. The lock is held for the server's
/// whole lifetime via the returned guard.
///
/// # Errors
///
/// Returns [`ServerError::Persistence`] when the data dir cannot be
/// created/locked, or the snapshot/journal cannot be loaded.
fn load_persistent_state(
    data_dir: Option<&std::path::PathBuf>,
    ipam: Ipam,
) -> Result<(AppState, Option<std::path::PathBuf>, Option<DirLock>), ServerError> {
    let Some(dir) = data_dir else {
        return Ok((AppState::with_ipam(ipam), None, None));
    };
    let dir = journal::ensure_data_dir(dir).map_err(|e| {
        ServerError::Persistence(format!(
            "cannot create data directory {}: {e}",
            dir.display()
        ))
    })?;
    let lock = DirLock::acquire(&dir)
        .map_err(|e| ServerError::Persistence(format!("cannot lock data dir: {e}")))?;
    let mut inner = StateInner {
        ipam,
        ..StateInner::default()
    };
    if let Some(durable) = journal::load_snapshot(&dir)
        .map_err(|e| ServerError::Persistence(format!("cannot load snapshot: {e}")))?
    {
        durable.restore(&mut inner, chrono::Utc::now());
    }
    let ops = journal::load_journal_ops(&dir)
        .map_err(|e| ServerError::Persistence(format!("cannot replay journal: {e}")))?;
    journal::apply_ops(&mut inner, &ops, chrono::Utc::now());
    let journal = journal::open_journal(&dir)
        .map_err(|e| ServerError::Persistence(format!("cannot open journal: {e}")))?;
    inner.journal = Some(journal);
    Ok((AppState::from_inner(inner), Some(dir), Some(lock)))
}

/// Telemetry provider for the `BigTop` server: Autumn's default
/// logging/OTLP initializer, but tolerant of an already-installed global
/// tracing dispatcher.
///
/// The e2e tests boot several servers inside one process (in parallel).
/// `tracing`'s global default can only be installed once per process, so
/// the default provider's `try_init()` fails on the second boot — and
/// Autumn answers that failure with `process::exit(1)`. Tolerating the
/// "already set" case keeps the later servers alive on the first server's
/// logging. In production there is exactly one server per process, so the
/// tolerance never triggers there.
#[derive(Debug, Default, Clone, Copy)]
struct TolerantTelemetryProvider;

impl autumn_web::telemetry::TelemetryProvider for TolerantTelemetryProvider {
    fn init(
        &self,
        log: &autumn_web::config::LogConfig,
        telemetry: &autumn_web::config::TelemetryConfig,
        profile: Option<&str>,
    ) -> Result<autumn_web::telemetry::TelemetryGuard, autumn_web::telemetry::TelemetryInitError>
    {
        use autumn_web::telemetry::{
            TelemetryGuard, TelemetryInitError, TracingOtlpTelemetryProvider,
        };
        match TracingOtlpTelemetryProvider::new().init(log, telemetry, profile) {
            Ok(guard) => Ok(guard),
            Err(TelemetryInitError::SubscriberInit(message))
                if message.contains("already been set") =>
            {
                Ok(TelemetryGuard::disabled())
            }
            Err(other) => Err(other),
        }
    }
}

/// is set, the server journals every mutation there (fsync per op),
/// replays the journal on startup, and compacts to a snapshot on clean
/// shutdown.
///
/// v0.5: the server binds `bind_host:bind_port` itself (Autumn owns the
/// listener), every control-plane route requires the Bearer <redacted>
/// (`config.api_token`, or a generated one printed once at startup), and
/// the `/v1` routes, `OpenAPI`, and an explicit MCP allowlist are served
/// through Autumn. The scheduler tick, state machine, IPAM, and journal
/// stay outside the framework's application logic.
///
/// # Errors
///
/// Returns [`ServerError::Network`] when `network_cidr` does not parse as
/// an IPv4 address or is not a `/16` base network,
/// [`ServerError::Persistence`] when the data directory cannot be opened,
/// locked, or replayed, or [`ServerError::Auth`] when the token store
/// cannot be provisioned. (Autumn owns the listener: a bind conflict is
/// logged and the process exits, so there is no listener error to
/// return.)
pub async fn serve_config(config: ServerConfig) -> Result<(), ServerError> {
    let addr_part: String = config
        .network_cidr
        .split('/')
        .next()
        .ok_or_else(|| ServerError::Network(config.network_cidr.clone()))?
        .to_string();
    let base: Ipv4Addr = addr_part
        .parse()
        .map_err(|_| ServerError::Network(config.network_cidr.clone()))?;
    let ipam = Ipam::new(base)
        .map_err(|e| ServerError::Network(format!("{}: {e}", config.network_cidr)))?;

    let (state, data_dir, lock) = load_persistent_state(config.data_dir.as_ref(), ipam)?;

    // Auth (v0.5): one bearer token gates the whole control plane. When
    // the operator did not configure one, issue it here and print it
    // once — it cannot be recovered later.
    let (store, token, generated) = auth::provision_token_store(config.api_token.clone()).await?;
    if generated {
        println!("bigtop server: generated API token: {token}");
        println!(
            "bigtop server: pass it to agents and CLI clients via --api-token or BIGTOP_API_TOKEN"
        );
    }

    // Scheduler tick: runs outside Autumn (application logic stays out of
    // the framework). The shutdown hook stops it before compacting, in
    // the same order as v0.4's `serve_state`.
    let sched_state = state.clone();
    let tick_interval = config.tick_interval;
    let tick_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        loop {
            interval.tick().await;
            let mut guard = sched_state.inner.write().await;
            let start = std::time::Instant::now();
            if let Err(err) = scheduler::tick(&mut guard, chrono::Utc::now()) {
                eprintln!("bigtop: scheduler tick failed: {err}");
            }
            guard.last_tick_ms =
                Some(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX));
        }
    });
    let tick_handle = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tick_handle)));

    // The sixteen typed `/v1` handlers take `Extension<AppState>` (axum's
    // extractor, reading request extensions — not router state), so the
    // scope's layer stack must install it. `ServiceBuilder::layer` adds each
    // layer outside the previous ones: the token layer is added last so it
    // is outermost (a bad/missing token gets `401` before anything else
    // runs), and the `Extension` sits innermost, closest to the handlers.
    // MCP `tools/call` replays through this same router, so dispatched tool
    // calls receive the extension too.
    let v1_layers = tower::ServiceBuilder::new()
        .layer(axum::Extension(state.clone()))
        .layer(RequireApiToken::new(std::sync::Arc::clone(&store)))
        .into_inner();

    let app = autumn_web::app()
        .with_config_loader(ServerBind {
            host: config.bind_host.clone(),
            port: config.bind_port,
        })
        // The framework's own routes (`/openapi.json`, `/swagger-ui`) are
        // mounted outside every user scope, so the per-scope token layers
        // don't cover them. This global layer closes that hole: the whole
        // control plane — REST, metrics, status page, docs, MCP — answers
        // `401` without a valid bearer token. Double-gating the
        // already-scoped routes is harmless (same token, same store).
        .layer(RequireApiToken::new(std::sync::Arc::clone(&store)))
        // `Extension<AppState>` for every typed handler. The `/v1` scope
        // also installs it innermost in its own layer stack (needed for MCP
        // `tools/call` replays through the scoped router); installing it
        // globally too is harmless and covers the root routes below.
        .layer(axum::Extension(state.clone()))
        // The two edge routes (`/`, `/metrics`) as typed Autumn routes.
        // Autumn's startup validation only counts `.routes()`
        // registrations — a scoped-only app panics at boot with "No routes
        // registered" — so these live here rather than on a merged raw
        // Axum router.
        .routes(api::root_routes())
        // Default logging, but tolerant of an already-installed global
        // tracing dispatcher (the e2e tests boot several servers in one
        // process; see `TolerantTelemetryProvider`).
        .with_telemetry_provider(TolerantTelemetryProvider)
        .scoped("/v1", v1_layers, api::autumn_routes())
        .mount_mcp("/mcp")
        .secure_mcp(RequireApiToken::new(std::sync::Arc::clone(&store)))
        .openapi(autumn_web::openapi::OpenApiConfig::new(
            "BigTop",
            // The workspace releases are versioned in CHANGELOG.md; the
            // crate itself stays at 0.1.0.
            "0.5",
        ))
        .on_shutdown({
            let tick_handle = std::sync::Arc::clone(&tick_handle);
            let state = state.clone();
            let data_dir = data_dir.clone();
            move || {
                let tick_handle = std::sync::Arc::clone(&tick_handle);
                let state = state.clone();
                let data_dir = data_dir.clone();
                async move {
                    // Stop the scheduler tick before compacting:
                    // otherwise a tick could journal an op between the
                    // snapshot capture and the journal truncate, and the
                    // op would be lost. A tick in flight runs to
                    // completion first (it holds no await points
                    // mid-mutation), so the state captured below is
                    // quiescent.
                    let handle = {
                        let mut guard = tick_handle.lock().await;
                        guard.take()
                    };
                    if let Some(handle) = handle {
                        handle.abort();
                        let _ = handle.await;
                    }
                    // Clean shutdown: compact the journal into a snapshot
                    // so the next boot replays at most the ops written
                    // after the snapshot.
                    if let Some(dir) = data_dir {
                        let guard = state.inner.read().await;
                        let durable = journal::DurableState::capture(&guard);
                        drop(guard);
                        if let Err(err) = journal::compact(&dir, &durable) {
                            eprintln!("bigtop: shutdown compaction failed: {err}");
                        }
                    }
                }
            }
        });

    // Hold the data-dir lock for the server's whole lifetime.
    let _lock = lock;
    app.run().await;
    Ok(())
}

/// Bind address for Autumn's runtime, from [`ServerConfig`].
#[derive(Debug, Clone)]
struct ServerBind {
    host: String,
    port: u16,
}

impl autumn_web::config::ConfigLoader for ServerBind {
    fn load(
        &self,
    ) -> impl std::future::Future<
        Output = Result<autumn_web::config::AutumnConfig, autumn_web::config::ConfigError>,
    > + Send {
        let host = self.host.clone();
        let port = self.port;
        async move {
            let mut config = autumn_web::config::AutumnConfig::default();
            config.server.host = host;
            config.server.port = port;
            Ok(config)
        }
    }
}
