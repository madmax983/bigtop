//! `BigTop` server: `REST` API, state store, scheduler, and persistence.

mod api;
mod ipam;
mod journal;
mod metrics;
mod scheduler;
mod state;

pub use api::router;
pub use ipam::{Ipam, IpamError};
pub use journal::{DirLock, JournalOp, JournalWriter, StoreError};
pub use scheduler::tick;
pub use state::{service_endpoints, AppState, StateInner};

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

/// Errors from running the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The TCP listener failed.
    #[error("listener error: {0}")]
    Listener(#[from] std::io::Error),
    /// The network CIDR was not a usable `/16` base.
    #[error("bad network CIDR: {0}")]
    Network(String),
    /// The data directory could not be opened, locked, or replayed.
    #[error("persistence error: {0}")]
    Persistence(String),
}

/// Server configuration: scheduler tick, task-network base, data directory.
#[derive(Debug, Clone)]
pub struct ServerConfig {
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
            tick_interval: Duration::from_millis(500),
            network_cidr: "172.28.0.0/16".to_string(),
            data_dir: None,
        }
    }
}

/// Run the server on `listener` with the default 500 ms scheduler tick.
///
/// # Errors
///
/// Returns [`ServerError::Listener`] if serving fails.
pub async fn serve(listener: tokio::net::TcpListener) -> Result<(), ServerError> {
    serve_with_tick(listener, Duration::from_millis(500)).await
}

/// Run the server on `listener`, ticking the scheduler every `tick_interval`.
///
/// # Errors
///
/// Returns [`ServerError::Listener`] if serving fails.
pub async fn serve_with_tick(
    listener: tokio::net::TcpListener,
    tick_interval: Duration,
) -> Result<(), ServerError> {
    serve_config(
        listener,
        ServerConfig {
            tick_interval,
            ..ServerConfig::default()
        },
    )
    .await
}

/// Run the server on `listener` with `config`.
///
/// The `network_cidr` seeds the task-network IPAM; anything after the `/`
/// is ignored, so plain `"172.28.0.0"` works too. When `config.data_dir`
/// is set, the server journals every mutation there (fsync per op),
/// replays the journal on startup, and compacts to a snapshot on clean
/// shutdown.
///
/// # Errors
///
/// Returns [`ServerError::Network`] when `network_cidr` does not parse as
/// an IPv4 address or is not a `/16` base network,
/// [`ServerError::Persistence`] when the data directory cannot be opened,
/// locked, or replayed, or [`ServerError::Listener`] if serving fails.
pub async fn serve_config(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
) -> Result<(), ServerError> {
    let addr_part = config
        .network_cidr
        .split('/')
        .next()
        .ok_or_else(|| ServerError::Network(config.network_cidr.clone()))?;
    let base: Ipv4Addr = addr_part
        .parse()
        .map_err(|_| ServerError::Network(config.network_cidr.clone()))?;
    let ipam = Ipam::new(base)
        .map_err(|e| ServerError::Network(format!("{}: {e}", config.network_cidr)))?;

    // Persistence (v0.4): open the data dir, take the exclusive lock, load
    // the snapshot, replay the journal. The lock is held for the server's
    // whole lifetime via `lock`.
    let (state, data_dir, lock) = match &config.data_dir {
        None => (AppState::with_ipam(ipam), None, None),
        Some(dir) => {
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
            (AppState::from_inner(inner), Some(dir), Some(lock))
        }
    };
    serve_state(listener, state, config.tick_interval, data_dir, lock).await
}

/// Serve with a fully-built state; compact on clean shutdown when a data
/// directory is in use. `_lock` pins the data-dir lock for the run.
async fn serve_state(
    listener: tokio::net::TcpListener,
    state: AppState,
    tick_interval: Duration,
    data_dir: Option<PathBuf>,
    _lock: Option<DirLock>,
) -> Result<(), ServerError> {
    let sched_state = state.clone();
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
    axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // Stop the scheduler tick before compacting: otherwise a tick could
    // journal an op between the snapshot capture and the journal
    // truncate, and the op would be lost. A tick in flight runs to
    // completion first (it holds no await points mid-mutation), so the
    // state captured below is quiescent.
    tick_handle.abort();
    let _ = tick_handle.await;
    // Clean shutdown: compact the journal into a snapshot so the next boot
    // replays at most the ops written after the snapshot.
    if let Some(dir) = data_dir {
        let guard = state.inner.read().await;
        let durable = journal::DurableState::capture(&guard);
        drop(guard);
        if let Err(err) = journal::compact(&dir, &durable) {
            eprintln!("bigtop: shutdown compaction failed: {err}");
        }
    }
    Ok(())
}

/// Wait for Ctrl-C (or SIGTERM on Unix). If a handler cannot be
/// installed, that signal is simply not watched.
async fn shutdown_signal() {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(err) => {
                eprintln!("bigtop: ctrl-c handler failed: {err}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                eprintln!("bigtop: sigterm handler failed: {err}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
