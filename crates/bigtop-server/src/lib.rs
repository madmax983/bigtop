//! `BigTop` server: `REST` API, in-memory store, and the scheduler.

mod api;
mod ipam;
mod scheduler;
mod state;

pub use api::router;
pub use ipam::{Ipam, IpamError};
pub use scheduler::tick;
pub use state::{AppState, StateInner};

use std::net::Ipv4Addr;
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
}

/// Server configuration: scheduler tick plus the task-network base.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// How often the scheduler ticks.
    pub tick_interval: Duration,
    /// Task network in CIDR form, e.g. `"172.28.0.0/16"` (or plain
    /// `"172.28.0.0"`); the IPAM takes the address before the `/`.
    pub network_cidr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(500),
            network_cidr: "172.28.0.0/16".to_string(),
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
/// is ignored, so plain `"172.28.0.0"` works too.
///
/// # Errors
///
/// Returns [`ServerError::Network`] when `network_cidr` does not parse as
/// an IPv4 address or is not a `/16` base network, or
/// [`ServerError::Listener`] if serving fails.
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
    let state = AppState::with_ipam(ipam);
    let sched_state = state.clone();
    let tick_interval = config.tick_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        loop {
            interval.tick().await;
            let mut guard = sched_state.inner.write().await;
            scheduler::tick(&mut guard, chrono::Utc::now());
        }
    });
    axum::serve(listener, router(state)).await?;
    Ok(())
}
