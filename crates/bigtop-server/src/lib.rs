//! `BigTop` server: `REST` API, in-memory store, and the scheduler.

mod api;
mod scheduler;
mod state;

pub use api::router;
pub use scheduler::tick;
pub use state::{AppState, StateInner};

use std::time::Duration;

/// Errors from running the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The TCP listener failed.
    #[error("listener error: {0}")]
    Listener(#[from] std::io::Error),
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
    let state = AppState::new();
    let sched_state = state.clone();
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
