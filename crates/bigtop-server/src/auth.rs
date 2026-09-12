//! Bearer-token auth for the control plane (v0.5).
//!
//! The server provisions one [`InMemoryApiTokenStore`]: either seeded with
//! the operator's token (`--api-token` / `BIGTOP_API_TOKEN`) or, when none
//! is configured, issued fresh at startup and printed once so the operator
//! can hand it to agents and CLI clients. Every control-plane route — HTTP
//! and MCP alike — goes through Autumn's [`RequireApiToken`] layer, so the
//! first request without a valid `Authorization: Bearer <token>` header
//! gets a `401`.

use std::sync::Arc;

use autumn_web::auth::{ApiTokenStore, InMemoryApiTokenStore};

use crate::ServerError;

/// Provision the API token store for this server run.
///
/// Returns the store and the raw token the operator should distribute.
/// When `provided` is `None` a token is issued at startup and must be
/// printed once — it cannot be recovered later.
///
/// # Errors
///
/// Returns [`ServerError::Auth`] when token issuance fails.
pub async fn provision_token_store(
    provided: Option<String>,
) -> Result<(Arc<InMemoryApiTokenStore>, String, bool), ServerError> {
    if let Some(raw) = provided {
        let store = InMemoryApiTokenStore::default().with_token(&raw, "bigtop-operator");
        Ok((Arc::new(store), raw, false))
    } else {
        let store = InMemoryApiTokenStore::default();
        let raw = store
            .issue("bigtop-operator")
            .await
            .map_err(|err| ServerError::Auth(format!("issuing API token: {err}")))?;
        Ok((Arc::new(store), raw, true))
    }
}
