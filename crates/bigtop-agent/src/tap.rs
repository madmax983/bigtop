//! Host TAP devices for guest networking.
//!
//! When a task's `[network]` is enabled, the agent creates a TAP device on
//! the host (named [`tap_name_for`](bigtop_core::tap_name_for) the task id)
//! before the microVM boots and hands it to Firecracker as `host_dev_name`
//! in `PUT /network-interfaces/eth0`. The executor destroys the device
//! after the task's terminal state is reported; a failed boot destroys it
//! in [`FirecrackerRuntime::spawn`](crate::FirecrackerRuntime).
//!
//! Everything here shells out to `ip` from iproute2 and needs
//! `CAP_NET_ADMIN` (or root). Without either, every operation returns
//! [`AgentError::Network`](crate::AgentError::Network): no panics, no
//! silent skips on unprivileged hosts.

use crate::AgentError;
use std::path::Path;
use tokio::process::Command;

/// A host TAP device created for one task's guest.
#[derive(Debug)]
pub struct TapDevice {
    name: String,
}

impl TapDevice {
    /// The device name, e.g. the [`tap_name_for`](bigtop_core::tap_name_for)
    /// value it was created with.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create a TAP device: `ip tuntap add dev <name> mode tap`.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when
    /// `ip` is missing or the call fails (e.g. `EPERM` without
    /// `CAP_NET_ADMIN`).
    pub async fn create(name: &str) -> Result<Self, AgentError> {
        run_ip(&["tuntap", "add", "dev", name, "mode", "tap"]).await?;
        Ok(Self {
            name: name.to_string(),
        })
    }

    /// Bring the device up: `ip link set dev <name> up`.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when
    /// `ip` is missing or the call fails.
    pub async fn set_up(&self) -> Result<(), AgentError> {
        run_ip(&["link", "set", "dev", &self.name, "up"]).await
    }

    /// Move the device into a network namespace:
    /// `ip link set dev <name> netns <path>`.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when
    /// `ip` is missing or the call fails.
    pub async fn move_to_netns(&self, netns: &Path) -> Result<(), AgentError> {
        let netns = netns.to_string_lossy();
        run_ip(&["link", "set", "dev", &self.name, "netns", &netns]).await
    }

    /// Delete the device: `ip link delete dev <name>`.
    ///
    /// Consumes the handle so a destroyed device cannot be reused.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when
    /// `ip` is missing or the call fails.
    pub async fn destroy(self) -> Result<(), AgentError> {
        let Self { name } = self;
        run_ip(&["link", "delete", "dev", &name]).await
    }
}

/// Run `ip <args>`, mapping *any* failure (missing binary, non-zero exit)
/// to [`AgentError::Network`](crate::AgentError::Network) with the stderr
/// and the privilege hint attached.
async fn run_ip(args: &[&str]) -> Result<(), AgentError> {
    let output = Command::new("ip")
        .args(args)
        .output()
        .await
        .map_err(|e| network_err(&format!("failed to run `ip`: {e}")))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        Err(network_err(&format!(
            "`ip {}` failed: {stderr}",
            args.join(" ")
        )))
    }
}

/// Wrap `detail` in the network error with the privilege hint the host
/// setup docs point at.
fn network_err(detail: &str) -> AgentError {
    AgentError::Network(format!(
        "{detail}; needs CAP_NET_ADMIN (or root) and iproute2; see README host setup"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_fails_cleanly_without_privilege() {
        // This sandbox is unprivileged: `ip tuntap add` must come back as a
        // clean `Err`, never a panic. (Do not assert on *why* it failed:
        // EPERM vs. missing binary is host-dependent.)
        let err = TapDevice::create("bigtop-test-tap0")
            .await
            .expect_err("tap creation needs CAP_NET_ADMIN");
        let message = err.to_string();
        assert!(!message.is_empty(), "error message must not be empty");
        assert!(
            message.contains("network setup failed"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn set_up_fails_cleanly_on_missing_device() {
        // `create` failed above, so drive the error path through a device
        // that was never created.
        let tap = TapDevice {
            name: "bigtop-test-tap-never-created".to_string(),
        };
        let err = tap
            .set_up()
            .await
            .expect_err("set_up on a missing device must fail");
        assert_ne!(err.to_string(), "");
    }
}
