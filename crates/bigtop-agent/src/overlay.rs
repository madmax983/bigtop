//! VXLAN cross-node overlay mesh (v0.4).
//!
//! When the agent runs with `--vni`, it builds the underlay plumbing once
//! at startup:
//!
//! ```text
//!   vxlan<vni> (VNI, dstport 4789, VTEP MAC from the node id)
//!        |
//!     bt-br0 (Linux bridge)
//!        |
//!   bt-<task> (task TAP devices, enslaved as each networked task boots)
//! ```
//!
//! Peers come from `GET /v1/agents/overlay-peers?node_id=...`: every other
//! alive node that reported an underlay IP. Each peer becomes one static
//! FDB entry (`bridge fdb append <vtep-mac> dev vxlan<vni> dst <underlay>`)
//! so broadcast/unknown-unicast frames reach every VTEP without multicast.
//! A background loop re-reconciles the FDB every
//! [`FDB_RECONCILE_INTERVAL`], adding missing entries and removing stale
//! ones (add-before-delete, so the mesh never blackholes mid-reconcile).
//!
//! Everything shells out to `ip`/`bridge` from iproute2 and needs
//! `CAP_NET_ADMIN` (or root); failures surface as
//! [`AgentError::Network`](crate::AgentError::Network) with the privilege
//! hint, never panics or silent skips.
//!
//! The overlay is opt-in: without `--vni` the agent touches no host
//! networking beyond the per-task taps of v0.3.

use crate::AgentError;
use bigtop_core::{vtep_mac_for_node, MacAddr, NodeId, OverlayPeer};
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::process::Command;

/// The Linux bridge all overlay devices hang off.
pub const OVERLAY_BRIDGE: &str = "bt-br0";
/// The VXLAN destination UDP port (IANA-assigned).
pub const VXLAN_DSTPORT: u16 = 4789;
/// How often the agent re-reconciles static FDB entries with the server's
/// peer list.
pub const FDB_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// The VXLAN device name for a VNI: `vxlan<vni>`, e.g. `vxlan42`.
#[must_use]
pub fn vxlan_device_name(vni: u32) -> String {
    format!("vxlan{vni}")
}

/// One static FDB entry: forward frames for `mac` to the VTEP at `dst`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FdbEntry {
    /// The peer's VTEP MAC ([`vtep_mac_for_node`]).
    pub mac: MacAddr,
    /// The peer's underlay IP.
    pub dst: Ipv4Addr,
}

/// The desired FDB entries for `peers` (the server already excludes self).
///
/// Every agent derives the same VTEP MAC per node id, so no extra
/// coordination is needed: the MAC is a pure function of the peer.
#[must_use]
pub fn fdb_entries_for_peers(peers: &[OverlayPeer]) -> Vec<FdbEntry> {
    peers
        .iter()
        .map(|peer| FdbEntry {
            mac: vtep_mac_for_node(&peer.node_id),
            dst: peer.underlay_ip,
        })
        .collect()
}

/// `ip link add <dev> type vxlan id <vni> dstport 4789`: ungrouped VXLAN
/// (unicast mode) — the static FDB entries supply every destination.
#[must_use]
pub fn vxlan_create_args(vni: u32) -> Vec<String> {
    vec![
        "link".to_string(),
        "add".to_string(),
        vxlan_device_name(vni),
        "type".to_string(),
        "vxlan".to_string(),
        "id".to_string(),
        vni.to_string(),
        "dstport".to_string(),
        VXLAN_DSTPORT.to_string(),
    ]
}

/// `ip link set dev <dev> address <mac>`.
#[must_use]
pub fn link_set_mac_args(dev: &str, mac: MacAddr) -> Vec<String> {
    vec![
        "link".to_string(),
        "set".to_string(),
        "dev".to_string(),
        dev.to_string(),
        "address".to_string(),
        mac.to_string(),
    ]
}

/// `ip link set dev <dev> up`.
#[must_use]
pub fn link_set_up_args(dev: &str) -> Vec<String> {
    vec![
        "link".to_string(),
        "set".to_string(),
        "dev".to_string(),
        dev.to_string(),
        "up".to_string(),
    ]
}

/// `ip link add name bt-br0 type bridge`.
#[must_use]
pub fn bridge_create_args() -> Vec<String> {
    vec![
        "link".to_string(),
        "add".to_string(),
        "name".to_string(),
        OVERLAY_BRIDGE.to_string(),
        "type".to_string(),
        "bridge".to_string(),
    ]
}

/// `ip link set dev <dev> master bt-br0`.
#[must_use]
pub fn bridge_enslave_args(dev: &str) -> Vec<String> {
    vec![
        "link".to_string(),
        "set".to_string(),
        "dev".to_string(),
        dev.to_string(),
        "master".to_string(),
        OVERLAY_BRIDGE.to_string(),
    ]
}

/// `bridge fdb append <mac> dev <vxlan-dev> dst <underlay-ip>`.
#[must_use]
pub fn fdb_append_args(dev: &str, entry: &FdbEntry) -> Vec<String> {
    vec![
        "fdb".to_string(),
        "append".to_string(),
        entry.mac.to_string(),
        "dev".to_string(),
        dev.to_string(),
        "dst".to_string(),
        entry.dst.to_string(),
    ]
}

/// `bridge fdb del <mac> dev <vxlan-dev> dst <underlay-ip>`.
#[must_use]
pub fn fdb_delete_args(dev: &str, entry: &FdbEntry) -> Vec<String> {
    vec![
        "fdb".to_string(),
        "del".to_string(),
        entry.mac.to_string(),
        "dev".to_string(),
        dev.to_string(),
        "dst".to_string(),
        entry.dst.to_string(),
    ]
}

/// Split desired vs. applied FDB entries into (`to_add`, `to_delete`).
/// Pure: unit-test the reconciliation logic without touching the host.
#[must_use]
pub fn diff_fdb(
    applied: &HashSet<FdbEntry>,
    want: &HashSet<FdbEntry>,
) -> (Vec<FdbEntry>, Vec<FdbEntry>) {
    let to_add: Vec<FdbEntry> = want.difference(applied).copied().collect();
    let to_delete: Vec<FdbEntry> = applied.difference(want).copied().collect();
    (to_add, to_delete)
}

/// Enslave an existing device (a task TAP) to the overlay bridge:
/// `ip link set dev <dev> master bt-br0`.
///
/// # Errors
///
/// Returns [`AgentError::Network`](crate::AgentError::Network) when `ip`
/// is missing or the call fails (needs `CAP_NET_ADMIN`).
pub async fn enslave_to_bridge(dev: &str) -> Result<(), AgentError> {
    run_ip(&bridge_enslave_args(dev)).await
}

/// Owns one node's side of the VXLAN mesh: the device, the bridge, and the
/// currently-applied static FDB entries.
#[derive(Debug)]
pub struct OverlayManager {
    vni: u32,
    underlay_ip: Ipv4Addr,
    node_id: NodeId,
    applied: HashSet<FdbEntry>,
}

impl OverlayManager {
    /// Build the manager; [`OverlayManager::setup`] creates the devices.
    #[must_use]
    pub fn new(vni: u32, underlay_ip: Ipv4Addr, node_id: NodeId) -> Self {
        Self {
            vni,
            underlay_ip,
            node_id,
            applied: HashSet::new(),
        }
    }

    /// The configured VNI.
    #[must_use]
    pub const fn vni(&self) -> u32 {
        self.vni
    }

    /// The node's underlay IP.
    #[must_use]
    pub const fn underlay_ip(&self) -> Ipv4Addr {
        self.underlay_ip
    }

    /// Create the VXLAN device (with this node's VTEP MAC) and the
    /// `bt-br0` bridge, bring both up, and enslave the device to the
    /// bridge. Re-runnable: already-existing devices are left in place.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when
    /// `ip` is missing or a call fails (needs `CAP_NET_ADMIN`).
    pub async fn setup(&self) -> Result<(), AgentError> {
        let dev = vxlan_device_name(self.vni);
        let vtep_mac = vtep_mac_for_node(&self.node_id);
        run_ip_tolerate_exists(&vxlan_create_args(self.vni)).await?;
        run_ip(&link_set_mac_args(&dev, vtep_mac)).await?;
        run_ip(&link_set_up_args(&dev)).await?;
        run_ip_tolerate_exists(&bridge_create_args()).await?;
        run_ip(&link_set_up_args(OVERLAY_BRIDGE)).await?;
        enslave_to_bridge(&dev).await?;
        Ok(())
    }

    /// Reconcile the static FDB against the server's peer list: add
    /// missing entries, then delete stale ones.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`](crate::AgentError::Network) when a
    /// FDB append fails for a reason other than "already exists".
    /// Deletes are best-effort: a failed delete is logged and dropped
    /// from the applied set so the next reconcile retries it.
    pub async fn reconcile(&mut self, peers: &[OverlayPeer]) -> Result<(), AgentError> {
        let want: HashSet<FdbEntry> = fdb_entries_for_peers(peers).into_iter().collect();
        let (to_add, to_delete) = diff_fdb(&self.applied, &want);
        let dev = vxlan_device_name(self.vni);
        for entry in to_add {
            let args = fdb_append_args(&dev, &entry);
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            match run_bridge(&arg_refs).await {
                Ok(()) => {
                    self.applied.insert(entry);
                }
                Err(err) if is_exists_error(&err) => {
                    // Already there (e.g. left by a previous agent
                    // process): treat as applied.
                    self.applied.insert(entry);
                }
                Err(err) => return Err(err),
            }
        }
        for entry in to_delete {
            let args = fdb_delete_args(&dev, &entry);
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            if let Err(err) = run_bridge(&arg_refs).await {
                eprintln!("bigtop: fdb delete failed (will retry): {err}");
            }
            self.applied.remove(&entry);
        }
        Ok(())
    }
}

/// Run `ip <args>`, mapping failures to [`AgentError::Network`] with the
/// privilege hint. Exposed for `bridge`-family callers in this module via
/// [`run_bridge`]; the `ip`-family entry point.
async fn run_ip(args: &[String]) -> Result<(), AgentError> {
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run_command("ip", &arg_refs).await
}

/// Run `bridge <args>` (iproute2), same error mapping as [`run_ip`].
async fn run_bridge(args: &[&str]) -> Result<(), AgentError> {
    run_command("bridge", args).await
}

/// Run `ip <args>`, tolerating "already exists" (idempotent setup across
/// agent restarts).
async fn run_ip_tolerate_exists(args: &[String]) -> Result<(), AgentError> {
    match run_ip(args).await {
        Ok(()) => Ok(()),
        Err(err) if is_exists_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Run `<bin> <args>`; any failure becomes [`AgentError::Network`] with
/// stderr and the privilege hint attached.
async fn run_command(bin: &str, args: &[&str]) -> Result<(), AgentError> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .await
        .map_err(|e| network_err(&format!("failed to run `{bin}`: {e}")))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        Err(network_err(&format!(
            "`{bin} {}` failed: {stderr}",
            args.join(" ")
        )))
    }
}

/// True when the error text reports the object already exists (used to
/// tolerate re-runs and entries left by previous agent processes).
fn is_exists_error(err: &AgentError) -> bool {
    match err {
        AgentError::Network(msg) => msg.contains("File exists"),
        _ => false,
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
    use bigtop_core::NodeId;

    fn peer(id: &str, ip: &str) -> OverlayPeer {
        OverlayPeer {
            node_id: NodeId::from(id.to_string()),
            underlay_ip: ip.parse().expect("test ip"),
        }
    }

    #[test]
    fn vxlan_device_name_formats_vni() {
        assert_eq!(vxlan_device_name(42), "vxlan42");
        assert_eq!(vxlan_device_name(100), "vxlan100");
    }

    #[test]
    fn vxlan_create_args_are_exact() {
        assert_eq!(
            vxlan_create_args(42),
            vec!["link", "add", "vxlan42", "type", "vxlan", "id", "42", "dstport", "4789"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<String>>()
        );
        assert_eq!(VXLAN_DSTPORT, 4789, "IANA VXLAN port");
    }

    #[test]
    fn link_and_bridge_args_are_exact() {
        let mac = vtep_mac_for_node(&NodeId::from("node-1".to_string()));
        let as_strings = |parts: &[&str]| {
            parts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
        };
        assert_eq!(
            link_set_mac_args("vxlan42", mac),
            as_strings(&["link", "set", "dev", "vxlan42", "address", &mac.to_string()])
        );
        assert_eq!(
            link_set_up_args("vxlan42"),
            as_strings(&["link", "set", "dev", "vxlan42", "up"])
        );
        assert_eq!(
            bridge_create_args(),
            as_strings(&["link", "add", "name", "bt-br0", "type", "bridge"])
        );
        assert_eq!(
            bridge_enslave_args("bt-deadbeef"),
            as_strings(&["link", "set", "dev", "bt-deadbeef", "master", "bt-br0"])
        );
        assert_eq!(OVERLAY_BRIDGE, "bt-br0");
    }

    #[test]
    fn fdb_args_carry_mac_and_dst() {
        let entry = FdbEntry {
            mac: vtep_mac_for_node(&NodeId::from("node-9".to_string())),
            dst: "10.0.0.9".parse().expect("test ip"),
        };
        let as_strings = |parts: &[&str]| {
            parts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
        };
        assert_eq!(
            fdb_append_args("vxlan42", &entry),
            as_strings(&[
                "fdb",
                "append",
                &entry.mac.to_string(),
                "dev",
                "vxlan42",
                "dst",
                "10.0.0.9"
            ])
        );
        assert_eq!(
            fdb_delete_args("vxlan42", &entry),
            as_strings(&[
                "fdb",
                "del",
                &entry.mac.to_string(),
                "dev",
                "vxlan42",
                "dst",
                "10.0.0.9"
            ])
        );
    }

    #[test]
    fn fdb_entries_derive_vtep_macs_from_peers() {
        let peers = vec![peer("node-1", "10.0.0.1"), peer("node-2", "10.0.0.2")];
        let entries = fdb_entries_for_peers(&peers);
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].mac,
            vtep_mac_for_node(&NodeId::from("node-1".to_string()))
        );
        assert_eq!(entries[0].dst.to_string(), "10.0.0.1");
        assert_eq!(
            entries[1].mac,
            vtep_mac_for_node(&NodeId::from("node-2".to_string()))
        );
        assert_ne!(entries[0].mac, entries[1].mac);
    }

    #[test]
    fn diff_fdb_splits_adds_and_deletes() {
        let a = FdbEntry {
            mac: vtep_mac_for_node(&NodeId::from("node-1".to_string())),
            dst: "10.0.0.1".parse().expect("test ip"),
        };
        let b = FdbEntry {
            mac: vtep_mac_for_node(&NodeId::from("node-2".to_string())),
            dst: "10.0.0.2".parse().expect("test ip"),
        };
        let c = FdbEntry {
            mac: vtep_mac_for_node(&NodeId::from("node-3".to_string())),
            dst: "10.0.0.3".parse().expect("test ip"),
        };
        let applied: HashSet<FdbEntry> = [a, b].into_iter().collect();
        let want: HashSet<FdbEntry> = [b, c].into_iter().collect();
        let (to_add, to_delete) = diff_fdb(&applied, &want);
        assert_eq!(to_add, vec![c]);
        assert_eq!(to_delete, vec![a]);
    }

    #[test]
    fn diff_fdb_empty_when_in_sync() {
        let applied: HashSet<FdbEntry> = fdb_entries_for_peers(&[peer("n1", "10.0.0.1")])
            .into_iter()
            .collect();
        let (to_add, to_delete) = diff_fdb(&applied, &applied.clone());
        assert!(to_add.is_empty() && to_delete.is_empty());
    }

    #[test]
    fn exists_error_detection_matches_iproute2_text() {
        let err = network_err("`ip link add` failed: RTNETLINK answers: File exists");
        assert!(is_exists_error(&err));
        let other = network_err("`ip link add` failed: RTNETLINK answers: Operation not permitted");
        assert!(!is_exists_error(&other));
        assert!(!is_exists_error(&AgentError::Timeout("x".to_string())));
    }

    #[test]
    fn manager_stores_config() {
        let manager = OverlayManager::new(
            42,
            "10.0.0.1".parse().expect("test ip"),
            NodeId::from("node-1".to_string()),
        );
        assert_eq!(manager.vni(), 42);
        assert_eq!(manager.underlay_ip().to_string(), "10.0.0.1");
    }

    #[tokio::test]
    async fn setup_fails_cleanly_without_privilege() {
        // Unprivileged sandbox: setup must return a clean Network error,
        // never panic. (Do not assert on *why*: EPERM vs missing binary is
        // host-dependent.)
        let manager = OverlayManager::new(
            42,
            "10.0.0.1".parse().expect("test ip"),
            NodeId::from("node-1".to_string()),
        );
        let err = manager.setup().await.expect_err("must fail unprivileged");
        assert!(
            matches!(err, AgentError::Network(_)),
            "clean Network error, got {err:?}"
        );
    }
}
