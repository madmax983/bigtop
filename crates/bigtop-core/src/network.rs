//! Networking types: per-task network requests, IPAM assignments, MAC
//! addresses, and deterministic tap interface names.
//!
//! Every networking identifier derives deterministically from the task id,
//! so agent restarts and task retries always get the same MAC and tap name
//! without any coordination.

use crate::ids::TaskId;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;

/// Per-task network request, from the `[network]` TOML section.
///
/// Disabled by default: the task gets no tap interface and the server
/// assigns no address.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NetworkSpec {
    /// Attach a tap interface to the microVM.
    #[serde(default)]
    pub enabled: bool,
    /// Guest hostname, e.g. `"web-1"`. Passed to the guest via boot args.
    #[serde(default)]
    pub hostname: Option<String>,
}

/// The server's IPAM assignment for one task.
///
/// Handed to the agent alongside the task so it can configure the tap
/// interface inside the guest's network namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkAssignment {
    /// Guest address on the task network.
    pub ip: Ipv4Addr,
    /// Next hop out of the task network.
    pub gateway: Ipv4Addr,
    /// Subnet mask for the task network.
    pub netmask: Ipv4Addr,
}

/// A MAC address, rendered `02:xx:xx:xx:xx:xx`.
///
/// Always locally-administered unicast when produced by
/// [`mac_for_task`]; the type itself carries no invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MacAddr([u8; 6]);

impl MacAddr {
    /// The six octets, most significant first.
    #[must_use]
    pub const fn octets(&self) -> [u8; 6] {
        self.0
    }
}

impl fmt::Display for MacAddr {
    /// Lowercase colon-separated hex, e.g. `02:1a:2b:3c:4d:5e`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let octets = self.octets();
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            octets[0], octets[1], octets[2], octets[3], octets[4], octets[5]
        )
    }
}

/// Hash the task id string with the default hasher.
fn hash_task_id(task_id: &TaskId) -> u64 {
    let mut hasher = DefaultHasher::new();
    task_id.as_ref().hash(&mut hasher);
    hasher.finish()
}

/// Deterministic, locally-administered unicast MAC from the task id
/// (stable across agent restarts/retries).
///
/// Takes 6 bytes of the task-id digest and forces the
/// locally-administered unicast bits: `b[0] = (b[0] & 0xFE) | 0x02`.
#[must_use]
pub fn mac_for_task(task_id: &TaskId) -> MacAddr {
    let digest = hash_task_id(task_id).to_be_bytes();
    let mut octets = [
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5],
    ];
    octets[0] = (octets[0] & 0xFE) | 0x02;
    MacAddr(octets)
}

/// Deterministic tap name from the task id: `bt-` + 8 lowercase hex chars
/// (11 chars total, within the 15-char Linux interface limit).
#[must_use]
pub fn tap_name_for(task_id: &TaskId) -> String {
    let bytes = hash_task_id(task_id).to_le_bytes();
    let low = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    format!("bt-{low:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JobSpec;

    fn task_id(n: u64) -> TaskId {
        TaskId::from(format!("task-{n}"))
    }

    #[test]
    fn network_spec_parses_from_toml_section() {
        let toml = r#"
            name = "web"

            [[task]]
            name = "web-1"
            command = "serve"

            [task.network]
            enabled = true
            hostname = "web-1"
        "#;
        let spec: JobSpec = toml::from_str(toml).expect("parse toml");
        let net = &spec.tasks[0].network;
        assert!(net.enabled);
        assert_eq!(net.hostname.as_deref(), Some("web-1"));
    }

    #[test]
    fn network_spec_defaults_when_section_omitted() {
        let toml = r#"
            name = "web"

            [[task]]
            name = "web-1"
            command = "serve"
        "#;
        let spec: JobSpec = toml::from_str(toml).expect("parse toml");
        let net = &spec.tasks[0].network;
        assert!(!net.enabled);
        assert_eq!(net.hostname, None);
    }

    #[test]
    fn network_types_json_roundtrip() {
        let spec = NetworkSpec {
            enabled: true,
            hostname: Some("web-1".to_string()),
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: NetworkSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, back);

        let defaulted: NetworkSpec =
            serde_json::from_str("{}").expect("empty object uses defaults");
        assert_eq!(defaulted, NetworkSpec::default());

        let assign = NetworkAssignment {
            ip: Ipv4Addr::new(10, 0, 7, 2),
            gateway: Ipv4Addr::new(10, 0, 7, 1),
            netmask: Ipv4Addr::new(255, 255, 255, 0),
        };
        let json = serde_json::to_string(&assign).expect("serialize");
        assert!(json.contains("10.0.7.2"));
        let back: NetworkAssignment = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(assign, back);
    }

    #[test]
    fn mac_is_deterministic_and_locally_administered_unicast() {
        let id = task_id(42);
        let first = mac_for_task(&id);
        let second = mac_for_task(&id);
        assert_eq!(first, second);

        let octets = first.octets();
        // Locally-administered: bit 1 set; unicast: bit 0 clear.
        assert_eq!(octets[0] & 0x02, 0x02);
        assert_eq!(octets[0] & 0x01, 0x00);

        let rendered = first.to_string();
        assert!(rendered.starts_with("02:") || (first.octets()[0] & 0x01) == 0);
        assert_eq!(rendered.len(), 17);
        assert!(
            rendered
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase() || c == ':'),
            "lowercase colon-hex, got {rendered}"
        );
    }

    #[test]
    fn different_task_ids_give_different_macs() {
        let macs: Vec<MacAddr> = (0..3).map(|n| mac_for_task(&task_id(n))).collect();
        assert_ne!(macs[0], macs[1]);
        assert_ne!(macs[1], macs[2]);
        assert_ne!(macs[0], macs[2]);
    }

    #[test]
    fn tap_name_is_short_deterministic_and_valid() {
        let id = task_id(7);
        let first = tap_name_for(&id);
        let second = tap_name_for(&id);
        assert_eq!(first, second);

        assert!(first.starts_with("bt-"));
        assert!(first.len() <= 15, "within Linux interface limit");
        assert!(
            first.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "valid interface chars, got {first}"
        );

        // Spot-check: 3 ids, distinct names.
        let names: Vec<String> = (0..3).map(|n| tap_name_for(&task_id(n))).collect();
        assert_ne!(names[0], names[1]);
        assert_ne!(names[1], names[2]);
        assert_ne!(names[0], names[2]);
    }
}
