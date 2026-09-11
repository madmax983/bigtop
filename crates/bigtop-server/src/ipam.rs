//! IP address management for the task network.
//!
//! The server owns one `/16` (default `172.28.0.0/16`). Each node gets a
//! `/24` slice of it (the third octet is the node index), and each task on
//! that node gets one host address. The `.0` (subnet), `.1` (gateway), and
//! `.255` (broadcast) addresses of every `/24` are never handed out.

use bigtop_core::{NodeId, TaskId};
use std::collections::HashMap;
use std::net::Ipv4Addr;

/// First host octet handed out inside a node's `/24`.
const FIRST_HOST: u8 = 2;
/// Last host octet handed out inside a node's `/24`.
const LAST_HOST: u8 = 254;

/// Errors from building an [`Ipam`].
#[derive(Debug, thiserror::Error)]
pub enum IpamError {
    /// The address is not the base of a `/16` (3rd and 4th octets non-zero).
    #[error("not a /16 base network: {0}")]
    NotBaseNetwork(Ipv4Addr),
}

/// IPAM over one `/16`: one `/24` per node, one host address per task.
#[derive(Debug, Clone)]
pub struct Ipam {
    /// First two octets of the `/16`.
    base: [u8; 2],
    /// Node -> third octet of its `/24`.
    node_subnets: HashMap<NodeId, u8>,
    /// Task -> its assigned IP.
    allocated: HashMap<TaskId, Ipv4Addr>,
}

impl Ipam {
    /// Build an IPAM over `network`, which must be a `/16` base address
    /// (third and fourth octets zero).
    ///
    /// # Errors
    ///
    /// Returns [`IpamError::NotBaseNetwork`] when `network` is not a `/16`
    /// base.
    pub fn new(network: Ipv4Addr) -> Result<Self, IpamError> {
        let o = network.octets();
        if o[2] != 0 || o[3] != 0 {
            return Err(IpamError::NotBaseNetwork(network));
        }
        Ok(Self {
            base: [o[0], o[1]],
            node_subnets: HashMap::new(),
            allocated: HashMap::new(),
        })
    }

    /// Assign an address to `task_id` on `node_id`'s `/24`.
    ///
    /// Idempotent: a task that already holds an address gets the same one
    /// back. A node seen for the first time takes the next free third
    /// octet. Returns `None` when the address space is exhausted (no free
    /// third octet, or no free host address in the node's `/24`).
    pub fn allocate(&mut self, node_id: &NodeId, task_id: &TaskId) -> Option<Ipv4Addr> {
        if let Some(ip) = self.allocated.get(task_id) {
            return Some(*ip);
        }
        let subnet = self.subnet_octet(node_id)?;
        let used: Vec<u8> = self
            .allocated
            .values()
            .filter(|ip| {
                let o = ip.octets();
                o[0] == self.base[0] && o[1] == self.base[1] && o[2] == subnet
            })
            .map(|ip| ip.octets()[3])
            .collect();
        let host = (FIRST_HOST..=LAST_HOST).find(|h| !used.contains(h))?;
        let ip = Ipv4Addr::new(self.base[0], self.base[1], subnet, host);
        self.allocated.insert(task_id.clone(), ip);
        Some(ip)
    }

    /// Return `task_id`'s address to the pool. `false` when unknown, so
    /// double-release is safe.
    pub fn release(&mut self, task_id: &TaskId) -> bool {
        self.allocated.remove(task_id).is_some()
    }

    /// The gateway (`x.y.z.1`) of `node_id`'s `/24`, if the node has one.
    #[must_use]
    pub fn gateway_for(&self, node_id: &NodeId) -> Option<Ipv4Addr> {
        self.node_subnets
            .get(node_id)
            .map(|z| Ipv4Addr::new(self.base[0], self.base[1], *z, 1))
    }

    /// The subnet base (`x.y.z.0`) of `node_id`'s `/24`, if the node has one.
    #[must_use]
    pub fn subnet_for(&self, node_id: &NodeId) -> Option<Ipv4Addr> {
        self.node_subnets
            .get(node_id)
            .map(|z| Ipv4Addr::new(self.base[0], self.base[1], *z, 0))
    }

    /// The netmask of every node `/24`.
    #[must_use]
    pub const fn netmask() -> Ipv4Addr {
        Ipv4Addr::new(255, 255, 255, 0)
    }

    /// The address currently held by `task_id`, if any.
    #[must_use]
    pub fn assigned(&self, task_id: &TaskId) -> Option<Ipv4Addr> {
        self.allocated.get(task_id).copied()
    }

    /// Third octet for `node_id`, assigning the next free one on first use.
    fn subnet_octet(&mut self, node_id: &NodeId) -> Option<u8> {
        if let Some(z) = self.node_subnets.get(node_id) {
            return Some(*z);
        }
        let taken: Vec<u8> = self.node_subnets.values().copied().collect();
        let z = (0..=u8::MAX).find(|z| !taken.contains(z))?;
        self.node_subnets.insert(node_id.clone(), z);
        Some(z)
    }
}

impl Default for Ipam {
    /// The default task network: `172.28.0.0/16`, built from octets.
    fn default() -> Self {
        Self {
            base: [172, 28],
            node_subnets: HashMap::new(),
            allocated: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ipam() -> Ipam {
        Ipam::new(Ipv4Addr::new(172, 28, 0, 0)).expect("base network")
    }

    fn node(id: &str) -> NodeId {
        NodeId::from(id.to_string())
    }

    fn task(id: &str) -> TaskId {
        TaskId::from(id.to_string())
    }

    #[test]
    fn default_is_172_28_16() {
        let mut ipam = Ipam::default();
        let ip = ipam
            .allocate(&node("n"), &task("t"))
            .expect("first allocation");
        assert_eq!(ip, Ipv4Addr::new(172, 28, 0, 2));
        assert_eq!(
            ipam.gateway_for(&node("n")),
            Some(Ipv4Addr::new(172, 28, 0, 1))
        );
        assert_eq!(
            ipam.subnet_for(&node("n")),
            Some(Ipv4Addr::new(172, 28, 0, 0))
        );
    }

    #[test]
    fn new_rejects_non_base_network() {
        let err = Ipam::new(Ipv4Addr::new(172, 28, 1, 5)).expect_err("not a base");
        assert!(matches!(err, IpamError::NotBaseNetwork(_)));
        assert_eq!(format!("{err}"), "not a /16 base network: 172.28.1.5");
        assert!(Ipam::new(Ipv4Addr::new(172, 28, 0, 1)).is_err());
        assert!(Ipam::new(Ipv4Addr::new(172, 28, 0, 0)).is_ok());
    }

    #[test]
    fn allocate_release_cycle_returns_same_ip() {
        let mut ipam = ipam();
        let first = ipam.allocate(&node("n"), &task("t")).expect("allocate");
        assert!(ipam.release(&task("t")));
        let second = ipam.allocate(&node("n"), &task("t")).expect("re-allocate");
        assert_eq!(first, second, "freed IP is the first free host again");
        assert_eq!(ipam.assigned(&task("t")), Some(first));
    }

    #[test]
    fn allocate_is_idempotent() {
        let mut ipam = ipam();
        let a = ipam.allocate(&node("n"), &task("t")).expect("allocate");
        let b = ipam.allocate(&node("n"), &task("t")).expect("re-allocate");
        assert_eq!(a, b);
        assert_eq!(ipam.allocated.len(), 1, "no duplicate entry");
    }

    #[test]
    fn node_subnet_exhaustion() {
        let mut ipam = ipam();
        let n = node("n");
        let mut ips = Vec::new();
        for i in 0..253 {
            let ip = ipam
                .allocate(&n, &task(&format!("task-{i}")))
                .expect("host available");
            ips.push(ip);
        }
        // Exactly .2..=.254, 253 distinct hosts.
        assert_eq!(ips.len(), 253);
        for ip in &ips {
            let o = ip.octets();
            assert_eq!(&o[0..3], &[172, 28, 0]);
            assert!((FIRST_HOST..=LAST_HOST).contains(&o[3]));
        }
        // .1 (gateway) was never handed out.
        assert!(!ips.contains(&Ipv4Addr::new(172, 28, 0, 1)));
        // The 254th allocation on this node fails.
        assert_eq!(ipam.allocate(&n, &task("task-253")), None);
        // But a fresh node still has room.
        assert!(ipam.allocate(&node("other"), &task("task-253")).is_some());
    }

    #[test]
    fn double_release_is_safe() {
        let mut ipam = ipam();
        ipam.allocate(&node("n"), &task("t")).expect("allocate");
        assert!(ipam.release(&task("t")));
        assert!(!ipam.release(&task("t")), "second release reports false");
        assert!(
            !ipam.release(&task("unknown")),
            "unknown task reports false"
        );
        assert_eq!(ipam.assigned(&task("t")), None);
    }

    #[test]
    fn two_nodes_get_distinct_subnets_no_overlap() {
        let mut ipam = ipam();
        let a = ipam.allocate(&node("a"), &task("t-a")).expect("a");
        let b = ipam.allocate(&node("b"), &task("t-b")).expect("b");
        assert_ne!(a.octets()[2], b.octets()[2], "different third octets");
        assert_eq!(a.octets()[2], 0);
        assert_eq!(b.octets()[2], 1);
        assert_eq!(
            ipam.gateway_for(&node("a")),
            Some(Ipv4Addr::new(172, 28, 0, 1))
        );
        assert_eq!(
            ipam.gateway_for(&node("b")),
            Some(Ipv4Addr::new(172, 28, 1, 1))
        );
        // A third allocation on node a reuses node a's subnet.
        let a2 = ipam.allocate(&node("a"), &task("t-a2")).expect("a2");
        assert_eq!(a2.octets()[2], 0);
        assert_ne!(a2, a);
    }

    #[test]
    fn gateway_for_unknown_node_is_none() {
        let ipam = ipam();
        assert_eq!(ipam.gateway_for(&node("ghost")), None);
        assert_eq!(ipam.subnet_for(&node("ghost")), None);
    }

    #[test]
    fn netmask_is_24() {
        assert_eq!(Ipam::netmask(), Ipv4Addr::new(255, 255, 255, 0));
    }
}
