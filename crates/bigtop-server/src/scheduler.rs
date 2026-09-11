//! The `BigTop` scheduler: least-loaded fit, plus dead-node requeue.
//!
//! One [`tick`] pass does three things, in order:
//!
//! 1. Tasks sitting on dead nodes (`Assigned` or `Running`) go back to
//!    `Pending` so they can be rescheduled.
//! 2. Per-node `used` resources are recomputed from live tasks, so the
//!    accounting is self-healing instead of drifting.
//! 3. Each pending task is placed on the alive node with the most headroom
//!    that still fits it (least-loaded fit), ties broken by node id for
//!    determinism.

use crate::ipam::Ipam;
use crate::journal::JournalOp;
use crate::state::{journal_err, journal_op, StateInner};
use bigtop_core::{Error, NetworkAssignment, NodeId, NodeInfo, Resources, Task, TaskId, TaskState};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;

/// Run one scheduling pass over the cluster state.
///
/// Every mutation in the pass is journaled and fsynced; a journal
/// failure aborts the pass with [`Error::Persistence`]. Mutations
/// already applied stay applied in memory — the error tells the caller
/// they are not guaranteed to survive a crash.
///
/// # Errors
///
/// Returns [`Error::Persistence`] when a journal append fails.
pub fn tick(inner: &mut StateInner, now: DateTime<Utc>) -> Result<(), Error> {
    requeue_dead_nodes(inner, now)?;
    recount_used(inner);
    place_pending(inner, now)?;
    Ok(())
}

/// Return `Pending` any task whose node stopped heartbeating.
///
/// # Errors
///
/// Returns [`Error::Persistence`] when a journal append fails.
fn requeue_dead_nodes(inner: &mut StateInner, now: DateTime<Utc>) -> Result<(), Error> {
    let dead: Vec<NodeId> = inner
        .nodes
        .values()
        .filter(|node| !node.is_alive(now))
        .map(|node| node.id.clone())
        .collect();
    if dead.is_empty() {
        return Ok(());
    }
    let orphans: Vec<TaskId> = inner
        .tasks
        .values()
        .filter(|task| {
            matches!(task.state, TaskState::Assigned | TaskState::Running)
                && task
                    .assigned_node
                    .as_ref()
                    .is_some_and(|id| dead.contains(id))
        })
        .map(|task| task.id.clone())
        .collect();
    for id in orphans {
        if let Some(task) = inner.tasks.get_mut(&id) {
            task.state = TaskState::Pending;
            task.assigned_node = None;
            // The dead node's subnet is gone: free the IP and the assignment.
            task.network = None;
        }
        if inner.ipam.release(&id) {
            journal_op(
                inner,
                &JournalOp::IpamRelease {
                    task_id: id.clone(),
                },
            )
            .map_err(|e| journal_err(&e))?;
        }
        journal_op(inner, &JournalOp::TaskRequeued { task_id: id }).map_err(|e| journal_err(&e))?;
    }
    Ok(())
}

/// Recompute `used` from tasks that currently hold resources.
///
/// `pub` so journal replay can rebuild the same accounting.
pub fn recount_used(inner: &mut StateInner) {
    for node in inner.nodes.values_mut() {
        node.used = Resources::default();
    }
    let mut held: HashMap<NodeId, Resources> = HashMap::new();
    for task in inner.tasks.values() {
        if !matches!(task.state, TaskState::Assigned | TaskState::Running) {
            continue;
        }
        if let Some(node_id) = &task.assigned_node {
            let entry = held.entry(node_id.clone()).or_default();
            *entry = entry.saturating_add(task.spec.resources);
        }
    }
    for (node_id, used) in held {
        if let Some(node) = inner.nodes.get_mut(&node_id) {
            node.used = used;
        }
    }
}

/// Place every pending task, least-loaded fit, deterministic order.
///
/// # Errors
///
/// Returns [`Error::Persistence`] when a journal append fails.
fn place_pending(inner: &mut StateInner, now: DateTime<Utc>) -> Result<(), Error> {
    let mut pending: Vec<TaskId> = inner
        .tasks
        .iter()
        .filter(|(_, task)| task.state == TaskState::Pending)
        .map(|(id, _)| id.clone())
        .collect();
    pending.sort();
    for task_id in pending {
        let task = match inner.tasks.get(&task_id) {
            Some(task) => task.clone(),
            None => continue,
        };
        let Some(node_id) = pick_node(inner, &task, now) else {
            continue;
        };
        // Network-enabled tasks get an IP on the chosen node's /24. When
        // the address space is exhausted the task stays Pending for a
        // later tick.
        let mut allocated_ip: Option<Ipv4Addr> = None;
        let network = if task.spec.network.enabled {
            let Some(ip) = inner.ipam.allocate(&node_id, &task_id) else {
                continue;
            };
            let Some(gateway) = inner.ipam.gateway_for(&node_id) else {
                let _ = inner.ipam.release(&task_id);
                continue;
            };
            allocated_ip = Some(ip);
            Some(NetworkAssignment {
                ip,
                gateway,
                netmask: Ipam::netmask(),
            })
        } else {
            None
        };
        // Service discovery (v0.4): inject the current endpoints of every
        // service this task wants to discover.
        let services_env = services_env_for(inner, &task);
        let placed = match (inner.tasks.get_mut(&task_id), inner.nodes.get_mut(&node_id)) {
            (Some(task), Some(node)) => {
                task.state = TaskState::Assigned;
                task.assigned_node = Some(node.id.clone());
                task.network = network;
                if let Some(env) = &services_env {
                    task.spec
                        .env
                        .insert("BIGTOP_SERVICES".to_string(), env.clone());
                }
                node.used = node.used.saturating_add(task.spec.resources);
                true
            }
            _ => false,
        };
        if placed {
            if let Some(ip) = allocated_ip {
                journal_op(
                    inner,
                    &JournalOp::IpamAllocate {
                        node_id: node_id.clone(),
                        task_id: task_id.clone(),
                        ip,
                    },
                )
                .map_err(|e| journal_err(&e))?;
            }
            journal_op(
                inner,
                &JournalOp::TaskAssigned {
                    task_id,
                    node_id,
                    network,
                    services_env,
                },
            )
            .map_err(|e| journal_err(&e))?;
        } else if allocated_ip.is_some() {
            // Defensive rollback: the address was allocated but the task
            // could not be placed.
            let _ = inner.ipam.release(&task_id);
        }
    }
    Ok(())
}

/// The `BIGTOP_SERVICES` value for a task: JSON mapping each discovered
/// service name to its current endpoint IPs (sorted). `None` when the
/// task discovers nothing.
fn services_env_for(inner: &StateInner, task: &Task) -> Option<String> {
    if task.spec.service.discover.is_empty() {
        return None;
    }
    let mut by_name: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for other in inner.tasks.values() {
        let (Some(name), Some(network)) =
            (other.spec.service.name.as_ref(), other.network.as_ref())
        else {
            continue;
        };
        if other.state != TaskState::Running {
            continue;
        }
        by_name
            .entry(name.as_str())
            .or_default()
            .push(network.ip.to_string());
    }
    let mut map: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for name in &task.spec.service.discover {
        let mut ips = by_name.get(name.as_str()).cloned().unwrap_or_default();
        ips.sort();
        map.insert(name.as_str(), ips);
    }
    serde_json::to_string(&map).ok()
}

/// The alive node with the lowest load that fits the task, if any.
/// A task with `node_affinity` is only eligible on its pinned node.
fn pick_node(inner: &StateInner, task: &Task, now: DateTime<Utc>) -> Option<NodeId> {
    let need = task.spec.resources;
    inner
        .nodes
        .values()
        .filter(|node| {
            node.is_alive(now)
                && need.fits_in(node.total, node.used)
                && task
                    .spec
                    .node_affinity
                    .as_ref()
                    .is_none_or(|pinned| *pinned == node.id)
        })
        .min_by(|a, b| cmp_load(a, b).then_with(|| a.id.to_string().cmp(&b.id.to_string())))
        .map(|node| node.id.clone())
}

/// Order nodes by CPU load ascending, then memory load, using exact integer
/// math (no floats). A node with zero capacity sorts as fully loaded.
fn cmp_load(a: &NodeInfo, b: &NodeInfo) -> Ordering {
    let usable = |node: &NodeInfo| node.total.cpu_millis > 0 && node.total.mem_mb > 0;
    match (usable(a), usable(b)) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => Ordering::Equal,
        (true, true) => {
            let cpu = (u128::from(a.used.cpu_millis) * u128::from(b.total.cpu_millis))
                .cmp(&(u128::from(b.used.cpu_millis) * u128::from(a.total.cpu_millis)));
            let mem = (u128::from(a.used.mem_mb) * u128::from(b.total.mem_mb))
                .cmp(&(u128::from(b.used.mem_mb) * u128::from(a.total.mem_mb)));
            cpu.then(mem)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigtop_core::{
        JobId, NetworkSpec, NodeId, ServiceSpec, SnapshotPolicy, Task, TaskSpec, VmSpec,
    };
    use std::collections::HashMap;

    fn node(id: &str, cpu: u64, mem: u64, now: DateTime<Utc>) -> NodeInfo {
        NodeInfo {
            id: NodeId::from(id.to_string()),
            name: id.to_string(),
            addr: "127.0.0.1".to_string(),
            underlay_ip: None,
            total: Resources {
                cpu_millis: cpu,
                mem_mb: mem,
            },
            used: Resources::default(),
            last_heartbeat: now,
        }
    }

    fn task(id: &str, cpu: u64, mem: u64) -> Task {
        Task {
            id: TaskId::from(id.to_string()),
            job_id: JobId::from("job-test".to_string()),
            name: "t".to_string(),
            spec: TaskSpec {
                name: "t".to_string(),
                command: "true".to_string(),
                args: vec![],
                env: HashMap::new(),
                resources: Resources {
                    cpu_millis: cpu,
                    mem_mb: mem,
                },
                count: 1,
                vm: VmSpec {
                    kernel_image: String::new(),
                    rootfs: String::new(),
                    vcpu_count: 1,
                    mem_mb: 128,
                    boot_args: None,
                    boot_snapshot: None,
                },
                node_affinity: None,
                snapshot_policy: SnapshotPolicy::None,
                network: NetworkSpec::default(),
                service: ServiceSpec::default(),
            },
            state: TaskState::Pending,
            assigned_node: None,
            exit_code: None,
            network: None,
        }
    }

    fn cluster(now: DateTime<Utc>) -> StateInner {
        let mut inner = StateInner::default();
        inner.nodes.insert(
            NodeId::from("node-a".to_string()),
            node("node-a", 1000, 1024, now),
        );
        inner.nodes.insert(
            NodeId::from("node-b".to_string()),
            node("node-b", 1000, 1024, now),
        );
        inner
    }

    fn assignments(inner: &StateInner) -> HashMap<String, String> {
        inner
            .tasks
            .iter()
            .map(|(id, task)| {
                (
                    id.to_string(),
                    task.assigned_node
                        .as_ref()
                        .map_or_else(|| "-".to_string(), ToString::to_string),
                )
            })
            .collect()
    }

    #[test]
    fn least_loaded_spreads_evenly() {
        let now = Utc::now();
        let mut inner = cluster(now);
        for i in 0..4 {
            let t = task(&format!("task-{i}"), 400, 64);
            inner.tasks.insert(t.id.clone(), t);
        }
        tick(&mut inner, now).expect("tick");
        let got = assignments(&inner);
        // task-0 -> a (tie, lower id), task-1 -> b (a is busier),
        // task-2 -> a (tie again), task-3 -> b.
        assert_eq!(got["task-0"], "node-a");
        assert_eq!(got["task-1"], "node-b");
        assert_eq!(got["task-2"], "node-a");
        assert_eq!(got["task-3"], "node-b");
        for task in inner.tasks.values() {
            assert_eq!(task.state, TaskState::Assigned);
        }
    }

    #[test]
    fn oversized_task_stays_pending() {
        let now = Utc::now();
        let mut inner = cluster(now);
        let t = task("task-big", 9999, 64);
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        let task = inner
            .tasks
            .get(&TaskId::from("task-big".to_string()))
            .expect("task");
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.assigned_node, None);
    }

    #[test]
    fn node_affinity_pins_placement() {
        let now = Utc::now();
        let mut inner = cluster(now);
        // Without affinity this task would land on node-a (tie, lower id).
        let mut t = task("task-pinned", 100, 64);
        t.spec.node_affinity = Some(NodeId::from("node-b".to_string()));
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        let task = inner
            .tasks
            .get(&TaskId::from("task-pinned".to_string()))
            .expect("task");
        assert_eq!(task.state, TaskState::Assigned);
        assert_eq!(task.assigned_node, Some(NodeId::from("node-b".to_string())));
    }

    #[test]
    fn affinity_to_missing_node_stays_pending() {
        let now = Utc::now();
        let mut inner = cluster(now);
        let mut t = task("task-lost", 100, 64);
        t.spec.node_affinity = Some(NodeId::from("node-gone".to_string()));
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        let task = inner
            .tasks
            .get(&TaskId::from("task-lost".to_string()))
            .expect("task");
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.assigned_node, None);
    }

    #[test]
    fn dead_node_tasks_are_requeued() {
        let now = Utc::now();
        let mut inner = StateInner::default();
        let stale_hb = now - chrono::Duration::seconds(30);
        inner.nodes.insert(
            NodeId::from("node-old".to_string()),
            node("node-old", 4000, 4096, stale_hb),
        );
        inner.nodes.insert(
            NodeId::from("node-new".to_string()),
            node("node-new", 4000, 4096, now),
        );
        let mut t = task("task-orphan", 100, 64);
        t.state = TaskState::Running;
        t.assigned_node = Some(NodeId::from("node-old".to_string()));
        inner.tasks.insert(t.id.clone(), t);
        let mut u = task("task-fine", 100, 64);
        u.state = TaskState::Running;
        u.assigned_node = Some(NodeId::from("node-new".to_string()));
        inner.tasks.insert(u.id.clone(), u);

        tick(&mut inner, now).expect("tick");

        // The orphan is requeued, then immediately placed on the live node.
        let orphan = inner
            .tasks
            .get(&TaskId::from("task-orphan".to_string()))
            .expect("orphan");
        assert_eq!(orphan.state, TaskState::Assigned);
        assert_eq!(
            orphan.assigned_node.as_ref().expect("node").to_string(),
            "node-new"
        );
        // The healthy task is untouched.
        let fine = inner
            .tasks
            .get(&TaskId::from("task-fine".to_string()))
            .expect("fine");
        assert_eq!(fine.state, TaskState::Running);
    }

    #[test]
    fn dead_node_with_no_live_capacity_leaves_task_pending() {
        let now = Utc::now();
        let mut inner = StateInner::default();
        inner.nodes.insert(
            NodeId::from("node-old".to_string()),
            node("node-old", 4000, 4096, now - chrono::Duration::seconds(30)),
        );
        let mut t = task("task-orphan", 100, 64);
        t.state = TaskState::Assigned;
        t.assigned_node = Some(NodeId::from("node-old".to_string()));
        inner.tasks.insert(t.id.clone(), t);

        tick(&mut inner, now).expect("tick");

        let orphan = inner
            .tasks
            .get(&TaskId::from("task-orphan".to_string()))
            .expect("orphan");
        assert_eq!(orphan.state, TaskState::Pending);
        assert_eq!(orphan.assigned_node, None);
    }

    #[test]
    fn network_task_gets_assignment_in_node_subnet() {
        let now = Utc::now();
        let mut inner = cluster(now);
        let mut t = task("task-net", 100, 64);
        t.spec.network.enabled = true;
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        let placed = inner
            .tasks
            .get(&TaskId::from("task-net".to_string()))
            .expect("task");
        assert_eq!(placed.state, TaskState::Assigned);
        let node_id = placed.assigned_node.clone().expect("placed on a node");
        let assignment = placed.network.expect("network assignment");
        assert_eq!(assignment.netmask, Ipam::netmask());
        let subnet = inner.ipam.subnet_for(&node_id).expect("node subnet");
        let octets = assignment.ip.octets();
        assert_eq!(&octets[0..3], &subnet.octets()[0..3]);
        assert!((2..=254).contains(&octets[3]));
        assert_eq!(
            assignment.gateway,
            inner.ipam.gateway_for(&node_id).expect("gateway")
        );
        assert_eq!(inner.ipam.assigned(&placed.id), Some(assignment.ip));
    }

    #[test]
    fn network_disabled_task_gets_no_assignment() {
        let now = Utc::now();
        let mut inner = cluster(now);
        let t = task("task-plain", 100, 64);
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        let placed = inner
            .tasks
            .get(&TaskId::from("task-plain".to_string()))
            .expect("task");
        assert_eq!(placed.state, TaskState::Assigned);
        assert_eq!(placed.network, None);
        assert_eq!(inner.ipam.assigned(&placed.id), None);
    }

    #[test]
    fn dead_node_requeue_releases_ip_and_clears_assignment() {
        let now = Utc::now();
        let mut inner = StateInner::default();
        inner.nodes.insert(
            NodeId::from("node-old".to_string()),
            node("node-old", 4000, 4096, now - chrono::Duration::seconds(30)),
        );
        let node_id = NodeId::from("node-old".to_string());
        let mut t = task("task-orphan", 100, 64);
        t.spec.network.enabled = true;
        t.state = TaskState::Assigned;
        t.assigned_node = Some(node_id.clone());
        let ip = inner
            .ipam
            .allocate(&node_id, &t.id)
            .expect("pre-allocation");
        t.network = Some(NetworkAssignment {
            ip,
            gateway: inner.ipam.gateway_for(&node_id).expect("gateway"),
            netmask: Ipam::netmask(),
        });
        inner.tasks.insert(t.id.clone(), t);

        tick(&mut inner, now).expect("tick");

        let orphan = inner
            .tasks
            .get(&TaskId::from("task-orphan".to_string()))
            .expect("orphan");
        // No live capacity: stays Pending with the IP freed and cleared.
        assert_eq!(orphan.state, TaskState::Pending);
        assert_eq!(orphan.assigned_node, None);
        assert_eq!(orphan.network, None);
        assert_eq!(inner.ipam.assigned(&orphan.id), None);
    }

    #[test]
    fn exhausted_subnet_leaves_network_task_pending() {
        let now = Utc::now();
        let mut inner = StateInner::default();
        let node_id = NodeId::from("node-solo".to_string());
        inner
            .nodes
            .insert(node_id.clone(), node("node-solo", 4000, 4096, now));
        // Fill the node's entire /24 through the IPAM directly.
        for i in 0..253 {
            let filler = TaskId::from(format!("filler-{i}"));
            assert!(
                inner.ipam.allocate(&node_id, &filler).is_some(),
                "filler {i} must fit"
            );
        }
        let mut t = task("task-starved", 100, 64);
        t.spec.network.enabled = true;
        inner.tasks.insert(t.id.clone(), t);

        tick(&mut inner, now).expect("tick");

        let starved = inner
            .tasks
            .get(&TaskId::from("task-starved".to_string()))
            .expect("task");
        assert_eq!(starved.state, TaskState::Pending);
        assert_eq!(starved.assigned_node, None);
        assert_eq!(starved.network, None);
        assert_eq!(inner.ipam.assigned(&starved.id), None);
    }

    #[test]
    fn used_resources_are_recounted_not_drifted() {
        let now = Utc::now();
        let mut inner = cluster(now);
        // Lie about `used`: the tick must recompute it from tasks.
        for n in inner.nodes.values_mut() {
            n.used = Resources {
                cpu_millis: 999_999,
                mem_mb: 999_999,
            };
        }
        let t = task("task-1", 400, 64);
        inner.tasks.insert(t.id.clone(), t);
        tick(&mut inner, now).expect("tick");
        for n in inner.nodes.values() {
            assert!(n.used.cpu_millis <= 400, "used: {:?}", n.used);
        }
    }

    #[test]
    fn cmp_load_orders_by_utilization() {
        let now = Utc::now();
        let mut busy = node("a", 1000, 1000, now);
        busy.used = Resources {
            cpu_millis: 800,
            mem_mb: 0,
        };
        let mut idle = node("b", 1000, 1000, now);
        idle.used = Resources {
            cpu_millis: 100,
            mem_mb: 0,
        };
        assert_eq!(cmp_load(&busy, &idle), Ordering::Greater);
        assert_eq!(cmp_load(&idle, &busy), Ordering::Less);
        let zero = node("c", 0, 0, now);
        assert_eq!(cmp_load(&zero, &idle), Ordering::Greater);
        assert_eq!(cmp_load(&idle, &zero), Ordering::Less);
    }

    fn open_test_journal(name: &str) -> (crate::journal::JournalWriter, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("bigtop-sched-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create tmp dir");
        let journal = crate::journal::open_journal(&dir).expect("open journal");
        (journal, dir)
    }

    #[test]
    fn tick_journals_assignment_and_ipam() {
        let now = Utc::now();
        let mut inner = cluster(now);
        let (journal, dir) = open_test_journal("assign");
        inner.journal = Some(journal);
        let mut t = task("task-net", 100, 64);
        t.spec.network.enabled = true;
        inner.tasks.insert(t.id.clone(), t);

        tick(&mut inner, now).expect("tick");

        let ops = crate::journal::load_journal_ops(&dir).expect("load");
        assert!(
            ops.iter()
                .any(|op| matches!(op, JournalOp::IpamAllocate { .. })),
            "expected an IpamAllocate op, got {ops:?}"
        );
        let assigned = ops
            .iter()
            .find_map(|op| match op {
                JournalOp::TaskAssigned {
                    task_id,
                    node_id,
                    network,
                    services_env,
                } => Some((task_id, node_id, network, services_env)),
                _ => None,
            })
            .expect("expected a TaskAssigned op");
        assert_eq!(
            assigned.0,
            &TaskId::from("task-net".to_string()),
            "assigned the right task"
        );
        assert!(assigned.2.is_some(), "network assignment journaled");
        assert_eq!(assigned.3, &None, "no discover, no services env");
        assert!(
            ["node-a", "node-b"].contains(&assigned.1.to_string().as_str()),
            "placed on a live node"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn requeue_journals_task_and_ip_release() {
        let now = Utc::now();
        let mut inner = StateInner::default();
        inner.nodes.insert(
            NodeId::from("node-old".to_string()),
            node("node-old", 4000, 4096, now - chrono::Duration::seconds(30)),
        );
        let (journal, dir) = open_test_journal("requeue");
        inner.journal = Some(journal);
        let node_id = NodeId::from("node-old".to_string());
        let mut t = task("task-orphan", 100, 64);
        t.spec.network.enabled = true;
        t.state = TaskState::Assigned;
        t.assigned_node = Some(node_id.clone());
        let ip = inner
            .ipam
            .allocate(&node_id, &t.id)
            .expect("pre-allocation");
        t.network = Some(NetworkAssignment {
            ip,
            gateway: inner.ipam.gateway_for(&node_id).expect("gateway"),
            netmask: Ipam::netmask(),
        });
        inner.tasks.insert(t.id.clone(), t);

        tick(&mut inner, now).expect("tick");

        let ops = crate::journal::load_journal_ops(&dir).expect("load");
        assert!(
            ops.iter().any(|op| matches!(
                op,
                JournalOp::TaskRequeued { task_id }
                if *task_id == TaskId::from("task-orphan".to_string())
            )),
            "expected TaskRequeued, got {ops:?}"
        );
        assert!(
            ops.iter().any(|op| matches!(
                op,
                JournalOp::IpamRelease { task_id }
                if *task_id == TaskId::from("task-orphan".to_string())
            )),
            "expected IpamRelease, got {ops:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_injects_bigtop_services_env() {
        use std::net::Ipv4Addr;
        let now = Utc::now();
        let mut inner = cluster(now);
        // A running db task with a pod IP.
        let node_id = NodeId::from("node-a".to_string());
        let db_ip: Ipv4Addr = "172.28.0.2".parse().expect("ip");
        let mut db = task("task-db", 100, 64);
        db.spec.service.name = Some("db".to_string());
        db.spec.network.enabled = true;
        db.state = TaskState::Running;
        db.assigned_node = Some(node_id);
        db.network = Some(NetworkAssignment {
            ip: db_ip,
            gateway: "172.28.0.1".parse().expect("ip"),
            netmask: Ipam::netmask(),
        });
        inner.tasks.insert(db.id.clone(), db);
        // A pending web task that discovers db (and an unknown service).
        let mut web = task("task-web", 100, 64);
        web.spec.service.discover = vec!["db".to_string(), "cache".to_string()];
        inner.tasks.insert(web.id.clone(), web);

        tick(&mut inner, now).expect("tick");

        let web = inner
            .tasks
            .get(&TaskId::from("task-web".to_string()))
            .expect("web");
        assert_eq!(web.state, TaskState::Assigned);
        let env = web
            .spec
            .env
            .get("BIGTOP_SERVICES")
            .expect("BIGTOP_SERVICES injected");
        let parsed: serde_json::Value = serde_json::from_str(env).expect("valid JSON");
        assert_eq!(parsed["db"], serde_json::json!(["172.28.0.2"]));
        assert_eq!(parsed["cache"], serde_json::json!([]));
        // A task that discovers nothing gets no env var.
        let mut inner2 = cluster(now);
        let plain = task("task-plain", 100, 64);
        inner2.tasks.insert(plain.id.clone(), plain);
        tick(&mut inner2, now).expect("tick");
        let plain = inner2
            .tasks
            .get(&TaskId::from("task-plain".to_string()))
            .expect("plain");
        assert_eq!(plain.spec.env.get("BIGTOP_SERVICES"), None);
    }
}
