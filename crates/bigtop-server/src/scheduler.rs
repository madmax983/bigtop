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

use crate::state::StateInner;
use bigtop_core::{NodeId, NodeInfo, Resources, Task, TaskId, TaskState};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Run one scheduling pass over the cluster state.
pub fn tick(inner: &mut StateInner, now: DateTime<Utc>) {
    requeue_dead_nodes(inner, now);
    recount_used(inner);
    place_pending(inner, now);
}

/// Return `Pending` any task whose node stopped heartbeating.
fn requeue_dead_nodes(inner: &mut StateInner, now: DateTime<Utc>) {
    let dead: Vec<NodeId> = inner
        .nodes
        .values()
        .filter(|node| !node.is_alive(now))
        .map(|node| node.id.clone())
        .collect();
    if dead.is_empty() {
        return;
    }
    for task in inner.tasks.values_mut() {
        let orphaned = matches!(task.state, TaskState::Assigned | TaskState::Running)
            && task
                .assigned_node
                .as_ref()
                .is_some_and(|id| dead.contains(id));
        if orphaned {
            task.state = TaskState::Pending;
            task.assigned_node = None;
        }
    }
}

/// Recompute `used` from tasks that currently hold resources.
fn recount_used(inner: &mut StateInner) {
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
fn place_pending(inner: &mut StateInner, now: DateTime<Utc>) {
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
        if let Some(node_id) = pick_node(inner, &task, now) {
            let placed = match (inner.tasks.get_mut(&task_id), inner.nodes.get_mut(&node_id)) {
                (Some(task), Some(node)) => {
                    task.state = TaskState::Assigned;
                    task.assigned_node = Some(node.id.clone());
                    node.used = node.used.saturating_add(task.spec.resources);
                    true
                }
                _ => false,
            };
            let _ = placed;
        }
    }
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
    use bigtop_core::{JobId, NodeId, SnapshotPolicy, Task, TaskSpec, VmSpec};
    use std::collections::HashMap;

    fn node(id: &str, cpu: u64, mem: u64, now: DateTime<Utc>) -> NodeInfo {
        NodeInfo {
            id: NodeId::from(id.to_string()),
            name: id.to_string(),
            addr: "127.0.0.1".to_string(),
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
            },
            state: TaskState::Pending,
            assigned_node: None,
            exit_code: None,
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
        tick(&mut inner, now);
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
        tick(&mut inner, now);
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
        tick(&mut inner, now);
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
        tick(&mut inner, now);
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

        tick(&mut inner, now);

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

        tick(&mut inner, now);

        let orphan = inner
            .tasks
            .get(&TaskId::from("task-orphan".to_string()))
            .expect("orphan");
        assert_eq!(orphan.state, TaskState::Pending);
        assert_eq!(orphan.assigned_node, None);
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
        tick(&mut inner, now);
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
}
