//! Prometheus text-format metrics (v0.4).
//!
//! Every value rendered here is maintained by the server itself: task
//! counts, live nodes, last scheduler tick duration, IPAM usage, and
//! completed snapshots. Agent-side metrics are deferred.

use crate::ipam::Ipam;
use crate::state::StateInner;
use bigtop_core::{SnapshotState, TaskState};
use chrono::{DateTime, Utc};
use std::fmt::Write as _;

/// Render the full `/metrics` body in Prometheus text format.
#[must_use]
pub fn render_metrics(inner: &StateInner, now: DateTime<Utc>) -> String {
    let mut pending = 0usize;
    let mut assigned = 0usize;
    let mut running = 0usize;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for task in inner.tasks.values() {
        match task.state {
            TaskState::Pending => pending += 1,
            TaskState::Assigned => assigned += 1,
            TaskState::Running => running += 1,
            TaskState::Succeeded => succeeded += 1,
            TaskState::Failed => failed += 1,
        }
    }
    let nodes_up = inner.nodes.values().filter(|n| n.is_alive(now)).count();
    let snapshots_done = inner
        .snapshots
        .values()
        .filter(|r| r.state == SnapshotState::Done)
        .count();
    let tick_ms = inner.last_tick_ms.unwrap_or(0);

    let mut out = String::new();
    out.push_str("# HELP bigtop_tasks Tasks by lifecycle state.\n");
    out.push_str("# TYPE bigtop_tasks gauge\n");
    for (label, count) in [
        ("pending", pending),
        ("assigned", assigned),
        ("running", running),
        ("succeeded", succeeded),
        ("failed", failed),
    ] {
        let _ = writeln!(out, "bigtop_tasks{{state=\"{label}\"}} {count}");
    }
    out.push_str("# HELP bigtop_nodes_up Nodes with a recent heartbeat.\n");
    out.push_str("# TYPE bigtop_nodes_up gauge\n");
    let _ = writeln!(out, "bigtop_nodes_up {nodes_up}");
    out.push_str("# HELP bigtop_scheduler_tick_ms Wall-clock duration of the last scheduler tick.");
    out.push_str("# TYPE bigtop_scheduler_tick_ms gauge\n");
    let _ = writeln!(out, "bigtop_scheduler_tick_ms {tick_ms}");
    out.push_str("# HELP bigtop_ipam_allocated Task pod IPs currently assigned.\n");
    out.push_str("# TYPE bigtop_ipam_allocated gauge\n");
    let _ = writeln!(
        out,
        "bigtop_ipam_allocated {}",
        inner.ipam.allocated_count()
    );
    out.push_str("# HELP bigtop_ipam_total Task pod IPs the /16 can hand out.\n");
    out.push_str("# TYPE bigtop_ipam_total gauge\n");
    let _ = writeln!(out, "bigtop_ipam_total {}", Ipam::capacity());
    out.push_str("# HELP bigtop_snapshots_done Snapshots that reached Done.\n");
    out.push_str("# TYPE bigtop_snapshots_done gauge\n");
    let _ = writeln!(out, "bigtop_snapshots_done {snapshots_done}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{create_job, register_node};
    use bigtop_core::{JobSpec, Resources, TaskSpec, VmSpec};
    use bigtop_core::{NetworkSpec, ServiceSpec, SnapshotPolicy};
    use std::collections::HashMap;

    fn task_spec() -> TaskSpec {
        TaskSpec {
            name: "t".to_string(),
            command: "true".to_string(),
            args: Vec::new(),
            env: HashMap::new(),
            resources: Resources::default(),
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
        }
    }

    #[test]
    fn metrics_render_all_series() {
        let mut inner = StateInner::default();
        let now = Utc::now();
        create_job(
            &mut inner,
            &JobSpec {
                name: "j".to_string(),
                tasks: vec![task_spec(), task_spec()],
            },
            now,
        )
        .expect("create");
        let node = register_node(
            &mut inner,
            "n1".to_string(),
            "x".to_string(),
            Resources::default(),
            None,
            now,
        )
        .expect("register");
        // One task running on the node, one still pending.
        let task_id = inner.tasks.keys().next().expect("task").clone();
        crate::state::set_task_state(&mut inner, &task_id, TaskState::Running, None)
            .expect("running");
        inner.tasks.get_mut(&task_id).expect("task").assigned_node = Some(node.clone());
        inner.last_tick_ms = Some(7);

        let body = render_metrics(&inner, now);
        assert!(
            body.contains("bigtop_tasks{state=\"pending\"} 1\n"),
            "{body}"
        );
        assert!(
            body.contains("bigtop_tasks{state=\"running\"} 1\n"),
            "{body}"
        );
        assert!(
            body.contains("bigtop_tasks{state=\"assigned\"} 0\n"),
            "{body}"
        );
        assert!(body.contains("bigtop_nodes_up 1\n"), "{body}");
        assert!(body.contains("bigtop_scheduler_tick_ms 7\n"), "{body}");
        assert!(body.contains("bigtop_ipam_allocated 0\n"), "{body}");
        assert!(
            body.contains(&format!("bigtop_ipam_total {}", Ipam::capacity())),
            "{body}"
        );
        assert!(body.contains("bigtop_snapshots_done 0\n"), "{body}");

        // A dead node is not counted as up.
        let old = now - chrono::Duration::seconds(3600);
        inner.nodes.get_mut(&node).expect("node").last_heartbeat = old;
        let body = render_metrics(&inner, now);
        assert!(body.contains("bigtop_nodes_up 0\n"), "{body}");
    }
}
