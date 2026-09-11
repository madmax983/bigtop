//! Scheduler throughput benches. Also the callgrind target:
//! see `profiling/profile-scheduler.sh`.

use bigtop_core::{
    JobId, NodeId, NodeInfo, Resources, SnapshotPolicy, Task, TaskId, TaskSpec, TaskState, VmSpec,
};
use bigtop_server::{tick, StateInner};
use chrono::Utc;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::collections::HashMap;
use std::hint::black_box;

/// Build a synthetic cluster: `node_count` identical nodes and
/// `task_count` identical pending tasks.
fn synthetic_cluster(node_count: usize, task_count: usize) -> StateInner {
    let now = Utc::now();
    let mut inner = StateInner::default();
    for i in 0..node_count {
        let id = NodeId::from(format!("node-{i:05}"));
        inner.nodes.insert(
            id.clone(),
            NodeInfo {
                id,
                name: format!("node-{i}"),
                addr: "127.0.0.1".to_string(),
                total: Resources {
                    cpu_millis: 8000,
                    mem_mb: 32768,
                },
                used: Resources::default(),
                last_heartbeat: now,
            },
        );
    }
    for i in 0..task_count {
        let id = TaskId::from(format!("task-{i:06}"));
        inner.tasks.insert(
            id.clone(),
            Task {
                id,
                job_id: JobId::from("job-bench".to_string()),
                name: "bench".to_string(),
                spec: TaskSpec {
                    name: "bench".to_string(),
                    command: "true".to_string(),
                    args: Vec::new(),
                    env: HashMap::new(),
                    resources: Resources {
                        cpu_millis: 500,
                        mem_mb: 128,
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
            },
        );
    }
    inner
}

fn bench_schedule(criterion: &mut Criterion) {
    for (nodes, tasks) in [(100, 1_000), (1_000, 10_000)] {
        criterion.bench_function(&format!("schedule/{nodes}_nodes/{tasks}_tasks"), |b| {
            b.iter_batched(
                || synthetic_cluster(nodes, tasks),
                |mut state| tick(black_box(&mut state), black_box(Utc::now())),
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, bench_schedule);
criterion_main!(benches);
