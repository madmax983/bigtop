//! Domain types: resources, VM specs, task specs, tasks, and nodes.

use crate::ids::{JobId, NodeId, TaskId};
use crate::network::{NetworkAssignment, NetworkSpec};
use crate::snapshots::{SnapshotLoadSpec, SnapshotPolicy};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// A node is dead when its heartbeat is older than this many seconds.
pub const HEARTBEAT_TIMEOUT_SECS: i64 = 10;

/// Compute resources: millicores of CPU and mebibytes of RAM.
///
/// This is the currency the scheduler bins. For microVM tasks, [`VmSpec`]
/// describes what the guest actually gets; `cpu_millis` also sizes the
/// guest vCPU count (1000 millicores = 1 vCPU, rounded up).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resources {
    /// CPU in millicores (1000 = one vCPU).
    #[serde(default)]
    pub cpu_millis: u64,
    /// Memory in MiB.
    #[serde(default)]
    pub mem_mb: u64,
}

impl Resources {
    /// True if `self` still fits into `total` once `used` is committed.
    #[must_use]
    pub const fn fits_in(self, total: Self, used: Self) -> bool {
        used.cpu_millis.saturating_add(self.cpu_millis) <= total.cpu_millis
            && used.mem_mb.saturating_add(self.mem_mb) <= total.mem_mb
    }

    /// Saturating addition, for resource accounting.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            mem_mb: self.mem_mb.saturating_add(other.mem_mb),
        }
    }
}

/// What the microVM guest looks like.
///
/// Every `BigTop` task is a Firecracker microVM: that is the whole point.
/// `BigTop` does not do generic OCI containers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmSpec {
    /// Path to the guest kernel image (`vmlinux`, uncompressed).
    #[serde(default)]
    pub kernel_image: String,
    /// Path to the guest rootfs image (ext4).
    #[serde(default)]
    pub rootfs: String,
    /// vCPUs for the microVM.
    #[serde(default = "default_vcpu_count")]
    pub vcpu_count: u32,
    /// Guest RAM in MiB.
    #[serde(default = "default_vm_mem_mb")]
    pub mem_mb: u64,
    /// Extra kernel boot args. `BigTop` appends its own task parameters.
    #[serde(default)]
    pub boot_args: Option<String>,
    /// Boot the microVM from a snapshot instead of kernel + rootfs.
    /// When set, the Firecracker `PUT /snapshot/load` path replaces the
    /// machine-config / boot-source / drive configuration.
    #[serde(default)]
    pub boot_snapshot: Option<SnapshotLoadSpec>,
}

impl Default for VmSpec {
    /// Sensible defaults: empty image paths (must be set before use),
    /// 1 vCPU, 128 MiB, no extra boot args, fresh boot.
    fn default() -> Self {
        Self {
            kernel_image: String::new(),
            rootfs: String::new(),
            vcpu_count: default_vcpu_count(),
            mem_mb: default_vm_mem_mb(),
            boot_args: None,
            boot_snapshot: None,
        }
    }
}

const fn default_vcpu_count() -> u32 {
    1
}

const fn default_vm_mem_mb() -> u64 {
    128
}

/// One runnable unit inside a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    /// Human name, e.g. `"greeter"`.
    pub name: String,
    /// Command to run inside the guest (see `VmSpec` for how it gets there).
    pub command: String,
    /// Arguments to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment for the command.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Scheduler resources; `cpu_millis` also sizes microVM vCPUs.
    #[serde(default)]
    pub resources: Resources,
    /// How many identical tasks to expand this spec into.
    #[serde(default = "default_count")]
    pub count: u32,
    /// The microVM to boot for each task.
    #[serde(default)]
    pub vm: VmSpec,
    /// Pin this task to a specific node. Used by snapshot restore so the
    /// new task lands where the snapshot files live.
    #[serde(default)]
    pub node_affinity: Option<NodeId>,
    /// Automatic microVM snapshotting. Only applies to microVM tasks.
    #[serde(default)]
    pub snapshot_policy: SnapshotPolicy,
    /// Per-task network request, from the `[network]` TOML section.
    /// Disabled by default.
    #[serde(default)]
    pub network: NetworkSpec,
}

const fn default_count() -> u32 {
    1
}

/// A job: a named bag of task specs, expanded into tasks on submit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    /// Job name, e.g. `"hello"`.
    pub name: String,
    /// Accepts `[[task]]` tables in `TOML` via the alias; serializes as `tasks`.
    #[serde(alias = "task", default)]
    pub tasks: Vec<TaskSpec>,
}

/// Lifecycle state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Waiting for the scheduler.
    Pending,
    /// Placed on a node, not yet acknowledged by its agent.
    Assigned,
    /// Agent reports the microVM is booted / process is running.
    Running,
    /// Finished with exit code 0.
    Succeeded,
    /// Finished with non-zero exit, or the agent failed to run it.
    Failed,
}

impl TaskState {
    /// True for `Succeeded` / `Failed`: no further transitions are allowed.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Assigned => "assigned",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        };
        f.write_str(s)
    }
}

/// A single scheduled unit of work: one microVM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// Task id.
    pub id: TaskId,
    /// Owning job id.
    pub job_id: JobId,
    /// Task spec name (denormalized for display).
    pub name: String,
    /// The spec, with `count` normalized to 1.
    pub spec: TaskSpec,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Node the task is placed on, if any.
    pub assigned_node: Option<NodeId>,
    /// Process exit code, once terminal.
    pub exit_code: Option<i32>,
    /// The server's IPAM assignment for this task. Set by the server when
    /// the task's [`NetworkSpec`] is enabled; the agent uses it to configure
    /// the tap interface.
    #[serde(default)]
    pub network: Option<NetworkAssignment>,
}

/// An agent node known to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node id.
    pub id: NodeId,
    /// Human name supplied at registration.
    pub name: String,
    /// Informational address label (v0.1; used by service discovery in v0.3).
    pub addr: String,
    /// Total resources the node offers.
    pub total: Resources,
    /// Resources currently committed to non-terminal tasks.
    pub used: Resources,
    /// Last heartbeat received from the agent.
    pub last_heartbeat: DateTime<Utc>,
}

impl NodeInfo {
    /// True when a heartbeat arrived within [`HEARTBEAT_TIMEOUT_SECS`].
    #[must_use]
    pub fn is_alive(&self, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(self.last_heartbeat).num_seconds() <= HEARTBEAT_TIMEOUT_SECS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resources_fit_logic() {
        let total = Resources {
            cpu_millis: 1000,
            mem_mb: 512,
        };
        let used = Resources {
            cpu_millis: 600,
            mem_mb: 100,
        };
        let fits = Resources {
            cpu_millis: 400,
            mem_mb: 100,
        };
        let too_much_cpu = Resources {
            cpu_millis: 401,
            mem_mb: 100,
        };
        let too_much_mem = Resources {
            cpu_millis: 100,
            mem_mb: 500,
        };
        assert!(fits.fits_in(total, used));
        assert!(!too_much_cpu.fits_in(total, used));
        assert!(!too_much_mem.fits_in(total, used));
    }

    #[test]
    fn saturating_add_does_not_overflow() {
        let big = Resources {
            cpu_millis: u64::MAX,
            mem_mb: u64::MAX,
        };
        let sum = big.saturating_add(big);
        assert_eq!(sum.cpu_millis, u64::MAX);
        assert_eq!(sum.mem_mb, u64::MAX);
    }

    #[test]
    fn terminal_states() {
        assert!(!TaskState::Pending.is_terminal());
        assert!(!TaskState::Assigned.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(TaskState::Succeeded.is_terminal());
        assert!(TaskState::Failed.is_terminal());
    }

    #[test]
    fn task_state_display_matches_json() {
        let json = serde_json::to_string(&TaskState::Running).expect("serialize");
        assert_eq!(json, r#""running""#);
        assert_eq!(TaskState::Running.to_string(), "running");
    }

    #[test]
    fn job_spec_accepts_toml_task_tables() {
        let toml = r#"
            name = "hello"

            [[task]]
            name = "greeter"
            command = "sh"
            args = ["-c", "echo hi"]
            count = 3

            [task.resources]
            cpu_millis = 100
            mem_mb = 64

            [task.vm]
            kernel_image = "/boot/vmlinux"
            rootfs = "/images/rootfs.ext4"
        "#;
        let spec: JobSpec = toml::from_str(toml).expect("parse toml");
        assert_eq!(spec.name, "hello");
        assert_eq!(spec.tasks.len(), 1);
        let task = &spec.tasks[0];
        assert_eq!(task.count, 3);
        assert_eq!(task.args, vec!["-c".to_string(), "echo hi".to_string()]);
        assert_eq!(task.resources.cpu_millis, 100);
        assert_eq!(task.vm.kernel_image, "/boot/vmlinux");
        assert_eq!(task.vm.vcpu_count, 1);
        assert_eq!(task.vm.mem_mb, 128);
    }

    #[test]
    fn job_spec_json_uses_tasks_key() {
        let json = r#"{"name":"j","tasks":[{"name":"t","command":"sh"}]}"#;
        let spec: JobSpec = serde_json::from_str(json).expect("parse json");
        assert_eq!(spec.tasks.len(), 1);
        assert_eq!(spec.tasks[0].count, 1);
        assert_eq!(spec.tasks[0].vm.mem_mb, 128);
    }

    #[test]
    fn task_spec_with_network_section_parses_end_to_end() {
        let toml = r#"
            name = "web"

            [[task]]
            name = "web-1"
            command = "serve"
            args = ["--port", "8080"]
            count = 2

            [task.resources]
            cpu_millis = 500
            mem_mb = 256

            [task.vm]
            kernel_image = "/boot/vmlinux"
            rootfs = "/images/rootfs.ext4"

            [task.network]
            enabled = true
            hostname = "web-1"
        "#;
        let spec: JobSpec = toml::from_str(toml).expect("parse toml");
        assert_eq!(spec.tasks.len(), 1);
        let task = &spec.tasks[0];
        assert_eq!(task.name, "web-1");
        assert_eq!(task.command, "serve");
        assert_eq!(task.count, 2);
        assert_eq!(task.resources.cpu_millis, 500);
        assert_eq!(task.vm.kernel_image, "/boot/vmlinux");
        assert!(task.network.enabled);
        assert_eq!(task.network.hostname.as_deref(), Some("web-1"));

        // Serialization keeps the network section.
        let json = serde_json::to_string(task).expect("serialize");
        assert!(json.contains(r#""enabled":true"#));
        let back: TaskSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(*task, back);
    }

    #[test]
    fn task_and_node_roundtrip() {
        let task = Task {
            id: TaskId::from("task-1".to_string()),
            job_id: JobId::from("job-1".to_string()),
            name: "t".to_string(),
            spec: TaskSpec {
                name: "t".to_string(),
                command: "sh".to_string(),
                args: vec![],
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
            },
            state: TaskState::Pending,
            assigned_node: None,
            exit_code: None,
            network: None,
        };
        let json = serde_json::to_string(&task).expect("serialize");
        let back: Task = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(task, back);

        let node = NodeInfo {
            id: NodeId::from("node-1".to_string()),
            name: "n".to_string(),
            addr: "127.0.0.1".to_string(),
            total: Resources {
                cpu_millis: 4000,
                mem_mb: 8192,
            },
            used: Resources::default(),
            last_heartbeat: Utc::now(),
        };
        let json = serde_json::to_string(&node).expect("serialize");
        let back: NodeInfo = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(node, back);
    }

    #[test]
    fn node_liveness_uses_heartbeat_timeout() {
        let now = Utc::now();
        let fresh = NodeInfo {
            id: NodeId::generate(),
            name: "n".to_string(),
            addr: String::new(),
            total: Resources::default(),
            used: Resources::default(),
            last_heartbeat: now,
        };
        assert!(fresh.is_alive(now));
        let stale = NodeInfo {
            last_heartbeat: now - chrono::Duration::seconds(HEARTBEAT_TIMEOUT_SECS + 1),
            ..fresh
        };
        assert!(!stale.is_alive(now));
    }
}
