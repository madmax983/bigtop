//! `MicroVM` snapshot types: taking them (`PUT /snapshot/create`), loading
//! them (`PUT /snapshot/load`), and the server-side records that track them.
//!
//! A snapshot is a point-in-time capture of a running Firecracker microVM:
//! the VMM device state plus a file holding guest memory. Restoring boots a
//! new microVM from those files instead of from kernel + rootfs.
//!
//! This crate does no I/O: these are the shared shapes; the agent performs
//! the Firecracker calls and the server stores the records.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{NodeId, SnapshotId, TaskId};
use crate::types::{Task, TaskSpec};

/// Which kind of snapshot Firecracker should take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum SnapshotType {
    /// Full snapshot: complete guest memory dump. This is the default and
    /// the only type that can boot on its own.
    #[default]
    Full,
    /// Incremental snapshot: only pages dirtied since the last snapshot.
    /// Requires the VM to have been booted with diff snapshots enabled.
    Diff,
}

/// Parameters for `PUT /snapshot/create`.
///
/// Paths are host paths *as seen by the firecracker process* (inside the
/// jail when the agent runs in jailer mode). An empty path means "let the
/// agent pick the default under the task's VM directory".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotSpec {
    /// Full or incremental snapshot.
    pub snapshot_type: SnapshotType,
    /// Destination for the guest memory file. Empty = agent default.
    pub mem_file_path: String,
    /// Destination for the snapshot state file. Empty = agent default.
    pub snapshot_path: String,
}

impl SnapshotSpec {
    /// Build a spec with agent-default paths.
    #[must_use]
    pub const fn with_defaults(snapshot_type: SnapshotType) -> Self {
        Self {
            snapshot_type,
            mem_file_path: String::new(),
            snapshot_path: String::new(),
        }
    }
}

/// Parameters for `PUT /snapshot/load`: boot a microVM from snapshot files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLoadSpec {
    /// Guest memory file, as seen by the firecracker process.
    pub mem_file_path: String,
    /// Snapshot state file, as seen by the firecracker process.
    pub snapshot_path: String,
    /// Allow this restored VM to take `Diff` snapshots later.
    #[serde(default)]
    pub enable_diff_snapshots: bool,
}

/// When the agent should snapshot a microVM on its own.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPolicy {
    /// Never snapshot automatically.
    #[default]
    None,
    /// Snapshot when the guest signals command completion over vsock
    /// (before it powers off), so the VMM is still alive to be snapshotted.
    /// Best-effort: if the guest never signals, no snapshot is taken.
    OnSuccess(SnapshotSpec),
}

/// Lifecycle of a snapshot request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotState {
    /// Requested via the API; the owning agent has not picked it up yet.
    Requested,
    /// The agent started the Firecracker call.
    InProgress,
    /// Snapshot files exist and the record holds their resolved paths.
    Done,
    /// The attempt failed; see `error`.
    Failed,
}

impl SnapshotState {
    /// Whether this is a terminal state (no further transitions allowed).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
}

/// Server-side record of one snapshot request and its outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    /// Snapshot id.
    pub id: SnapshotId,
    /// Task whose microVM was (or will be) snapshotted.
    pub task_id: TaskId,
    /// Node that owns the task (and its snapshot files).
    pub node_id: NodeId,
    /// Requested parameters; paths are resolved to absolute locations once
    /// the snapshot reaches [`SnapshotState::Done`].
    pub spec: SnapshotSpec,
    /// Current lifecycle state.
    pub state: SnapshotState,
    /// Failure detail when [`SnapshotState::Failed`].
    pub error: Option<String>,
    /// When the snapshot was requested.
    pub created_at: DateTime<Utc>,
}

impl SnapshotRecord {
    /// Build a task spec that restores this snapshot: it pins the new task
    /// to the node holding the snapshot files and boots the microVM from
    /// them instead of from kernel + rootfs.
    ///
    /// The snapshot should be in [`SnapshotState::Done`]; the recorded
    /// (resolved) paths are used verbatim.
    #[must_use]
    pub fn restore_task_spec(&self, task: &Task) -> TaskSpec {
        let mut spec = task.spec.clone();
        spec.name = format!("{}-restored", task.spec.name);
        spec.node_affinity = Some(self.node_id.clone());
        // A restored task starts clean: it does not inherit the snapshotted
        // task's automatic snapshot policy.
        spec.snapshot_policy = SnapshotPolicy::None;
        spec.vm.boot_snapshot = Some(SnapshotLoadSpec {
            mem_file_path: self.spec.mem_file_path.clone(),
            snapshot_path: self.spec.snapshot_path.clone(),
            enable_diff_snapshots: false,
        });
        spec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_type_serializes_as_firecracker_spells_it() {
        assert_eq!(
            serde_json::to_string(&SnapshotType::Full).expect("serialize"),
            r#""Full""#
        );
        assert_eq!(
            serde_json::to_string(&SnapshotType::Diff).expect("serialize"),
            r#""Diff""#
        );
        assert_eq!(SnapshotType::default(), SnapshotType::Full);
    }

    #[test]
    fn snapshot_spec_roundtrips() {
        let spec = SnapshotSpec {
            snapshot_type: SnapshotType::Diff,
            mem_file_path: "/snap/x.mem".to_string(),
            snapshot_path: "/snap/x.snap".to_string(),
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: SnapshotSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, back);
    }

    #[test]
    fn snapshot_policy_defaults_to_none() {
        assert_eq!(SnapshotPolicy::default(), SnapshotPolicy::None);
        let json = serde_json::to_string(&SnapshotPolicy::None).expect("serialize");
        assert_eq!(json, r#""none""#);
    }

    #[test]
    fn snapshot_policy_on_success_parses_from_toml_shape() {
        let json =
            r#"{"on_success":{"snapshot_type":"Full","mem_file_path":"","snapshot_path":""}}"#;
        let policy: SnapshotPolicy = serde_json::from_str(json).expect("deserialize");
        assert_eq!(
            policy,
            SnapshotPolicy::OnSuccess(SnapshotSpec::with_defaults(SnapshotType::Full))
        );
    }

    #[test]
    fn restore_task_spec_pins_node_and_boots_snapshot() {
        use crate::types::{Resources, Task, TaskState, VmSpec};

        let task = Task {
            id: TaskId::from("task-1".to_string()),
            job_id: crate::ids::JobId::from("job-1".to_string()),
            name: "web".to_string(),
            spec: TaskSpec {
                name: "web".to_string(),
                command: "run".to_string(),
                args: vec![],
                env: std::collections::HashMap::new(),
                resources: Resources::default(),
                count: 1,
                vm: VmSpec::default(),
                node_affinity: None,
                snapshot_policy: SnapshotPolicy::None,
            },
            state: TaskState::Running,
            assigned_node: Some(NodeId::from("node-9".to_string())),
            exit_code: None,
        };
        let record = SnapshotRecord {
            id: SnapshotId::from("snap-1".to_string()),
            task_id: task.id.clone(),
            node_id: NodeId::from("node-9".to_string()),
            spec: SnapshotSpec {
                snapshot_type: SnapshotType::Full,
                mem_file_path: "/vms/task-1/snapshots/snap-1.mem".to_string(),
                snapshot_path: "/vms/task-1/snapshots/snap-1.snap".to_string(),
            },
            state: SnapshotState::Done,
            error: None,
            created_at: Utc::now(),
        };
        let restored = record.restore_task_spec(&task);
        assert_eq!(restored.name, "web-restored");
        assert_eq!(
            restored.node_affinity,
            Some(NodeId::from("node-9".to_string()))
        );
        let boot = restored.vm.boot_snapshot.expect("boot snapshot");
        assert_eq!(boot.mem_file_path, "/vms/task-1/snapshots/snap-1.mem");
        assert_eq!(boot.snapshot_path, "/vms/task-1/snapshots/snap-1.snap");
        assert!(!boot.enable_diff_snapshots);
        // The original task is untouched.
        assert!(task.spec.vm.boot_snapshot.is_none());
        assert!(task.spec.node_affinity.is_none());
    }
}
