//! Wire DTOs for the `BigTop` HTTP API (v1).
//!
//! These are the JSON shapes the server, agent, and CLI agree on.

use crate::ids::{JobId, NodeId, SnapshotId, TaskId};
use crate::snapshots::{SnapshotSpec, SnapshotState, SnapshotType};
use crate::types::Resources;
use crate::types::TaskState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

/// `POST /v1/nodes/register` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterNodeRequest {
    /// Human name for the node.
    pub name: String,
    /// Informational address label.
    pub addr: String,
    /// Total resources the node offers.
    pub total: Resources,
    /// Underlay IP for the VXLAN overlay (v0.4). `None` when the agent
    /// does not participate in the overlay.
    #[serde(default)]
    pub underlay_ip: Option<Ipv4Addr>,
}

/// Heartbeat body. Every field is optional so a bare `POST` (empty body)
/// keeps working; fields present are refreshed on the node record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Underlay IP for the VXLAN overlay (v0.4). Refreshes the address
    /// the node registered with, if the agent's has changed.
    #[serde(default)]
    pub underlay_ip: Option<Ipv4Addr>,
}

/// `POST /v1/nodes/register` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterNodeResponse {
    /// The assigned node id.
    pub id: NodeId,
}

/// `POST /v1/tasks/{id}/state` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetTaskStateRequest {
    /// The state the agent reports.
    pub state: TaskState,
    /// Exit code, when the state is terminal.
    pub exit_code: Option<i32>,
}

/// `POST /v1/tasks/{id}/logs` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLogsRequest {
    /// Log lines to append (server keeps a bounded tail).
    pub lines: Vec<String>,
}

/// `GET /v1/tasks/{id}/logs` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogLinesResponse {
    /// Retained log lines, oldest first.
    pub lines: Vec<String>,
}

/// One row of `GET /v1/jobs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    /// Job id.
    pub id: JobId,
    /// Job name.
    pub name: String,
    /// Submission time.
    pub created_at: DateTime<Utc>,
    /// Number of tasks the job expanded into.
    pub tasks: usize,
}

/// `POST /v1/jobs` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    /// The new job id.
    pub job_id: JobId,
}

/// `POST /v1/tasks/{id}/snapshot` body: ask the owning agent to snapshot a
/// running microVM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestSnapshotRequest {
    /// Full or incremental snapshot. Defaults to `Full`.
    #[serde(default)]
    pub snapshot_type: SnapshotType,
    /// Destination for the guest memory file. `None` = agent default.
    #[serde(default)]
    pub mem_file_path: Option<String>,
    /// Destination for the snapshot state file. `None` = agent default.
    #[serde(default)]
    pub snapshot_path: Option<String>,
}

/// `POST /v1/tasks/{id}/snapshot` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestSnapshotResponse {
    /// The new snapshot request id.
    pub snapshot_id: SnapshotId,
}

/// `GET /v1/agents/snapshot-requests` row: a snapshot the agent should take.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingSnapshot {
    /// Snapshot request id.
    pub snapshot_id: SnapshotId,
    /// Task whose microVM to snapshot.
    pub task_id: TaskId,
    /// Requested parameters; empty paths mean "use the agent default".
    pub spec: SnapshotSpec,
}

/// `POST /v1/tasks/{id}/snapshots/{snapshot_id}/result` body: the agent
/// reports progress or the final outcome of a snapshot request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSnapshotResult {
    /// New state: `InProgress`, `Done`, or `Failed`.
    pub state: SnapshotState,
    /// Node that performed the snapshot.
    pub node_id: NodeId,
    /// Resolved guest memory file path (set on `Done`).
    #[serde(default)]
    pub mem_file_path: Option<String>,
    /// Resolved snapshot state file path (set on `Done`).
    #[serde(default)]
    pub snapshot_path: Option<String>,
    /// Failure detail (set on `Failed`).
    #[serde(default)]
    pub error: Option<String>,
}

/// `GET /v1/agents/overlay-peers` row: another node in the VXLAN mesh.
///
/// The agent builds static FDB entries from these: each peer's VTEP MAC
/// is [`vtep_mac_for_node`](crate::network::vtep_mac_for_node) of its id,
/// reachable at its underlay IP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayPeer {
    /// The peer node's id.
    pub node_id: NodeId,
    /// The peer node's underlay (VTEP) IP.
    pub underlay_ip: Ipv4Addr,
}

/// One reachable endpoint of a service: a `Running` task's pod IP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEndpoint {
    /// The serving task.
    pub task_id: TaskId,
    /// Its pod IP.
    pub ip: Ipv4Addr,
}

/// `GET /v1/services` row: a service name and its current endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    /// Service name, from the task's `[service] name`.
    pub name: String,
    /// Endpoints, sorted by task id. Only `Running` tasks with a pod IP.
    pub endpoints: Vec<ServiceEndpoint>,
}
