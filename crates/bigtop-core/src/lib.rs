//! `BigTop` core: domain types, wire DTOs, and the shared error type.
//!
//! This crate does no I/O and no async work on purpose: it is the stable
//! contract between the server, the agent, and the CLI.

pub mod api;
pub mod error;
pub mod ids;
pub mod network;
pub mod snapshots;
pub mod types;

pub use api::{
    HeartbeatRequest, JobSummary, LogLinesResponse, OverlayPeer, PendingSnapshot, PushLogsRequest,
    RegisterNodeRequest, RegisterNodeResponse, ReportSnapshotResult, RequestSnapshotRequest,
    RequestSnapshotResponse, ServiceEndpoint, ServiceInfo, SetTaskStateRequest, SubmitJobResponse,
};
pub use error::Error;
pub use ids::{JobId, NodeId, SnapshotId, TaskId};
pub use network::{
    mac_for_str, mac_for_task, tap_name_for, vtep_mac_for_node, MacAddr, NetworkAssignment,
    NetworkSpec, ServiceSpec,
};
pub use snapshots::{
    SnapshotLoadSpec, SnapshotPolicy, SnapshotRecord, SnapshotSpec, SnapshotState, SnapshotType,
};
pub use types::{JobSpec, NodeInfo, Resources, Task, TaskSpec, TaskState, VmSpec};
