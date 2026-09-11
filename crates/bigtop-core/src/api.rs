//! Wire DTOs for the `BigTop` HTTP API (v1).
//!
//! These are the JSON shapes the server, agent, and CLI agree on.

use crate::ids::{JobId, NodeId};
use crate::types::Resources;
use crate::types::TaskState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `POST /v1/nodes/register` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterNodeRequest {
    /// Human name for the node.
    pub name: String,
    /// Informational address label.
    pub addr: String,
    /// Total resources the node offers.
    pub total: Resources,
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
