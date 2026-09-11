//! `BigTop` core: domain types, wire DTOs, and the shared error type.
//!
//! This crate does no I/O and no async work on purpose: it is the stable
//! contract between the server, the agent, and the CLI.

pub mod api;
pub mod error;
pub mod ids;
pub mod types;

pub use api::{
    JobSummary, LogLinesResponse, PushLogsRequest, RegisterNodeRequest, RegisterNodeResponse,
    SetTaskStateRequest, SubmitJobResponse,
};
pub use error::Error;
pub use ids::{JobId, NodeId, TaskId};
pub use types::{JobSpec, NodeInfo, Resources, Task, TaskSpec, TaskState, VmSpec};
