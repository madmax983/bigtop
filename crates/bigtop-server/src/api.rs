//! The `REST` API (v1).

use crate::state::{self, AppState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bigtop_core::{
    api::{
        JobSummary, LogLinesResponse, PendingSnapshot, PushLogsRequest, RegisterNodeRequest,
        RegisterNodeResponse, ReportSnapshotResult, RequestSnapshotRequest,
        RequestSnapshotResponse, SetTaskStateRequest, SubmitJobResponse,
    },
    Error, JobSpec, NodeId, NodeInfo, SnapshotId, SnapshotRecord, Task, TaskId, TaskState,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

/// Build the v1 router. (`Router` is already `#[must_use]`.)
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/jobs", post(submit_job).get(list_jobs))
        .route("/v1/tasks", get(list_tasks))
        .route("/v1/tasks/{id}/state", post(set_task_state))
        .route("/v1/tasks/{id}/logs", post(push_logs).get(get_logs))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/nodes/register", post(register_node))
        .route("/v1/nodes/{id}/heartbeat", post(heartbeat))
        .route("/v1/agents/assignments", get(assignments))
        .route("/v1/agents/snapshot-requests", get(snapshot_requests))
        .route("/v1/tasks/{id}/snapshot", post(request_snapshot))
        .route("/v1/tasks/{id}/snapshots", get(list_snapshots))
        .route(
            "/v1/tasks/{id}/snapshots/{snapshot_id}/result",
            post(report_snapshot_result),
        )
        .with_state(state)
}

/// Maps [`Error`] to an HTTP status plus a JSON error body.
struct ApiError(Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            Error::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Error::InvalidJobSpec(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Error::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            Error::Serde(e) => (StatusCode::BAD_REQUEST, e.to_string()),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

async fn submit_job(
    State(state): State<AppState>,
    Json(spec): Json<JobSpec>,
) -> Result<impl IntoResponse, ApiError> {
    let job_id = {
        let mut inner = state.inner.write().await;
        state::create_job(&mut inner, &spec, Utc::now()).map_err(ApiError)?
    };
    Ok((StatusCode::CREATED, Json(SubmitJobResponse { job_id })))
}

async fn list_jobs(State(state): State<AppState>) -> Json<Vec<JobSummary>> {
    let mut jobs: Vec<JobSummary> = {
        let inner = state.inner.read().await;
        inner
            .jobs
            .iter()
            .map(|(id, rec)| JobSummary {
                id: id.clone(),
                name: rec.name.clone(),
                created_at: rec.created_at,
                tasks: inner.tasks.values().filter(|t| &t.job_id == id).count(),
            })
            .collect()
    };
    jobs.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Json(jobs)
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<Task>> {
    let mut tasks: Vec<Task> = {
        let inner = state.inner.read().await;
        inner.tasks.values().cloned().collect()
    };
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    Json(tasks)
}

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeInfo>> {
    let mut nodes: Vec<NodeInfo> = {
        let inner = state.inner.read().await;
        inner.nodes.values().cloned().collect()
    };
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    Json(nodes)
}

async fn register_node(
    State(state): State<AppState>,
    Json(req): Json<RegisterNodeRequest>,
) -> impl IntoResponse {
    let id = {
        let mut inner = state.inner.write().await;
        state::register_node(&mut inner, req.name, req.addr, req.total, Utc::now())
    };
    (StatusCode::CREATED, Json(RegisterNodeResponse { id }))
}

async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<NodeId>,
) -> Result<impl IntoResponse, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::heartbeat(&mut inner, &id, Utc::now()).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

#[derive(Deserialize)]
struct AssignmentsQuery {
    node_id: NodeId,
}

async fn assignments(
    State(state): State<AppState>,
    Query(query): Query<AssignmentsQuery>,
) -> Json<Vec<Task>> {
    let mut tasks: Vec<Task> = {
        let inner = state.inner.read().await;
        inner
            .tasks
            .values()
            .filter(|t| {
                t.state == TaskState::Assigned && t.assigned_node.as_ref() == Some(&query.node_id)
            })
            .cloned()
            .collect()
    };
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    Json(tasks)
}

async fn set_task_state(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<SetTaskStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::set_task_state(&mut inner, &id, req.state, req.exit_code).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

async fn push_logs(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<PushLogsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::push_logs(&mut inner, &id, &req.lines).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

async fn get_logs(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<impl IntoResponse, ApiError> {
    let lines = {
        let inner = state.inner.read().await;
        inner
            .logs
            .get(&id)
            .ok_or_else(|| ApiError(Error::NotFound(format!("task {id}"))))?
            .iter()
            .cloned()
            .collect::<Vec<String>>()
    };
    Ok(Json(LogLinesResponse { lines }))
}

async fn request_snapshot(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<RequestSnapshotRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let snapshot_id = {
        let mut inner = state.inner.write().await;
        state::request_snapshot(&mut inner, &id, &req, Utc::now()).map_err(ApiError)?
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(RequestSnapshotResponse { snapshot_id }),
    ))
}

async fn list_snapshots(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Vec<SnapshotRecord>>, ApiError> {
    let records = {
        let inner = state.inner.read().await;
        state::task_snapshots(&inner, &id).map_err(ApiError)?
    };
    Ok(Json(records))
}

#[derive(Deserialize)]
struct SnapshotRequestsQuery {
    node_id: NodeId,
}

async fn snapshot_requests(
    State(state): State<AppState>,
    Query(query): Query<SnapshotRequestsQuery>,
) -> Json<Vec<PendingSnapshot>> {
    let inner = state.inner.read().await;
    Json(state::snapshot_requests_for_node(&inner, &query.node_id))
}

async fn report_snapshot_result(
    State(state): State<AppState>,
    Path((id, snapshot_id)): Path<(TaskId, SnapshotId)>,
    Json(req): Json<ReportSnapshotResult>,
) -> Result<impl IntoResponse, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::report_snapshot_result(&mut inner, &id, &snapshot_id, &req).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}
