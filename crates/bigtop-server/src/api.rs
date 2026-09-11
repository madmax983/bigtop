//! The `REST` API (v1).

use crate::state::{self, AppState, StateInner};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bigtop_core::{
    api::{
        HeartbeatRequest, JobSummary, LogLinesResponse, OverlayPeer, PendingSnapshot,
        PushLogsRequest, RegisterNodeRequest, RegisterNodeResponse, ReportSnapshotResult,
        RequestSnapshotRequest, RequestSnapshotResponse, ServiceInfo, SetTaskStateRequest,
        SubmitJobResponse,
    },
    Error, JobSpec, NodeId, NodeInfo, SnapshotId, SnapshotRecord, Task, TaskId, TaskState,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use std::fmt::Write as _;

/// Build the v1 router. (`Router` is already `#[must_use]`.)
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(status_page))
        .route("/metrics", get(metrics))
        .route("/v1/jobs", post(submit_job).get(list_jobs))
        .route("/v1/tasks", get(list_tasks))
        .route("/v1/tasks/{id}/state", post(set_task_state))
        .route("/v1/tasks/{id}/logs", post(push_logs).get(get_logs))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/nodes/register", post(register_node))
        .route("/v1/nodes/{id}/heartbeat", post(heartbeat))
        .route("/v1/services", get(list_services))
        .route("/v1/agents/assignments", get(assignments))
        .route("/v1/agents/overlay-peers", get(overlay_peers))
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
            Error::Persistence(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
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
) -> Result<impl IntoResponse, ApiError> {
    let id = {
        let mut inner = state.inner.write().await;
        state::register_node(
            &mut inner,
            req.name,
            req.addr,
            req.total,
            req.underlay_ip,
            Utc::now(),
        )
        .map_err(ApiError)?
    };
    Ok((StatusCode::CREATED, Json(RegisterNodeResponse { id })))
}

async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<NodeId>,
    body: Option<Json<HeartbeatRequest>>,
) -> Result<impl IntoResponse, ApiError> {
    {
        let mut inner = state.inner.write().await;
        let underlay_ip = body.and_then(|b| b.underlay_ip);
        state::heartbeat(&mut inner, &id, underlay_ip, Utc::now()).map_err(ApiError)?;
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

/// Prometheus text-format metrics (v0.4).
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = {
        let inner = state.inner.read().await;
        crate::metrics::render_metrics(&inner, Utc::now())
    };
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

/// Service discovery: every named service and its running endpoints (v0.4).
async fn list_services(State(state): State<AppState>) -> Json<Vec<ServiceInfo>> {
    let inner = state.inner.read().await;
    Json(state::service_endpoints(&inner))
}

#[derive(Deserialize)]
struct OverlayPeersQuery {
    node_id: NodeId,
}

/// The VXLAN mesh as seen by `node_id`: every *other* alive node that
/// reported an underlay IP (v0.4). The agent turns these into static FDB
/// entries on its `vxlan<vni>` device.
async fn overlay_peers(
    State(state): State<AppState>,
    Query(query): Query<OverlayPeersQuery>,
) -> Json<Vec<OverlayPeer>> {
    let now = Utc::now();
    let mut peers: Vec<OverlayPeer> = {
        let inner = state.inner.read().await;
        inner
            .nodes
            .values()
            .filter(|node| node.id != query.node_id && node.is_alive(now))
            .filter_map(|node| {
                node.underlay_ip.map(|underlay_ip| OverlayPeer {
                    node_id: node.id.clone(),
                    underlay_ip,
                })
            })
            .collect()
    };
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Json(peers)
}

/// Minimal HTML escape for the status page.
fn esc(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Server-rendered status page (v0.4): nodes, services, tasks, IPAM.
/// Static HTML, no JavaScript.
async fn status_page(State(state): State<AppState>) -> Html<String> {
    let now = Utc::now();
    // The read guard is dropped before the response is built: it is only
    // needed while the row strings are rendered.
    let (node_rows, node_count, task_rows, task_count, service_rows, ipam_allocated) = {
        let inner = state.inner.read().await;
        (
            render_node_rows(&inner, now),
            inner.nodes.len(),
            render_task_rows(&inner),
            inner.tasks.len(),
            render_service_rows(&inner),
            inner.ipam.allocated_count(),
        )
    };

    let body = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>BigTop status</title>\n\
         <style>body{{font-family:monospace;margin:2em}}table{{border-collapse:collapse}}\
         td,th{{border:1px solid #999;padding:4px 8px;text-align:left}}\
         th{{background:#eee}}</style>\n</head>\n<body>\n\
         <h1>BigTop status</h1>\n\
         <h2>Nodes ({})</h2>\n\
         <table>\n<tr><th>id</th><th>name</th><th>addr</th><th>underlay</th><th>alive</th>\
         <th>cpu (m)</th><th>mem (mb)</th></tr>\n{node_rows}</table>\n\
         <h2>Services</h2>\n<table>\n<tr><th>service</th><th>endpoints</th></tr>\n\
         {service_rows}</table>\n\
         <h2>Tasks ({})</h2>\n<table>\n\
         <tr><th>id</th><th>name</th><th>state</th><th>node</th><th>ip</th><th>service</th></tr>\n\
         {task_rows}</table>\n\
         <h2>IPAM</h2>\n<p>{} allocated / {} total</p>\n\
         <p><a href=\"/metrics\">Prometheus metrics</a></p>\n\
         </body>\n</html>",
        node_count,
        task_count,
        ipam_allocated,
        crate::ipam::Ipam::capacity(),
    );
    Html(body)
}

/// Render the nodes table rows for the status page.
fn render_node_rows(inner: &StateInner, now: DateTime<Utc>) -> String {
    let mut nodes: Vec<&NodeInfo> = inner.nodes.values().collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    let mut rows = String::new();
    for node in nodes {
        let alive = if node.is_alive(now) { "yes" } else { "no" };
        let underlay = node
            .underlay_ip
            .map_or_else(|| "-".to_string(), |ip| ip.to_string());
        let _ = writeln!(
            rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td><td>{}/{}</td></tr>",
            esc(node.id.as_ref()),
            esc(&node.name),
            esc(&node.addr),
            underlay,
            alive,
            node.used.cpu_millis,
            node.total.cpu_millis,
            node.used.mem_mb,
            node.total.mem_mb,
        );
    }
    rows
}

/// Render the tasks table rows for the status page.
fn render_task_rows(inner: &StateInner) -> String {
    let mut tasks: Vec<&Task> = inner.tasks.values().collect();
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    let mut rows = String::new();
    for task in tasks {
        let node = task
            .assigned_node
            .as_ref()
            .map_or_else(|| "-".to_string(), |id| esc(id.as_ref()));
        let ip = task
            .network
            .as_ref()
            .map_or_else(|| "-".to_string(), |net| net.ip.to_string());
        let service = task
            .spec
            .service
            .name
            .as_deref()
            .map_or_else(|| "-".to_string(), esc);
        let _ = writeln!(
            rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            esc(task.id.as_ref()),
            esc(&task.name),
            task.state,
            node,
            ip,
            service,
        );
    }
    rows
}

/// Render the services table rows for the status page.
fn render_service_rows(inner: &StateInner) -> String {
    let mut rows = String::new();
    for service in state::service_endpoints(inner) {
        let endpoints: Vec<String> = service
            .endpoints
            .iter()
            .map(|e| format!("{} ({})", esc(e.task_id.as_ref()), e.ip))
            .collect();
        let _ = writeln!(
            rows,
            "<tr><td>{}</td><td>{}</td></tr>",
            esc(&service.name),
            endpoints.join(", ")
        );
    }
    if rows.is_empty() {
        rows.push_str("<tr><td colspan=\"2\">no services registered</td></tr>\n");
    }
    rows
}
