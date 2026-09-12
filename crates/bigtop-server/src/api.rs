//! The `REST` API (v1).
//!
//! v0.5: the control plane is served through Autumn. The sixteen typed `/v1`
//! handlers are Autumn routes scoped under `/v1` behind bearer-token auth;
//! the two edge routes (`/`, `/metrics`) are typed Autumn routes registered
//! globally through `.routes()`. Wire behavior is unchanged.

use crate::state::{self, AppState, StateInner};
use autumn_web::{get, post, routes, Route};
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Extension, Json,
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

/// Build the edge routes.
///
/// The status page and Prometheus metrics as typed Autumn routes, registered
/// globally (not under the `/v1` scope). They are typed handlers exactly
/// like the `/v1` ones: Autumn's startup validation only counts routes
/// registered through `.routes()` — `.scoped()` groups and `.merge()`d raw
/// routers do not satisfy it, so a scoped-only app panics at boot with "No
/// routes registered". Auth comes from the app's global token layer and
/// `Extension<AppState>` from the app's global extension layer, matching
/// what the old merged Axum router installed.
#[must_use]
pub fn root_routes() -> Vec<Route> {
    routes![status_page, metrics]
}

/// The sixteen typed `/v1` handlers as Autumn routes, mounted under the
/// `/v1` scope in the app builder.
#[must_use]
pub fn autumn_routes() -> Vec<Route> {
    routes![
        submit_job,
        list_jobs,
        list_tasks,
        set_task_state,
        push_logs,
        get_logs,
        list_nodes,
        register_node,
        heartbeat,
        list_services,
        assignments,
        overlay_peers,
        snapshot_requests,
        request_snapshot,
        list_snapshots,
        report_snapshot_result,
    ]
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

#[post("/jobs")]
#[api_doc(
    mcp,
    tag = "jobs",
    summary = "Submit a job spec; the scheduler fans it out into tasks"
)]
async fn submit_job(
    Extension(state): Extension<AppState>,
    Json(spec): Json<JobSpec>,
) -> Result<(StatusCode, Json<SubmitJobResponse>), ApiError> {
    let job_id = {
        let mut inner = state.inner.write().await;
        state::create_job(&mut inner, &spec, Utc::now()).map_err(ApiError)?
    };
    Ok((StatusCode::CREATED, Json(SubmitJobResponse { job_id })))
}

#[get("/jobs")]
#[api_doc(
    mcp,
    tag = "jobs",
    summary = "List all submitted jobs with task counts"
)]
async fn list_jobs(Extension(state): Extension<AppState>) -> Json<Vec<JobSummary>> {
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

#[get("/tasks")]
#[api_doc(mcp, tag = "tasks", summary = "List every task across all jobs")]
async fn list_tasks(Extension(state): Extension<AppState>) -> Json<Vec<Task>> {
    let mut tasks: Vec<Task> = {
        let inner = state.inner.read().await;
        inner.tasks.values().cloned().collect()
    };
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    Json(tasks)
}

#[get("/nodes")]
#[api_doc(mcp, tag = "nodes", summary = "List every registered agent node")]
async fn list_nodes(Extension(state): Extension<AppState>) -> Json<Vec<NodeInfo>> {
    let mut nodes: Vec<NodeInfo> = {
        let inner = state.inner.read().await;
        inner.nodes.values().cloned().collect()
    };
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    Json(nodes)
}

#[post("/nodes/register")]
#[api_doc(tag = "nodes", summary = "Register an agent node (agent handshake)")]
async fn register_node(
    Extension(state): Extension<AppState>,
    Json(req): Json<RegisterNodeRequest>,
) -> Result<(StatusCode, Json<RegisterNodeResponse>), ApiError> {
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

#[post("/nodes/{id}/heartbeat")]
#[api_doc(tag = "nodes", summary = "Agent heartbeat; refreshes liveness")]
async fn heartbeat(
    Extension(state): Extension<AppState>,
    Path(id): Path<NodeId>,
    body: Option<Json<HeartbeatRequest>>,
) -> Result<Json<serde_json::Value>, ApiError> {
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

#[get("/agents/assignments")]
#[api_doc(
    mcp,
    tag = "agents",
    summary = "Tasks assigned to a node but not yet picked up"
)]
async fn assignments(
    Extension(state): Extension<AppState>,
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

#[post("/tasks/{id}/state")]
#[api_doc(tag = "tasks", summary = "Report a task state transition (agent)")]
async fn set_task_state(
    Extension(state): Extension<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<SetTaskStateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::set_task_state(&mut inner, &id, req.state, req.exit_code).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

#[post("/tasks/{id}/logs")]
#[api_doc(tag = "tasks", summary = "Append log lines to a task (agent)")]
async fn push_logs(
    Extension(state): Extension<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<PushLogsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::push_logs(&mut inner, &id, &req.lines).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

#[get("/tasks/{id}/logs")]
#[api_doc(mcp, tag = "tasks", summary = "Fetch a task's buffered log lines")]
async fn get_logs(
    Extension(state): Extension<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<LogLinesResponse>, ApiError> {
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

#[post("/tasks/{id}/snapshot")]
#[api_doc(tag = "snapshots", summary = "Request a snapshot of a task")]
async fn request_snapshot(
    Extension(state): Extension<AppState>,
    Path(id): Path<TaskId>,
    Json(req): Json<RequestSnapshotRequest>,
) -> Result<(StatusCode, Json<RequestSnapshotResponse>), ApiError> {
    let snapshot_id = {
        let mut inner = state.inner.write().await;
        state::request_snapshot(&mut inner, &id, &req, Utc::now()).map_err(ApiError)?
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(RequestSnapshotResponse { snapshot_id }),
    ))
}

#[get("/tasks/{id}/snapshots")]
#[api_doc(mcp, tag = "snapshots", summary = "List a task's snapshot records")]
async fn list_snapshots(
    Extension(state): Extension<AppState>,
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

#[get("/agents/snapshot-requests")]
#[api_doc(
    mcp,
    tag = "agents",
    summary = "Pending snapshot requests for a node (agent poll)"
)]
async fn snapshot_requests(
    Extension(state): Extension<AppState>,
    Query(query): Query<SnapshotRequestsQuery>,
) -> Json<Vec<PendingSnapshot>> {
    let inner = state.inner.read().await;
    Json(state::snapshot_requests_for_node(&inner, &query.node_id))
}

#[post("/tasks/{id}/snapshots/{snapshot_id}/result")]
#[api_doc(tag = "snapshots", summary = "Report a snapshot outcome (agent)")]
async fn report_snapshot_result(
    Extension(state): Extension<AppState>,
    Path((id, snapshot_id)): Path<(TaskId, SnapshotId)>,
    Json(req): Json<ReportSnapshotResult>,
) -> Result<Json<serde_json::Value>, ApiError> {
    {
        let mut inner = state.inner.write().await;
        state::report_snapshot_result(&mut inner, &id, &snapshot_id, &req).map_err(ApiError)?;
    }
    Ok(Json(json!({ "ok": true })))
}

/// Prometheus text-format metrics (v0.4).
#[get("/metrics")]
#[api_doc(tag = "ops", summary = "Prometheus metrics for the control plane")]
async fn metrics(Extension(state): Extension<AppState>) -> impl IntoResponse {
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
#[get("/services")]
#[api_doc(
    mcp,
    tag = "services",
    summary = "Service discovery: named services and their running endpoints"
)]
async fn list_services(Extension(state): Extension<AppState>) -> Json<Vec<ServiceInfo>> {
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
#[get("/agents/overlay-peers")]
#[api_doc(
    mcp,
    tag = "agents",
    summary = "VXLAN mesh peers for a node (underlay IPs of other alive nodes)"
)]
async fn overlay_peers(
    Extension(state): Extension<AppState>,
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
#[get("/")]
#[api_doc(tag = "ops", summary = "Static HTML status page")]
async fn status_page(Extension(state): Extension<AppState>) -> Html<String> {
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
