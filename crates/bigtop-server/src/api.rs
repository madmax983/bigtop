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
        shadow_verdicts,
    ]
}

/// Maps [`Error`] to an HTTP status plus a JSON error body.
#[derive(Debug)]
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
    let record = {
        let mut inner = state.inner.write().await;
        state::request_snapshot(&mut inner, &id, &req, Utc::now()).map_err(ApiError)?
    };
    // Shadow hook (v0.6): only after the authoritative mutation, journal
    // append, and lock release. Best-effort and infallible — never fails
    // the request. The record carries the real node id; no second lookup.
    if let Some(shadow) = &state.shadow {
        shadow.mirror_snapshot(&record);
        shadow.notify_requested(
            record.id.clone(),
            record.task_id.clone(),
            record.node_id.clone(),
        );
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(RequestSnapshotResponse {
            snapshot_id: record.id,
        }),
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
    let record = {
        let mut inner = state.inner.write().await;
        state::report_snapshot_result(&mut inner, &id, &snapshot_id, &req).map_err(ApiError)?
    };
    // Shadow hook (v0.6): mirror the outcome after the authoritative
    // mutation, journal append, and lock release. Best-effort and
    // infallible — never fails the report.
    if let Some(shadow) = &state.shadow {
        shadow.mirror_snapshot(&record);
    }
    Ok(Json(json!({ "ok": true })))
}

/// Harvest shadow verdicts (v0.6, opt-in). Read-only: every tracked
/// snapshot, newest first. 404 when the shadow is disabled.
#[get("/shadow/snapshots")]
#[api_doc(
    mcp,
    tag = "shadow",
    summary = "Harvest shadow verdicts for tracked snapshots (read-only)"
)]
async fn shadow_verdicts(
    Extension(state): Extension<AppState>,
) -> Result<Json<Vec<crate::harvest_shadow::ShadowTrack>>, ApiError> {
    let shadow = state
        .shadow
        .as_ref()
        .ok_or_else(|| ApiError(Error::NotFound("harvest shadow is not enabled".to_string())))?;
    shadow
        .tracks()
        .map(Json)
        .map_err(|e| ApiError(Error::Persistence(format!("shadow store: {e}"))))
}

/// Prometheus text-format metrics (v0.4).
#[get("/metrics")]
#[api_doc(tag = "ops", summary = "Prometheus metrics for the control plane")]
async fn metrics(Extension(state): Extension<AppState>) -> impl IntoResponse {
    let mut out = {
        let inner = state.inner.read().await;
        crate::metrics::render_metrics(&inner, Utc::now())
    };
    // Harvest shadow metrics (v0.6): only when the shadow is enabled.
    // The state lock is dropped before this — shadow stats are independent.
    if let Some(shadow) = &state.shadow {
        render_shadow_metrics(shadow, &mut out);
    }
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
}

/// Append the four Harvest-shadow metric families (SPEC v0.6): audits
/// started, observations recorded, contained shadow failures, and the
/// per-verdict gauge. A verdict-store failure only costs the gauge —
/// the counters above are lock-free.
fn render_shadow_metrics(shadow: &crate::harvest_shadow::ShadowHandle, out: &mut String) {
    use std::fmt::Write as _;
    let shadow_stats = shadow.stats();
    out.push_str("# HELP bigtop_harvest_shadow_workflows_started_total Snapshot audits started by the Harvest shadow.\n");
    out.push_str("# TYPE bigtop_harvest_shadow_workflows_started_total counter\n");
    let _ = writeln!(
        out,
        "bigtop_harvest_shadow_workflows_started_total {}",
        shadow_stats.started
    );
    out.push_str("# HELP bigtop_harvest_shadow_observations_total Observations recorded by the Harvest shadow.\n");
    out.push_str("# TYPE bigtop_harvest_shadow_observations_total counter\n");
    let _ = writeln!(
        out,
        "bigtop_harvest_shadow_observations_total {}",
        shadow_stats.observations
    );
    out.push_str("# HELP bigtop_harvest_shadow_errors_total Contained Harvest shadow failures.\n");
    out.push_str("# TYPE bigtop_harvest_shadow_errors_total counter\n");
    let _ = writeln!(
        out,
        "bigtop_harvest_shadow_errors_total {}",
        shadow_stats.errors
    );
    // Per-verdict breakdown (v0.6).
    if let Ok(verdicts) = shadow.metrics_snapshot() {
        out.push_str(
            "# HELP bigtop_harvest_shadow_verdict_total Tracked snapshots by current shadow verdict.\n",
        );
        out.push_str("# TYPE bigtop_harvest_shadow_verdict_total gauge\n");
        for (verdict, count) in [
            ("pending", verdicts.pending),
            ("agree", verdicts.agree),
            ("stuck", verdicts.stuck),
            ("missing", verdicts.missing),
            ("harvest_error", verdicts.harvest_error),
        ] {
            let _ = writeln!(
                out,
                "bigtop_harvest_shadow_verdict_total{{verdict=\"{verdict}\"}} {count}"
            );
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalWriter;
    use bigtop_core::{
        JobId, NetworkSpec, Resources, ServiceSpec, SnapshotPolicy, SnapshotType, TaskSpec, VmSpec,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// A `StateInner` holding one `Running` task on a node.
    fn running_task_inner() -> (StateInner, TaskId) {
        let mut inner = StateInner::default();
        let task_id = TaskId::generate();
        inner.tasks.insert(
            task_id.clone(),
            Task {
                id: task_id.clone(),
                job_id: JobId::generate(),
                name: "t".to_string(),
                spec: TaskSpec {
                    name: "t".to_string(),
                    command: "sh".to_string(),
                    args: Vec::new(),
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
                    network: NetworkSpec {
                        enabled: false,
                        hostname: None,
                    },
                    service: ServiceSpec {
                        name: None,
                        discover: Vec::new(),
                    },
                },
                state: TaskState::Running,
                assigned_node: Some(NodeId::generate()),
                exit_code: None,
                network: None,
            },
        );
        (inner, task_id)
    }

    fn shadow_db_path(tag: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("bigtop-shadow-api-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn snapshot_req() -> RequestSnapshotRequest {
        RequestSnapshotRequest {
            snapshot_type: SnapshotType::Full,
            mem_file_path: None,
            snapshot_path: None,
        }
    }

    /// A failed journal append must fail the request *and* leave the
    /// shadow mirror untouched: the handler mirrors only after
    /// `state::request_snapshot` returns `Ok`.
    #[tokio::test]
    async fn failed_journal_append_never_touches_shadow_mirror() {
        let db_path = shadow_db_path("ordering-fail");
        let handle = crate::harvest_shadow::spawn_shadow(&db_path, Vec::new())
            .expect("test shadow must spawn");
        assert_eq!(handle.mirror_len(), 0);

        // Every journal append fails (/dev/full): the authoritative write
        // cannot be journaled.
        let (mut inner, task_id) = running_task_inner();
        inner.journal = Some(JournalWriter::failing().expect("failing journal"));
        let state = AppState {
            inner: Arc::new(RwLock::new(inner)),
            shadow: Some(handle),
        };

        let result = request_snapshot(
            Extension(state.clone()),
            Path(task_id),
            Json(snapshot_req()),
        )
        .await;
        assert!(
            result.is_err(),
            "a journal failure must fail the snapshot request"
        );

        let shadow = state.shadow.as_ref().expect("shadow is enabled");
        assert_eq!(
            shadow.mirror_len(),
            0,
            "a failed journal append must not touch the shadow mirror"
        );
        let _ = std::fs::remove_file(&db_path);
    }

    /// Happy path: journal append succeeds, so the record lands in the
    /// shadow mirror exactly once.
    #[tokio::test]
    async fn successful_request_mirrors_record_once() {
        let db_path = shadow_db_path("ordering-ok");
        let handle = crate::harvest_shadow::spawn_shadow(&db_path, Vec::new())
            .expect("test shadow must spawn");
        let (inner, task_id) = running_task_inner();
        // No journal (in-memory mode): the request succeeds.
        let state = AppState {
            inner: Arc::new(RwLock::new(inner)),
            shadow: Some(handle),
        };

        let result = request_snapshot(
            Extension(state.clone()),
            Path(task_id),
            Json(snapshot_req()),
        )
        .await;
        assert!(result.is_ok(), "request must succeed: {result:?}");

        let shadow = state.shadow.as_ref().expect("shadow is enabled");
        assert_eq!(shadow.mirror_len(), 1);
        let _ = std::fs::remove_file(&db_path);
    }
}
