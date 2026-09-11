//! `BigTop` agent: registers with the server, heartbeats, and runs tasks.
//!
//! Two runtimes: [`ProcessRuntime`] (local processes; dev/CI) and
//! [`FirecrackerRuntime`] (one microVM per task; the real deal). The agent
//! picks with `--runtime firecracker|process|auto` (`auto` = Firecracker
//! when `/dev/kvm` exists, else processes).

mod firecracker;
mod jailer;
mod runtime;
mod snapshot;
mod tap;
mod vsock;

pub use firecracker::{FirecrackerConfig, FirecrackerRuntime};
pub use jailer::{JailerConfig, JailerOptions, JAILED_API_SOCK, JAILED_LOG_PATH};
pub use runtime::{ProcessRuntime, RunningTask, Runtime};
pub use snapshot::{
    resolve_snapshot_paths, snapshot_create_body, snapshot_load_body, SnapshotManager,
};
pub use tap::TapDevice;
pub use vsock::{LogFrame, LogStream, VsockLogHub, VSOCK_HOST_CID, VSOCK_LOG_PORT};

use bigtop_core::{
    api::{
        PendingSnapshot, PushLogsRequest, RegisterNodeRequest, RegisterNodeResponse,
        ReportSnapshotResult, RequestSnapshotRequest, RequestSnapshotResponse, SetTaskStateRequest,
    },
    NodeId, Resources, SnapshotId, SnapshotPolicy, SnapshotSpec, SnapshotState, Task, TaskId,
    TaskState,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;

/// Which runtime the agent uses for tasks.
#[derive(Debug, Clone)]
pub enum RuntimeKind {
    /// Plain child processes (no KVM needed).
    Process(ProcessRuntime),
    /// One Firecracker microVM per task.
    Firecracker(FirecrackerRuntime),
}

impl Runtime for RuntimeKind {
    async fn spawn(&self, task: &Task) -> Result<RunningTask, AgentError> {
        match self {
            Self::Process(inner) => inner.spawn(task).await,
            Self::Firecracker(inner) => inner.spawn(task).await,
        }
    }
}

/// Pick a runtime automatically: Firecracker when `kvm` exists, else the
/// process stand-in. (`kvm` is `/dev/kvm` in production; a parameter so
/// tests can fake it.)
#[must_use]
pub fn auto_runtime(kvm: &Path, fc_config: FirecrackerConfig) -> RuntimeKind {
    if kvm.exists() {
        RuntimeKind::Firecracker(FirecrackerRuntime::new(fc_config))
    } else {
        RuntimeKind::Process(ProcessRuntime)
    }
}

/// Agent configuration.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Server base URL, e.g. `http://127.0.0.1:4667`.
    pub server_url: String,
    /// Human name for this node.
    pub name: String,
    /// How often to heartbeat.
    pub heartbeat_interval: Duration,
    /// How often to poll for assignments.
    pub poll_interval: Duration,
}

impl AgentConfig {
    /// Build a config with the v0.1 defaults (2 s heartbeat, 1 s poll).
    #[must_use]
    pub const fn new(server_url: String, name: String) -> Self {
        Self {
            server_url,
            name,
            heartbeat_interval: Duration::from_secs(2),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Errors from the agent.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// HTTP request to the server failed.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// A local I/O operation failed (VM dir, socket, ...).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// Spawning the task process / VMM failed.
    #[error("spawn failed: {0}")]
    Spawn(std::io::Error),
    /// JSON encoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// The Firecracker API answered with an error.
    #[error("firecracker API error {status}: {message}")]
    FirecrackerApi { status: u16, message: String },
    /// A VM image path is missing or empty.
    #[error("VM image problem: {0}")]
    ImageMissing(String),
    /// Timed out waiting for the Firecracker API socket.
    #[error("timed out: {0}")]
    Timeout(String),
    /// The server answered with an error status.
    #[error("server error: {0}")]
    Server(String),
    /// Guest network setup failed (TAP device, netns, interface wiring).
    /// Needs `CAP_NET_ADMIN` (or root) and iproute2 on the host.
    #[error("network setup failed: {0}")]
    Network(String),
}

/// Snapshot ids this agent already handles: the poll loop skips them so one
/// snapshot request is never taken twice (e.g. an `OnSuccess` watcher and
/// the poll loop racing on the same record).
type SnapshotClaims = Arc<std::sync::Mutex<HashSet<SnapshotId>>>;

/// Run the agent forever: register, heartbeat, and execute assigned tasks.
///
/// With the Firecracker runtime, fresh boots get a virtio-vsock device and
/// the agent serves each task's guest log connections into a shared hub
/// (see `vsock`); otherwise guests log over the serial console. Snapshot
/// requests are picked up alongside task assignments.
///
/// # Errors
///
/// Returns [`AgentError`] if registration keeps failing.
pub async fn run_agent(config: AgentConfig, runtime: RuntimeKind) -> Result<(), AgentError> {
    let client = reqwest::Client::new();
    let node_id = register_with_retry(&client, &config).await?;
    // Snapshot ids this agent already handles (dispatched from the poll
    // loop or created by an OnSuccess watcher): the poll loop skips them
    // so one request is never snapshotted twice.
    let snapshots_seen: SnapshotClaims = Arc::new(std::sync::Mutex::new(HashSet::new()));
    let hub = if matches!(runtime, RuntimeKind::Firecracker(_)) {
        Some(VsockLogHub::new())
    } else {
        None
    };
    let runtime = match runtime {
        RuntimeKind::Firecracker(fc) => {
            let fc = match &hub {
                Some(hub) => fc.with_vsock_hub(hub.clone()),
                None => fc,
            };
            RuntimeKind::Firecracker(fc)
        }
        RuntimeKind::Process(p) => RuntimeKind::Process(p),
    };
    tokio::join!(
        heartbeat_loop(&client, &config, &node_id),
        poll_loop(
            &client,
            &config,
            &node_id,
            runtime,
            hub,
            snapshots_seen.clone()
        ),
    );
    Ok(())
}

/// Register, retrying while the server is still coming up (or restarting).
/// Tries for ~30 s with a 500 ms backoff before giving up and returning the
/// last error.
async fn register_with_retry(
    client: &reqwest::Client,
    config: &AgentConfig,
) -> Result<NodeId, AgentError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match register(client, config).await {
            Ok(id) => return Ok(id),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                eprintln!("bigtop agent: registration failed ({e}); retrying...");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Register this agent as a node. Returns the assigned node id.
async fn register(client: &reqwest::Client, config: &AgentConfig) -> Result<NodeId, AgentError> {
    let request = RegisterNodeRequest {
        name: config.name.clone(),
        addr: local_label(),
        total: node_resources(),
    };
    let url = format!("{}/v1/nodes/register", config.server_url);
    let response = post_json(client, &url, &request).await?;
    let body: RegisterNodeResponse = response.json().await?;
    Ok(body.id)
}

/// Heartbeat until the process dies.
async fn heartbeat_loop(client: &reqwest::Client, config: &AgentConfig, node_id: &NodeId) {
    let mut ticker = tokio::time::interval(config.heartbeat_interval);
    loop {
        ticker.tick().await;
        let url = format!("{}/v1/nodes/{node_id}/heartbeat", config.server_url);
        match client.post(&url).send().await {
            Ok(response) => {
                if let Err(e) = check_ok(response).await {
                    eprintln!("bigtop agent: heartbeat rejected: {e}");
                }
            }
            Err(e) => eprintln!("bigtop agent: heartbeat failed: {e}"),
        }
    }
}

/// Poll for assignments; spawn a supervisor per new task.
/// Also polls for snapshot requests and dispatches them.
async fn poll_loop(
    client: &reqwest::Client,
    config: &AgentConfig,
    node_id: &NodeId,
    runtime: RuntimeKind,
    hub: Option<VsockLogHub>,
    snapshots_seen: SnapshotClaims,
) {
    let mut running: HashMap<TaskId, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut ticker = tokio::time::interval(config.poll_interval);
    loop {
        ticker.tick().await;
        running.retain(|_, handle| !handle.is_finished());
        match fetch_assignments(client, config, node_id).await {
            Ok(tasks) => {
                for task in tasks {
                    if running.contains_key(&task.id) {
                        continue;
                    }
                    let task_id = task.id.clone();
                    let handle = tokio::spawn(execute_task(
                        client.clone(),
                        config.clone(),
                        node_id.clone(),
                        runtime.clone(),
                        task,
                        hub.clone(),
                        snapshots_seen.clone(),
                    ));
                    running.insert(task_id, handle);
                }
            }
            Err(e) => eprintln!("bigtop agent: assignment poll failed: {e}"),
        }
        match fetch_snapshot_requests(client, config, node_id).await {
            Ok(requests) => {
                for request in requests {
                    // The set doubles as the dedup guard: `insert` is false
                    // when this agent already handles the request.
                    let fresh = snapshots_seen
                        .lock()
                        .is_ok_and(|mut seen| seen.insert(request.snapshot_id.clone()));
                    if !fresh {
                        continue;
                    }
                    tokio::spawn(handle_snapshot_request(
                        client.clone(),
                        config.clone(),
                        node_id.clone(),
                        runtime.clone(),
                        request,
                    ));
                }
            }
            Err(e) => eprintln!("bigtop agent: snapshot poll failed: {e}"),
        }
    }
}

/// Spawn one task, report `Running`, stream logs, report the terminal state.
/// When the task carries an `OnSuccess` snapshot policy and the vsock hub
/// is active, a watcher snapshots the microVM as soon as the guest signals
/// completion (best-effort: the guest may power off first).
async fn execute_task(
    client: reqwest::Client,
    config: AgentConfig,
    node_id: NodeId,
    runtime: RuntimeKind,
    task: Task,
    hub: Option<VsockLogHub>,
    snapshots_seen: SnapshotClaims,
) {
    // Register the vsock log channel before the VMM boots, so a fast guest
    // cannot dial in before its channel exists.
    let vsock_rx = match &hub {
        Some(hub) => {
            let (vtx, vrx) = tokio::sync::mpsc::channel::<String>(512);
            hub.register(task.id.clone(), vtx).await;
            Some(vrx)
        }
        None => None,
    };
    let snapshot_watch = match (&runtime, &task.spec.snapshot_policy, &hub) {
        (RuntimeKind::Firecracker(fc), SnapshotPolicy::OnSuccess(spec), Some(hub)) => {
            Some(tokio::spawn(snapshot_on_success(OnSuccessWatch {
                client: client.clone(),
                config: config.clone(),
                node_id,
                fc: fc.clone(),
                task_id: task.id.clone(),
                spec: spec.clone(),
                hub: hub.clone(),
                snapshots_seen,
            })))
        }
        _ => None,
    };
    match runtime.spawn(&task).await {
        Ok(running) => {
            let task_id = running.task_id.clone();
            // Stops the task's vsock listener once the task is done; the
            // listener task removes its own socket file on exit.
            let vsock_stop = running.vsock_stop;
            // The host TAP device outlives the guest: destroy it after the
            // terminal state is reported (best-effort, like `vsock_stop`).
            let tap = running.tap;
            report_state(&client, &config, &task_id, TaskState::Running, None).await;
            let (state, code) =
                match supervise(&client, &config, &task_id, running.child, vsock_rx).await {
                    Ok(status) => (
                        if status.success() {
                            TaskState::Succeeded
                        } else {
                            TaskState::Failed
                        },
                        status.code(),
                    ),
                    Err(e) => {
                        eprintln!("bigtop agent: task {task_id} wait failed: {e}");
                        (TaskState::Failed, None)
                    }
                };
            report_state(&client, &config, &task_id, state, code).await;
            if let Some(stop) = vsock_stop {
                let _ = stop.send(());
            }
            if let Some(tap) = tap {
                if let Err(e) = tap.destroy().await {
                    eprintln!("bigtop agent: tap destroy failed: {e}");
                }
            }
        }
        Err(e) => {
            let task_id = task.id.clone();
            eprintln!("bigtop agent: spawn failed for {task_id}: {e}");
            push_logs(
                &client,
                &config,
                &task_id,
                &["bigtop: spawn failed".to_string()],
            )
            .await;
            report_state(&client, &config, &task_id, TaskState::Failed, None).await;
        }
    }
    if let Some(handle) = snapshot_watch {
        handle.abort();
    }
    if let Some(hub) = &hub {
        hub.unregister(&task.id).await;
    }
}

/// Inputs for the on-success snapshot watcher (bundled: clippy caps
/// function arity at seven).
struct OnSuccessWatch {
    client: reqwest::Client,
    config: AgentConfig,
    node_id: NodeId,
    fc: FirecrackerRuntime,
    task_id: TaskId,
    spec: SnapshotSpec,
    hub: VsockLogHub,
    snapshots_seen: SnapshotClaims,
}

/// Wait for the guest's completion signal, then snapshot the microVM.
///
/// Creates the server snapshot record first (via the same endpoint the
/// CLI uses), reports `InProgress`, snapshots, and reports the outcome.
/// The completion frame arrives while the VMM is still alive, so the
/// snapshot races the guest's power-off. If the VM is already gone the
/// snapshot fails and is reported as `Failed`; the task's own terminal
/// state is unaffected.
async fn snapshot_on_success(watch: OnSuccessWatch) {
    let OnSuccessWatch {
        client,
        config,
        node_id,
        fc,
        task_id,
        spec,
        hub,
        snapshots_seen,
    } = watch;
    let done = hub.subscribe_completion(&task_id).await;
    if done.await.is_err() {
        return;
    }
    let snapshot_id = match request_snapshot_record(&client, &config, &task_id, &spec).await {
        Ok(id) => id,
        Err(e) => {
            eprintln!("bigtop agent: on-success snapshot request failed for {task_id}: {e}");
            return;
        }
    };
    // Claim the id before the next poll tick sees the new record: no await
    // between here and the insert, so the poll loop cannot interleave.
    if let Ok(mut seen) = snapshots_seen.lock() {
        seen.insert(snapshot_id.clone());
    }
    eprintln!("bigtop agent: guest {task_id} completed; taking on-success snapshot");
    report_snapshot_result(
        &client,
        &config,
        &task_id,
        &snapshot_id,
        &ReportSnapshotResult {
            state: SnapshotState::InProgress,
            node_id: node_id.clone(),
            mem_file_path: None,
            snapshot_path: None,
            error: None,
        },
    )
    .await;
    let result = fc.take_snapshot(&task_id, &snapshot_id, &spec).await;
    let report = match result {
        Ok((mem, snap)) => ReportSnapshotResult {
            state: SnapshotState::Done,
            node_id,
            mem_file_path: Some(mem.to_string_lossy().into_owned()),
            snapshot_path: Some(snap.to_string_lossy().into_owned()),
            error: None,
        },
        Err(e) => {
            eprintln!("bigtop agent: on-success snapshot failed for {task_id}: {e}");
            ReportSnapshotResult {
                state: SnapshotState::Failed,
                node_id,
                mem_file_path: None,
                snapshot_path: None,
                error: Some(e.to_string()),
            }
        }
    };
    report_snapshot_result(&client, &config, &task_id, &snapshot_id, &report).await;
}

/// Handle one server snapshot request: report `InProgress`, snapshot the
/// running microVM, report the outcome. Reports `Failed` immediately when
/// the agent is not running the Firecracker runtime.
async fn handle_snapshot_request(
    client: reqwest::Client,
    config: AgentConfig,
    node_id: NodeId,
    runtime: RuntimeKind,
    request: PendingSnapshot,
) {
    let task_id = request.task_id.clone();
    let snapshot_id = request.snapshot_id.clone();
    let RuntimeKind::Firecracker(fc) = runtime else {
        report_snapshot_result(
            &client,
            &config,
            &task_id,
            &snapshot_id,
            &ReportSnapshotResult {
                state: SnapshotState::Failed,
                node_id,
                mem_file_path: None,
                snapshot_path: None,
                error: Some("snapshot needs the firecracker runtime".to_string()),
            },
        )
        .await;
        return;
    };
    report_snapshot_result(
        &client,
        &config,
        &task_id,
        &snapshot_id,
        &ReportSnapshotResult {
            state: SnapshotState::InProgress,
            node_id: node_id.clone(),
            mem_file_path: None,
            snapshot_path: None,
            error: None,
        },
    )
    .await;
    let result = fc
        .take_snapshot(&task_id, &snapshot_id, &request.spec)
        .await;
    let final_report = match result {
        Ok((mem, snap)) => ReportSnapshotResult {
            state: SnapshotState::Done,
            node_id,
            mem_file_path: Some(mem.to_string_lossy().into_owned()),
            snapshot_path: Some(snap.to_string_lossy().into_owned()),
            error: None,
        },
        Err(e) => {
            eprintln!("bigtop agent: snapshot {snapshot_id} failed for {task_id}: {e}");
            ReportSnapshotResult {
                state: SnapshotState::Failed,
                node_id,
                mem_file_path: None,
                snapshot_path: None,
                error: Some(e.to_string()),
            }
        }
    };
    report_snapshot_result(&client, &config, &task_id, &snapshot_id, &final_report).await;
}

/// Stream the child's stdout/stderr (plus vsock lines, when `vsock_rx` is
/// `Some`) to the server, then wait for exit.
async fn supervise(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    mut child: tokio::process::Child,
    vsock_rx: Option<tokio::sync::mpsc::Receiver<String>>,
) -> std::io::Result<std::process::ExitStatus> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(512);
    // Vsock lines ride a forwarder into the same channel, so ordering with
    // serial-console lines is by arrival. The forwarder exits on its own
    // when `tx` drops.
    if let Some(mut vrx) = vsock_rx {
        let tx = tx.clone();
        tokio::spawn(async move {
            while let Some(line) = vrx.recv().await {
                if tx.send(line).await.is_err() {
                    break;
                }
            }
        });
    }
    if let Some(pipe) = child.stdout.take() {
        let tx = tx.clone();
        tokio::spawn(async move {
            pump_stream(pipe, "stdout", tx).await;
        });
    }
    if let Some(pipe) = child.stderr.take() {
        let tx = tx.clone();
        tokio::spawn(async move {
            pump_stream(pipe, "stderr", tx).await;
        });
    }
    drop(tx);
    let mut pending: Vec<String> = Vec::new();
    let mut flush_tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tokio::select! {
            line = rx.recv() => {
                let Some(line) = line else { break };
                pending.push(line);
                if pending.len() >= 50 {
                    flush_logs(client, config, task_id, &mut pending).await;
                }
            }
            _ = flush_tick.tick() => {
                flush_logs(client, config, task_id, &mut pending).await;
            }
        }
    }
    flush_logs(client, config, task_id, &mut pending).await;
    child.wait().await
}

/// Pump one output stream into the log channel, tagged by stream.
async fn pump_stream<R>(pipe: R, stream: &'static str, tx: tokio::sync::mpsc::Sender<String>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = tokio::io::BufReader::new(pipe).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if tx.send(format!("[{stream}] {line}")).await.is_err() {
            break;
        }
    }
}

/// Push buffered lines if there are any.
async fn flush_logs(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    pending: &mut Vec<String>,
) {
    if pending.is_empty() {
        return;
    }
    let chunk: Vec<String> = std::mem::take(pending);
    push_logs(client, config, task_id, &chunk).await;
}

/// Push log lines to the server; log locally on failure.
async fn push_logs(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    lines: &[String],
) {
    let request = PushLogsRequest {
        lines: lines.to_vec(),
    };
    let url = format!("{}/v1/tasks/{task_id}/logs", config.server_url);
    if let Err(e) = post_json(client, &url, &request).await {
        eprintln!("bigtop agent: log push failed for {task_id}: {e}");
    }
}

/// Report a state transition to the server; log locally on failure.
async fn report_state(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    state: TaskState,
    exit_code: Option<i32>,
) {
    let request = SetTaskStateRequest { state, exit_code };
    let url = format!("{}/v1/tasks/{task_id}/state", config.server_url);
    if let Err(e) = post_json(client, &url, &request).await {
        eprintln!("bigtop agent: state report failed for {task_id}: {e}");
    }
}

/// Fetch tasks assigned to this node and not yet acknowledged.
async fn fetch_assignments(
    client: &reqwest::Client,
    config: &AgentConfig,
    node_id: &NodeId,
) -> Result<Vec<Task>, AgentError> {
    let url = format!(
        "{}/v1/agents/assignments?node_id={node_id}",
        config.server_url
    );
    let response = client.get(&url).send().await?;
    let response = check_ok(response).await?;
    Ok(response.json::<Vec<Task>>().await?)
}

/// Fetch snapshot requests waiting for this node.
async fn fetch_snapshot_requests(
    client: &reqwest::Client,
    config: &AgentConfig,
    node_id: &NodeId,
) -> Result<Vec<PendingSnapshot>, AgentError> {
    let url = format!(
        "{}/v1/agents/snapshot-requests?node_id={node_id}",
        config.server_url
    );
    let response = client.get(&url).send().await?;
    let response = check_ok(response).await?;
    Ok(response.json::<Vec<PendingSnapshot>>().await?)
}

/// Ask the server to record a snapshot request; returns the new id.
/// Used for `OnSuccess` snapshots so they show up in `snapshot list` and
/// can be restored like CLI-requested ones.
async fn request_snapshot_record(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    spec: &SnapshotSpec,
) -> Result<SnapshotId, AgentError> {
    let request = RequestSnapshotRequest {
        snapshot_type: spec.snapshot_type,
        mem_file_path: (!spec.mem_file_path.is_empty()).then(|| spec.mem_file_path.clone()),
        snapshot_path: (!spec.snapshot_path.is_empty()).then(|| spec.snapshot_path.clone()),
    };
    let url = format!("{}/v1/tasks/{task_id}/snapshot", config.server_url);
    let response = post_json(client, &url, &request).await?;
    Ok(response
        .json::<RequestSnapshotResponse>()
        .await?
        .snapshot_id)
}

/// Report a snapshot outcome to the server; log locally on failure.
async fn report_snapshot_result(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    snapshot_id: &SnapshotId,
    result: &ReportSnapshotResult,
) {
    let url = format!(
        "{}/v1/tasks/{task_id}/snapshots/{snapshot_id}/result",
        config.server_url
    );
    if let Err(e) = post_json(client, &url, result).await {
        eprintln!("bigtop agent: snapshot report failed for {snapshot_id}: {e}");
    }
}
/// `POST` JSON, mapping error statuses to [`AgentError::Server`].
async fn post_json(
    client: &reqwest::Client,
    url: &str,
    body: &(impl serde::Serialize + Sync),
) -> Result<reqwest::Response, AgentError> {
    let response = client.post(url).json(body).send().await?;
    check_ok(response).await
}

/// Map non-2xx responses to [`AgentError::Server`].
async fn check_ok(response: reqwest::Response) -> Result<reqwest::Response, AgentError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        Err(AgentError::Server(format!("{status}: {text}")))
    }
}

/// What this machine offers: all CPUs, `/proc/meminfo` RAM.
fn node_resources() -> Resources {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    Resources {
        cpu_millis: u64::try_from(cpus).unwrap_or(1) * 1000,
        mem_mb: mem_total_mb().unwrap_or(1024),
    }
}

/// Parse `MemTotal` out of `/proc/meminfo`.
fn mem_total_mb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    text.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?;
        let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
        Some(kb / 1024)
    })
}

/// Informational label for the node registry.
fn local_label() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn auto_runtime_prefers_firecracker_when_kvm_exists() {
        let dir = std::env::temp_dir();
        let fake_kvm = dir.join(format!("bigtop-fake-kvm-{}", std::process::id()));
        std::fs::File::create(&fake_kvm)
            .expect("touch")
            .write_all(b"")
            .expect("write");
        let kind = auto_runtime(&fake_kvm, FirecrackerConfig::default());
        assert!(matches!(kind, RuntimeKind::Firecracker(_)));
        std::fs::remove_file(&fake_kvm).expect("cleanup");

        let kind = auto_runtime(
            Path::new("/definitely/not/kvm"),
            FirecrackerConfig::default(),
        );
        assert!(matches!(kind, RuntimeKind::Process(_)));
    }
}
