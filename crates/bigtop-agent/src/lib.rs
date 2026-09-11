//! `BigTop` agent: registers with the server, heartbeats, and runs tasks.
//!
//! Two runtimes: [`ProcessRuntime`] (local processes; dev/CI) and
//! [`FirecrackerRuntime`] (one microVM per task; the real deal). The agent
//! picks with `--runtime firecracker|process|auto` (`auto` = Firecracker
//! when `/dev/kvm` exists, else processes).

mod firecracker;
mod runtime;

pub use firecracker::{FirecrackerConfig, FirecrackerRuntime};
pub use runtime::{ProcessRuntime, RunningTask, Runtime};

use bigtop_core::{
    api::{PushLogsRequest, RegisterNodeRequest, RegisterNodeResponse, SetTaskStateRequest},
    NodeId, Resources, Task, TaskId, TaskState,
};
use std::collections::HashMap;
use std::path::Path;
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
}

/// Run the agent forever: register, heartbeat, and execute assigned tasks.
///
/// # Errors
///
/// Returns [`AgentError`] if registration fails.
pub async fn run_agent(config: AgentConfig, runtime: RuntimeKind) -> Result<(), AgentError> {
    let client = reqwest::Client::new();
    let node_id = register(&client, &config).await?;
    tokio::join!(
        heartbeat_loop(&client, &config, &node_id),
        poll_loop(&client, &config, &node_id, runtime),
    );
    Ok(())
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
async fn poll_loop(
    client: &reqwest::Client,
    config: &AgentConfig,
    node_id: &NodeId,
    runtime: RuntimeKind,
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
                        runtime.clone(),
                        task,
                    ));
                    running.insert(task_id, handle);
                }
            }
            Err(e) => eprintln!("bigtop agent: assignment poll failed: {e}"),
        }
    }
}

/// Spawn one task, report `Running`, stream logs, report the terminal state.
async fn execute_task(
    client: reqwest::Client,
    config: AgentConfig,
    runtime: RuntimeKind,
    task: Task,
) {
    match runtime.spawn(&task).await {
        Ok(running) => {
            let task_id = running.task_id.clone();
            report_state(&client, &config, &task_id, TaskState::Running, None).await;
            let (state, code) = match supervise(&client, &config, &task_id, running.child).await {
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
}

/// Stream the child's stdout/stderr to the server, then wait for exit.
async fn supervise(
    client: &reqwest::Client,
    config: &AgentConfig,
    task_id: &TaskId,
    mut child: tokio::process::Child,
) -> std::io::Result<std::process::ExitStatus> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(512);
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
