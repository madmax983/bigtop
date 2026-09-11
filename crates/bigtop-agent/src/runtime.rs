//! Task runtimes: how an agent turns an assigned task into something running.

use bigtop_core::{Task, TaskId};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::{Child, Command};

/// Turns an assigned [`Task`] into a running thing.
///
/// v0.1 ships two runtimes: [`ProcessRuntime`] (local child processes, the
/// dev/CI stand-in) and `FirecrackerRuntime` (one microVM per task, the
/// real deal).
pub trait Runtime: Send + Sync {
    /// Spawn the task.
    ///
    /// # Errors
    ///
    /// Returns an [`AgentError`](crate::AgentError) when the task cannot start.
    fn spawn(
        &self,
        task: &Task,
    ) -> impl std::future::Future<Output = Result<RunningTask, crate::AgentError>> + Send;
}

/// A spawned task. The agent streams the child's stdout/stderr as task logs
/// and maps its exit status to the task's terminal state.
pub struct RunningTask {
    /// The task that was spawned.
    pub task_id: TaskId,
    /// The child process: the task binary, or the `firecracker` VMM process
    /// (whose stdout carries the guest serial console).
    pub child: Child,
    /// VM working dir, for runtimes that need one (`None` for processes).
    pub vm_dir: Option<PathBuf>,
    /// Stop signal for the task's vsock log listener (`None` when the
    /// runtime does not serve one). The executor sends this when the task
    /// finishes so the per-task listener does not linger.
    pub vsock_stop: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Spawns tasks as plain OS child processes via `tokio::process`.
///
/// This is the local-dev and CI stand-in: no `/dev/kvm`, no microVM, just
/// processes. Scheduling, state machine, and log plumbing are identical.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessRuntime;

impl Runtime for ProcessRuntime {
    fn spawn(
        &self,
        task: &Task,
    ) -> impl std::future::Future<Output = Result<RunningTask, crate::AgentError>> + Send {
        let result = Command::new(&task.spec.command)
            .args(&task.spec.args)
            .envs(&task.spec.env)
            .env("BIGTOP_TASK", task.id.to_string())
            .env("BIGTOP_JOB", task.job_id.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map(|child| RunningTask {
                task_id: task.id.clone(),
                child,
                vm_dir: None,
                vsock_stop: None,
            })
            .map_err(crate::AgentError::Spawn);
        std::future::ready(result)
    }
}
