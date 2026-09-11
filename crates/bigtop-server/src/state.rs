//! In-memory store and the mutations the API performs on it.

use bigtop_core::{Error, JobId, JobSpec, NodeId, NodeInfo, Resources, Task, TaskId, TaskState};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Lines of log retained per task: a bounded in-memory tail.
const MAX_LOG_LINES: usize = 200;

/// Metadata kept per submitted job.
#[derive(Debug, Clone)]
pub struct JobRecord {
    /// Job name.
    pub name: String,
    /// Submission time.
    pub created_at: DateTime<Utc>,
}

/// All mutable server state. One lock: v0.1 simplicity.
///
/// The fields are public so the scheduler benchmark can build synthetic
/// clusters directly; outside benches, prefer the API.
#[derive(Debug, Default)]
pub struct StateInner {
    /// Jobs by id.
    pub jobs: HashMap<JobId, JobRecord>,
    /// Tasks by id.
    pub tasks: HashMap<TaskId, Task>,
    /// Nodes by id.
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// Bounded log tail per task.
    pub logs: HashMap<TaskId, VecDeque<String>>,
}

/// Shared handle to the server state.
#[derive(Debug, Clone, Default)]
pub struct AppState {
    pub(crate) inner: Arc<RwLock<StateInner>>,
}

impl AppState {
    /// Create empty shared state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Validate a job spec, returning the first problem found.
///
/// # Errors
///
/// Returns [`Error::InvalidJobSpec`] describing the first validation failure.
pub fn validate(spec: &JobSpec) -> Result<(), Error> {
    if spec.name.trim().is_empty() {
        return Err(Error::InvalidJobSpec("job name is empty".to_string()));
    }
    if spec.tasks.is_empty() {
        return Err(Error::InvalidJobSpec("job has no task specs".to_string()));
    }
    for task in &spec.tasks {
        if task.name.trim().is_empty() {
            return Err(Error::InvalidJobSpec(
                "a task spec has an empty name".to_string(),
            ));
        }
        if task.command.trim().is_empty() {
            return Err(Error::InvalidJobSpec(format!(
                "task spec '{}' has an empty command",
                task.name
            )));
        }
        if task.count == 0 {
            return Err(Error::InvalidJobSpec(format!(
                "task spec '{}' has count 0",
                task.name
            )));
        }
    }
    Ok(())
}

/// Expand a job spec into pending tasks. Returns the new job id.
///
/// # Errors
///
/// Returns [`Error::InvalidJobSpec`] when the spec fails validation.
pub fn create_job(
    inner: &mut StateInner,
    spec: &JobSpec,
    now: DateTime<Utc>,
) -> Result<JobId, Error> {
    validate(spec)?;
    let job_id = JobId::generate();
    for task_spec in &spec.tasks {
        for _ in 0..task_spec.count {
            let mut one = task_spec.clone();
            one.count = 1;
            let task = Task {
                id: TaskId::generate(),
                job_id: job_id.clone(),
                name: one.name.clone(),
                spec: one,
                state: TaskState::Pending,
                assigned_node: None,
                exit_code: None,
            };
            inner.logs.insert(task.id.clone(), VecDeque::new());
            inner.tasks.insert(task.id.clone(), task);
        }
    }
    inner.jobs.insert(
        job_id.clone(),
        JobRecord {
            name: spec.name.clone(),
            created_at: now,
        },
    );
    Ok(job_id)
}

/// Register a node. Returns the assigned node id.
pub fn register_node(
    inner: &mut StateInner,
    name: String,
    addr: String,
    total: Resources,
    now: DateTime<Utc>,
) -> NodeId {
    let id = NodeId::generate();
    inner.nodes.insert(
        id.clone(),
        NodeInfo {
            id: id.clone(),
            name,
            addr,
            total,
            used: Resources::default(),
            last_heartbeat: now,
        },
    );
    id
}

/// Record a heartbeat.
///
/// # Errors
///
/// Returns [`Error::NotFound`] for an unknown node id.
pub fn heartbeat(inner: &mut StateInner, id: &NodeId, now: DateTime<Utc>) -> Result<(), Error> {
    let node = inner
        .nodes
        .get_mut(id)
        .ok_or_else(|| Error::NotFound(format!("node {id}")))?;
    node.last_heartbeat = now;
    Ok(())
}

/// Move a task to a new state.
///
/// # Errors
///
/// Returns [`Error::NotFound`] for an unknown task, or [`Error::Conflict`]
/// when the task is already terminal.
pub fn set_task_state(
    inner: &mut StateInner,
    id: &TaskId,
    state: TaskState,
    exit_code: Option<i32>,
) -> Result<(), Error> {
    let task = inner
        .tasks
        .get_mut(id)
        .ok_or_else(|| Error::NotFound(format!("task {id}")))?;
    if task.state.is_terminal() {
        return Err(Error::Conflict(format!(
            "task {id} is already {}",
            task.state
        )));
    }
    task.state = state;
    if state.is_terminal() {
        task.exit_code = exit_code;
    }
    Ok(())
}

/// Append log lines, keeping only the bounded tail.
///
/// # Errors
///
/// Returns [`Error::NotFound`] for an unknown task.
pub fn push_logs(inner: &mut StateInner, id: &TaskId, lines: &[String]) -> Result<(), Error> {
    let buf = inner
        .logs
        .get_mut(id)
        .ok_or_else(|| Error::NotFound(format!("task {id}")))?;
    buf.extend(lines.iter().cloned());
    while buf.len() > MAX_LOG_LINES {
        buf.pop_front();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigtop_core::{TaskSpec, VmSpec};
    use std::collections::HashMap;

    fn job_spec(count: u32) -> JobSpec {
        JobSpec {
            name: "j".to_string(),
            tasks: vec![TaskSpec {
                name: "t".to_string(),
                command: "sh".to_string(),
                args: vec![],
                env: HashMap::new(),
                resources: Resources::default(),
                count,
                vm: VmSpec {
                    kernel_image: String::new(),
                    rootfs: String::new(),
                    vcpu_count: 1,
                    mem_mb: 128,
                    boot_args: None,
                },
            }],
        }
    }

    #[test]
    fn create_job_expands_count_into_tasks() {
        let mut inner = StateInner::default();
        let job_id = create_job(&mut inner, &job_spec(3), Utc::now()).expect("create");
        assert_eq!(inner.jobs.len(), 1);
        assert_eq!(inner.tasks.len(), 3);
        assert_eq!(inner.logs.len(), 3);
        for task in inner.tasks.values() {
            assert_eq!(task.job_id, job_id);
            assert_eq!(task.state, TaskState::Pending);
            assert_eq!(task.spec.count, 1);
        }
    }

    #[test]
    fn create_job_rejects_bad_specs() {
        let mut inner = StateInner::default();
        let empty_name = JobSpec {
            name: String::new(),
            tasks: job_spec(1).tasks,
        };
        assert!(create_job(&mut inner, &empty_name, Utc::now()).is_err());
        let no_tasks = JobSpec {
            name: "j".to_string(),
            tasks: vec![],
        };
        assert!(create_job(&mut inner, &no_tasks, Utc::now()).is_err());
        let zero_count = job_spec(0);
        assert!(create_job(&mut inner, &zero_count, Utc::now()).is_err());
        assert!(inner.jobs.is_empty(), "no partial job must remain");
    }

    #[test]
    fn terminal_transition_is_rejected() {
        let mut inner = StateInner::default();
        create_job(&mut inner, &job_spec(1), Utc::now()).expect("create");
        let id = inner.tasks.keys().next().expect("task").clone();
        set_task_state(&mut inner, &id, TaskState::Running, None).expect("running");
        set_task_state(&mut inner, &id, TaskState::Succeeded, Some(0)).expect("succeeded");
        let err =
            set_task_state(&mut inner, &id, TaskState::Failed, Some(1)).expect_err("conflict");
        assert!(matches!(err, Error::Conflict(_)));
        let task = inner.tasks.get(&id).expect("task");
        assert_eq!(task.state, TaskState::Succeeded);
        assert_eq!(task.exit_code, Some(0));
    }

    #[test]
    fn log_tail_is_bounded() {
        let mut inner = StateInner::default();
        create_job(&mut inner, &job_spec(1), Utc::now()).expect("create");
        let id = inner.tasks.keys().next().expect("task").clone();
        let lines: Vec<String> = (0..250).map(|i| format!("line {i}")).collect();
        push_logs(&mut inner, &id, &lines).expect("push");
        let buf = inner.logs.get(&id).expect("logs");
        assert_eq!(buf.len(), MAX_LOG_LINES);
        assert_eq!(buf.front().expect("front"), "line 50");
        assert_eq!(buf.back().expect("back"), "line 249");
    }

    #[test]
    fn heartbeat_rejects_unknown_node() {
        let mut inner = StateInner::default();
        let err = heartbeat(&mut inner, &NodeId::generate(), Utc::now()).expect_err("not found");
        assert!(matches!(err, Error::NotFound(_)));
    }
}
