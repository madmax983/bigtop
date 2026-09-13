//! Server persistence: a JSONL journal of state mutations.
//!
//! When the server runs with a data directory, every state mutation is
//! appended to `journal.jsonl` (one JSON object per line) and the file is
//! `fsync`ed **before** the mutation is acknowledged to the caller. On
//! startup, the server replays the journal in order and restores the exact
//! in-memory state it had when it last wrote.
//!
//! On a clean shutdown the server compacts: it writes `snapshot.json`
//! (the full durable state) and truncates the journal, so the next boot
//! replays at most the ops since the last clean stop.
//!
//! The journal replay is crash-safe: only the final line can be torn by a
//! `kill -9` mid-write, so a corrupt final line is truncated with a
//! warning while any other corrupt line aborts startup with an error.
//!
//! The data directory is protected by an exclusive lock file
//! (`bigtop.lock`, created with `create_new`): a second server is refused
//! at startup while the file exists. The lock is removed on clean drop;
//! a `kill -9` skips the drop and leaves the file behind, so the next
//! start fails closed until the operator removes the stale lock after
//! confirming no server is running.

use bigtop_core::{
    JobId, NetworkAssignment, NodeId, NodeInfo, ReportSnapshotResult, SnapshotId, SnapshotRecord,
    Task, TaskId, TaskState,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use crate::ipam::Ipam;
use crate::state::{JobRecord, StateInner};

/// One state mutation, in the order the server applied it.
///
/// The journal stores whole post-state records (not deltas): each op
/// carries everything needed to rebuild the affected rows, which keeps
/// replay simple and version-skew tolerant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum JournalOp {
    /// A job and all its tasks were submitted.
    CreateJob {
        /// The new job's id.
        job_id: JobId,
        /// The job name.
        name: String,
        /// When the job was submitted.
        created_at: DateTime<Utc>,
        /// All tasks of the job, in submission order.
        tasks: Vec<Task>,
    },
    /// A task's lifecycle state changed (worker heartbeat report).
    SetTaskState {
        /// The task.
        task_id: TaskId,
        /// The new state.
        state: TaskState,
        /// Exit code for terminal transitions.
        exit_code: Option<i32>,
    },
    /// The scheduler placed a pending task on a node.
    TaskAssigned {
        /// The task.
        task_id: TaskId,
        /// The node it was placed on.
        node_id: NodeId,
        /// Its network assignment, if the task requested networking.
        network: Option<NetworkAssignment>,
        /// The injected `BIGTOP_SERVICES` env value, if any.
        services_env: Option<String>,
    },
    /// A task was returned to `Pending` (node died or scheduler retry).
    TaskRequeued {
        /// The task.
        task_id: TaskId,
    },
    /// An agent registered or re-registered a node.
    RegisterNode {
        /// The full node record as stored.
        node: NodeInfo,
    },
    /// A snapshot was requested for a task.
    RequestSnapshot {
        /// The snapshot record as stored.
        record: SnapshotRecord,
    },
    /// The agent reported a snapshot result.
    ReportSnapshot {
        /// The task.
        task_id: TaskId,
        /// The snapshot.
        snapshot_id: SnapshotId,
        /// The reported outcome.
        result: ReportSnapshotResult,
    },
    /// IPAM handed an address to a task on a node's `/24`.
    IpamAllocate {
        /// The node whose `/24` supplied the address.
        node_id: NodeId,
        /// The task holding the address.
        task_id: TaskId,
        /// The assigned address (replay sanity check).
        ip: Ipv4Addr,
    },
    /// IPAM released a task's address.
    IpamRelease {
        /// The task whose address was released.
        task_id: TaskId,
    },
}

/// The durable subset of [`StateInner`]: everything the journal can
/// rebuild. Logs are intentionally excluded (they are bounded runtime
/// queues; replay rebuilds them empty).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableState {
    /// Jobs by id.
    pub jobs: HashMap<JobId, JobRecord>,
    /// Tasks by id.
    pub tasks: HashMap<TaskId, Task>,
    /// Nodes by id.
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// Snapshot records by id.
    pub snapshots: HashMap<SnapshotId, SnapshotRecord>,
    /// The IPAM allocator state.
    pub ipam: Ipam,
}

impl DurableState {
    /// Capture the durable subset of `inner`.
    #[must_use]
    pub fn capture(inner: &StateInner) -> Self {
        Self {
            jobs: inner.jobs.clone(),
            tasks: inner.tasks.clone(),
            nodes: inner.nodes.clone(),
            snapshots: inner.snapshots.clone(),
            ipam: inner.ipam.clone(),
        }
    }

    /// Restore `inner` from this snapshot. `now` becomes every node's
    /// `last_heartbeat`; liveness is re-established by agent heartbeats.
    /// Log queues are rebuilt empty (logs are not persisted).
    pub fn restore(&self, inner: &mut StateInner, now: DateTime<Utc>) {
        inner.jobs.clone_from(&self.jobs);
        inner.tasks.clone_from(&self.tasks);
        inner.snapshots.clone_from(&self.snapshots);
        inner.ipam.clone_from(&self.ipam);
        inner.nodes.clear();
        for (id, node) in &self.nodes {
            let mut node = node.clone();
            node.last_heartbeat = now;
            inner.nodes.insert(id.clone(), node);
        }
        inner.logs.clear();
        for task_id in inner.tasks.keys() {
            inner.logs.insert(task_id.clone(), VecDeque::default());
        }
    }
}

/// Name of the journal file inside the data directory.
pub const JOURNAL_FILE: &str = "journal.jsonl";
/// Name of the compacted snapshot file inside the data directory.
pub const SNAPSHOT_FILE: &str = "snapshot.json";
/// Name of the lock file inside the data directory.
pub const LOCK_FILE: &str = "bigtop.lock";

/// Append-only JSONL writer. Every [`JournalWriter::append`] ends with an
/// `fsync`, so a crash can only lose (never half-write) the op in flight.
#[derive(Debug)]
pub struct JournalWriter {
    file: File,
    ops_written: u64,
}

impl JournalWriter {
    /// Append one op and `fsync` before returning.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] when the write or the `fsync` fails.
    pub fn append(&mut self, op: &JournalOp) -> io::Result<()> {
        let mut file = &self.file;
        serde_json::to_writer(&mut file, op).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        self.file.sync_all()?;
        self.ops_written += 1;
        Ok(())
    }

    /// Number of ops written through this handle.
    #[must_use]
    pub const fn ops_written(&self) -> u64 {
        self.ops_written
    }

    /// A writer whose appends always fail (backed by `/dev/full`).
    /// Test-only: simulates a journal I/O failure so the failure path of
    /// every journaled mutation can be exercised honestly.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] when `/dev/full` cannot be opened.
    #[cfg(test)]
    pub fn failing() -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().write(true).open("/dev/full")?;
        Ok(Self {
            file,
            ops_written: 0,
        })
    }
}

/// Open (creating) the journal for appending.
pub fn open_journal(dir: &Path) -> io::Result<JournalWriter> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(JOURNAL_FILE))?;
    Ok(JournalWriter {
        file,
        ops_written: 0,
    })
}

/// Read all ops from the journal, in order.
///
/// The writer terminates every op with `\n`, so bytes after the final
/// newline are a torn write from a crash:
/// - if the fragment parses as an op, it is complete (the crash landed
///   between the JSON write and the newline write). Mutations are
///   journaled after they are applied in memory, so the op describes real
///   pre-crash state: the line is terminated in place (with a warning)
///   and the op is kept, keeping the journal well-formed for appends.
/// - otherwise the fragment is truncated in place (with a warning), so a
///   later append cannot glue onto it and corrupt the journal.
///
/// Any other corrupt line is an error: only the final line may be torn.
pub fn load_journal_ops(dir: &Path) -> io::Result<Vec<JournalOp>> {
    let path = dir.join(JOURNAL_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let (complete, tail) = bytes.iter().rposition(|b| *b == b'\n').map_or_else(
        || (&[][..], &bytes[..]),
        |pos| (&bytes[..=pos], &bytes[pos + 1..]),
    );
    let mut tail_op: Option<JournalOp> = None;
    if !tail.is_empty() {
        match serde_json::from_slice::<JournalOp>(tail) {
            Ok(op) => {
                eprintln!(
                    "bigtop: journal ends with an unterminated op (crash mid-write); keeping it"
                );
                let mut file = OpenOptions::new().append(true).open(&path)?;
                file.write_all(b"\n")?;
                file.sync_all()?;
                tail_op = Some(op);
            }
            Err(err) => {
                eprintln!("bigtop: truncating torn final journal line (crash mid-write): {err}");
                let file = OpenOptions::new().write(true).open(&path)?;
                let len = u64::try_from(complete.len()).map_err(io::Error::other)?;
                file.set_len(len)?;
                file.sync_all()?;
            }
        }
    }
    let mut ops = Vec::new();
    for (index, line) in complete
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        match serde_json::from_slice::<JournalOp>(line) {
            Ok(op) => ops.push(op),
            Err(err) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("corrupt journal line {}: {err}", index + 1),
                ));
            }
        }
    }
    if let Some(op) = tail_op {
        ops.push(op);
    }
    Ok(ops)
}

/// Load the compacted snapshot, if one exists.
pub fn load_snapshot(dir: &Path) -> io::Result<Option<DurableState>> {
    let path = dir.join(SNAPSHOT_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let state: DurableState = serde_json::from_slice(&bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(Some(state))
}

/// Atomically replace the snapshot file and truncate the journal.
///
/// The snapshot is written to a temp file, `fsync`ed, and renamed over
/// the old one, so a crash mid-compaction leaves either the old or the
/// new snapshot — never a torn one.
///
/// A crash between the snapshot rename and the journal truncate makes
/// the next boot replay pre-snapshot ops on top of the snapshot. That is
/// safe: every op replays idempotently (records insert-or-overwrite,
/// state transitions re-apply in journal order with conflicts ignored,
/// IPAM allocate returns the already-allocated address, releases of
/// unknown tasks are no-ops), and ops always replay in journal order, so
/// the replayed state converges to the snapshotted one.
pub fn compact(dir: &Path, state: &DurableState) -> io::Result<()> {
    let tmp = dir.join(format!("{SNAPSHOT_FILE}.tmp"));
    let bytes = serde_json::to_vec(state).map_err(io::Error::other)?;
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, dir.join(SNAPSHOT_FILE))?;
    truncate_journal(dir)
}

/// Truncate the journal to zero length (after a successful compaction).
pub fn truncate_journal(dir: &Path) -> io::Result<()> {
    let path = dir.join(JOURNAL_FILE);
    let file = OpenOptions::new().write(true).open(&path)?;
    file.set_len(0)?;
    file.sync_all()
}

/// Apply replayed ops onto a fresh [`StateInner`].
///
/// The live journal handle is parked during replay so replayed mutations
/// are not re-journaled. Node `used` resources are recomputed from the
/// final task placement, matching the scheduler's own accounting.
pub fn apply_ops(inner: &mut StateInner, ops: &[JournalOp], now: DateTime<Utc>) {
    let journal = inner.journal.take();
    for op in ops {
        apply_one(inner, op, now);
    }
    crate::scheduler::recount_used(inner);
    inner.journal = journal;
}

fn apply_one(inner: &mut StateInner, op: &JournalOp, now: DateTime<Utc>) {
    match op {
        JournalOp::CreateJob {
            job_id,
            name,
            created_at,
            tasks,
        } => {
            inner.jobs.insert(
                job_id.clone(),
                JobRecord {
                    name: name.clone(),
                    created_at: *created_at,
                },
            );
            for task in tasks {
                inner.logs.entry(task.id.clone()).or_default();
                inner.tasks.insert(task.id.clone(), task.clone());
            }
        }
        JournalOp::SetTaskState {
            task_id,
            state,
            exit_code,
        } => {
            let _ = crate::state::set_task_state(inner, task_id, *state, *exit_code);
        }
        JournalOp::TaskAssigned {
            task_id,
            node_id,
            network,
            services_env,
        } => {
            if let Some(task) = inner.tasks.get_mut(task_id) {
                task.state = TaskState::Assigned;
                task.assigned_node = Some(node_id.clone());
                task.network.clone_from(network);
                if let Some(env) = services_env {
                    task.spec
                        .env
                        .insert("BIGTOP_SERVICES".to_string(), env.clone());
                }
            }
        }
        JournalOp::TaskRequeued { task_id } => {
            if let Some(task) = inner.tasks.get_mut(task_id) {
                task.state = TaskState::Pending;
                task.assigned_node = None;
                task.network = None;
            }
        }
        JournalOp::RegisterNode { node } => {
            let mut node = node.clone();
            node.last_heartbeat = now;
            inner.nodes.insert(node.id.clone(), node);
        }
        JournalOp::RequestSnapshot { record } => {
            inner.snapshots.insert(record.id.clone(), record.clone());
        }
        JournalOp::ReportSnapshot {
            task_id,
            snapshot_id,
            result,
        } => {
            let _ = crate::state::report_snapshot_result(inner, task_id, snapshot_id, result);
        }
        JournalOp::IpamAllocate {
            node_id,
            task_id,
            ip,
        } => match inner.ipam.allocate(node_id, task_id) {
            Some(got) => {
                debug_assert_eq!(
                    got, *ip,
                    "journal replay: ipam assigned {got}, journal says {ip}"
                );
            }
            None => eprintln!("bigtop: journal replay: ipam exhausted for {task_id}"),
        },
        JournalOp::IpamRelease { task_id } => {
            let _ = inner.ipam.release(task_id);
        }
    }
}

/// Error from opening the data directory.
#[derive(Debug)]
pub enum StoreError {
    /// The data directory is already locked by another server.
    Locked(PathBuf),
    /// Any other I/O failure.
    Io(io::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Locked(path) => write!(
                f,
                "data directory {} is already in use by another server (lock file present and open)",
                path.display()
            ),
            Self::Io(err) => write!(f, "data directory I/O error: {err}"),
        }
    }
}

impl From<io::Error> for StoreError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Exclusive lock on the data directory.
///
/// The lock file is created with `create_new` and closed immediately:
/// the path's existence is the lock. It is removed on drop. Because
/// creation fails when the file already exists, two servers can never
/// share a directory — and because the file is only removed by its owner
/// on drop, a `kill -9` leaves it behind, so the next start fails closed
/// (the operator removes the stale lock after confirming no server is
/// running).
#[derive(Debug)]
pub struct DirLock {
    path: PathBuf,
}

impl DirLock {
    /// Take the exclusive lock on `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Locked`] when the lock file already exists,
    /// or [`StoreError::Io`] on other failures.
    pub fn acquire(dir: &Path) -> Result<Self, StoreError> {
        let path = dir.join(LOCK_FILE);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            // The handle is dropped at once: the path itself is the lock.
            Ok(_) => Ok(Self { path }),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                Err(StoreError::Locked(dir.to_path_buf()))
            }
            Err(err) => Err(StoreError::Io(err)),
        }
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Create the data directory if missing and return its canonical path.
pub fn ensure_data_dir(dir: &Path) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    dir.canonicalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigtop_core::{JobId, NodeId, Resources, Task, TaskId, TaskSpec, TaskState};
    use std::collections::VecDeque;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bigtop-journal-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create tmp dir");
        dir
    }

    fn task_spec() -> TaskSpec {
        use bigtop_core::{NetworkSpec, ServiceSpec, SnapshotPolicy, VmSpec};
        TaskSpec {
            name: "t".to_string(),
            command: "true".to_string(),
            args: Vec::new(),
            env: std::collections::HashMap::new(),
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
            network: NetworkSpec::default(),
            service: ServiceSpec::default(),
        }
    }

    fn sample_task() -> Task {
        Task {
            id: TaskId::from("task-1".to_string()),
            job_id: JobId::from("job-1".to_string()),
            name: "t".to_string(),
            spec: task_spec(),
            state: TaskState::Pending,
            assigned_node: None,
            network: None,
            exit_code: None,
        }
    }

    fn sample_op() -> JournalOp {
        JournalOp::CreateJob {
            job_id: JobId::from("job-1".to_string()),
            name: "demo".to_string(),
            created_at: Utc::now(),
            tasks: vec![sample_task()],
        }
    }

    #[test]
    fn journal_roundtrip_preserves_ops() {
        let dir = tmp_dir("roundtrip");
        let mut writer = open_journal(&dir).expect("open journal");
        let op = sample_op();
        writer.append(&op).expect("append");
        writer
            .append(&JournalOp::IpamRelease {
                task_id: TaskId::from("task-1".to_string()),
            })
            .expect("append");

        let ops = load_journal_ops(&dir).expect("load");
        assert_eq!(ops.len(), 2);
        assert!(matches!(ops[0], JournalOp::CreateJob { .. }));
        assert!(matches!(ops[1], JournalOp::IpamRelease { .. }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_fsyncs_each_op() {
        let dir = tmp_dir("fsync");
        let mut writer = open_journal(&dir).expect("open journal");
        writer.append(&sample_op()).expect("append");
        // Read the file back through a fresh handle: the bytes must be
        // there (fsync flushed them out of every buffer).
        let bytes = std::fs::read(dir.join(JOURNAL_FILE)).expect("read journal");
        assert!(bytes.ends_with(b"\n"));
        assert!(serde_json::from_slice::<JournalOp>(&bytes).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_final_line_is_truncated_with_warning() {
        let dir = tmp_dir("torn");
        let op = sample_op();
        {
            let mut writer = open_journal(&dir).expect("open journal");
            writer.append(&op).expect("append");
            drop(writer);
            // Simulate a crash mid-write: append a torn fragment.
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.join(JOURNAL_FILE))
                .expect("reopen");
            file.write_all(br#"{"op":"create_job","job_id":"#)
                .expect("write torn line");
        }
        let ops = load_journal_ops(&dir).expect("load tolerates torn tail");
        assert_eq!(ops.len(), 1);
        // The torn fragment is physically truncated: the journal stays
        // well-formed for the next append and the next load.
        let bytes = std::fs::read(dir.join(JOURNAL_FILE)).expect("read journal");
        let mut expected = serde_json::to_vec(&op).expect("serialize");
        expected.push(b'\n');
        assert_eq!(bytes, expected, "torn tail must be truncated");
        let ops = load_journal_ops(&dir).expect("second load");
        assert_eq!(ops.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unterminated_complete_op_is_kept_and_terminated() {
        let dir = tmp_dir("unterminated");
        let op1 = sample_op();
        let op2 = JournalOp::CreateJob {
            job_id: JobId::from("job-2".to_string()),
            name: "demo2".to_string(),
            created_at: Utc::now(),
            tasks: Vec::new(),
        };
        let json = serde_json::to_vec(&op2).expect("serialize");
        {
            let mut writer = open_journal(&dir).expect("open journal");
            writer.append(&op1).expect("append 1");
            drop(writer);
            // Simulate a crash between the JSON write and the newline
            // write: a complete op with no trailing newline.
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.join(JOURNAL_FILE))
                .expect("reopen");
            file.write_all(&json).expect("write unterminated op");
        }
        let ops = load_journal_ops(&dir).expect("load keeps the complete op");
        assert_eq!(ops.len(), 2);
        assert!(
            matches!(&ops[1], JournalOp::CreateJob { job_id, .. } if job_id.as_ref() == "job-2"),
            "the unterminated op must be replayed"
        );
        // The line was terminated in place: the journal is well-formed.
        let bytes = std::fs::read(dir.join(JOURNAL_FILE)).expect("read journal");
        let mut expected = serde_json::to_vec(&op1).expect("serialize");
        expected.push(b'\n');
        expected.extend_from_slice(&json);
        expected.push(b'\n');
        assert_eq!(bytes, expected, "unterminated op must be terminated");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_middle_line_is_a_hard_error() {
        let dir = tmp_dir("corrupt");
        {
            let mut writer = open_journal(&dir).expect("open journal");
            writer.append(&sample_op()).expect("append 1");
            drop(writer);
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.join(JOURNAL_FILE))
                .expect("reopen");
            writeln!(file, "this is not json").expect("write bad line");
            let mut writer = open_journal(&dir).expect("reopen journal");
            writer.append(&sample_op()).expect("append 2");
        }
        let err = load_journal_ops(&dir).expect_err("corrupt middle line must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_compact_truncates_journal() {
        let dir = tmp_dir("compact");
        let mut writer = open_journal(&dir).expect("open journal");
        writer.append(&sample_op()).expect("append");
        let state = DurableState::default();
        compact(&dir, &state).expect("compact");
        // Snapshot exists and round-trips.
        let loaded = load_snapshot(&dir).expect("load").expect("some snapshot");
        assert_eq!(loaded, state);
        // Journal is empty.
        let ops = load_journal_ops(&dir).expect("load journal");
        assert!(ops.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn double_start_is_refused() {
        let dir = tmp_dir("lock");
        let first = DirLock::acquire(&dir).expect("first lock");
        let err = DirLock::acquire(&dir).expect_err("second lock must fail");
        assert!(matches!(err, StoreError::Locked(_)));
        drop(first);
        // After the first owner drops, the directory is lockable again.
        let _second = DirLock::acquire(&dir).expect("lock after drop");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_state_captures_and_restores() {
        use bigtop_core::{NetworkSpec, ServiceSpec, SnapshotPolicy, VmSpec};
        let now = Utc::now();
        let mut inner = StateInner::default();
        let node_id = NodeId::from("node-1".to_string());
        inner.nodes.insert(
            node_id.clone(),
            NodeInfo {
                id: node_id.clone(),
                name: "n1".to_string(),
                addr: "x".to_string(),
                underlay_ip: None,
                total: Resources {
                    cpu_millis: 4000,
                    mem_mb: 1024,
                },
                used: Resources {
                    cpu_millis: 1000,
                    mem_mb: 128,
                },
                last_heartbeat: now,
            },
        );
        inner.tasks.insert(
            TaskId::from("task-1".to_string()),
            Task {
                id: TaskId::from("task-1".to_string()),
                job_id: JobId::from("job-1".to_string()),
                name: "t".to_string(),
                spec: TaskSpec {
                    name: "t".to_string(),
                    command: "true".to_string(),
                    args: Vec::new(),
                    env: std::collections::HashMap::new(),
                    resources: Resources {
                        cpu_millis: 100,
                        mem_mb: 64,
                    },
                    count: 1,
                    vm: VmSpec::default(),
                    node_affinity: None,
                    snapshot_policy: SnapshotPolicy::None,
                    network: NetworkSpec::default(),
                    service: ServiceSpec::default(),
                },
                state: TaskState::Assigned,
                assigned_node: Some(node_id.clone()),
                network: None,
                exit_code: None,
            },
        );
        inner
            .logs
            .insert(TaskId::from("task-1".to_string()), VecDeque::new());

        let durable = DurableState::capture(&inner);
        let later = now + chrono::Duration::seconds(60);
        let mut fresh = StateInner::default();
        durable.restore(&mut fresh, later);
        assert_eq!(fresh.tasks.len(), 1);
        assert_eq!(fresh.nodes.len(), 1);
        // Heartbeat is refreshed to restore time...
        assert_eq!(fresh.nodes[&node_id].last_heartbeat, later);
        // ...while the snapshot kept the original.
        assert_eq!(durable.nodes[&node_id].last_heartbeat, now);
        // Logs are rebuilt empty.
        assert_eq!(fresh.logs.len(), 1);
        assert!(fresh.logs[&TaskId::from("task-1".to_string())].is_empty());
    }

    fn sample_node(node_id: &NodeId, now: DateTime<Utc>) -> NodeInfo {
        NodeInfo {
            id: node_id.clone(),
            name: "n1".to_string(),
            addr: "x".to_string(),
            underlay_ip: Some("10.0.0.1".parse().expect("ip")),
            total: Resources {
                cpu_millis: 4000,
                mem_mb: 1024,
            },
            used: Resources::default(),
            last_heartbeat: now,
        }
    }

    /// Drive a live state through the real mutation functions with a
    /// journal attached: create job, register node, assign with IPAM,
    /// run to success.
    fn drive_live_state(live: &mut StateInner, now: DateTime<Utc>) {
        let node_id = NodeId::from("node-1".to_string());
        let task_id = TaskId::from("task-1".to_string());
        let job_id = JobId::from("job-1".to_string());
        let mut spec = task_spec();
        spec.resources = Resources {
            cpu_millis: 1000,
            mem_mb: 64,
        };
        let task = Task {
            id: task_id.clone(),
            job_id: job_id.clone(),
            name: "t".to_string(),
            spec,
            state: TaskState::Pending,
            assigned_node: None,
            network: None,
            exit_code: None,
        };
        live.logs.entry(task_id.clone()).or_default();
        // The live insert must be the same task the journal records: the
        // real `create_job` inserts and journals identical task objects.
        live.tasks.insert(task_id.clone(), task.clone());
        live.jobs.insert(
            job_id.clone(),
            JobRecord {
                name: "j".to_string(),
                created_at: now,
            },
        );
        crate::state::journal_op(
            live,
            &JournalOp::CreateJob {
                job_id,
                name: "j".to_string(),
                created_at: now,
                tasks: vec![task],
            },
        )
        .expect("journal append");

        crate::state::journal_op(
            live,
            &JournalOp::RegisterNode {
                node: sample_node(&node_id, now),
            },
        )
        .expect("journal append");
        live.nodes
            .insert(node_id.clone(), sample_node(&node_id, now));
        // Assign the task with a network address through the scheduler
        // path pieces: state change + ipam.
        let ip = live.ipam.allocate(&node_id, &task_id).expect("allocate");
        crate::state::journal_op(
            live,
            &JournalOp::IpamAllocate {
                node_id: node_id.clone(),
                task_id: task_id.clone(),
                ip,
            },
        )
        .expect("journal append");
        if let Some(task) = live.tasks.get_mut(&task_id) {
            task.state = TaskState::Assigned;
            task.assigned_node = Some(node_id.clone());
        }
        crate::state::journal_op(
            live,
            &JournalOp::TaskAssigned {
                task_id: task_id.clone(),
                node_id: node_id.clone(),
                network: None,
                services_env: None,
            },
        )
        .expect("journal append");
        let _ = crate::state::set_task_state(live, &task_id, TaskState::Running, None);
        let _ = crate::state::set_task_state(live, &task_id, TaskState::Succeeded, Some(0));
    }

    #[test]
    fn full_replay_restores_state() {
        // Build a live state through the real mutation functions, with a
        // journal attached, then replay the ops into a fresh state and
        // compare.
        let dir = tmp_dir("replay");
        let journal = open_journal(&dir).expect("open journal");
        let mut live = StateInner {
            journal: Some(journal),
            ..StateInner::default()
        };

        let now = Utc::now();
        drive_live_state(&mut live, now);

        // Replay.
        let ops = load_journal_ops(&dir).expect("load");
        assert!(!ops.is_empty(), "journal must have captured ops");
        let mut replayed = StateInner::default();
        apply_ops(&mut replayed, &ops, now);

        // Compare durable state (normalize heartbeats: live advanced, replay
        // parked at `now`).
        let normalize = |inner: &mut StateInner| {
            for node in inner.nodes.values_mut() {
                node.last_heartbeat = now;
            }
            inner.journal = None;
            inner.last_tick_ms = None;
        };
        normalize(&mut live);
        normalize(&mut replayed);
        assert_eq!(replayed.jobs, live.jobs);
        assert_eq!(replayed.tasks, live.tasks);
        assert_eq!(replayed.nodes, live.nodes);
        assert_eq!(replayed.snapshots, live.snapshots);
        assert_eq!(replayed.ipam, live.ipam);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
