//! Harvest shadow for snapshot orchestration (v0.6).
//!
//! Opt-in, read-only auditing: one durable Harvest workflow per snapshot
//! request observes the real snapshot state machine and records a verdict
//! (`agree` / `stuck` / `missing` / `harvest_error`). The shadow never
//! writes `BigTop` state, never touches Firecracker, and never fails the
//! real snapshot path — every Harvest failure is contained to a log line
//! and an error counter.
//!
//! Design notes:
//! - The Harvest [`SqliteRuntime`] is owned by a dedicated driver thread
//!   (it is `Send` but not `Sync`, and its activity bodies are synchronous
//!   closures). The rest of the server talks to it through an
//!   [`mpsc`] command channel and a shared verdict table.
//! - Activity bodies read a shadow-owned synchronous snapshot mirror
//!   (never the server's async state lock): the API path updates the
//!   mirror after the authoritative write lock is released, and only
//!   after the journal append succeeded.
//! - The verdict table lives in the *same* `SQLite` file as Harvest's own
//!   tables (a BigTop-owned table Harvest never touches), so one path
//!   owns the whole shadow.

// The `db.lock().map_err(...)?` pattern holds a `MutexGuard` in a named
// binding — the `significant_drop_tightening` lint is a false positive here.
#![allow(clippy::significant_drop_tightening)]

use autumn_harvest::{ActivityContext, ExecutionId, HarvestResult, WorkflowContext};
use autumn_harvest_macros::{activity, workflow};
use autumn_harvest_sqlite::{ExecutionOutcome, SqliteRuntime};
use bigtop_core::{NodeId, SnapshotId, SnapshotRecord, SnapshotState, TaskId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{self, RecvTimeoutError},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

/// Seconds between shadow observations of one snapshot.
const POLL_SECS: u64 = 5;
/// Observations before the audit gives up and calls the snapshot stuck.
/// 72 × 5 s = 6 minutes — far beyond any healthy snapshot attempt.
const MAX_POLLS: u32 = 72;
/// Driver cadence; also the worst-case delay before a new request is picked up.
const DRIVER_TICK: Duration = Duration::from_secs(1);

/// Errors from the shadow subsystem. Every one is contained: callers log it,
/// bump the error counter, and leave the real snapshot path untouched.
#[derive(Debug, thiserror::Error)]
pub enum ShadowError {
    /// The Harvest runtime failed (open, start, poll, outcome).
    #[error("harvest runtime error: {0}")]
    Harvest(String),
    /// `BigTop`'s own verdict table failed.
    #[error("verdict store error: {0}")]
    Store(String),
    /// JSON (de)serialization of workflow input/output failed.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// The driver thread could not be spawned.
    #[error("could not spawn shadow driver thread: {0}")]
    Spawn(String),
    /// A driver future did not resolve (hung backend — never observed).
    #[error("shadow driver future did not resolve")]
    DriverHung,
}

/// Input to the `snapshot_shadow` workflow: what to watch and for how long.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowInput {
    /// The snapshot request being audited.
    pub snapshot_id: SnapshotId,
    /// Task the snapshot belongs to (for the read surface).
    pub task_id: TaskId,
    /// Node that owns the task (for the read surface).
    pub node_id: NodeId,
    /// Seconds between observations.
    pub poll_secs: u64,
    /// Maximum observations before the verdict becomes `stuck`.
    pub max_polls: u32,
}

/// One read-only sample of a snapshot record. `state == None` means the
/// record is gone (the server crashed between the in-memory insert and the
/// journal append, or the id was never real).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    /// Snapshot lifecycle state at sample time; `None` = record missing.
    pub state: Option<SnapshotState>,
    /// Failure detail when the record is `Failed`.
    pub error: Option<String>,
    /// Resolved guest-memory path (once known).
    pub mem_file_path: Option<String>,
    /// Resolved state-file path (once known).
    pub snapshot_path: Option<String>,
    /// Wall-clock time of the sample (recorded once; replay reuses history).
    pub observed_at: DateTime<Utc>,
}

/// The audit's conclusion for one snapshot request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Still being audited.
    Pending,
    /// Reached a terminal state through legal transitions, on time.
    Agree,
    /// Deadline expired while non-terminal, or an illegal transition was
    /// observed — the known durability holes (lost report, stranded
    /// `Requested`/`InProgress`, verbatim replay that never resumes).
    Stuck,
    /// The record vanished mid-audit.
    Missing,
    /// The shadow itself failed (never the snapshot's fault).
    HarvestError,
}

impl Verdict {
    /// Storage / metric label form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Agree => "agree",
            Self::Stuck => "stuck",
            Self::Missing => "missing",
            Self::HarvestError => "harvest_error",
        }
    }
}

/// The durable output of the `snapshot_shadow` workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowOutput {
    /// The audited snapshot request.
    pub snapshot_id: SnapshotId,
    /// The audit's conclusion.
    pub verdict: Verdict,
    /// Every observation the audit took, oldest first.
    pub observations: Vec<Observation>,
}

/// A tracked snapshot for the read surface (`GET /v1/shadow/snapshots`, MCP).
#[derive(Debug, Clone, Serialize)]
pub struct ShadowTrack {
    /// The audited snapshot request.
    pub snapshot_id: SnapshotId,
    /// Task the snapshot belongs to.
    pub task_id: TaskId,
    /// Current verdict (`pending` while the audit runs).
    pub verdict: Verdict,
    /// Observations so far (complete once the verdict is terminal).
    pub observations: Vec<Observation>,
    /// Operator-facing detail (e.g. the shadow's own error message).
    pub detail: String,
    /// Last update to this row.
    pub updated_at: DateTime<Utc>,
}

/// Counters for `/metrics`. Built from atomics plus a verdict-grouped count.
#[derive(Debug, Default)]
pub struct ShadowMetrics {
    /// Audit workflows started.
    pub started: u64,
    /// Observations recorded across completed audits.
    pub observations: u64,
    /// Contained shadow failures.
    pub errors: u64,
    /// Current rows per verdict.
    pub pending: u64,
    pub agree: u64,
    pub stuck: u64,
    pub missing: u64,
    pub harvest_error: u64,
}

/// Is `to` a legal successor of `from` in the real snapshot state machine?
/// Mirrors `state::report_snapshot_result`'s transition table: the shadow
/// must agree with the server on what the machine can do.
const fn is_legal_transition(from: SnapshotState, to: SnapshotState) -> bool {
    use SnapshotState::{Done, Failed, InProgress, Requested};
    matches!(
        (from, to),
        (Requested, Requested | InProgress | Done | Failed)
            | (InProgress, InProgress | Done | Failed)
            | (Done, Done)
            | (Failed, Failed)
    )
}

const fn is_terminal(state: SnapshotState) -> bool {
    matches!(state, SnapshotState::Done | SnapshotState::Failed)
}

/// The audit's verdict over a finished observation sequence.
///
/// - Any vanished record → `Missing` (the crash window between the
///   in-memory insert and the journal append).
/// - Terminal state via legal transitions → `Agree`.
/// - Anything else (deadline exhausted while non-terminal, or a transition
///   the real machine should never produce) → `Stuck`: the durable
///   detector for the known snapshot durability holes.
#[must_use]
pub fn judge(observations: &[Observation]) -> Verdict {
    if observations.iter().any(|o| o.state.is_none()) {
        return Verdict::Missing;
    }
    let states: Vec<SnapshotState> = observations.iter().filter_map(|o| o.state).collect();
    let mut legal = true;
    for pair in states.windows(2) {
        if !is_legal_transition(pair[0], pair[1]) {
            legal = false;
            break;
        }
    }
    match states.last() {
        Some(state) if is_terminal(*state) && legal => Verdict::Agree,
        _ => Verdict::Stuck,
    }
}

/// The shadow workflow: observe, wait, observe, … until the snapshot is
/// terminal or the audit deadline expires, then judge.
///
/// Determinism: the poll loop is bounded by `max_polls`, timer ids derive
/// from the loop counter, and every branch depends only on recorded
/// activity results — replay reproduces the run exactly. Durable timers
/// mean a server restart mid-audit resumes the workflow from history.
#[workflow]
async fn snapshot_shadow(ctx: &WorkflowContext, input: ShadowInput) -> HarvestResult<ShadowOutput> {
    let mut observations = Vec::new();
    let mut obs: Observation = ctx
        .execute_activity(&observe_snapshot_state_info(), &input.snapshot_id)
        .await?;
    observations.push(obs.clone());
    let mut polls: u32 = 0;
    while obs.state.is_some_and(|s| !is_terminal(s)) && polls < input.max_polls {
        ctx.timer(&format!("shadow-poll-{polls}"), input.poll_secs)
            .await?;
        obs = ctx
            .execute_activity(&observe_snapshot_state_info(), &input.snapshot_id)
            .await?;
        observations.push(obs.clone());
        polls += 1;
    }
    let verdict = judge(&observations);
    Ok(ShadowOutput {
        snapshot_id: input.snapshot_id,
        verdict,
        observations,
    })
}

/// The shadow's only activity: a read-only sample of one snapshot record.
///
/// Declared with plain `#[activity]` (no attributes): single attempt, none
/// of the Postgres-only knobs — the `SQLite` backend's setup audit accepts
/// it. The `SQLite` backend ignores this async handler and runs the
/// synchronous body registered in [`ShadowDriver::open`]; this body is
/// unreachable in production and kept honest anyway.
#[activity]
#[allow(clippy::unused_async)]
async fn observe_snapshot_state(
    ctx: &ActivityContext,
    snapshot_id: SnapshotId,
) -> Result<Observation, String> {
    let _ = (ctx, snapshot_id);
    Err("sqlite backend dispatches the registered sync body".to_string())
}

/// The real activity body (`SQLite` backend): copy the record's observable
/// fields from the synchronous mirror. No writes, no I/O, no Firecracker,
/// no network. The mirror (not the tokio lock) is what makes this safe to
/// run on the driver thread inside a Tokio runtime context.
fn observe_body(
    mirror: &Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>>,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let snapshot_id: SnapshotId =
        serde_json::from_value(input).map_err(|e: serde_json::Error| e.to_string())?;
    // Recover from a poisoned lock rather than fail the audit over it.
    let mirror = mirror
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let obs = mirror.get(&snapshot_id).map_or_else(
        || Observation {
            state: None,
            error: None,
            mem_file_path: None,
            snapshot_path: None,
            observed_at: Utc::now(),
        },
        |rec| Observation {
            state: Some(rec.state),
            error: rec.error.clone(),
            mem_file_path: non_empty(&rec.spec.mem_file_path),
            snapshot_path: non_empty(&rec.spec.snapshot_path),
            observed_at: Utc::now(),
        },
    );
    serde_json::to_value(obs).map_err(|e: serde_json::Error| e.to_string())
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Command from the API path to the driver thread.
#[derive(Debug)]
#[allow(clippy::struct_field_names)]
struct TrackCmd {
    snapshot_id: SnapshotId,
    task_id: TaskId,
    node_id: NodeId,
}

/// Live counters owned by the shadow.
#[derive(Debug, Default)]
pub struct ShadowStats {
    /// Audit workflows started.
    pub started: AtomicU64,
    /// Observations recorded across finished audits.
    pub observations: AtomicU64,
    /// Contained shadow failures (never fail the snapshot path).
    pub errors: AtomicU64,
}

/// A point-in-time snapshot of the shadow's counters.
#[derive(Debug, Clone, Copy)]
pub struct ShadowStatsSnapshot {
    pub started: u64,
    pub observations: u64,
    pub errors: u64,
}

/// Owns the Harvest runtime and the verdict table. Lives on the driver
/// thread (or in tests, on the test thread) — never shared across threads.
pub struct ShadowDriver {
    runtime: SqliteRuntime,
    db: Arc<Mutex<rusqlite::Connection>>,
    stats: Arc<ShadowStats>,
    /// Runtime whose context the driver enters to poll Harvest
    /// (`poll_once` needs `tokio::time`).
    handle: tokio::runtime::Handle,
}

impl ShadowDriver {
    /// Open the shadow on a `SQLite` file: migrate Harvest's tables, register
    /// the workflow and the inert activity body, create the verdict table.
    ///
    /// `mirror` is the shadow-owned synchronous snapshot mirror — the only
    /// state the activity bodies touch. `handle` is a Tokio runtime whose
    /// context the driver enters around each `poll_once`.
    pub fn open(
        db_path: &Path,
        mirror: Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>>,
        db: Arc<Mutex<rusqlite::Connection>>,
        stats: Arc<ShadowStats>,
        handle: tokio::runtime::Handle,
    ) -> Result<Self, ShadowError> {
        let mut runtime =
            SqliteRuntime::open(db_path).map_err(|e| ShadowError::Harvest(e.to_string()))?;
        runtime.register_workflow(&snapshot_shadow_info());
        runtime.register_activity(&observe_snapshot_state_info(), move |input| {
            observe_body(&mirror, input)
        });
        init_verdict_table(&db)?;
        // Resume: rows left `pending` by a previous process are picked up
        // by the sweep on the next tick; Harvest resumes their workflows
        // from its own tables.
        Ok(Self {
            runtime,
            db,
            stats,
            handle,
        })
    }

    /// Start auditing one snapshot request. Called on the driver thread.
    /// Idempotent: a duplicate notification for the same snapshot (e.g. a
    /// retried API call) reuses the existing audit instead of starting a
    /// second workflow.
    pub fn track(
        &mut self,
        snapshot_id: SnapshotId,
        task_id: TaskId,
        node_id: NodeId,
    ) -> Result<(), ShadowError> {
        self.track_input(&ShadowInput {
            snapshot_id,
            task_id,
            node_id,
            poll_secs: POLL_SECS,
            max_polls: MAX_POLLS,
        })
    }

    /// Start auditing with an explicit input (tests: tiny deadlines).
    #[cfg(test)]
    fn track_with(&mut self, input: &ShadowInput) -> Result<(), ShadowError> {
        self.track_input(input)
    }

    /// Record a new snapshot for auditing (deduplicated).
    ///
    /// # Errors
    ///
    /// Returns [`ShadowError`] if the workflow cannot be started or the
    /// verdict row cannot be inserted.
    fn track_input(&mut self, input: &ShadowInput) -> Result<(), ShadowError> {
        if self.is_tracked(&input.snapshot_id)? {
            return Ok(());
        }
        let input_json =
            serde_json::to_value(input).map_err(|e| ShadowError::Serialization(e.to_string()))?;
        let exec = self
            .runtime
            .start_workflow("snapshot_shadow", input_json)
            .map_err(|e| ShadowError::Harvest(e.to_string()))?;
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        db.execute(
            "INSERT INTO bigtop_shadow_tracks
             (snapshot_id, execution_id, task_id, verdict, observations_json, detail, updated_at)
             VALUES (?1, ?2, ?3, 'pending', '[]', '', ?4)",
            rusqlite::params![
                input.snapshot_id.as_ref(),
                exec.to_string(),
                input.task_id.as_ref(),
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| ShadowError::Store(e.to_string()))?;
        self.stats.started.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// One driver step: advance Harvest, then sweep finished audits into
    /// the verdict table.
    ///
    /// `poll_once` needs a Tokio runtime context (its executor uses
    /// `tokio::time::timeout`); `Handle::block_on` enters it and drives
    /// the future with a real waker. The activity bodies it runs only
    /// touch the synchronous mirror, so this is safe from any thread
    /// outside the runtime.
    pub fn tick(&mut self) -> Result<(), ShadowError> {
        let progressed = self
            .handle
            .block_on(self.runtime.poll_once())
            .map_err(|e| ShadowError::Harvest(e.to_string()))?;
        let _ = progressed;
        self.sweep()
    }

    /// Async variant of [`tick`](Self::tick) for use inside a Tokio
    /// runtime (tests). Awaits `poll_once` directly.
    #[cfg(test)]
    pub async fn tick_async(&mut self) -> Result<(), ShadowError> {
        let progressed = self
            .runtime
            .poll_once()
            .await
            .map_err(|e| ShadowError::Harvest(e.to_string()))?;
        let _ = progressed;
        self.sweep()
    }

    /// Deterministic-simulation tick (tests): drive the runtime as of an
    /// injected wall-clock time. Lets a test fire durable timers exactly
    /// when it chooses, with no wall-clock sleeps. Never mix with
    /// [`tick_async`](Self::tick_async) for the same execution.
    /// Returns whether any execution made progress.
    #[cfg(test)]
    pub async fn tick_as_of(&mut self, now: DateTime<Utc>) -> Result<bool, ShadowError> {
        let progressed = self
            .runtime
            .poll_once_as_of(now)
            .await
            .map_err(|e| ShadowError::Harvest(e.to_string()))?;
        self.sweep()?;
        Ok(progressed)
    }

    fn is_tracked(&self, snapshot_id: &SnapshotId) -> Result<bool, ShadowError> {
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM bigtop_shadow_tracks WHERE snapshot_id = ?1",
                rusqlite::params![snapshot_id.as_ref()],
                |row| row.get(0),
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        Ok(count > 0)
    }

    /// Move terminal Harvest outcomes into the verdict table.
    fn sweep(&self) -> Result<(), ShadowError> {
        let pending = self.pending_tracks()?;
        for (snapshot_id, exec_str) in pending {
            let exec = ExecutionId::from_str(&exec_str)
                .map_err(|e| ShadowError::Serialization(format!("bad execution id: {e}")))?;
            match self
                .runtime
                .outcome(exec)
                .map_err(|e| ShadowError::Harvest(e.to_string()))?
            {
                ExecutionOutcome::Completed(value) => {
                    let output: ShadowOutput = serde_json::from_value(value)
                        .map_err(|e| ShadowError::Serialization(e.to_string()))?;
                    let n = output.observations.len() as u64;
                    self.record_verdict(&snapshot_id, output.verdict, &output.observations, "")?;
                    self.stats.observations.fetch_add(n, Ordering::Relaxed);
                }
                ExecutionOutcome::Failed(msg) => {
                    self.record_verdict(&snapshot_id, Verdict::HarvestError, &[], &msg)?;
                }
                ExecutionOutcome::Terminated(state) => {
                    self.record_verdict(
                        &snapshot_id,
                        Verdict::HarvestError,
                        &[],
                        &format!("terminated as {state}"),
                    )?;
                }
                ExecutionOutcome::Running => {}
            }
        }
        Ok(())
    }

    fn pending_tracks(&self) -> Result<Vec<(String, String)>, ShadowError> {
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let mut stmt = db
            .prepare(
                "SELECT snapshot_id, execution_id FROM bigtop_shadow_tracks
                 WHERE verdict = 'pending'",
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let snapshot_id: String = row.get(0)?;
                let execution_id: String = row.get(1)?;
                Ok((snapshot_id, execution_id))
            })
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let mut pending = Vec::new();
        for row in rows {
            pending.push(row.map_err(|e| ShadowError::Store(e.to_string()))?);
        }
        Ok(pending)
    }

    fn record_verdict(
        &self,
        snapshot_id: &str,
        verdict: Verdict,
        observations: &[Observation],
        detail: &str,
    ) -> Result<(), ShadowError> {
        let obs_json = serde_json::to_string(observations)
            .map_err(|e| ShadowError::Serialization(e.to_string()))?;
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        db.execute(
            "UPDATE bigtop_shadow_tracks
             SET verdict = ?1, observations_json = ?2, detail = ?3, updated_at = ?4
             WHERE snapshot_id = ?5",
            rusqlite::params![
                verdict.as_str(),
                obs_json,
                detail,
                Utc::now().to_rfc3339(),
                snapshot_id,
            ],
        )
        .map_err(|e| ShadowError::Store(e.to_string()))?;
        Ok(())
    }
}

/// Create `BigTop`'s verdict table (and WAL pragmas) on our own connection.
/// Harvest's six tables are never touched.
fn init_verdict_table(db: &Arc<Mutex<rusqlite::Connection>>) -> Result<(), ShadowError> {
    let db = db
        .lock()
        .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
    db.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         PRAGMA busy_timeout=5000;
         CREATE TABLE IF NOT EXISTS bigtop_shadow_tracks (
             snapshot_id     TEXT PRIMARY KEY,
             execution_id    TEXT NOT NULL,
             task_id         TEXT NOT NULL,
             verdict         TEXT NOT NULL,
             observations_json TEXT NOT NULL,
             detail          TEXT NOT NULL DEFAULT '',
             updated_at      TEXT NOT NULL
         );",
    )
    .map_err(|e| ShadowError::Store(e.to_string()))?;
    Ok(())
}

/// Open `BigTop`'s verdict-table connection on a `SQLite` file.
pub fn open_verdict_db(db_path: &Path) -> Result<Arc<Mutex<rusqlite::Connection>>, ShadowError> {
    let conn =
        rusqlite::Connection::open(db_path).map_err(|e| ShadowError::Store(e.to_string()))?;
    Ok(Arc::new(Mutex::new(conn)))
}

/// The server's handle to the shadow: cloneable, infallible to notify.
/// Owns the synchronous snapshot mirror — the only snapshot state the
/// Harvest activity bodies read.
#[derive(Debug, Clone)]
pub struct ShadowHandle {
    tx: mpsc::Sender<TrackCmd>,
    db: Arc<Mutex<rusqlite::Connection>>,
    stats: Arc<ShadowStats>,
    mirror: Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>>,
}

impl ShadowHandle {
    /// Record the latest authoritative snapshot state in the mirror.
    /// Best-effort and infallible by design: call after the authoritative
    /// write lock is released and the journal append succeeded. A poisoned
    /// mirror lock is recovered, never propagated.
    pub fn mirror_snapshot(&self, record: &SnapshotRecord) {
        let mut mirror = self
            .mirror
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        mirror.insert(record.id.clone(), record.clone());
    }

    /// Tell the shadow about an accepted snapshot request. Infallible by
    /// design: a full/disconnected channel only costs a log line and an
    /// error counter — never the request.
    pub fn notify_requested(&self, snapshot_id: SnapshotId, task_id: TaskId, node_id: NodeId) {
        if let Err(e) = self.tx.send(TrackCmd {
            snapshot_id,
            task_id,
            node_id,
        }) {
            eprintln!("bigtop: harvest shadow notify failed: {e}");
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of records in the shadow-owned mirror (tests only).
    #[cfg(test)]
    pub fn mirror_len(&self) -> usize {
        self.mirror
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Snapshot of the shadow's counters for Prometheus.
    #[must_use]
    pub fn stats(&self) -> ShadowStatsSnapshot {
        ShadowStatsSnapshot {
            started: self.stats.started.load(Ordering::Relaxed),
            observations: self.stats.observations.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
        }
    }

    /// Every tracked snapshot, newest first — the REST/MCP read surface.
    ///
    /// # Errors
    ///
    /// Returns [`ShadowError::Store`] if the verdict database cannot be read.
    pub fn tracks(&self) -> Result<Vec<ShadowTrack>, ShadowError> {
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let mut stmt = db
            .prepare(
                "SELECT snapshot_id, task_id, verdict, observations_json, detail, updated_at
                 FROM bigtop_shadow_tracks ORDER BY updated_at DESC",
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(|e| ShadowError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, rusqlite::Error>>()
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let mut tracks = Vec::with_capacity(rows.len());
        for (snapshot_id, task_id, verdict, observations_json, detail, updated_at) in rows {
            let observations: Vec<Observation> = serde_json::from_str(&observations_json)
                .map_err(|e| ShadowError::Serialization(e.to_string()))?;
            let updated_at: DateTime<Utc> = updated_at
                .parse()
                .map_err(|e: chrono::ParseError| ShadowError::Serialization(e.to_string()))?;
            tracks.push(ShadowTrack {
                snapshot_id: SnapshotId::from(snapshot_id),
                task_id: TaskId::from(task_id),
                verdict: match verdict.as_str() {
                    "agree" => Verdict::Agree,
                    "stuck" => Verdict::Stuck,
                    "missing" => Verdict::Missing,
                    "harvest_error" => Verdict::HarvestError,
                    _ => Verdict::Pending,
                },
                observations,
                detail,
                updated_at,
            });
        }
        Ok(tracks)
    }

    /// Counters for `/metrics`.
    ///
    /// # Errors
    ///
    /// Returns [`ShadowError::Store`] if the verdict database cannot be read.
    pub fn metrics_snapshot(&self) -> Result<ShadowMetrics, ShadowError> {
        let mut metrics = ShadowMetrics {
            started: self.stats.started.load(Ordering::Relaxed),
            observations: self.stats.observations.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
            ..ShadowMetrics::default()
        };
        let db = self
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let mut stmt = db
            .prepare("SELECT verdict, COUNT(*) FROM bigtop_shadow_tracks GROUP BY verdict")
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let counts = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| ShadowError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, rusqlite::Error>>()
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        for (verdict, count) in counts {
            let count = count.max(0).cast_unsigned();
            match verdict.as_str() {
                "pending" => metrics.pending = count,
                "agree" => metrics.agree = count,
                "stuck" => metrics.stuck = count,
                "missing" => metrics.missing = count,
                "harvest_error" => metrics.harvest_error = count,
                _ => {}
            }
        }
        Ok(metrics)
    }
}

/// Start the shadow.
///
/// Opens the `SQLite` file, registers the workflow and the inert activity,
/// and spawns the driver thread. `Err` (bad path, locked DB, …) means
/// "run without the shadow" — the caller logs and continues.
///
/// `seed` carries the snapshot records replayed from the journal: the
/// mirror is seeded after replay and before the driver thread starts, so
/// no workflow can observe a pre-replay mirror. Must be called from
/// within a Tokio runtime (the driver enters its context to poll).
///
/// # Errors
///
/// Returns [`ShadowError`] if the database cannot be opened, the Tokio
/// runtime context is unavailable, or the driver thread cannot be spawned.
pub fn spawn_shadow(
    db_path: &Path,
    seed: Vec<SnapshotRecord>,
) -> Result<ShadowHandle, ShadowError> {
    let handle = tokio::runtime::Handle::try_current().map_err(|_| {
        ShadowError::Harvest(
            "no Tokio runtime context; spawn_shadow must run inside the server runtime".to_string(),
        )
    })?;
    let stats = Arc::new(ShadowStats::default());
    let db = open_verdict_db(db_path)?;
    // The shadow owns its mirror. Seeding is infallible (in-memory insert)
    // and happens-before the driver thread starts.
    let mirror: Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>> = Arc::new(Mutex::new(
        seed.into_iter()
            .map(|record| (record.id.clone(), record))
            .collect(),
    ));
    let mut driver = ShadowDriver::open(
        db_path,
        Arc::clone(&mirror),
        Arc::clone(&db),
        Arc::clone(&stats),
        handle,
    )?;
    // The driver thread owns the receiver; the handle keeps the sender.
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name("harvest-shadow".to_string())
        .spawn(move || drive(&mut driver, rx))
        .map_err(|e| ShadowError::Spawn(e.to_string()))?;
    Ok(ShadowHandle {
        tx,
        db,
        stats,
        mirror,
    })
}

/// The driver thread: drain track commands, then tick Harvest and sweep
/// verdicts. Every failure is contained to a log line and the error
/// counter — the thread never panics out of a tick.
#[allow(clippy::needless_pass_by_value)]
fn drive(driver: &mut ShadowDriver, rx: mpsc::Receiver<TrackCmd>) {
    let track_one = |driver: &mut ShadowDriver, cmd: &TrackCmd| {
        if let Err(e) = driver.track(
            cmd.snapshot_id.clone(),
            cmd.task_id.clone(),
            cmd.node_id.clone(),
        ) {
            eprintln!("bigtop: harvest shadow track failed: {e}");
            driver.stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    };
    loop {
        match rx.recv_timeout(DRIVER_TICK) {
            Ok(cmd) => {
                track_one(driver, &cmd);
                while let Ok(cmd) = rx.try_recv() {
                    track_one(driver, &cmd);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if let Err(e) = driver.tick() {
            eprintln!("bigtop: harvest shadow tick failed: {e}");
            driver.stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StateInner;
    use bigtop_core::{SnapshotRecord, SnapshotSpec, SnapshotType};
    use std::path::PathBuf;

    fn obs(state: Option<SnapshotState>) -> Observation {
        Observation {
            state,
            error: None,
            mem_file_path: None,
            snapshot_path: None,
            observed_at: Utc::now(),
        }
    }

    #[test]
    fn judge_agrees_on_legal_path_to_done() {
        use SnapshotState::{Done, InProgress, Requested};
        let o = vec![obs(Some(Requested)), obs(Some(InProgress)), obs(Some(Done))];
        assert_eq!(judge(&o), Verdict::Agree);
    }

    #[test]
    fn judge_agrees_on_failed_terminal() {
        use SnapshotState::{Failed, Requested};
        let o = vec![obs(Some(Requested)), obs(Some(Failed))];
        assert_eq!(judge(&o), Verdict::Agree);
    }

    #[test]
    fn judge_stuck_when_deadline_expires_non_terminal() {
        use SnapshotState::Requested;
        let o = vec![obs(Some(Requested)); 4];
        assert_eq!(judge(&o), Verdict::Stuck);
    }

    #[test]
    fn judge_missing_when_record_vanishes() {
        use SnapshotState::Requested;
        let o = vec![obs(Some(Requested)), obs(None)];
        assert_eq!(judge(&o), Verdict::Missing);
    }

    #[test]
    fn judge_stuck_on_illegal_transition() {
        // The real machine can never go InProgress -> Requested; if the
        // shadow ever saw it, something is deeply wrong — stuck, not agree.
        use SnapshotState::{Done, InProgress, Requested};
        let o = vec![
            obs(Some(Requested)),
            obs(Some(InProgress)),
            obs(Some(Requested)),
            obs(Some(Done)),
        ];
        assert_eq!(judge(&o), Verdict::Stuck);
    }

    #[test]
    fn verdict_labels_round_trip() {
        assert_eq!(Verdict::Agree.as_str(), "agree");
        assert_eq!(Verdict::Pending.as_str(), "pending");
        assert_eq!(Verdict::Stuck.as_str(), "stuck");
        assert_eq!(Verdict::Missing.as_str(), "missing");
        assert_eq!(Verdict::HarvestError.as_str(), "harvest_error");
    }

    /// A live snapshot table plus its synchronous mirror, which the tests
    /// mutate directly (single-threaded; no async lock involved).
    struct Fixture {
        inner: StateInner,
        mirror: Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>>,
        snapshot_id: SnapshotId,
        task_id: TaskId,
        node_id: NodeId,
        db_path: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let snapshot_id = SnapshotId::generate();
            let task_id = TaskId::generate();
            let node_id = NodeId::generate();
            let record = SnapshotRecord {
                id: snapshot_id.clone(),
                task_id: task_id.clone(),
                node_id: node_id.clone(),
                spec: SnapshotSpec::with_defaults(SnapshotType::Full),
                state: SnapshotState::Requested,
                error: None,
                created_at: Utc::now(),
            };
            let mut inner = StateInner::default();
            // The fixture owns its mirror, like ShadowHandle does: the test
            // simulates the API path (authoritative write, then
            // post-lock mirror update).
            let mirror: Arc<Mutex<HashMap<SnapshotId, SnapshotRecord>>> =
                Arc::new(Mutex::new(HashMap::new()));
            inner.snapshots.insert(snapshot_id.clone(), record.clone());
            mirror
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(snapshot_id.clone(), record);
            let mut db_path = std::env::temp_dir();
            db_path.push(format!("bigtop-shadow-test-{tag}-{}", std::process::id()));
            // Delete the DB and its WAL/SHM sidecars; otherwise SQLite
            // recovers old history from the WAL.
            let _ = std::fs::remove_file(&db_path);
            let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
            let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
            // The sidecars use -wal/-shm suffixes, not extensions.
            let wal = format!("{}-wal", db_path.display());
            let shm = format!("{}-shm", db_path.display());
            let _ = std::fs::remove_file(&wal);
            let _ = std::fs::remove_file(&shm);
            Self {
                inner,
                mirror,
                snapshot_id,
                task_id,
                node_id,
                db_path,
            }
        }

        fn driver(&self) -> Result<ShadowDriver, ShadowError> {
            let stats = Arc::new(ShadowStats::default());
            let db = open_verdict_db(&self.db_path)?;
            // Tests run under #[tokio::test]; use the ambient runtime.
            let handle = tokio::runtime::Handle::current();
            ShadowDriver::open(
                &self.db_path,
                Arc::clone(&self.mirror),
                Arc::clone(&db),
                stats,
                handle,
            )
        }

        fn set_state(&mut self, state: SnapshotState) {
            if let Some(rec) = self.inner.snapshots.get_mut(&self.snapshot_id) {
                rec.state = state;
                let updated = rec.clone();
                // Post-lock mirror update, as the API path does.
                self.mirror
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(self.snapshot_id.clone(), updated);
            }
        }

        fn remove_record(&mut self) {
            self.inner.snapshots.remove(&self.snapshot_id);
            // The mirror keeps the last known copy on purpose: a vanished
            // record must read as missing, so drop it from the mirror too.
            // (A real crash would drop the whole in-memory mirror.)
            if let Ok(mut mirror) = self.mirror.lock() {
                mirror.remove(&self.snapshot_id);
            }
        }

        fn input(&self, poll_secs: u64, max_polls: u32) -> ShadowInput {
            ShadowInput {
                snapshot_id: self.snapshot_id.clone(),
                task_id: self.task_id.clone(),
                node_id: self.node_id.clone(),
                poll_secs,
                max_polls,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.db_path);
            let mut wal = self.db_path.clone();
            wal.set_extension("db-wal");
            let _ = std::fs::remove_file(&wal);
            let mut shm = self.db_path.clone();
            shm.set_extension("db-shm");
            let _ = std::fs::remove_file(&shm);
        }
    }

    /// Tick until the verdict table holds a terminal verdict (or give up).
    async fn drive_until_verdict(
        driver: &mut ShadowDriver,
        snapshot_id: &SnapshotId,
        max_ticks: u32,
    ) -> Result<Option<Verdict>, ShadowError> {
        for _ in 0..max_ticks {
            driver.tick_async().await?;
            if let Some(verdict) = verdict_row(driver, snapshot_id)? {
                return Ok(Some(verdict));
            }
        }
        Ok(None)
    }

    /// Read the current verdict for a snapshot, if it has left `pending`.
    fn verdict_row(
        driver: &ShadowDriver,
        snapshot_id: &SnapshotId,
    ) -> Result<Option<Verdict>, ShadowError> {
        let db = driver
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let verdict: String = db
            .query_row(
                "SELECT verdict FROM bigtop_shadow_tracks WHERE snapshot_id = ?1",
                rusqlite::params![snapshot_id.as_ref()],
                |row| row.get(0),
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        drop(db);
        if verdict == "pending" {
            return Ok(None);
        }
        Ok(Some(match verdict.as_str() {
            "agree" => Verdict::Agree,
            "stuck" => Verdict::Stuck,
            "missing" => Verdict::Missing,
            _ => Verdict::HarvestError,
        }))
    }

    /// Drive the shadow to quiescence at one instant: a timer firing and
    /// its activity finalizing can span two polls, so settle until idle.
    async fn settle_as_of(driver: &mut ShadowDriver, now: DateTime<Utc>) -> Result<(), ShadowError> {
        for _ in 0..10 {
            if !driver.tick_as_of(now).await? {
                break;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn workflow_reaches_agree_on_healthy_snapshot() -> Result<(), ShadowError> {
        // Deterministic simulation: inject the wall-clock time, so each
        // durable timer fires exactly when the test says — no sleeps,
        // no races. Every record state is observed exactly once.
        let mut fx = Fixture::new("agree");
        let mut driver = fx.driver()?;
        // 60-second polls; the test jumps past each deadline and only
        // then advances the record.
        driver.track_with(&fx.input(60, 72))?;

        let t0 = Utc::now();
        settle_as_of(&mut driver, t0).await?; // observes Requested
        fx.set_state(SnapshotState::InProgress);
        settle_as_of(&mut driver, t0 + chrono::Duration::seconds(61)).await?; // observes InProgress
        fx.set_state(SnapshotState::Done);
        settle_as_of(&mut driver, t0 + chrono::Duration::seconds(122)).await?; // observes Done
        let verdict = verdict_row(&driver, &fx.snapshot_id)?
            .ok_or_else(|| ShadowError::Store("no verdict recorded".to_string()))?;
        assert_eq!(verdict, Verdict::Agree);

        // The durable trail holds every observation, oldest first, and
        // proves the intermediate state was really seen.
        let db = driver
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let obs_json: String = db
            .query_row(
                "SELECT observations_json FROM bigtop_shadow_tracks WHERE snapshot_id = ?1",
                rusqlite::params![fx.snapshot_id.as_ref()],
                |row| row.get(0),
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        let observations: Vec<Observation> = serde_json::from_str(&obs_json)
            .map_err(|e| ShadowError::Serialization(e.to_string()))?;
        let states: Vec<Option<SnapshotState>> = observations.iter().map(|o| o.state).collect();
        assert_eq!(
            states,
            vec![
                Some(SnapshotState::Requested),
                Some(SnapshotState::InProgress),
                Some(SnapshotState::Done),
            ],
            "expected the durable trail to prove Requested -> InProgress -> Done"
        );
        Ok(())
    }

    #[tokio::test]
    async fn workflow_calls_stuck_when_deadline_expires() -> Result<(), ShadowError> {
        let fx = Fixture::new("stuck");
        let mut driver = fx.driver()?;
        // Three polls, zero seconds apart: the record never leaves
        // Requested, so the audit must conclude `stuck`.
        driver.track_with(&fx.input(0, 3))?;
        let verdict = drive_until_verdict(&mut driver, &fx.snapshot_id, 20)
            .await?
            .ok_or_else(|| ShadowError::Store("no verdict recorded".to_string()))?;
        assert_eq!(verdict, Verdict::Stuck);
        Ok(())
    }

    #[tokio::test]
    async fn workflow_calls_missing_when_record_vanishes() -> Result<(), ShadowError> {
        let mut fx = Fixture::new("missing");
        let mut driver = fx.driver()?;
        driver.track_with(&fx.input(0, 72))?;
        driver.tick_async().await?; // observes Requested
        fx.remove_record(); // the crash window: insert never journaled
        let verdict = drive_until_verdict(&mut driver, &fx.snapshot_id, 20)
            .await?
            .ok_or_else(|| ShadowError::Store("no verdict recorded".to_string()))?;
        assert_eq!(verdict, Verdict::Missing);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_track_starts_one_workflow() -> Result<(), ShadowError> {
        let fx = Fixture::new("idem");
        let mut driver = fx.driver()?;
        driver.track(
            fx.snapshot_id.clone(),
            fx.task_id.clone(),
            fx.node_id.clone(),
        )?;
        driver.track(
            fx.snapshot_id.clone(),
            fx.task_id.clone(),
            fx.node_id.clone(),
        )?;
        assert_eq!(driver.stats.started.load(Ordering::Relaxed), 1);
        let db = driver
            .db
            .lock()
            .map_err(|_| ShadowError::Store("verdict db lock poisoned".to_string()))?;
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM bigtop_shadow_tracks WHERE snapshot_id = ?1",
                rusqlite::params![fx.snapshot_id.as_ref()],
                |row| row.get(0),
            )
            .map_err(|e| ShadowError::Store(e.to_string()))?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn restart_resumes_audit_from_history() -> Result<(), ShadowError> {
        let mut fx = Fixture::new("restart");
        // First process: start the audit, observe Requested once, die.
        let mut driver = fx.driver()?;
        driver.track_with(&fx.input(0, 72))?;
        driver.tick_async().await?;
        drop(driver);

        // Second process: the record is Done now; reopening must resume
        // the workflow from Harvest history and record the verdict.
        fx.set_state(SnapshotState::Done);
        let mut driver2 = fx.driver()?;
        let verdict = drive_until_verdict(&mut driver2, &fx.snapshot_id, 20)
            .await?
            .ok_or_else(|| ShadowError::Store("no verdict recorded".to_string()))?;
        assert_eq!(verdict, Verdict::Agree);
        Ok(())
    }

    #[test]
    fn shadow_open_failure_is_contained() {
        // An unwritable path must fail cleanly — no panic, no thread.
        // (Outside a Tokio runtime this fails at the runtime check first;
        // inside the server it would fail at the DB open. Either way: Err.)
        let bad = Path::new("/nonexistent-dir-xyz/bigtop/shadow.db");
        assert!(spawn_shadow(bad, Vec::new()).is_err());
    }
}
