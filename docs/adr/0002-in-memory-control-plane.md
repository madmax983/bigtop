# ADR 0002 — Single-process in-memory control plane for v0.1

Date: 2026-09-10
Status: accepted

## Context

v0.1 "spark" exists to prove the loop: submit → schedule → boot microVM →
stream logs → terminal state. Durability, scale, and multi-scheduler HA are
later problems.

## Decision

The v0.1 server is a **single process with all state in memory**
(`tokio::sync::RwLock<StateInner>`): jobs, tasks, nodes, and a 200-line
per-task log tail. No database, no etcd, no persistence layer.

- Scheduler ticks every 500 ms in-process: dead-node requeue (10 s
  heartbeat timeout), resource accounting recomputed from live tasks,
  deterministic least-loaded-fit placement.
- A server crash loses everything; agents re-register as new node ids on
  restart. This is documented in `SPEC.md`, not hidden.

## Consequences

- v0.2 must add persistence (journal/snapshot of the task table) before
  anyone trusts BigTop with real work.
- The scheduler is written so the hot path (`tick`) is benchmarkable in
  isolation (`cargo bench -p bigtop-server`) — the profiling budget in
  `PROFILING.md` assumes this shape.
- No network partition handling beyond heartbeat timeouts; split-brain is
  out of scope for v0.1.
