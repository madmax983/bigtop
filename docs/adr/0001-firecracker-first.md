# ADR 0001 — Firecracker-first: every workload is a microVM

Date: 2026-09-10
Status: accepted

## Context

BigTop started as a "container orchestrator". The orchestrator market does
not need another generic OCI runner — that space is crowded, and
container-level isolation is a permanent CVE treadmill.

## Decision

Every production workload in BigTop is a **Firecracker microVM**. BigTop
does not run generic OCI containers. Positioning: *the security of VMs with
the speed of containers*.

- `FirecrackerRuntime` is the production runtime: one microVM per task,
  configured through the real Firecracker REST API (`PUT /machine-config`,
  `/boot-source`, `/drives/rootfs`, `/actions` → `InstanceStart`).
- `ProcessRuntime` exists only as the local-dev and CI stand-in (same
  scheduling, same state machine, same log plumbing — no virtualization).
- The agent's `--runtime` flag is `auto | firecracker | process`;
  `auto` picks Firecracker when `/dev/kvm` exists.

## Consequences

- v0.1 ships no container runtime, no OCI image support, no registry.
- The v0.1 "guest contract" (kernel cmdline `bigtop.cmd_b64` carrying the
  shell line; guest init decodes and execs it) is part of the spec and must
  be honored by the reference guest init in v0.2.
- Hardening (jailer, vsock logs, snapshots, tap networking) is explicitly
  v0.2/v0.3 work, listed in the README roadmap.
