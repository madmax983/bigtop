# BigTop 🎪

**Every workload is a Firecracker microVM. The security of VMs with the
speed of containers. One binary. Loud, fast, opinionated.**

Kubernetes made you learn 47 resource kinds and write YAML novels to run
three processes. BigTop says: a job is a TOML file, a task is a microVM,
and the whole orchestrator is a single Rust binary. No etcd quorum to
babysit, no container runtime shim stack — just Firecracker microVMs.
Guest boot is ~125 ms per Firecracker's published numbers (ours is
unmeasured — see PROFILING.md), isolated like it's 1999 and
virtualization just dropped.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    bigtop (one binary)                   │
│                                                          │
│  ┌──────────────┐   HTTP/JSON    ┌───────────────────┐   │
│  │   server     │◄──────────────►│   agent(s)        │   │
│  │              │                │                   │   │
│  │ REST API     │   heartbeat    │ poll assignments  │   │
│  │ in-mem store │   2s           │ 1s                │   │
│  │ scheduler    ├────────────────┤                   │   │
│  │ 500ms tick   │                │ ┌───────────────┐ │   │
│  │ least-loaded │                │ │ Firecracker   │ │   │
│  │ fit          │                │ │ 1 task = 1 µVM│ │   │
│  └──────────────┘                │ └───────────────┘ │   │
│         │                        │  (or processes    │   │
│    bigtop ps/logs               │   w/o /dev/kvm)   │   │
│    bigtop run                   └───────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

Crates:

- `bigtop-core` — domain types (`JobId`/`TaskId`/`NodeId` newtypes,
  `TaskSpec` + `VmSpec`, `Task`, `NodeInfo`), wire DTOs, error type.
- `bigtop-server` — axum REST API, in-memory store, scheduler
  (least-loaded fit, dead-node requeue).
- `bigtop-agent` — registers, heartbeats, polls assignments, runs tasks via
  `FirecrackerRuntime` (real microVMs) or `ProcessRuntime` (dev/CI).
- `bigtop` — the CLI: `server`, `agent`, `run`, `ps`, `nodes`, `logs`.

## Quickstart

```bash
cargo build --release

# Terminal 1: the server
./target/release/bigtop server --port 4667

# Terminal 2: the agent (auto: firecracker if /dev/kvm exists, else processes)
./target/release/bigtop agent --server http://127.0.0.1:4667 --name node-1

# Terminal 3: submit, watch, read logs
./target/release/bigtop run examples/hello.toml
./target/release/bigtop ps
./target/release/bigtop nodes
./target/release/bigtop logs <task-id>
```

On a machine with `/dev/kvm` and a `firecracker` binary, point
`examples/hello.toml`'s `[task.vm]` at a real kernel + rootfs and each
task boots in its own microVM. Without KVM, the agent uses the process
runtime — same scheduling, same logs, no virtualization.

## The v0.1 deal (honest)

- ✅ Job submit → schedule → run → logs, end to end (verified, incl. an
  integration test that runs a real 2-task job to `Succeeded`).
- ✅ `FirecrackerRuntime` implemented against the real Firecracker REST API
  (machine-config, boot-source, drives, InstanceStart over the Unix
  socket); config builders + HTTP plumbing unit-tested against a fake API.
- ⚠️ End-to-end microVM boot is **unverified**: it needs `/dev/kvm` and a
  guest kernel/rootfs, which this dev environment doesn't have. The live
  demo runs on the process runtime.
- ⚠️ No jailer sandboxing, no vsock log channel, no tap networking, no
  snapshots, no persistence (server state is in-memory).

## Roadmap

- **v0.2** — Harden `FirecrackerRuntime`: jailer sandboxing, vsock log
  streaming, snapshot/restore, reference guest init that honors
  `bigtop.cmd_b64`; server persistence.
- **v0.3** — Networking: tap devices + CNI-lite, per-task IPs.
- **v0.4** — Service discovery + virtual IPs, web dashboard.

## Profiling

See [PROFILING.md](PROFILING.md): criterion benches for the scheduler,
callgrind/DHAT scripts under `profiling/`, and the v0.1 performance
budget. Measure first, optimize hot paths only.

## Spec

[SPEC.md](SPEC.md) — API, scheduler policy, agent lifecycle, guest
contract, failure semantics.
