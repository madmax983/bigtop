# BigTop v0.1 "spark" — Spec

BigTop is a Firecracker-first orchestrator. Every workload is a microVM:
the security of VMs with the speed of containers. One binary, opinionated,
loud. v0.1 is the "spark": job submission, scheduling, agents that boot
microVMs (or plain processes where there is no KVM), state reporting, logs.

## Terms

- **Job**: a named bag of task specs, submitted once.
- **Task**: one unit of work = one microVM. A task spec with `count = N`
  expands into N tasks.
- **Node**: an agent machine registered with the server.
- **Runtime**: how the agent runs a task. `firecracker` (one microVM per
  task) or `process` (plain child process; dev/CI stand-in).

## API (v1)

| Method | Path | Body / Query | Result |
|---|---|---|---|
| `POST` | `/v1/jobs` | `JobSpec` JSON | `201 {job_id}`; `400` on invalid spec |
| `GET` | `/v1/jobs` | — | `[{id, name, created_at, tasks}]` |
| `GET` | `/v1/tasks` | — | `[Task]` sorted by id |
| `POST` | `/v1/nodes/register` | `{name, addr, total}` | `201 {id}` |
| `POST` | `/v1/nodes/{id}/heartbeat` | — | `200`; `404` unknown node |
| `GET` | `/v1/nodes` | — | `[NodeInfo]` sorted by id |
| `GET` | `/v1/agents/assignments?node_id=` | — | tasks `Assigned` to that node |
| `POST` | `/v1/tasks/{id}/state` | `{state, exit_code?}` | `200`; `404`; `409` if already terminal |
| `POST` | `/v1/tasks/{id}/logs` | `{lines: [...]}` | `200`; `404` unknown task |
| `GET` | `/v1/tasks/{id}/logs` | — | `{lines: [...]}` oldest first |

Job spec validation (`400 invalid job spec`): non-empty name, at least one
task spec, non-empty command per spec, `count >= 1`.

## TaskSpec

```toml
name = "hello"

[[task]]
name = "greeter"
command = "sh"
args = ["-c", "echo hello from $BIGTOP_TASK; sleep 30"]
count = 3
[task.env]
FOO = "bar"

[task.resources]
cpu_millis = 100   # scheduler currency; also sizes guest vCPUs (ceil to vCPU)
mem_mb = 64

[task.vm]
kernel_image = "/var/lib/bigtop/images/vmlinux"  # guest kernel (vmlinux)
rootfs = "/var/lib/bigtop/images/rootfs.ext4"    # guest rootfs (ext4)
vcpu_count = 1
mem_mb = 128
# boot_args = "console=ttyS0 quiet"               # optional extra kernel args
```

`[[task]]` also deserializes from JSON as `"tasks": [...]`.

## Scheduler

- Ticks every 500 ms.
- **Dead nodes**: no heartbeat for 10 s → dead. Tasks `Assigned`/`Running`
  on a dead node return to `Pending` (requeued, then placed elsewhere).
- **Accounting**: per-node `used` is recomputed from live tasks every tick
  (self-healing, never drifts).
- **Placement**: least-loaded fit. Among alive nodes where the task fits
  (`used + need <= total` on CPU and memory), pick the lowest CPU
  utilization (exact integer math, no floats), memory as tiebreak, node id
  as final tiebreak (deterministic). Tasks that fit nowhere stay `Pending`.

## Agent

1. `POST /v1/nodes/register` with name, addr label, total resources
   (all host CPUs; RAM from `/proc/meminfo`).
2. Heartbeat every 2 s.
3. Poll `GET /v1/agents/assignments?node_id=` every 1 s; spawn each new
   assignment.
4. Report `Running`, stream stdout/stderr lines to
   `POST /v1/tasks/{id}/logs`, then report `Succeeded`/`Failed` with the
   exit code. Spawn failure → `Failed` immediately.

### Runtime selection

`bigtop agent --runtime firecracker|process|auto` (default `auto`).
`auto` = `firecracker` when `/dev/kvm` exists, else `process`. Extra flags:
`--vm-dir` (default `/tmp/bigtop-vms`), `--firecracker-bin` (default
`firecracker`).

### FirecrackerRuntime (the real deal)

Per task:

1. Create `<vm_dir>/<task-id>/`; socket at `<vm_dir>/<task-id>/fc.sock`.
2. Fail fast if `vm.kernel_image`/`vm.rootfs` are empty or missing.
3. Spawn `firecracker --api-sock <sock> --log-path <dir>/firecracker.log
   --id <task-id>` with stdout/stderr piped (`kill_on_drop`).
4. Wait up to 10 s for the API socket.
5. `PUT /machine-config` `{vcpu_count, mem_size_mib, smt: false}` —
   `vcpu_count = max(vm.vcpu_count, ceil(resources.cpu_millis/1000))`,
   minimum 1; memory = `max(vm.mem_mb, 64)`.
6. `PUT /boot-source` `{kernel_image_path, boot_args}`.
7. `PUT /drives/rootfs` `{drive_id: "rootfs", path_on_host, is_root_device:
   true, is_read_only: false}`.
8. `PUT /actions` `{action_type: "InstanceStart"}`.
9. The guest serial console (VMM stdout) streams as task logs; the VMM's
   exit status becomes the task's terminal state.

Before step 3, the agent writes `<vm_dir>/<task-id>/bigtop-vm.json`: a
read-only record of exactly what steps 5–7 configure (vcpu count, memory,
kernel, rootfs, rendered boot args, plus the original command/args/env and
the API socket path). The REST API stays the source of truth; the file is
for inspection and replay.

**v0.1 guest contract.** `boot_args` = user args (default
`console=ttyS0 reboot=k panic=1 pci=off`) plus
`bigtop.task=<task-id> bigtop.cmd_b64=<base64>`, where the base64 payload is
the shell line built from env + command + args, single-quote escaped
(e.g. `GREETING='hi' 'sh' '-c' 'echo hi'`). The guest init is expected to
read `/proc/cmdline`, decode `bigtop.cmd_b64`, and exec it. A reference
guest init ships in v0.2.

**v0.1 limits (honest):** no jailer sandboxing, no vsock log channel, no
snapshot/restore, no tap networking. End-to-end microVM boot is implemented
against the real Firecracker API and unit-tested (config builders, HTTP
plumbing against a fake API socket), but **unverified on real KVM hardware**
— this environment has no `/dev/kvm`.

### ProcessRuntime (dev/CI stand-in)

Same lifecycle, but `tokio::process::Command` directly. Injects
`BIGTOP_TASK` and `BIGTOP_JOB` env vars. Used by the integration test and
the local demo.

## Failure semantics

- Terminal states are final: `409 conflict` on any transition out of
  `Succeeded`/`Failed`.
- Dead node → its tasks requeue (see scheduler). No checkpointing in v0.1:
  a requeued task restarts from scratch.
- Agent crash: heartbeats stop → node dies after 10 s → tasks requeue.
- Server crash: all state is in memory; everything is lost (persistence is
  v0.2+). Agents keep heartbeating and re-register as a new node id.
- Logs: last 200 lines per task, in memory.

## IDs

`job-<hex nanos>-<hex pid>-<hex seq>` (likewise `task-…`, `node-…`).
Unique per process; sortable; safe in env vars and shell.

## CLI

```
bigtop server [--port 4667] [--bind 127.0.0.1]
bigtop agent --server http://127.0.0.1:4667 [--name NAME] [--runtime auto] [--vm-dir DIR] [--firecracker-bin BIN]
bigtop run <job.toml> [--server URL]
bigtop ps [--server URL]
bigtop nodes [--server URL]
bigtop logs <task-id> [--server URL]
```
