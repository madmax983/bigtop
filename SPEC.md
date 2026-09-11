# BigTop v0.4 — Spec

BigTop is a Firecracker-first orchestrator. Every workload is a microVM:
the security of VMs with the speed of containers. One binary, opinionated,
loud. v0.1 "spark" shipped job submission, scheduling, and agents that boot
microVMs. v0.2 hardened the Firecracker path: snapshot/restore, vsock log
streaming, and `firecracker-jailer` sandboxing. v0.3 gave every task an
identity on the wire: per-task taps, server IPAM, and a static guest
network contract. v0.4 is **one big network, and it remembers**: a VXLAN
cross-node overlay mesh, service discovery over the overlay, Prometheus
metrics, and crash-safe server persistence — plus a tiny status page on
top.

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
| `POST` | `/v1/tasks/{id}/snapshot` | `{snapshot_type?, mem_file_path?, snapshot_path?}` | `202 {snapshot_id}`; `404`; `409` task not running |
| `GET` | `/v1/tasks/{id}/snapshots` | — | `[SnapshotRecord]` oldest first |
| `GET` | `/v1/agents/snapshot-requests?node_id=` | — | `[PendingSnapshot]` for that node, oldest first |
| `POST` | `/v1/tasks/{id}/snapshots/{snapshot_id}/result` | `{state, node_id, mem_file_path?, snapshot_path?, error?}` | `200`; `404`; `409` illegal transition |
| `GET` | `/v1/services` | — | `[{name, endpoints: [{task_id, ip}]}]` sorted by name |
| `GET` | `/v1/agents/overlay-peers?node_id=` | — | `[{node_id, underlay_ip}]` for other alive overlay nodes |
| `GET` | `/metrics` | — | Prometheus text format (see "Metrics (v0.4)") |
| `GET` | `/` | — | static HTML status page (see "Status page (v0.4)") |

Node registration (`POST /v1/nodes/register`) takes
`{name, addr, total, underlay_ip?}`: `underlay_ip` is the node's address
on the underlay network, used as its VXLAN VTEP address. Nodes that omit
it do not join the overlay mesh.

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

# Snapshot from this microVM when the guest signals completion (best-effort).
# Omit paths to let the agent pick defaults under the task's VM dir.
# [task.snapshot_policy.on_success]
# snapshot_type = "Full"   # or "Diff" (needs diff snapshots enabled at boot)

# Boot from a snapshot instead of kernel + rootfs. Set by
# `bigtop snapshot restore`; hand-editing is not expected.
# [task.vm.boot_snapshot]
# mem_file_path = "/vms/task-1/snapshots/snap-1.mem"
# snapshot_path = "/vms/task-1/snapshots/snap-1.snap"
# enable_diff_snapshots = false

# Pin this task to one node. Set by `bigtop snapshot restore` so the new
# task lands where the snapshot files live. The scheduler only places
# such tasks on the pinned node; if that node is gone they stay Pending.
# node_affinity = "node-abc123"

# Networking (v0.3). Disabled by default. When enabled, the server's IPAM
# assigns the task a static IP on placement and the agent creates a tap
# device for its microVM (see "Networking (v0.3)" below).
# [task.network]
# enabled = true
# hostname = "web-1"   # optional; passed to the guest as bigtop.hostname

# Service identity and discovery (v0.4). Optional. Needs [task.network]
# enabled: without a pod IP there is nothing to discover.
# [task.service]
# name = "api"            # register this task's pod IP under "api"
# discover = ["db"]       # inject BIGTOP_SERVICES with db's current IPs
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
  A task with `node_affinity` is only eligible on its pinned node.

## Agent

1. `POST /v1/nodes/register` with name, addr label, total resources
   (all host CPUs; RAM from `/proc/meminfo`).
2. Heartbeat every 2 s.
3. Poll `GET /v1/agents/assignments?node_id=` every 1 s; spawn each new
   assignment.
4. Poll `GET /v1/agents/snapshot-requests?node_id=` every 1 s; for each
   pending request report `InProgress`, snapshot the running microVM, and
   report `Done` (with the resolved file paths) or `Failed`.
5. Report `Running`, stream stdout/stderr lines to
   `POST /v1/tasks/{id}/logs`, then report `Succeeded`/`Failed` with the
   exit code. Spawn failure → `Failed` immediately.

### Runtime selection

`bigtop agent --runtime firecracker|process|auto` (default `auto`).
`auto` = `firecracker` when `/dev/kvm` exists, else `process`. Extra flags:
`--vm-dir` (default `/tmp/bigtop-vms`), `--firecracker-bin` (default
`firecracker`), and the jailer set: `--jailer`, `--jailer-bin` (default
`jailer`), `--jailer-uid`/`--jailer-gid` (default `1234`),
`--chroot-base-dir` (default `/srv/jailer`), `--netns` (optional).

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

**v0.2 guest contract.** `boot_args` = user args (default
`console=ttyS0 reboot=k panic=1 pci=off`) plus
`bigtop.task=<task-id> bigtop.cmd_b64=<base64>`, where the base64 payload is
the shell line built from env + command + args, single-quote escaped
(e.g. `GREETING='hi' 'sh' '-c' 'echo hi'`). The guest init reads
`/proc/cmdline`, decodes `bigtop.cmd_b64`, and execs it. A reference
guest init still does not ship in v0.2 — bring your own init that honors
this contract.

### Guest log streaming over vsock (v0.2)

Serial-console logging works, but it multiplexes through the VMM process
and disappears in jailer mode (the jailer owns the stdio). Guests that
want structured, per-stream logs dial the host directly.

Transport, per Firecracker's `docs/vsock.md`: there is **no `AF_VSOCK` on
the host side**. On every fresh boot the agent configures the guest's
virtio-vsock device with `PUT /vsock`
(`{"guest_cid": <derived from task id>, "uds_path": <task socket>}`), and
Firecracker bridges guest `AF_VSOCK` connections to `(CID 2, port)` into
the agent's per-task `AF_UNIX` listener at `<uds_path>_<port>`. Only the
guest ever touches `AF_VSOCK`.

- Destination: vsock CID `2` (`VMADDR_CID_HOST`), port `4668`.
- The agent binds `<task-vm-dir>/vsock.sock_4668` (in jailer mode, the
  host path backing the jailed `/vsock.sock`) and routes each connection
  by the task id in its handshake frame.
- Wire protocol — every frame is `u32` big-endian length (stream byte +
  payload), then `u8` stream selector, then payload bytes:

| Selector | Name | Payload |
|---|---|---|
| `0` | handshake | UTF-8 task id — **must be the first frame** |
| `1` | stdout | one log line |
| `2` | stderr | one log line |
| `3` | complete | empty — guest finished; host may snapshot, then closes |

- The agent tags forwarded lines `[vsock:stdout]` / `[vsock:stderr]` and
  merges them with serial-console lines by arrival time.
- A `complete` frame (or EOF after a handshake) fires the task's
  completion signal, which drives the `OnSuccess` snapshot policy. The
  guest should send `complete` *before* powering off, while the VMM is
  still alive to be snapshotted.
- Frame payloads are capped at 1 MiB; oversize length prefixes are rejected
  before allocation.
- The per-task listener is torn down when the task finishes (its socket
  file is removed), so listeners never linger.
- Snapshot boots do **not** reconfigure vsock: the restored VM keeps
  whatever device state the snapshot captured, and its guest falls back
  to the serial console. vsock log streaming is a fresh-boot feature.
- No vsock device in the guest, or no listener on the host? Nothing
  breaks: the serial console keeps streaming as before.

### Snapshot/restore (v0.2)

- `bigtop snapshot create <task-id>` → `POST /v1/tasks/{id}/snapshot`
  → `202 {snapshot_id}`. The task must be `Running` on a node, else `409`.
- The owning agent polls `GET /v1/agents/snapshot-requests`, reports
  `InProgress`, runs Firecracker's `PUT /snapshot/create`
  (`{"snapshot_type": "Full"|"Diff", "snapshot_path", "mem_file_path"}`),
  and reports `Done` with the resolved paths (defaults:
  `<vm_dir>/<task-id>/snapshots/<snapshot-id>.mem`/`.snap`) or `Failed`.
  Legal state transitions: `Requested → {InProgress, Done, Failed}`,
  `InProgress → {Done, Failed}`.
- `bigtop snapshot list <task-id>` shows every record and its state.
- `bigtop snapshot restore <task-id> <snapshot-id>` submits a new one-task
  job whose spec boots from the snapshot (`PUT /snapshot/load` with
  `{"snapshot_path", "mem_backend": {"backend_type": "File",
  "backend_path"}, "enable_diff_snapshots", "resume_vm": true}`) and pins
  the task to the node holding the snapshot files via `node_affinity`.
- `snapshot_policy = on_success` in a task spec asks the agent to snapshot
  automatically when the guest signals completion over vsock
  (best-effort: a guest that never signals is never snapshotted).

### Jailer sandboxing (v0.2)

`bigtop agent --jailer` boots every microVM under `firecracker-jailer`
instead of raw `firecracker`. The jailer builds a chroot, drops to
`--uid`/`--gid`, optionally joins `--netns`, and execs firecracker. The
exact argv is:

```
jailer --id <task-id> --uid <uid> --gid <gid> \
  --chroot-base-dir <base> [--netns <path>] [--daemonize] \
  --exec-file <firecracker-bin> -- \
  --api-sock /fc.sock --log-path /firecracker.log --id <task-id>
```

Paths in `--exec-file`, kernel/rootfs images, and snapshot files are
interpreted **inside the jail** — the operator must stage them under
`<base>/<id>/root/` (see README "Jailer host setup"). The API socket the
agent dials is `<base>/<id>/root/fc.sock` on the host. The jailer owns the
VMM's stdio, so in jailer mode the serial console is unavailable: guests
must log over vsock. `--daemonize` is off by default — the agent
supervises the jailer process as the VM's lifetime handle, and a
daemonizing jailer would look like an instantly-exited VM.

**v0.2 limits (honest):** no reference guest init yet, no server
persistence. End-to-end microVM boot, real snapshot files, and the real
vsock bridge are implemented against the real Firecracker API and
unit-tested (config builders including `PUT /vsock`, frame codec over a
loopback, the full accept/serve path over real `AF_UNIX` sockets, HTTP
plumbing against fake API sockets), but **unverified on real KVM
hardware** — this environment has no `/dev/kvm`. Only the guest side of
the bridge needs `AF_VSOCK`; the agent side is `AF_UNIX` and fully
exercised in tests.

### Networking (v0.3)

Every network-enabled task gets a tap device, a MAC, and a static IP.

**Server IPAM.** `bigtop server --network-cidr 172.28.0.0/16` (the
default) carves the /16 into /24s, one per node: the first node seen gets
`172.28.0.0/24`, the next `172.28.1.0/24`, and so on. On placement, the
scheduler allocates one IP per network-enabled task from its node's /24
(`.1` is the gateway, `.2`–`.254` are guests; `.0`/`.255` are never handed
out). The IP is released when the task goes terminal and when a dead
node's tasks requeue — a requeued task gets a fresh IP on its next
placement. Exhaustion is not an error: the task stays `Pending` until an
address frees up. 253 usable IPs per node; the allocator refuses past 256
nodes (a limit, not a target). `bigtop ps` shows the assigned IP.

**Agent tap provisioning.** Before the VMM boots, the agent creates a tap
named `bt-<8 hex chars of the task id>` (11 chars, inside the 15-char
Linux interface limit) via `iproute2`, brings it up, and — when
`--jailer --netns <path>` is set — moves it into that netns so the jailed
Firecracker can see it. Tap creation needs `CAP_NET_ADMIN` (or root) and
the `ip` binary; without them the task fails with a clear error instead
of booting dark. The tap is destroyed after the task's terminal state is
reported, and on every boot failure path — taps never linger.

**Firecracker wiring.** On fresh boots the agent PUTs
`/network-interfaces/eth0`
(`{"iface_id": "eth0", "guest_mac": "<mac>", "host_dev_name": "<tap>"}`)
right after the vsock device setup. The MAC is deterministic per task id
(locally-administered unicast, `02:xx:…`), stable across retries. Tap
name, MAC, and IP are also recorded in `bigtop-vm.json`. Snapshot boots
do **not** reconfigure networking: the restored VM keeps the snapshot's
device state (and its old IP — see limits).

**v0.3 guest contract.** The guest gets static networking on the kernel
cmdline, next to the existing `bigtop.*` parameters:

```text
ip=172.28.3.5::172.28.3.1:255.255.255.0::eth0:off bigtop.hostname=web-1
```

i.e. `ip=<addr>::<gateway>:<netmask>::eth0:off`, plus
`bigtop.hostname=<hostname>` when the task spec sets one. The guest init
parses `/proc/cmdline` and configures `eth0` itself; a minimal init
fragment:

```sh
# static networking from the BigTop cmdline
for kv in $(cat /proc/cmdline); do
  case "$kv" in
    ip=*)              IPCFG="${kv#ip=}" ;;
    bigtop.hostname=*) HOSTNAME="${kv#bigtop.hostname=}" ;;
  esac
done
ADDR="${IPCFG%%:*}"; REST="${IPCFG#*:*:}"; GW="${REST%%:*}"
ip link set eth0 up
ip addr add "$ADDR/24" dev eth0
ip route add default via "$GW" dev eth0
[ -n "${HOSTNAME:-}" ] && hostname "$HOSTNAME"
```

The guest sees its MAC on `eth0` automatically (virtio-net). DHCP is a
deliberate non-goal for v0.3: static assignment is deterministic, needs no
guest DHCP client, and keeps the orchestrator's IPAM the single source of
truth.

**Host plumbing.** The operator runs `scripts/setup-nat.sh` once per node
(as root): it enables IPv4 forwarding and installs an nftables masquerade
for the pod CIDR so guests can reach the outside world. The agent never
mutates host firewall rules itself — explicit operator action only.
Requirements recap: `/dev/kvm`, `CAP_NET_ADMIN` (or root) + `iproute2`
for the agent, nftables for NAT, and (for jailer mode) the v0.2 chroot
setup plus `/dev/net/tun` inside the jail.

**v0.3 limits (honest):** pod IPs are node-local — there is no cross-node
overlay yet, so a guest on node A cannot reach a guest on node B by pod
IP (that's the v0.4 sketch). A snapshot-restored task keeps the
snapshot's guest IP even though the scheduler assigns a new one; the
guest must tolerate that or re-read its cmdline on boot. No reference
guest init ships yet; DHCP is out of scope. Real tap creation, the real
`PUT /network-interfaces` round-trip, and packet flow are implemented
against the real Firecracker API and unit-tested but **unverified on real
KVM hardware** — this environment has no `/dev/kvm` or `CAP_NET_ADMIN`.

### ProcessRuntime (dev/CI stand-in)

Same lifecycle, but `tokio::process::Command` directly. Injects
`BIGTOP_TASK` and `BIGTOP_JOB` env vars. Used by the integration test and
the local demo.

### VXLAN overlay (v0.4)

One big network: every node's pod subnets stitched into a single L2
overlay with VXLAN.

**Agent flags.** `bigtop agent --vni 42 --underlay-ip 10.0.0.5` opts the
node into the mesh (both required together; `--vni` also requires the
firecracker runtime — the process runtime never touches host networking).
`--vni` defaults to nothing (overlay off); the conventional default when
enabling is VNI 42. The underlay IP is the address other nodes'
encapsulated packets arrive at: it must be reachable from every peer.

**Device model.** At startup the agent creates `vxlan<vni>` (`ip link add
<dev> type vxlan id <vni> dstport 4789`, ungrouped/unicast mode) with a
deterministic VTEP MAC derived from the node id
(`vtep_mac_for_node`: `02:xx:…` locally-administered unicast, the same
derivation as task MACs), creates the `bt-br0` bridge, brings both up,
and enslaves the VXLAN device to the bridge. Every task tap is then
enslaved to `bt-br0` as it is created (after `up`, before any jailer
netns move). Re-running setup is idempotent: already-existing devices
are left in place.

**Mesh.** Each agent polls
`GET /v1/agents/overlay-peers?node_id=<self>` and maintains one static
FDB entry per peer:
`bridge fdb append <peer-vtep-mac> dev vxlan<vni> dst <peer-underlay-ip>`.
VTEP MACs are pure functions of node ids, so no extra coordination is
needed. Reconciliation runs every 10 s, add-before-delete; a failed
delete is logged and retried next round. Broadcast/unknown-unicast
frames are flooded to every VTEP — no multicast underlay required.

**Limits (honest):** implemented against real iproute2 semantics and
unit-tested (exact argv, FDB diffing, VTEP MAC derivation), but
**unverified on real hardware** — this environment has no `CAP_NET_ADMIN`
or second host, so no VXLAN device, bridge, or FDB entry has ever been
created by this code, and no encapsulated packet has ever flown. With
`--jailer --netns`, the tap lives in the jail's netns and the operator
must bridge it there; the agent documents this and does not attempt it.

### Service discovery (v0.4)

Tasks opt in with `[task.service]`: `name` registers the task's pod IP
under a service name while the task is `Running`; `discover` lists the
service names the task wants to find.

The server derives the registry from task state — no separate store to
drift. `GET /v1/services` returns every service with its current
endpoints (`[{task_id, ip}]`, sorted), and `bigtop services` prints the
same table. A task contributes an endpoint only while `Running` with a
network assignment; terminal transitions and dead-node requeues remove
it.

At schedule time, the scheduler injects `BIGTOP_SERVICES` into the
task's environment: JSON mapping each discovered name to its current
sorted IP list, e.g. `{"api":["172.28.0.2"],"db":[]}`. Names with no
running endpoints map to `[]` — the task decides how to handle an empty
discovery result.

**Non-goal:** virtual IPs / load balancers. There is no VIP, no
kube-proxy-style DNAT, no health-checked endpoint selection — discovery
hands out the raw pod IPs and the client picks. That is the v0.5+
conversation, not this one.

### Metrics (v0.4)

`GET /metrics` renders Prometheus text format, only values the server
actually maintains:

- `bigtop_tasks{state="pending|assigned|running|succeeded|failed"}`
- `bigtop_nodes_up` — nodes with a recent heartbeat
- `bigtop_scheduler_tick_ms` — wall-clock duration of the last tick
- `bigtop_ipam_allocated` / `bigtop_ipam_total` — pod IP usage vs capacity
- `bigtop_snapshots_done` — snapshots that reached `Done`

Agent-side metrics are deferred.

### Persistence (v0.4)

The server remembers: `bigtop server --data-dir ./bigtop-data` journals
every state mutation to `<dir>/journal.jsonl` — one JSON object per
line, `fsync`ed before the mutation is acknowledged. On startup the
server replays the journal in order and resumes exactly where it left
off (tasks, nodes, IPAM, snapshots, and the derived service registry;
log tails are rebuilt empty — logs are not persisted).

The journal stores whole post-state records per mutation, not deltas.
A corrupt final line is treated as a torn write from a crash and
truncated with a warning; any other corrupt line aborts startup loudly.

On clean shutdown (Ctrl-C/SIGTERM) the server compacts: it writes
`<dir>/snapshot.json` (the full durable state, via temp-file + rename)
and truncates the journal, so the next boot replays at most the ops
since the last clean stop.

The data directory is protected by an exclusive lock
(`<dir>/bigtop.lock`, created with `create_new` and removed on drop):
a second server on the same directory is refused at startup.

### Status page (v0.4)

`GET /` serves a static, server-rendered HTML page — no JavaScript, no
framework: tables of nodes (liveness, underlay, resource use), services
with endpoints, tasks (state, node, pod IP, service), and IPAM usage,
plus a link to `/metrics`.

## Failure semantics

- Terminal states are final: `409 conflict` on any transition out of
  `Succeeded`/`Failed`.
- Dead node → its tasks requeue (see scheduler). No checkpointing in v0.1:
  a requeued task restarts from scratch.
- Agent crash: heartbeats stop → node dies after 10 s → tasks requeue.
- Server crash: with `--data-dir`, the journal replays on restart and
  the cluster resumes (in-flight tasks return to `Pending` via the
  dead-node path once their agents re-register — agents re-register as
  *new* node ids, so pre-crash placements are not trusted). Without
  `--data-dir`, all state is in memory and a crash loses everything
  (the v0.3 behavior).
- Double-start on one data directory: the second server is refused by
  the lock file.
- Logs: last 200 lines per task, in memory, not persisted.

## IDs

`job-<hex nanos>-<hex pid>-<hex seq>` (likewise `task-…`, `node-…`).
Unique per process; sortable; safe in env vars and shell.

## CLI

```
bigtop server [--port 4667] [--bind 127.0.0.1] [--network-cidr 172.28.0.0/16] [--data-dir DIR]
bigtop agent --server http://127.0.0.1:4667 [--name NAME] [--runtime auto] [--vm-dir DIR] [--firecracker-bin BIN] [--jailer] [--jailer-bin BIN] [--jailer-uid UID] [--jailer-gid GID] [--chroot-base-dir DIR] [--netns PATH] [--vni VNI] [--underlay-ip IP]
bigtop run <job.toml> [--server URL]
bigtop ps [--server URL]
bigtop nodes [--server URL]
bigtop services [--server URL]
bigtop logs <task-id> [--server URL]
bigtop snapshot create <task-id> [--kind full|diff] [--mem-path PATH] [--snap-path PATH] [--server URL]
bigtop snapshot list <task-id> [--server URL]
bigtop snapshot restore <task-id> <snapshot-id> [--server URL]
```
