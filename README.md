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
- `bigtop` — the CLI: `server`, `agent`, `run`, `ps`, `nodes`, `logs`,
  `snapshot create|list|restore`.

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

## The v0.2 deal (honest)

- ✅ Job submit → schedule → run → logs, end to end (verified, incl. an
  integration test that runs a real 2-task job to `Succeeded`).
- ✅ `FirecrackerRuntime` implemented against the real Firecracker REST API
  (machine-config, boot-source, drives, InstanceStart over the Unix
  socket); config builders + HTTP plumbing unit-tested against a fake API.
- ✅ Snapshot/restore: `bigtop snapshot create|list|restore`, server
  records, agent `PUT /snapshot/create` + boot-from-snapshot via
  `PUT /snapshot/load`, `on_success` snapshot policy — API bodies
  unit-tested byte-for-byte against the Firecracker API.
- ✅ Vsock log streaming: length-prefixed frame codec, agent hub routing
  by task id, guest completion signal — tested over a loopback stand-in
  (real `AF_VSOCK` needs KVM; the socket code fails cleanly without it).
- ✅ Jailer sandboxing: `--jailer` mode builds the exact `firecracker-jailer`
  argv (unit-tested) and maps the jailed API socket back to the host.
- ⚠️ End-to-end microVM boot, real snapshot files, and real `AF_VSOCK`
  are **unverified**: they need `/dev/kvm` and a guest kernel/rootfs,
  which this dev environment doesn't have. The live demo runs on the
  process runtime.
- ⚠️ No reference guest init yet, no server persistence (state is
  in-memory), and no cross-node pod routing (v0.3 pod IPs are node-local;
  see SPEC.md "Networking (v0.3)" limits).

## The v0.4 deal (honest)

**One big network, and it remembers.**

- ✅ VXLAN cross-node overlay: `bigtop agent --vni 42 --underlay-ip
  10.0.0.5` builds a `vxlan42` device + `bt-br0` bridge, enslaves every
  task tap, and maintains static FDB entries for every other alive
  overlay node (VTEP MACs are deterministic per node id, no extra
  coordination; reconciled every 10 s). Opt-in — without the flags the
  agent touches no host networking.
- ✅ Service discovery: `[task.service] name = "api"` registers the
  task's pod IP while it runs; `discover = ["db"]` injects
  `BIGTOP_SERVICES` (`{"db":["172.28.0.2"],"cache":[]}`) at schedule
  time. `bigtop services` / `GET /v1/services` show the live registry,
  derived from task state — nothing to drift.
- ✅ Prometheus metrics at `GET /metrics`: task counts by state,
  `bigtop_nodes_up`, `bigtop_scheduler_tick_ms`, IPAM usage, and
  `bigtop_snapshots_done`. Agent-side metrics are deferred.
- ✅ Server persistence: `bigtop server --data-dir ./data` journals
  every mutation to `journal.jsonl` (fsync per op, before the mutation
  is acknowledged), replays on startup, and compacts to
  `snapshot.json` on clean shutdown (Ctrl-C/SIGTERM). A second server
  on the same directory is refused by the lock file.
- ✅ Status page: `GET /` serves static, server-rendered HTML — nodes,
  services, tasks, IPAM, link to `/metrics`. No JS.
- ⚠️ The overlay, FDB reconciliation, and tap bridging are implemented
  against real iproute2 semantics and unit-tested (exact argv, FDB
  diffing), but **unverified on real hardware**: no `CAP_NET_ADMIN` or
  second host here, so no VXLAN device has ever been created by this
  code and no encapsulated packet has ever flown. With
  `--jailer --netns` the tap lives in the jail's netns and bridging it
  there is the operator's job (the agent says so in the docs, and does
  not attempt it).
- ⚠️ Virtual IPs / load balancers are an explicit non-goal: discovery
  hands out raw pod IPs and the client picks. That's the v0.5
  conversation.

## Jailer host setup

`bigtop agent --jailer` needs a cooperating host. The jailer creates
`<chroot-base>/<id>/root/` and chroots there, so everything the VMM
touches must exist *inside* the jail:

```bash
# 1. Build firecracker with jailer support and install both binaries.
# 2. Pick a chroot base dir (default /srv/jailer) and a jail user:
sudo useradd -r -s /usr/sbin/nologin jailer-bt   # uid/gid for --jailer-uid/--jailer-gid
sudo mkdir -p /srv/jailer && sudo chown root:root /srv/jailer && sudo chmod 755 /srv/jailer

# 3. Per task id <id> (the agent uses the task id as the jail id), stage
#    what the jail needs under /srv/jailer/<id>/root/:
sudo mkdir -p /srv/jailer/<id>/root/dev
sudo mknod -m 666 /srv/jailer/<id>/root/dev/kvm c 10 232
sudo mknod -m 666 /srv/jailer/<id>/root/dev/net/tun c 10 200   # only for v0.3 networking
sudo cp /path/to/vmlinux /path/to/rootfs.ext4 /srv/jailer/<id>/root/
sudo chown -R 1234:1234 /srv/jailer/<id>/root   # match --jailer-uid/--jailer-gid

# 4. Run the agent:
bigtop agent --jailer --jailer-uid 1234 --jailer-gid 1234 \
  --chroot-base-dir /srv/jailer [--netns /var/run/netns/bt0]
```

The jailer needs `CAP_SYS_ADMIN` (for chroot/mount) — run the agent as
root or give the jailer binary the right capabilities. The guest kernel
needs vsock support (`CONFIG_VIRTIO_VSOCK`) for log streaming; in jailer
mode the serial console is unavailable, so guests **must** log over
vsock (CID 2, port 4668 — see SPEC.md). The vsock backing socket lives at
the jailed `/vsock.sock` (no host setup needed — Firecracker creates it);
the agent listens for guest logs at the host path
`<chroot-base>/<id>/root/vsock.sock_4668`.

## Networking host setup (v0.3)

Three things, all explicit operator actions — the agent never touches
host firewall rules on its own:

```bash
# 1. One-time NAT per node (as root): IPv4 forwarding + nftables
#    masquerade for the pod CIDR, so guests can reach the outside world.
sudo ./scripts/setup-nat.sh
#    Override the CIDR with BIGTOP_POD_CIDR=10.42.0.0/16 (must match
#    `bigtop server --network-cidr`).

# 2. The agent needs CAP_NET_ADMIN (or root) plus iproute2 to create tap
#    devices. Without them, network-enabled tasks fail with a clear
#    error instead of booting dark:
sudo setcap cap_net_admin+ep $(which bigtop)   # or just run the agent as root

# 3. For jailer mode, /dev/net/tun must exist inside the jail
#    (already in the v0.2 chroot checklist above).
```

`bigtop server --network-cidr 172.28.0.0/16` (the default) carves the
/16 into /24s, one per node; the scheduler hands each network-enabled
task one static IP (`.2`–`.254`, `.1` is the gateway). Enable it per
task in the job TOML — see `examples/network.toml`:

```toml
[task.network]
enabled = true
hostname = "web-1"
```

The guest configures `eth0` from the kernel cmdline
(`ip=<addr>::<gateway>:<netmask>::eth0:off`); the full guest-side
contract, including a minimal init snippet, is in SPEC.md
"Networking (v0.3)". DHCP is a deliberate non-goal: static assignment
keeps the server's IPAM the single source of truth.

## Overlay host setup (v0.4)

Opt-in per agent. Without `--vni` the agent creates no VXLAN device, no
bridge, and no FDB entries — v0.3 behavior is unchanged.

```bash
# Needs CAP_NET_ADMIN (or root) + iproute2, same as v0.3 tap provisioning:
sudo setcap cap_net_admin+ep $(which bigtop)   # or run the agent as root

# Join the mesh (VNI 42 is the convention; any u32 works):
bigtop agent --server http://10.0.0.1:4667 --name node-2 \
  --vni 42 --underlay-ip 10.0.0.5
```

`--vni` requires `--underlay-ip` (the address other nodes' encapsulated
packets arrive at — it must be reachable from every peer) and the
firecracker runtime (the process runtime never touches host
networking). At startup the agent creates `vxlan<vni>` (destination
port 4789) with a deterministic VTEP MAC, creates `bt-br0`, brings both
up, and enslaves the VXLAN device to the bridge; task taps join
`bt-br0` as they are created. Setup is idempotent across agent
restarts. Each agent polls `GET /v1/agents/overlay-peers` and keeps one
static FDB entry per peer — broadcast/unknown-unicast is flooded, so no
multicast underlay is needed.

Jailer + netns: the tap is moved into the jail's netns and **not**
bridged there — wire it into the overlay inside the jail yourself (the
agent logs what it skipped).

## Persistence (v0.4)

```bash
bigtop server --port 4667 --data-dir ./bigtop-data
```

Every mutation is appended to `journal.jsonl` and fsynced before it is
acknowledged — expect a small latency cost per mutation in exchange for
crash safety (the journal stores whole records, not deltas, so it also
grows: watch disk on write-heavy clusters). On startup the server loads
`snapshot.json` if present, then replays the journal; a torn final line
is truncated with a warning, any other corruption aborts startup
loudly. On clean shutdown the server writes a fresh `snapshot.json`
(temp file + rename) and truncates the journal. `bigtop.lock` in the
directory refuses a second server — a `kill -9` leaves the stale lock
behind on purpose (fail closed); remove it only after confirming no
server is running.

## Roadmap

- **v0.2** — ✅ Done: snapshot/restore, vsock log streaming, jailer
  sandboxing. (Deferred: reference guest init, server persistence.)
- **v0.3** — ✅ Done: networking — tap provisioning per task,
  `PUT /network-interfaces` wiring, server IPAM (`/16` → `/24` per node,
  one static IP per task), kernel-cmdline guest contract, `setup-nat.sh`
  host plumbing. (Deferred: reference guest init, server persistence,
  cross-node pod routing.)
- **v0.4** — ✅ Done: one big network, and it remembers — VXLAN
  cross-node overlay (opt-in, VNI 42), service discovery over pod IPs
  (`BIGTOP_SERVICES`, `bigtop services`), Prometheus metrics, durable
  server persistence (JSONL journal + fsync + snapshots), and a tiny
  server-rendered status page. (Deferred: virtual IPs / load balancers.)
- **v0.5** — Dial by name: service virtual IPs with load-balanced
  endpoints and cluster DNS, finishing the service-networking story
  v0.4 started.

## Profiling

See [PROFILING.md](PROFILING.md): criterion benches for the scheduler,
callgrind/DHAT scripts under `profiling/`, and the v0.1 performance
budget. Measure first, optimize hot paths only.

## Spec

[SPEC.md](SPEC.md) — API, scheduler policy, agent lifecycle, guest
contract, failure semantics.
