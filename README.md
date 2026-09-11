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
- ⚠️ No reference guest init yet, no tap networking, no persistence
  (server state is in-memory).

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

## Roadmap

- **v0.2** — ✅ Done: snapshot/restore, vsock log streaming, jailer
  sandboxing. (Deferred: reference guest init, server persistence.)
- **v0.3** — Networking: tap devices + CNI-lite, per-task IPs, `--netns`
  wiring for jailer mode; reference guest init honoring `bigtop.cmd_b64`
  and the vsock log contract.
- **v0.4** — Service discovery + virtual IPs, web dashboard, server
  persistence.

## Profiling

See [PROFILING.md](PROFILING.md): criterion benches for the scheduler,
callgrind/DHAT scripts under `profiling/`, and the v0.1 performance
budget. Measure first, optimize hot paths only.

## Spec

[SPEC.md](SPEC.md) — API, scheduler policy, agent lifecycle, guest
contract, failure semantics.
