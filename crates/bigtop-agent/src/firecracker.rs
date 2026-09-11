//! `FirecrackerRuntime`: one microVM per task.
//!
//! This talks to the real Firecracker `REST` API over a Unix socket:
//! fresh boots do `PUT /machine-config`, `PUT /boot-source`,
//! `PUT /drives/rootfs`, `PUT /vsock`, then `PUT /actions` with
//! `InstanceStart`; snapshot boots do `PUT /snapshot/load` with
//! `resume_vm`.
//!
//! The guest's serial console arrives on the `firecracker` process's stdout
//! and is streamed as task logs. Fresh boots also get a virtio-vsock
//! device: the guest dials `(CID 2, 4668)` over `AF_VSOCK` and Firecracker
//! bridges it into the agent's per-task `AF_UNIX` listener, which feeds
//! the same log pipeline with lower overhead than the serial console.
//! With `--jailer`, the VMM boots inside `firecracker-jailer` instead, and
//! the serial console is unavailable: guests must log over vsock there.
//!
//! Before booting, the agent writes `bigtop-vm.json` into the task's VM dir:
//! a read-only record of exactly what the `REST` calls configure, for
//! inspection and replay. The API stays the source of truth.
//!
//! End-to-end microVM boot needs `/dev/kvm` and is unverified in CI-like
//! environments; the config builders below are unit-tested, including
//! against a fake API socket.

use crate::jailer::{JailerConfig, JailerOptions, JAILED_API_SOCK, JAILED_LOG_PATH};
use crate::runtime::{RunningTask, Runtime};
use crate::snapshot::{resolve_snapshot_paths, SnapshotManager};
use crate::tap::TapDevice;
use crate::vsock::{serve_vsock_logs, VsockLogHub, VSOCK_LOG_PORT};
use crate::AgentError;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bigtop_core::{
    mac_for_task, tap_name_for, MacAddr, SnapshotId, SnapshotLoadSpec, SnapshotSpec, Task, TaskId,
};
use serde_json::json;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::oneshot;

/// Configuration for [`FirecrackerRuntime`].
#[derive(Debug, Clone)]
pub struct FirecrackerConfig {
    /// The `firecracker` binary (a bare name resolves via `PATH`).
    pub bin: PathBuf,
    /// Directory holding one subdir per VM (`<vm_dir>/<task-id>/`).
    pub vm_dir: PathBuf,
    /// How long to wait for the API socket after spawning the VMM.
    pub boot_timeout: Duration,
    /// Sandbox every microVM in `firecracker-jailer`. `None` spawns
    /// firecracker directly.
    pub jailer: Option<JailerOptions>,
}

impl Default for FirecrackerConfig {
    fn default() -> Self {
        Self {
            bin: PathBuf::from("firecracker"),
            vm_dir: PathBuf::from("/tmp/bigtop-vms"),
            boot_timeout: Duration::from_secs(10),
            jailer: None,
        }
    }
}

/// Boots one Firecracker microVM per task.
#[derive(Debug, Clone)]
pub struct FirecrackerRuntime {
    config: FirecrackerConfig,
    /// When set, fresh boots get a virtio-vsock device and the agent serves
    /// the guest's log connections into this hub. `None` means serial
    /// console only.
    vsock_hub: Option<VsockLogHub>,
}

/// virtio-vsock backing socket, as the *firecracker process* sees it.
/// In jailer mode this is inside the chroot; the agent maps it back to the
/// host when binding the listener.
const JAILED_VSOCK_SOCK: &str = "/vsock.sock";

/// How a microVM boots: fresh (kernel + rootfs) or from a snapshot.
#[derive(Debug, Clone, Copy)]
pub enum BootKind<'a> {
    /// Kernel + rootfs, configured via `PUT /machine-config` and friends.
    Fresh,
    /// `PUT /snapshot/load` with `resume_vm`; images are not needed.
    Snapshot(&'a SnapshotLoadSpec),
}

impl FirecrackerRuntime {
    /// Build a runtime from `config`.
    #[must_use]
    pub const fn new(config: FirecrackerConfig) -> Self {
        Self {
            config,
            vsock_hub: None,
        }
    }

    /// Serve guest vsock log connections into `hub` on fresh boots.
    #[must_use]
    pub fn with_vsock_hub(mut self, hub: VsockLogHub) -> Self {
        self.vsock_hub = Some(hub);
        self
    }

    /// Unix socket path for a task's VMM API.
    #[must_use]
    pub fn socket_path(&self, task_id: &TaskId) -> PathBuf {
        self.config.vm_dir.join(task_id.to_string()).join("fc.sock")
    }

    /// Task VM working directory (`<vm_dir>/<task-id>/`): socket (direct
    /// mode), `bigtop-vm.json`, and default snapshot files.
    #[must_use]
    pub fn vm_task_dir(&self, task_id: &TaskId) -> PathBuf {
        self.config.vm_dir.join(task_id.to_string())
    }

    /// Base VM directory from the config.
    #[must_use]
    pub fn vm_dir(&self) -> &Path {
        &self.config.vm_dir
    }

    /// API socket path the *agent* dials. In jailer mode the socket lives
    /// inside the jail, so this maps the jailed path back to the host.
    #[must_use]
    pub fn api_socket_for(&self, task_id: &TaskId) -> PathBuf {
        self.config.jailer.as_ref().map_or_else(
            || self.socket_path(task_id),
            |opts| {
                JailerConfig::new(task_id.to_string(), opts, self.config.bin.clone())
                    .host_path(Path::new(JAILED_API_SOCK))
            },
        )
    }

    /// Host path of the vsock backing socket for `task_id`. Firecracker
    /// itself binds `uds_path`; the agent binds the per-port listener at
    /// [`Self::vsock_listen_path`].
    fn vsock_backing_host_path(&self, task_id: &TaskId) -> PathBuf {
        self.config.jailer.as_ref().map_or_else(
            || self.vm_task_dir(task_id).join("vsock.sock"),
            |opts| {
                JailerConfig::new(task_id.to_string(), opts, self.config.bin.clone())
                    .host_path(Path::new(JAILED_VSOCK_SOCK))
            },
        )
    }

    /// vsock backing socket as the *firecracker process* sees it: the
    /// `uds_path` sent in `PUT /vsock`.
    fn vsock_device_path(&self, task_id: &TaskId) -> PathBuf {
        if self.config.jailer.is_some() {
            PathBuf::from(JAILED_VSOCK_SOCK)
        } else {
            self.vsock_backing_host_path(task_id)
        }
    }

    /// Agent-side listener path: Firecracker pairs guest connections to
    /// `(CID 2, VSOCK_LOG_PORT)` with the `AF_UNIX` listener at
    /// `<uds_path>_<port>`. Always a host path, even in jailer mode.
    fn vsock_listen_path(&self, task_id: &TaskId) -> PathBuf {
        let mut path = self.vsock_backing_host_path(task_id).into_os_string();
        path.push(format!("_{VSOCK_LOG_PORT}"));
        PathBuf::from(path)
    }

    /// Create and bring up the host TAP device for a networked task.
    ///
    /// Returns `None` when the task's `[network]` is disabled. In jailer
    /// mode the TAP moves into the jail's netns so the VMM can attach it.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Network`] when networking is enabled but the
    /// server assigned no IP, or when any `ip` call fails.
    async fn setup_tap(&self, task: &Task) -> Result<Option<TapDevice>, AgentError> {
        if !task.spec.network.enabled {
            return Ok(None);
        }
        if task.network.is_none() {
            return Err(AgentError::Network(
                "task has networking enabled but the server assigned no IP".to_string(),
            ));
        }
        let tap = TapDevice::create(&tap_name_for(&task.id)).await?;
        tap.set_up().await?;
        if let Some(netns) = self
            .config
            .jailer
            .as_ref()
            .and_then(|opts| opts.netns.as_ref())
        {
            tap.move_to_netns(netns).await?;
        }
        Ok(Some(tap))
    }

    /// Take a snapshot of `task_id`'s running microVM per `spec`.
    ///
    /// Resolves default paths, creates the snapshot directory, and calls
    /// `PUT /snapshot/create`. Returns the resolved `(mem_file_path,
    /// snapshot_path)`.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the API socket is unreachable or
    /// Firecracker rejects the call.
    pub async fn take_snapshot(
        &self,
        task_id: &TaskId,
        snapshot_id: &SnapshotId,
        spec: &SnapshotSpec,
    ) -> Result<(PathBuf, PathBuf), AgentError> {
        let (mem, snap) = resolve_snapshot_paths(spec, &self.config.vm_dir, task_id, snapshot_id);
        if let Some(parent) = mem.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(AgentError::Io)?;
        }
        let resolved = SnapshotSpec {
            snapshot_type: spec.snapshot_type,
            mem_file_path: mem.to_string_lossy().into_owned(),
            snapshot_path: snap.to_string_lossy().into_owned(),
        };
        let sock = self.api_socket_for(task_id);
        SnapshotManager::create(&sock, &resolved).await?;
        Ok((mem, snap))
    }

    /// Configure a freshly spawned VMM, then start the instance.
    /// `vcpu_count`, `mem_mb`, and `boot_args` are the values recorded in
    /// `bigtop-vm.json`, so the API calls can never drift from the record.
    /// The caller waits for the API socket first. When the runtime serves
    /// vsock logs, the virtio-vsock device is configured before the
    /// instance starts; the agent's per-task listener is already bound by
    /// then (see [`FirecrackerRuntime::spawn`]).
    async fn configure(
        &self,
        sock: &Path,
        task: &Task,
        vcpu_count: u32,
        mem_mb: u64,
        boot_args: &str,
    ) -> Result<(), AgentError> {
        let vm = &task.spec.vm;
        fc_put(
            sock,
            "/machine-config",
            &machine_config_body(vcpu_count, mem_mb),
        )
        .await?;
        fc_put(
            sock,
            "/boot-source",
            &boot_source_body(&vm.kernel_image, boot_args),
        )
        .await?;
        fc_put(sock, "/drives/rootfs", &drive_body(&vm.rootfs)).await?;
        if self.vsock_hub.is_some() {
            fc_put(
                sock,
                "/vsock",
                &vsock_device_body(guest_cid_for(&task.id), &self.vsock_device_path(&task.id)),
            )
            .await?;
        }
        if task.spec.network.enabled {
            // The interface body carries no addresses, but a task with
            // networking enabled and no server assignment is a server bug:
            // fail here, before the guest boots with no IP to configure.
            let Some(_assign) = task.network else {
                return Err(AgentError::Network(
                    "task has networking enabled but the server assigned no IP".to_string(),
                ));
            };
            fc_put(
                sock,
                "/network-interfaces/eth0",
                &network_interface_body("eth0", &mac_for_task(&task.id), &tap_name_for(&task.id)),
            )
            .await?;
        }
        fc_put(sock, "/actions", &instance_start_body()).await?;
        Ok(())
    }

    /// Spawn the VMM process: `firecracker` directly, or `jailer` when the    /// config enables it. Returns the child plus the jailer argv when used
    /// (`None` for direct spawns).
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Spawn`] when the process cannot be started.
    fn spawn_vmm(
        &self,
        task: &Task,
        api_sock: &Path,
    ) -> Result<(tokio::process::Child, Option<Vec<String>>), AgentError> {
        if let Some(opts) = &self.config.jailer {
            let jailer = JailerConfig::new(task.id.to_string(), opts, self.config.bin.clone());
            let fc_args = vec![
                "--api-sock".to_string(),
                JAILED_API_SOCK.to_string(),
                "--log-path".to_string(),
                JAILED_LOG_PATH.to_string(),
                "--id".to_string(),
                task.id.to_string(),
            ];
            let argv = jailer.argv(&fc_args);
            let child = Command::new(&argv[0])
                .args(&argv[1..])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .map_err(AgentError::Spawn)?;
            return Ok((child, Some(argv)));
        }
        let vm_task_dir = self.vm_task_dir(&task.id);
        let child = Command::new(&self.config.bin)
            .arg("--api-sock")
            .arg(api_sock)
            .arg("--log-path")
            .arg(vm_task_dir.join("firecracker.log"))
            .arg("--id")
            .arg(task.id.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(AgentError::Spawn)?;
        Ok((child, None))
    }

    /// Bind this task's vsock log listener and serve guest connections into
    /// the hub. Returns the stop signal for [`RunningTask::vsock_stop`]
    /// (`None` when the runtime does not serve vsock logs).
    ///
    /// # Errors
    ///
    /// Returns [`AgentError::Io`] when the listener socket cannot be bound.
    async fn start_vsock_server(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<oneshot::Sender<()>>, AgentError> {
        let Some(hub) = self.vsock_hub.clone() else {
            return Ok(None);
        };
        let listen_path = self.vsock_listen_path(task_id);
        if tokio::fs::try_exists(&listen_path)
            .await
            .map_err(AgentError::Io)?
        {
            tokio::fs::remove_file(&listen_path)
                .await
                .map_err(AgentError::Io)?;
        }
        let listener = UnixListener::bind(&listen_path).map_err(AgentError::Io)?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::select! {
                result = serve_vsock_logs(hub, listener) => {
                    if let Err(e) = result {
                        eprintln!("bigtop agent: vsock log server failed: {e}");
                    }
                }
                _ = &mut stop_rx => {}
            }
            let _ = tokio::fs::remove_file(&listen_path).await;
        });
        Ok(Some(stop_tx))
    }
}

impl Runtime for FirecrackerRuntime {
    async fn spawn(&self, task: &Task) -> Result<RunningTask, AgentError> {
        let vm = &task.spec.vm;
        let boot = vm
            .boot_snapshot
            .as_ref()
            .map_or(BootKind::Fresh, BootKind::Snapshot);
        let jailed = self.config.jailer.is_some();
        if !jailed {
            // Fresh boots need images; snapshot boots carry their own block
            // devices. In jailer mode the image paths are interpreted inside
            // the jail, so the host-side check is skipped: the operator must
            // make them visible there (see README "Jailer host setup").
            if vm.kernel_image.trim().is_empty() {
                return Err(AgentError::ImageMissing(
                    "vm.kernel_image is empty".to_string(),
                ));
            }
            if vm.rootfs.trim().is_empty() {
                return Err(AgentError::ImageMissing("vm.rootfs is empty".to_string()));
            }
            for (label, path) in [
                ("kernel_image", vm.kernel_image.as_str()),
                ("rootfs", vm.rootfs.as_str()),
            ] {
                if !tokio::fs::try_exists(path).await.map_err(AgentError::Io)? {
                    return Err(AgentError::ImageMissing(format!(
                        "vm.{label} not found: {path}"
                    )));
                }
            }
        }
        let vm_task_dir = self.vm_task_dir(&task.id);
        tokio::fs::create_dir_all(&vm_task_dir)
            .await
            .map_err(AgentError::Io)?;
        let api_sock = self.api_socket_for(&task.id);
        if !jailed
            && tokio::fs::try_exists(&api_sock)
                .await
                .map_err(AgentError::Io)?
        {
            tokio::fs::remove_file(&api_sock)
                .await
                .map_err(AgentError::Io)?;
        }
        // Guest networking: create the host TAP before the VMM boots so the
        // `PUT /network-interfaces/eth0` in `configure` has a device to
        // attach. `execute_task` destroys the TAP after the terminal state;
        // any boot failure below destroys it too, so no path leaks a tap.
        let tap = self.setup_tap(task).await?;
        // Render the exact values the REST calls will use, and record them
        // in bigtop-vm.json before the VMM boots.
        let vcpu_count = vcpu_for(task.spec.resources.cpu_millis, vm.vcpu_count);
        let mem_mb = vm.mem_mb.max(64);
        let args = boot_args(vm.boot_args.as_deref(), task);
        let launched = async {
            let (mut child, jailer_argv) = self.spawn_vmm(task, &api_sock)?;
            let record = serde_json::to_string_pretty(&launch_record(
                task,
                boot,
                vcpu_count,
                mem_mb,
                &args,
                &api_sock,
                jailer_argv.as_deref(),
            ))
            .map_err(AgentError::Json)?;
            tokio::fs::write(vm_task_dir.join("bigtop-vm.json"), record)
                .await
                .map_err(AgentError::Io)?;
            wait_for_socket(&api_sock, self.config.boot_timeout).await?;
            // Bind the per-task vsock listener before the guest can dial: the
            // guest connects to (CID 2, VSOCK_LOG_PORT) and Firecracker pairs it
            // with the AF_UNIX socket at <uds_path>_<port>.
            let vsock_stop = self.start_vsock_server(&task.id).await?;
            let booted = match &boot {
                BootKind::Fresh => {
                    self.configure(&api_sock, task, vcpu_count, mem_mb, &args)
                        .await
                }
                BootKind::Snapshot(load) => SnapshotManager::load(&api_sock, load).await,
            };
            if let Err(e) = booted {
                let _ = child.kill().await;
                return Err(e);
            }
            Ok::<_, AgentError>((child, vsock_stop))
        }
        .await;
        let (child, vsock_stop) = match launched {
            Ok(ok) => ok,
            Err(e) => {
                if let Some(tap) = tap {
                    if let Err(te) = tap.destroy().await {
                        eprintln!("bigtop agent: tap destroy failed: {te}");
                    }
                }
                return Err(e);
            }
        };
        Ok(RunningTask {
            task_id: task.id.clone(),
            child,
            vm_dir: Some(vm_task_dir),
            vsock_stop,
            tap,
        })
    }
}

/// vCPUs for the microVM: the spec's ask, or `cpu_millis` rounded up to whole
/// vCPUs, whichever is larger. Always at least 1.
#[must_use]
pub fn vcpu_for(cpu_millis: u64, vm_vcpu: u32) -> u32 {
    let from_millis = u32::try_from(cpu_millis.saturating_add(999) / 1000).unwrap_or(u32::MAX);
    from_millis.max(vm_vcpu).max(1)
}

/// `PUT /machine-config` body.
#[must_use]
pub fn machine_config_body(vcpu_count: u32, mem_mb: u64) -> serde_json::Value {
    json!({
        "vcpu_count": vcpu_count,
        "mem_size_mib": mem_mb,
        "smt": false,
    })
}

/// `PUT /boot-source` body.
#[must_use]
pub fn boot_source_body(kernel_image: &str, boot_args: &str) -> serde_json::Value {
    json!({
        "kernel_image_path": kernel_image,
        "boot_args": boot_args,
    })
}

/// `PUT /drives/rootfs` body.
#[must_use]
pub fn drive_body(rootfs: &str) -> serde_json::Value {
    json!({
        "drive_id": "rootfs",
        "path_on_host": rootfs,
        "is_root_device": true,
        "is_read_only": false,
    })
}

/// `PUT /actions` body that starts the instance.
#[must_use]
pub fn instance_start_body() -> serde_json::Value {
    json!({ "action_type": "InstanceStart" })
}

/// `PUT /vsock` body: give the guest `guest_cid` and back its virtio-vsock
/// device with the `AF_UNIX` socket at `uds_path`. Firecracker binds
/// `uds_path` itself; the agent listens for guest log connections at
/// `<uds_path>_<port>`.
#[must_use]
pub fn vsock_device_body(guest_cid: u32, uds_path: &Path) -> serde_json::Value {
    json!({ "guest_cid": guest_cid, "uds_path": uds_path })
}

/// `PUT /network-interfaces/<iface_id>` body: attach the host TAP device
/// as the guest's network interface, with a deterministic MAC.
///
/// Takes the MAC by reference per the v0.3 agent/core contract (the
/// sibling's `MacAddr` is `Copy`, so this is stylistic, not a cost).
#[must_use]
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn network_interface_body(
    iface_id: &str,
    guest_mac: &MacAddr,
    host_dev_name: &str,
) -> serde_json::Value {
    json!({
        "iface_id": iface_id,
        "guest_mac": guest_mac.to_string(),
        "host_dev_name": host_dev_name,
    })
}

/// Static guest IP configuration for the kernel cmdline, in the kernel's
/// `ip=` syntax: `ip=<client-ip>:<server-ip>:<gw-ip>:<netmask>:<hostname>:
/// <device>:<autoconf>`. The guest init parses this (plus
/// `bigtop.hostname=`) to configure `eth0`.
#[must_use]
pub fn network_boot_args(
    ip: Ipv4Addr,
    gateway: Ipv4Addr,
    netmask: Ipv4Addr,
    hostname: Option<&str>,
) -> String {
    let base = format!("ip={ip}::{gateway}:{netmask}::eth0:off");
    match hostname {
        Some(name) => format!("{base} bigtop.hostname={name}"),
        None => base,
    }
}

/// Deterministic guest CID for `task_id`, in `3..=u32::MAX - 1`
/// (`0`/`1`/`2` and `u32::MAX` are reserved). Derived from the task id so
/// a retried task keeps its CID across agent restarts.
#[must_use]
pub fn guest_cid_for(task_id: &TaskId) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    task_id.to_string().hash(&mut hasher);
    let digest = hasher.finish();
    u32::try_from(3 + digest % u64::from(u32::MAX - 3)).unwrap_or(3)
}

/// The `bigtop-vm.json` record: everything the agent configures for one
/// task's microVM, written before the VMM boots. It mirrors the `REST` API
/// bodies so an operator can inspect or replay a task's VM from the file
/// alone; the API stays the source of truth.
#[must_use]
pub fn launch_record(
    task: &Task,
    boot: BootKind<'_>,
    vcpu_count: u32,
    mem_mb: u64,
    boot_args: &str,
    api_sock: &Path,
    jailer_argv: Option<&[String]>,
) -> serde_json::Value {
    let mut record = json!({
        "task_id": task.id.to_string(),
        "job_id": task.job_id.to_string(),
        "created_at": chrono::Utc::now().to_rfc3339(),
        "vcpu_count": vcpu_count,
        "mem_mb": mem_mb,
        "kernel_image": task.spec.vm.kernel_image,
        "rootfs": task.spec.vm.rootfs,
        "boot_args": boot_args,
        "command": task.spec.command,
        "args": task.spec.args,
        "env": task.spec.env,
        "api_socket": api_sock.to_string_lossy(),
    });
    match boot {
        BootKind::Fresh => {
            record["boot"] = json!("fresh");
        }
        BootKind::Snapshot(load) => {
            record["boot"] = json!("snapshot");
            record["snapshot_path"] = json!(load.snapshot_path);
            record["mem_file_path"] = json!(load.mem_file_path);
        }
    }
    if let Some(argv) = jailer_argv {
        record["jailer_argv"] = json!(argv);
    }
    if task.spec.network.enabled {
        if let Some(assign) = &task.network {
            record["network"] = json!({
                "iface_id": "eth0",
                "guest_mac": mac_for_task(&task.id).to_string(),
                "tap_name": tap_name_for(&task.id),
                "ip": assign.ip.to_string(),
                "gateway": assign.gateway.to_string(),
            });
        }
    }
    record
}

/// Join command + args + env into one shell line, single-quote escaped.
#[must_use]
pub fn shell_join(command: &str, args: &[String], env: &HashMap<String, String>) -> String {
    let mut pairs: Vec<(&String, &String)> = env.iter().collect();
    pairs.sort();
    let mut parts: Vec<String> = Vec::with_capacity(pairs.len() + args.len() + 1);
    for (key, val) in pairs {
        parts.push(format!("{key}={}", shell_quote(val)));
    }
    parts.push(shell_quote(command));
    for arg in args {
        parts.push(shell_quote(arg));
    }
    parts.join(" ")
}

/// Single-quote a shell word.
fn shell_quote(word: &str) -> String {
    format!("'{}'", word.replace('\'', "'\\''"))
}

/// Kernel cmdline for the guest: base args plus `BigTop` task parameters.
///
/// v0.1 guest contract: the guest init reads `/proc/cmdline`, decodes
/// `bigtop.cmd_b64` (base64 of the shell line from [`shell_join`]), and
/// execs it. `bigtop.task` carries the task id for the guest's own logging.
///
/// When the task's `[network]` is enabled and the server assigned an IP,
/// the static guest configuration from [`network_boot_args`] is appended
/// so the guest init can bring up `eth0`.
#[must_use]
pub fn boot_args(base: Option<&str>, task: &Task) -> String {
    let cmd_b64 = STANDARD.encode(shell_join(
        &task.spec.command,
        &task.spec.args,
        &task.spec.env,
    ));
    let base_args = base
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("console=ttyS0 reboot=k panic=1 pci=off");
    let mut args = format!(
        "{base_args} bigtop.task={} bigtop.cmd_b64={cmd_b64}",
        task.id
    );
    if task.spec.network.enabled {
        if let Some(assign) = &task.network {
            args.push(' ');
            args.push_str(&network_boot_args(
                assign.ip,
                assign.gateway,
                assign.netmask,
                task.spec.network.hostname.as_deref(),
            ));
        }
    }
    args
}

/// Wait until the VMM's API socket appears.
pub async fn wait_for_socket(sock: &Path, timeout: Duration) -> Result<(), AgentError> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::fs::try_exists(sock).await.map_err(AgentError::Io)? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(AgentError::Timeout(format!(
        "firecracker API socket never appeared: {}",
        sock.display()
    )))
}

/// `PUT` a JSON body to the Firecracker API over its Unix socket.
/// Success is any 2xx status.
pub async fn fc_put(sock: &Path, path: &str, body: &serde_json::Value) -> Result<(), AgentError> {
    let body_str = serde_json::to_string(body).map_err(AgentError::Json)?;
    let request = format!(
        "PUT {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body_str}",
        body_str.len()
    );
    let mut stream = tokio::net::UnixStream::connect(sock)
        .await
        .map_err(AgentError::Io)?;
    {
        use tokio::io::AsyncWriteExt;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(AgentError::Io)?;
        stream.shutdown().await.map_err(AgentError::Io)?;
    }
    let mut response = Vec::new();
    {
        use tokio::io::AsyncReadExt;
        stream
            .read_to_end(&mut response)
            .await
            .map_err(AgentError::Io)?;
    }
    let (status, message) = parse_status(&response);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(AgentError::FirecrackerApi { status, message })
    }
}

/// Parse the status line of a raw HTTP response: `(code, status_line)`.
fn parse_status(response: &[u8]) -> (u16, String) {
    let head_end = response
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(response.len());
    let head = String::from_utf8_lossy(&response[..head_end]);
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (code, head.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigtop_core::{
        JobId, NetworkAssignment, NetworkSpec, Resources, SnapshotLoadSpec, SnapshotPolicy,
        TaskSpec, TaskState, VmSpec,
    };
    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    fn test_task() -> Task {
        Task {
            id: TaskId::from("task-abc".to_string()),
            job_id: JobId::from("job-1".to_string()),
            name: "t".to_string(),
            spec: TaskSpec {
                name: "t".to_string(),
                command: "echo".to_string(),
                args: vec!["hi".to_string()],
                env: HashMap::new(),
                resources: Resources {
                    cpu_millis: 1500,
                    mem_mb: 64,
                },
                count: 1,
                vm: VmSpec {
                    kernel_image: "/boot/vmlinux".to_string(),
                    rootfs: "/images/rootfs.ext4".to_string(),
                    vcpu_count: 1,
                    mem_mb: 128,
                    boot_args: None,
                    boot_snapshot: None,
                },
                node_affinity: None,
                snapshot_policy: SnapshotPolicy::None,
                network: NetworkSpec {
                    enabled: false,
                    hostname: None,
                },
            },
            state: TaskState::Pending,
            assigned_node: None,
            exit_code: None,
            network: None,
        }
    }

    /// A task with `[network]` enabled and a server-assigned IP.
    fn networked_task() -> Task {
        let mut task = test_task();
        task.spec.network.enabled = true;
        task.spec.network.hostname = Some("web-1".to_string());
        task.network = Some(NetworkAssignment {
            ip: Ipv4Addr::new(10, 0, 0, 2),
            gateway: Ipv4Addr::new(10, 0, 0, 1),
            netmask: Ipv4Addr::new(255, 255, 255, 0),
        });
        task
    }

    #[test]
    fn vcpu_rounds_up_millis() {
        assert_eq!(vcpu_for(1, 1), 1);
        assert_eq!(vcpu_for(1000, 1), 1);
        assert_eq!(vcpu_for(1001, 1), 2);
        assert_eq!(vcpu_for(1500, 1), 2);
        assert_eq!(vcpu_for(500, 4), 4);
        assert_eq!(vcpu_for(0, 0), 1);
    }

    #[test]
    fn vsock_device_body_shape() {
        let body = vsock_device_body(7, Path::new("/tmp/bigtop-vms/task-1/vsock.sock"));
        assert_eq!(body["guest_cid"], 7);
        assert_eq!(body["uds_path"], "/tmp/bigtop-vms/task-1/vsock.sock");
    }

    #[test]
    fn guest_cid_is_deterministic_and_in_range() {
        let first = guest_cid_for(&TaskId::from("task-abc".to_string()));
        assert_eq!(first, guest_cid_for(&TaskId::from("task-abc".to_string())));
        for id in ["task-1", "task-abc", "x", ""] {
            let cid = guest_cid_for(&TaskId::from(id.to_string()));
            assert!((3..=u32::MAX - 1).contains(&cid), "cid {cid} out of range");
        }
        // Distinct task ids should (almost surely) map to distinct CIDs.
        let other = guest_cid_for(&TaskId::from("task-abd".to_string()));
        assert_ne!(first, other);
    }

    #[test]
    fn vsock_paths_shape() {
        let rt = FirecrackerRuntime::new(FirecrackerConfig {
            vm_dir: PathBuf::from("/tmp/bigtop-vms"),
            ..FirecrackerConfig::default()
        });
        let task_id = TaskId::from("task-1".to_string());
        assert_eq!(
            rt.vsock_device_path(&task_id),
            PathBuf::from("/tmp/bigtop-vms/task-1/vsock.sock")
        );
        assert_eq!(
            rt.vsock_listen_path(&task_id),
            PathBuf::from("/tmp/bigtop-vms/task-1/vsock.sock_4668")
        );
    }

    #[test]
    fn shell_join_quotes_safely() {
        let mut env = HashMap::new();
        env.insert("GREETING".to_string(), "it's alive".to_string());
        let line = shell_join("echo", &["a b".to_string()], &env);
        assert_eq!(line, "GREETING='it'\\''s alive' 'echo' 'a b'");
    }

    #[test]
    fn boot_args_carry_task_params() {
        let task = test_task();
        let args = boot_args(None, &task);
        assert!(args.starts_with("console=ttyS0"), "{args}");
        assert!(args.contains("bigtop.task=task-abc"), "{args}");
        let b64 = args
            .split("bigtop.cmd_b64=")
            .nth(1)
            .expect("cmd_b64 present");
        let decoded = STANDARD.decode(b64).expect("valid base64");
        assert_eq!(String::from_utf8(decoded).expect("utf8"), "'echo' 'hi'");
        // Custom base args are honored.
        let custom = boot_args(Some("console=ttyS0 quiet"), &task);
        assert!(custom.starts_with("console=ttyS0 quiet"), "{custom}");
    }

    #[test]
    fn network_boot_args_exact_shape() {
        let ip = Ipv4Addr::new(10, 0, 0, 2);
        let gw = Ipv4Addr::new(10, 0, 0, 1);
        let mask = Ipv4Addr::new(255, 255, 255, 0);
        assert_eq!(
            network_boot_args(ip, gw, mask, None),
            "ip=10.0.0.2::10.0.0.1:255.255.255.0::eth0:off"
        );
        assert_eq!(
            network_boot_args(ip, gw, mask, Some("web-1")),
            "ip=10.0.0.2::10.0.0.1:255.255.255.0::eth0:off bigtop.hostname=web-1"
        );
    }

    #[test]
    fn boot_args_appends_network_when_enabled_and_assigned() {
        let task = networked_task();
        let args = boot_args(None, &task);
        assert!(
            args.contains("ip=10.0.0.2::10.0.0.1:255.255.255.0::eth0:off"),
            "{args}"
        );
        assert!(args.contains("bigtop.hostname=web-1"), "{args}");
    }

    #[test]
    fn boot_args_omits_network_when_disabled() {
        let task = test_task();
        assert!(!task.spec.network.enabled);
        let args = boot_args(None, &task);
        assert!(!args.contains("::eth0:off"), "{args}");
        assert!(!args.contains("bigtop.hostname"), "{args}");
    }

    #[test]
    fn boot_args_omits_network_without_assignment() {
        // Enabled but the server assigned no IP: nothing to configure.
        let mut task = test_task();
        task.spec.network.enabled = true;
        let args = boot_args(None, &task);
        assert!(!args.contains("::eth0:off"), "{args}");
        assert!(!args.contains("bigtop.hostname"), "{args}");
    }

    #[test]
    fn network_interface_body_exact_shape() {
        let mac = mac_for_task(&TaskId::from("task-abc".to_string()));
        let body = network_interface_body("eth0", &mac, "tap-bt-abc");
        assert_eq!(body["iface_id"], "eth0");
        assert_eq!(body["guest_mac"], mac.to_string());
        assert_eq!(body["host_dev_name"], "tap-bt-abc");
        // Exact shape: nothing else.
        let object = body.as_object().expect("object body");
        assert_eq!(object.len(), 3, "{object:?}");
    }

    #[test]
    fn launch_config_mirrors_api_bodies() {
        let task = test_task();
        let vcpu = vcpu_for(task.spec.resources.cpu_millis, task.spec.vm.vcpu_count);
        let mem_mb = task.spec.vm.mem_mb.max(64);
        let args = boot_args(task.spec.vm.boot_args.as_deref(), &task);
        let sock = Path::new("/tmp/vm/fc.sock");
        let cfg = launch_record(&task, BootKind::Fresh, vcpu, mem_mb, &args, sock, None);
        // Same numbers the REST calls send.
        assert_eq!(
            cfg["vcpu_count"],
            machine_config_body(vcpu, mem_mb)["vcpu_count"]
        );
        assert_eq!(
            cfg["mem_mb"],
            machine_config_body(vcpu, mem_mb)["mem_size_mib"]
        );
        assert_eq!(
            cfg["boot_args"],
            boot_source_body(&task.spec.vm.kernel_image, &args)["boot_args"]
        );
        assert_eq!(cfg["kernel_image"], task.spec.vm.kernel_image);
        assert_eq!(cfg["rootfs"], task.spec.vm.rootfs);
        assert_eq!(cfg["task_id"], "task-abc");
        assert_eq!(cfg["job_id"], "job-1");
        assert_eq!(cfg["command"], "echo");
        assert_eq!(cfg["api_socket"], "/tmp/vm/fc.sock");
        assert_eq!(cfg["boot"], "fresh");
        assert!(cfg.get("jailer_argv").is_none());
        // No `[network]`: no network block.
        assert!(cfg.get("network").is_none());
        // Serializes cleanly, which is what the agent writes to disk.
        let text = serde_json::to_string_pretty(&cfg).expect("serialize");
        let back: serde_json::Value = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, cfg);
    }

    #[test]
    fn launch_record_captures_snapshot_boot() {
        let task = test_task();
        let load = SnapshotLoadSpec {
            mem_file_path: "/vms/t/task-abc.snap.mem".to_string(),
            snapshot_path: "/vms/t/task-abc.snap".to_string(),
            enable_diff_snapshots: true,
        };
        let cfg = launch_record(
            &task,
            BootKind::Snapshot(&load),
            2,
            256,
            "console=ttyS0",
            Path::new("/tmp/vm/fc.sock"),
            None,
        );
        assert_eq!(cfg["boot"], "snapshot");
        assert_eq!(cfg["snapshot_path"], "/vms/t/task-abc.snap");
        assert_eq!(cfg["mem_file_path"], "/vms/t/task-abc.snap.mem");
    }

    #[test]
    fn launch_record_captures_jailer_argv() {
        let task = test_task();
        let argv = vec![
            "jailer".to_string(),
            "--id".to_string(),
            "task-abc".to_string(),
        ];
        let cfg = launch_record(
            &task,
            BootKind::Fresh,
            1,
            128,
            "console=ttyS0",
            Path::new("/srv/jailer/task-abc/root/fc.sock"),
            Some(&argv),
        );
        assert_eq!(cfg["jailer_argv"], json!(argv));
    }

    #[test]
    fn launch_record_includes_network_when_enabled() {
        let task = networked_task();
        let cfg = launch_record(
            &task,
            BootKind::Fresh,
            1,
            128,
            "console=ttyS0",
            Path::new("/tmp/vm/fc.sock"),
            None,
        );
        assert_eq!(cfg["network"]["iface_id"], "eth0");
        assert_eq!(
            cfg["network"]["guest_mac"],
            mac_for_task(&task.id).to_string()
        );
        assert_eq!(cfg["network"]["tap_name"], tap_name_for(&task.id));
        assert_eq!(cfg["network"]["ip"], "10.0.0.2");
        assert_eq!(cfg["network"]["gateway"], "10.0.0.1");
    }

    #[test]
    fn config_bodies_match_firecracker_api() {
        let mc = machine_config_body(2, 256);
        assert_eq!(mc["vcpu_count"], 2);
        assert_eq!(mc["mem_size_mib"], 256);
        assert_eq!(mc["smt"], false);
        let bs = boot_source_body("/kern", "console=ttyS0");
        assert_eq!(bs["kernel_image_path"], "/kern");
        assert_eq!(bs["boot_args"], "console=ttyS0");
        let dr = drive_body("/rootfs");
        assert_eq!(dr["drive_id"], "rootfs");
        assert_eq!(dr["is_root_device"], true);
        assert_eq!(dr["is_read_only"], false);
        assert_eq!(instance_start_body()["action_type"], "InstanceStart");
    }

    #[test]
    fn parse_status_reads_code() {
        let (code, _) = parse_status(b"HTTP/1.1 204 No Content\r\n\r\n");
        assert_eq!(code, 204);
        let (code, _) = parse_status(b"garbage");
        assert_eq!(code, 0);
    }

    /// A fake Firecracker API on a Unix socket: proves the HTTP plumbing.
    async fn fake_api(status_line: &'static str, body: &'static str) -> PathBuf {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = std::env::temp_dir().join(format!(
            "bigtop-fc-test-{}-{}",
            std::process::id(),
            status_line.replace(' ', "_")
        ));
        tokio::fs::create_dir_all(&dir).await.expect("mkdir");
        let sock = dir.join("fc.sock");
        let listener = tokio::net::UnixListener::bind(&sock).expect("bind");
        tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 8192];
            let n = conn.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            assert!(req.starts_with("PUT /machine-config HTTP/1.1"), "{req}");
            assert!(req.contains("\"vcpu_count\":2"), "{req}");
            let resp = format!(
                "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            conn.write_all(resp.as_bytes()).await.expect("write");
        });
        sock
    }

    #[tokio::test]
    async fn fc_put_accepts_2xx() {
        let sock = fake_api("HTTP/1.1 204 No Content", "").await;
        fc_put(&sock, "/machine-config", &machine_config_body(2, 256))
            .await
            .expect("2xx is ok");
    }

    #[tokio::test]
    async fn fc_put_surfaces_api_errors() {
        let sock = fake_api("HTTP/1.1 400 Bad Request", r#"{"fault_message":"boom"}"#).await;
        let err = fc_put(&sock, "/machine-config", &machine_config_body(2, 256))
            .await
            .expect_err("4xx is an error");
        match err {
            AgentError::FirecrackerApi { status, message } => {
                assert_eq!(status, 400);
                assert!(message.contains("400"), "{message}");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }
}
