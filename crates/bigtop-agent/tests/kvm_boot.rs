//! Tier 1 of the KVM CI lane: boot a **real** Firecracker microVM.
//!
//! This is the test that answers "does `BigTop` actually boot a microVM?"
//! Everything else in the workspace runs on the process runtime or on
//! Firecracker's configuration surface without a hypervisor underneath.
//!
//! # Gating
//!
//! The test runs only when all three prerequisites hold:
//!
//! - `/dev/kvm` exists and is readable/writable,
//! - `BIGTOP_KVM_KERNEL` points at a readable guest kernel image,
//! - `BIGTOP_KVM_ROOTFS` points at a readable guest rootfs image.
//!
//! Otherwise it **skips cleanly**, printing the reason. Skipping is a
//! deliberate outcome: ordinary dev machines (and this sandbox) have no
//! KVM, and a red test there would be noise. The KVM workflow
//! (`.github/workflows/kvm.yml`) runs a fail-fast preflight *before*
//! `cargo test`, so a mislabeled runner fails the job loudly instead of
//! silently skipping here.
//!
//! # Guest contract
//!
//! The agent boots the guest with `bigtop.cmd_b64=<base64>` on the kernel
//! cmdline; the guest's `/sbin/init` (see `scripts/kvm/guest-init.sh`)
//! decodes it and runs the command with its stdout wired to the serial
//! console, which Firecracker relays to the child's stdout. Markers the
//! guest prints are therefore observable from this test.

use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bigtop_agent::{FirecrackerConfig, FirecrackerRuntime, Runtime};
use bigtop_core::{
    JobId, NetworkSpec, ServiceSpec, SnapshotId, SnapshotLoadSpec, SnapshotPolicy, SnapshotSpec,
    SnapshotType, Task, TaskId, TaskSpec, TaskState, VmSpec,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStdout;

/// How long a cold runner may take to boot the microVM and print the first
/// marker. Firecracker itself boots in ~100ms; this budget is for the
/// runner being slow, not the VM.
const MARKER_TIMEOUT: Duration = Duration::from_secs(180);
/// Post-restore observation window: long enough for a resumed guest to
/// print several heartbeat lines.
const RESUME_TIMEOUT: Duration = Duration::from_secs(90);

/// Marker printed once per guest boot, before the heartbeat loop starts.
const BOOT_MARKER: &str = "BIGTOP_KVM_BOOT_MARKER";
/// Marker printed every 2s by the heartbeat loop.
const BEAT_MARKER: &str = "BIGTOP_KVM_BEAT_";
/// Marker for the plain boot test.
const HELLO_MARKER: &str = "BIGTOP_KVM_HELLO";

struct KvmPrereqs {
    kernel: PathBuf,
    rootfs: PathBuf,
}

/// Check the three prerequisites. Returns `None` (after printing the
/// reason) when this machine cannot run a real microVM.
fn kvm_prereqs() -> Option<KvmPrereqs> {
    let kvm = Path::new("/dev/kvm");
    if !kvm.exists() {
        println!("SKIP: /dev/kvm not present; real Firecracker boot requires KVM");
        return None;
    }
    // Opening the device node read/write proves the permission bits grant
    // access; the open itself needs no privileges beyond that.
    if let Err(e) = std::fs::File::options().read(true).write(true).open(kvm) {
        println!("SKIP: /dev/kvm not accessible ({e}); need read/write permission");
        return None;
    }
    let kernel = env::var_os("BIGTOP_KVM_KERNEL").map(PathBuf::from);
    let rootfs = env::var_os("BIGTOP_KVM_ROOTFS").map(PathBuf::from);
    if let (Some(kernel), Some(rootfs)) = (kernel, rootfs) {
        for (label, path) in [
            ("BIGTOP_KVM_KERNEL", &kernel),
            ("BIGTOP_KVM_ROOTFS", &rootfs),
        ] {
            if std::fs::metadata(path).is_err() {
                println!(
                    "SKIP: {label} points at an unreadable file: {}",
                    path.display()
                );
                return None;
            }
        }
        Some(KvmPrereqs { kernel, rootfs })
    } else {
        println!(
            "SKIP: set BIGTOP_KVM_KERNEL and BIGTOP_KVM_ROOTFS to guest images \
             (see scripts/kvm/build-rootfs.sh)"
        );
        None
    }
}

/// Unique scratch dir for one test's Firecracker state.
fn scratch_dir(test: &str) -> Result<PathBuf, String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("clock went backwards: {e}"))?
        .as_nanos();
    let dir = env::temp_dir().join(format!(
        "bigtop-kvm-{test}-{pid}-{nanos}",
        pid = std::process::id()
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create scratch dir: {e}"))?;
    Ok(dir)
}

fn firecracker_config(vm_dir: PathBuf) -> FirecrackerConfig {
    FirecrackerConfig {
        bin: env::var("BIGTOP_FIRECRACKER_BIN")
            .unwrap_or_else(|_| "firecracker".to_string())
            .into(),
        vm_dir,
        boot_timeout: Duration::from_secs(120),
        jailer: None,
        overlay_vni: None,
    }
}

fn boot_task(name: &str, kernel: &Path, rootfs: &Path, command: &str, args: &[&str]) -> Task {
    Task {
        id: TaskId::generate(),
        job_id: JobId::generate(),
        name: name.to_string(),
        state: TaskState::Pending,
        assigned_node: None,
        exit_code: None,
        network: None,
        spec: TaskSpec {
            name: name.to_string(),
            command: command.to_string(),
            args: args.iter().map(ToString::to_string).collect(),
            env: std::collections::HashMap::new(),
            resources: bigtop_core::Resources::default(),
            count: 1,
            vm: VmSpec {
                kernel_image: kernel.display().to_string(),
                rootfs: rootfs.display().to_string(),
                vcpu_count: 1,
                mem_mb: 128,
                boot_args: None,
                boot_snapshot: None,
            },
            node_affinity: None,
            snapshot_policy: SnapshotPolicy::default(),
            network: NetworkSpec::default(),
            service: ServiceSpec::default(),
        },
    }
}

/// Read serial lines until `want` appears, failing if `forbidden` appears
/// first or `timeout` elapses.
async fn wait_for_marker(
    lines: &mut tokio::io::Lines<BufReader<ChildStdout>>,
    want: &str,
    forbidden: Option<&str>,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!("timed out after {timeout:?} waiting for `{want}`"));
        }
        match tokio::time::timeout(remaining, lines.next_line()).await {
            Err(_) => return Err(format!("timed out after {timeout:?} waiting for `{want}`")),
            Ok(Err(e)) => return Err(format!("reading guest serial output: {e}")),
            Ok(Ok(None)) => return Err("guest serial stream ended before marker appeared".into()),
            Ok(Ok(Some(line))) => {
                if let Some(bad) = forbidden {
                    if line.contains(bad) {
                        return Err(format!("saw forbidden marker `{bad}`: {line}"));
                    }
                }
                if line.contains(want) {
                    return Ok(());
                }
            }
        }
    }
}

/// Boot a real microVM and prove the guest command ran: the guest's
/// `echo` must appear on the serial console.
#[tokio::test]
async fn kvm_boot_runs_guest_command() -> Result<(), String> {
    let Some(prereqs) = kvm_prereqs() else {
        return Ok(());
    };

    let vm_dir = scratch_dir("boot")?;
    let runtime = FirecrackerRuntime::new(firecracker_config(vm_dir));
    let task = boot_task(
        "kvm-hello",
        &prereqs.kernel,
        &prereqs.rootfs,
        "sh",
        &["-c", &format!("echo {HELLO_MARKER}")],
    );

    let mut running = tokio::time::timeout(Duration::from_secs(300), runtime.spawn(&task))
        .await
        .map_err(|_| "spawn timed out after 300s".to_string())
        .and_then(|r| r.map_err(|e| format!("spawn failed: {e}")))?;

    let stdout = running
        .child
        .stdout
        .take()
        .ok_or("firecracker child has no piped stdout")?;
    let mut lines = BufReader::new(stdout).lines();

    wait_for_marker(&mut lines, HELLO_MARKER, None, MARKER_TIMEOUT).await?;

    running
        .child
        .kill()
        .await
        .map_err(|e| format!("killing microVM: {e}"))?;
    Ok(())
}

/// Boot a real microVM running a heartbeat loop, snapshot the **live** VM,
/// kill the original, restore from the snapshot under a new task id, and
/// prove execution resumes: heartbeat lines must continue while the boot
/// marker must never reappear.
#[tokio::test]
async fn kvm_snapshot_restore_resumes_execution() -> Result<(), String> {
    let Some(prereqs) = kvm_prereqs() else {
        return Ok(());
    };

    let vm_dir = scratch_dir("snapshot")?;
    let runtime = FirecrackerRuntime::new(firecracker_config(vm_dir));

    let heartbeat = format!(
        "echo {BOOT_MARKER}; n=0; while true; do echo {BEAT_MARKER}$n; n=$((n+1)); sleep 2; done"
    );
    let task = boot_task(
        "kvm-snapshot",
        &prereqs.kernel,
        &prereqs.rootfs,
        "sh",
        &["-c", &heartbeat],
    );

    let mut running = tokio::time::timeout(Duration::from_secs(300), runtime.spawn(&task))
        .await
        .map_err(|_| "spawn timed out after 300s".to_string())
        .and_then(|r| r.map_err(|e| format!("spawn failed: {e}")))?;

    let stdout = running
        .child
        .stdout
        .take()
        .ok_or("firecracker child has no piped stdout")?;
    let mut lines = BufReader::new(stdout).lines();

    // The guest booted and the heartbeat loop is alive: one boot marker,
    // then two consecutive beats.
    wait_for_marker(&mut lines, BOOT_MARKER, None, MARKER_TIMEOUT).await?;
    wait_for_marker(&mut lines, BEAT_MARKER, None, MARKER_TIMEOUT).await?;
    wait_for_marker(&mut lines, BEAT_MARKER, None, MARKER_TIMEOUT).await?;

    // Snapshot the live VM.
    let snapshot_id = SnapshotId::generate();
    let (mem_file, state_file) = runtime
        .take_snapshot(
            &task.id,
            &snapshot_id,
            &SnapshotSpec::with_defaults(SnapshotType::Full),
        )
        .await
        .map_err(|e| format!("take_snapshot failed: {e}"))?;
    for (label, path) in [("memory", &mem_file), ("state", &state_file)] {
        let meta = std::fs::metadata(path)
            .map_err(|e| format!("snapshot {label} file missing at {}: {e}", path.display()))?;
        if meta.len() == 0 {
            return Err(format!(
                "snapshot {label} file is empty: {}",
                path.display()
            ));
        }
    }

    // Kill the original VM and wait for it: the rootfs file is attached
    // read-write, so the restore must not boot while the original still
    // holds the image.
    running
        .child
        .kill()
        .await
        .map_err(|e| format!("killing original microVM: {e}"))?;
    running
        .child
        .wait()
        .await
        .map_err(|e| format!("waiting for original microVM: {e}"))?;

    // Restore under a new task id from the snapshot files.
    let mut restore = boot_task(
        "kvm-restore",
        &prereqs.kernel,
        &prereqs.rootfs,
        "sh",
        &["-c", "echo SHOULD_NOT_RUN"],
    );
    restore.spec.vm.boot_snapshot = Some(SnapshotLoadSpec {
        mem_file_path: mem_file.display().to_string(),
        snapshot_path: state_file.display().to_string(),
        enable_diff_snapshots: false,
    });

    let mut restored = tokio::time::timeout(Duration::from_secs(300), runtime.spawn(&restore))
        .await
        .map_err(|_| "restore spawn timed out after 300s".to_string())
        .and_then(|r| r.map_err(|e| format!("restore spawn failed: {e}")))?;

    let stdout = restored
        .child
        .stdout
        .take()
        .ok_or("restored firecracker child has no piped stdout")?;
    let mut lines = BufReader::new(stdout).lines();

    // Execution resumed, not rebooted: heartbeat lines continue, and the
    // one-time boot marker must never appear again.
    wait_for_marker(&mut lines, BEAT_MARKER, Some(BOOT_MARKER), RESUME_TIMEOUT).await?;
    wait_for_marker(&mut lines, BEAT_MARKER, Some(BOOT_MARKER), RESUME_TIMEOUT).await?;

    restored
        .child
        .kill()
        .await
        .map_err(|e| format!("killing restored microVM: {e}"))?;
    Ok(())
}
