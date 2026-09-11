//! `FirecrackerRuntime`: one microVM per task.
//!
//! This talks to the real Firecracker `REST` API over a Unix socket:
//! `PUT /machine-config`, `PUT /boot-source`, `PUT /drives/rootfs`, then
//! `PUT /actions` with `InstanceStart`. The guest's serial console arrives
//! on the `firecracker` process's stdout and is streamed as task logs.
//!
//! Before booting, the agent writes `bigtop-vm.json` into the task's VM dir:
//! a read-only record of exactly what the `REST` calls configure, for
//! inspection and replay. The API stays the source of truth.
//!
//! v0.1 notes: no jailer sandboxing, no vsock log channel, no snapshot
//! support. Those are v0.2. End-to-end microVM boot needs `/dev/kvm` and
//! is unverified in CI-like environments; the config builders below are
//! unit-tested, including against a fake API socket.

use crate::runtime::{RunningTask, Runtime};
use crate::AgentError;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bigtop_core::{Task, TaskId};
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// Configuration for [`FirecrackerRuntime`].
#[derive(Debug, Clone)]
pub struct FirecrackerConfig {
    /// The `firecracker` binary (a bare name resolves via `PATH`).
    pub bin: PathBuf,
    /// Directory holding one subdir per VM (`<vm_dir>/<task-id>/`).
    pub vm_dir: PathBuf,
    /// How long to wait for the API socket after spawning the VMM.
    pub boot_timeout: Duration,
}

impl Default for FirecrackerConfig {
    fn default() -> Self {
        Self {
            bin: PathBuf::from("firecracker"),
            vm_dir: PathBuf::from("/tmp/bigtop-vms"),
            boot_timeout: Duration::from_secs(10),
        }
    }
}

/// Boots one Firecracker microVM per task.
#[derive(Debug, Clone)]
pub struct FirecrackerRuntime {
    config: FirecrackerConfig,
}

impl FirecrackerRuntime {
    /// Build a runtime from `config`.
    #[must_use]
    pub const fn new(config: FirecrackerConfig) -> Self {
        Self { config }
    }

    /// Unix socket path for a task's VMM API.
    #[must_use]
    pub fn socket_path(&self, task_id: &TaskId) -> PathBuf {
        self.config.vm_dir.join(task_id.to_string()).join("fc.sock")
    }

    /// Configure a freshly spawned VMM, then start the instance.
    /// `vcpu_count`, `mem_mb`, and `boot_args` are the values recorded in
    /// `bigtop-vm.json`, so the API calls can never drift from the record.
    async fn configure(
        &self,
        sock: &Path,
        task: &Task,
        vcpu_count: u32,
        mem_mb: u64,
        boot_args: &str,
    ) -> Result<(), AgentError> {
        wait_for_socket(sock, self.config.boot_timeout).await?;
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
        fc_put(sock, "/actions", &instance_start_body()).await?;
        Ok(())
    }
}

impl Runtime for FirecrackerRuntime {
    async fn spawn(&self, task: &Task) -> Result<RunningTask, AgentError> {
        let vm = &task.spec.vm;
        if vm.kernel_image.trim().is_empty() {
            return Err(AgentError::ImageMissing(
                "vm.kernel_image is empty".to_string(),
            ));
        }
        if vm.rootfs.trim().is_empty() {
            return Err(AgentError::ImageMissing("vm.rootfs is empty".to_string()));
        }
        let vm_task_dir = self.config.vm_dir.join(task.id.to_string());
        tokio::fs::create_dir_all(&vm_task_dir)
            .await
            .map_err(AgentError::Io)?;
        let sock = self.socket_path(&task.id);
        if tokio::fs::try_exists(&sock).await.map_err(AgentError::Io)? {
            tokio::fs::remove_file(&sock)
                .await
                .map_err(AgentError::Io)?;
        }
        // Fail fast on missing images instead of a cryptic API error later.
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
        // Render the exact values the REST calls will use, and record them
        // in bigtop-vm.json before the VMM boots.
        let vcpu_count = vcpu_for(task.spec.resources.cpu_millis, vm.vcpu_count);
        let mem_mb = vm.mem_mb.max(64);
        let args = boot_args(vm.boot_args.as_deref(), task);
        let record = serde_json::to_string_pretty(&launch_config_json(
            task, vcpu_count, mem_mb, &args, &sock,
        ))
        .map_err(AgentError::Json)?;
        tokio::fs::write(vm_task_dir.join("bigtop-vm.json"), record)
            .await
            .map_err(AgentError::Io)?;
        let mut child = Command::new(&self.config.bin)
            .arg("--api-sock")
            .arg(&sock)
            .arg("--log-path")
            .arg(vm_task_dir.join("firecracker.log"))
            .arg("--id")
            .arg(task.id.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(AgentError::Spawn)?;
        if let Err(e) = self.configure(&sock, task, vcpu_count, mem_mb, &args).await {
            let _ = child.kill().await;
            return Err(e);
        }
        Ok(RunningTask {
            task_id: task.id.clone(),
            child,
            vm_dir: Some(vm_task_dir),
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

/// The `bigtop-vm.json` record: everything the agent configures for one
/// task's microVM, written before the VMM boots. It mirrors the `REST` API
/// bodies so an operator can inspect or replay a task's VM from the file
/// alone; the API stays the source of truth.
#[must_use]
pub fn launch_config_json(
    task: &Task,
    vcpu_count: u32,
    mem_mb: u64,
    boot_args: &str,
    api_sock: &Path,
) -> serde_json::Value {
    json!({
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
    })
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
    format!(
        "{base_args} bigtop.task={} bigtop.cmd_b64={cmd_b64}",
        task.id
    )
}

/// Wait until the VMM's API socket appears.
async fn wait_for_socket(sock: &Path, timeout: Duration) -> Result<(), AgentError> {
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
async fn fc_put(sock: &Path, path: &str, body: &serde_json::Value) -> Result<(), AgentError> {
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
    use bigtop_core::{JobId, Resources, TaskSpec, TaskState, VmSpec};
    use std::collections::HashMap;

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
                },
            },
            state: TaskState::Pending,
            assigned_node: None,
            exit_code: None,
        }
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
    fn launch_config_mirrors_api_bodies() {
        let task = test_task();
        let vcpu = vcpu_for(task.spec.resources.cpu_millis, task.spec.vm.vcpu_count);
        let mem_mb = task.spec.vm.mem_mb.max(64);
        let args = boot_args(task.spec.vm.boot_args.as_deref(), &task);
        let sock = Path::new("/tmp/vm/fc.sock");
        let cfg = launch_config_json(&task, vcpu, mem_mb, &args, sock);
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
        // Serializes cleanly, which is what the agent writes to disk.
        let text = serde_json::to_string_pretty(&cfg).expect("serialize");
        let back: serde_json::Value = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, cfg);
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
