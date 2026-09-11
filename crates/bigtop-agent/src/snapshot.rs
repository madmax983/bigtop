//! `MicroVM` snapshots: Firecracker `PUT /snapshot/create` and
//! `PUT /snapshot/load`, plus the path conventions the agent uses for
//! snapshot files.
//!
//! The bodies here must match the Firecracker API exactly; the tests assert
//! the JSON byte-for-byte.

use crate::firecracker::fc_put;
use crate::AgentError;
use bigtop_core::{SnapshotId, SnapshotLoadSpec, SnapshotSpec, SnapshotType, TaskId};
use serde_json::json;
use std::path::{Path, PathBuf};

/// Build the exact `PUT /snapshot/create` body for `spec`.
///
/// Firecracker expects:
/// `{"snapshot_type": "Full"|"Diff", "snapshot_path": ..., "mem_file_path": ...}`.
#[must_use]
pub fn snapshot_create_body(spec: &SnapshotSpec) -> serde_json::Value {
    let snapshot_type = match spec.snapshot_type {
        SnapshotType::Full => "Full",
        SnapshotType::Diff => "Diff",
    };
    json!({
        "snapshot_type": snapshot_type,
        "snapshot_path": spec.snapshot_path,
        "mem_file_path": spec.mem_file_path,
    })
}

/// Build the exact `PUT /snapshot/load` body for `spec`.
///
/// Firecracker expects the snapshot path plus a file-backed memory backend;
/// `resume_vm` starts the guest immediately after the load.
#[must_use]
pub fn snapshot_load_body(spec: &SnapshotLoadSpec) -> serde_json::Value {
    json!({
        "snapshot_path": spec.snapshot_path,
        "mem_backend": {
            "backend_type": "File",
            "backend_path": spec.mem_file_path,
        },
        "enable_diff_snapshots": spec.enable_diff_snapshots,
        "resume_vm": true,
    })
}

/// Resolve snapshot file paths: explicit paths win, empty ones default to
/// `<vm_dir>/<task_id>/snapshots/<snapshot_id>.mem` / `.snap`.
#[must_use]
pub fn resolve_snapshot_paths(
    spec: &SnapshotSpec,
    vm_dir: &Path,
    task_id: &TaskId,
    snapshot_id: &SnapshotId,
) -> (PathBuf, PathBuf) {
    let dir = vm_dir.join(task_id.as_ref()).join("snapshots");
    let mem = if spec.mem_file_path.is_empty() {
        dir.join(format!("{snapshot_id}.mem"))
    } else {
        PathBuf::from(&spec.mem_file_path)
    };
    let snap = if spec.snapshot_path.is_empty() {
        dir.join(format!("{snapshot_id}.snap"))
    } else {
        PathBuf::from(&spec.snapshot_path)
    };
    (mem, snap)
}

/// Runs the Firecracker snapshot API calls against a VMM socket.
pub struct SnapshotManager;

impl SnapshotManager {
    /// `PUT /snapshot/create`: capture the running microVM.
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the API socket is unreachable or
    /// Firecracker rejects the call.
    pub async fn create(sock: &Path, spec: &SnapshotSpec) -> Result<(), AgentError> {
        fc_put(sock, "/snapshot/create", &snapshot_create_body(spec)).await
    }

    /// `PUT /snapshot/load`: boot a microVM from snapshot files.
    /// The VM resumes immediately (`resume_vm: true`).
    ///
    /// # Errors
    ///
    /// Returns [`AgentError`] when the API socket is unreachable or
    /// Firecracker rejects the call.
    pub async fn load(sock: &Path, spec: &SnapshotLoadSpec) -> Result<(), AgentError> {
        fc_put(sock, "/snapshot/load", &snapshot_load_body(spec)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};

    fn spec() -> SnapshotSpec {
        SnapshotSpec {
            snapshot_type: SnapshotType::Full,
            mem_file_path: "/vms/t/mem".to_string(),
            snapshot_path: "/vms/t/snap".to_string(),
        }
    }

    #[test]
    fn create_body_matches_firecracker_api() {
        let body = snapshot_create_body(&spec());
        assert_eq!(
            body,
            json!({
                "snapshot_type": "Full",
                "snapshot_path": "/vms/t/snap",
                "mem_file_path": "/vms/t/mem",
            })
        );
        let diff = SnapshotSpec {
            snapshot_type: SnapshotType::Diff,
            ..spec()
        };
        assert_eq!(diff.snapshot_type, SnapshotType::Diff);
        assert_eq!(snapshot_create_body(&diff)["snapshot_type"], json!("Diff"));
    }

    #[test]
    fn load_body_matches_firecracker_api() {
        let load = SnapshotLoadSpec {
            mem_file_path: "/vms/t/mem".to_string(),
            snapshot_path: "/vms/t/snap".to_string(),
            enable_diff_snapshots: true,
        };
        assert_eq!(
            snapshot_load_body(&load),
            json!({
                "snapshot_path": "/vms/t/snap",
                "mem_backend": {
                    "backend_type": "File",
                    "backend_path": "/vms/t/mem",
                },
                "enable_diff_snapshots": true,
                "resume_vm": true,
            })
        );
    }

    #[test]
    fn resolve_paths_prefers_explicit() {
        let (mem, snap) = resolve_snapshot_paths(
            &spec(),
            Path::new("/vms"),
            &TaskId::from("task-1".to_string()),
            &SnapshotId::from("snap-1".to_string()),
        );
        assert_eq!(mem, PathBuf::from("/vms/t/mem"));
        assert_eq!(snap, PathBuf::from("/vms/t/snap"));
    }

    #[test]
    fn resolve_paths_defaults_under_task_dir() {
        let spec = SnapshotSpec {
            snapshot_type: SnapshotType::Full,
            mem_file_path: String::new(),
            snapshot_path: String::new(),
        };
        let (mem, snap) = resolve_snapshot_paths(
            &spec,
            Path::new("/vms"),
            &TaskId::from("task-1".to_string()),
            &SnapshotId::from("snap-1".to_string()),
        );
        assert_eq!(mem, PathBuf::from("/vms/task-1/snapshots/snap-1.mem"));
        assert_eq!(snap, PathBuf::from("/vms/task-1/snapshots/snap-1.snap"));
    }

    /// Fake Firecracker API socket: records the request and answers 204.
    /// The listener moves into the server thread, which owns it for the
    /// life of the test.
    struct FakeApi {
        sock: PathBuf,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl FakeApi {
        fn start(name: &str) -> Self {
            let sock =
                std::env::temp_dir().join(format!("bigtop-snaptest-{name}-{}", std::process::id()));
            let _ = std::fs::remove_file(&sock);
            let listener = UnixListener::bind(&sock).expect("bind fake api");
            let seen = Arc::new(Mutex::new(Vec::new()));
            let seen_clone = seen.clone();
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = vec![0u8; 65536];
                let mut raw = Vec::new();
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            raw.extend_from_slice(&buf[..n]);
                            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                seen_clone
                    .lock()
                    .expect("lock")
                    .push(String::from_utf8_lossy(&raw).into_owned());
                let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
            });
            Self { sock, seen }
        }
    }

    #[tokio::test]
    async fn create_hits_snapshot_create_endpoint() {
        let api = FakeApi::start("create");
        SnapshotManager::create(&api.sock, &spec())
            .await
            .expect("snapshot create");
        // Give the fake a moment to record.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let seen: Vec<String> = api.seen.lock().expect("lock").clone();
        assert_eq!(seen.len(), 1, "one request: {seen:?}");
        let req = &seen[0];
        assert!(
            req.starts_with("PUT /snapshot/create HTTP/1.1"),
            "request: {req}"
        );
        let body = req.split("\r\n\r\n").nth(1).expect("body");
        let value: serde_json::Value = serde_json::from_str(body).expect("json body");
        assert_eq!(value, snapshot_create_body(&spec()));
    }

    #[tokio::test]
    async fn load_hits_snapshot_load_endpoint() {
        let api = FakeApi::start("load");
        let load = SnapshotLoadSpec {
            mem_file_path: "/vms/t/mem".to_string(),
            snapshot_path: "/vms/t/snap".to_string(),
            enable_diff_snapshots: false,
        };
        SnapshotManager::load(&api.sock, &load)
            .await
            .expect("snapshot load");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let seen: Vec<String> = api.seen.lock().expect("lock").clone();
        assert_eq!(seen.len(), 1, "one request: {seen:?}");
        let req = &seen[0];
        assert!(
            req.starts_with("PUT /snapshot/load HTTP/1.1"),
            "request: {req}"
        );
        let body = req.split("\r\n\r\n").nth(1).expect("body");
        let value: serde_json::Value = serde_json::from_str(body).expect("json body");
        assert_eq!(value, snapshot_load_body(&load));
    }
}
