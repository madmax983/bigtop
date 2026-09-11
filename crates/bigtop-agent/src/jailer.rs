//! `firecracker-jailer` sandboxing: build the exact jailer argv and map the
//! jailed API socket back to its host path.
//!
//! When the agent runs with `--jailer`, each microVM boots as
//! `jailer ... --exec-file <firecracker> -- <firecracker args>` instead of
//! as a bare firecracker process. The jailer sets up the chroot, drops
//! privileges, optionally joins a network namespace, and then execs
//! firecracker inside the jail.

use std::path::{Path, PathBuf};

/// API socket path *inside* the jail. It lives at the chroot root: the only
/// directory the jailer guarantees to exist. The agent reaches it at
/// `<chroot-base>/<id>/root/fc.sock` on the host.
pub const JAILED_API_SOCK: &str = "/fc.sock";
/// VMM log path *inside* the jail. Recorded in `bigtop-vm.json`; the agent
/// does not tail it in v0.2 (guest logs arrive over vsock).
pub const JAILED_LOG_PATH: &str = "/firecracker.log";

/// Operator-controlled jailer settings (no per-task fields).
#[derive(Debug, Clone)]
pub struct JailerOptions {
    /// Path to the `jailer` binary.
    pub bin: PathBuf,
    /// UID firecracker runs as inside the jail.
    pub uid: u32,
    /// GID firecracker runs as inside the jail.
    pub gid: u32,
    /// `<chroot-base>/<id>/root` becomes the jail's `/`.
    pub chroot_base_dir: PathBuf,
    /// Optional network namespace path (`--netns`).
    pub netns: Option<PathBuf>,
    /// `--daemonize`. Keep `false` under the agent: the agent supervises the
    /// jailer process as the VM's lifetime handle, so a daemonizing jailer
    /// would look like an instantly-exited VM.
    pub daemonize: bool,
}

/// Fully-resolved per-task jailer invocation.
#[derive(Debug, Clone)]
pub struct JailerConfig {
    bin: PathBuf,
    id: String,
    uid: u32,
    gid: u32,
    chroot_base_dir: PathBuf,
    netns: Option<PathBuf>,
    daemonize: bool,
    /// Firecracker binary the jailer will exec.
    exec_file: PathBuf,
}

impl JailerConfig {
    /// Resolve per-task jailer settings from the operator options.
    #[must_use]
    pub fn new(id: String, opts: &JailerOptions, exec_file: PathBuf) -> Self {
        Self {
            bin: opts.bin.clone(),
            id,
            uid: opts.uid,
            gid: opts.gid,
            chroot_base_dir: opts.chroot_base_dir.clone(),
            netns: opts.netns.clone(),
            daemonize: opts.daemonize,
            exec_file,
        }
    }

    /// The full argv, starting with the jailer binary itself; firecracker
    /// args follow the `--` separator exactly as jailer expects.
    #[must_use]
    pub fn argv(&self, fc_args: &[String]) -> Vec<String> {
        let mut argv = vec![
            self.bin.display().to_string(),
            "--id".to_string(),
            self.id.clone(),
            "--uid".to_string(),
            self.uid.to_string(),
            "--gid".to_string(),
            self.gid.to_string(),
            "--chroot-base-dir".to_string(),
            self.chroot_base_dir.display().to_string(),
        ];
        if let Some(netns) = &self.netns {
            argv.push("--netns".to_string());
            argv.push(netns.display().to_string());
        }
        if self.daemonize {
            argv.push("--daemonize".to_string());
        }
        argv.push("--exec-file".to_string());
        argv.push(self.exec_file.display().to_string());
        argv.push("--".to_string());
        argv.extend(fc_args.iter().cloned());
        argv
    }

    /// Host-side path for a jailed absolute path, e.g. `/fc.sock` ->
    /// `<chroot-base>/<id>/root/fc.sock`.
    #[must_use]
    pub fn host_path(&self, jailed: &Path) -> PathBuf {
        let relative = jailed.strip_prefix("/").map_or(jailed, |path| path);
        self.chroot_base_dir
            .join(&self.id)
            .join("root")
            .join(relative)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> JailerOptions {
        JailerOptions {
            bin: PathBuf::from("jailer"),
            uid: 1234,
            gid: 1234,
            chroot_base_dir: PathBuf::from("/srv/jailer"),
            netns: Some(PathBuf::from("/var/run/netns/bt0")),
            daemonize: false,
        }
    }

    #[test]
    fn jailer_argv_is_exact() {
        let config = JailerConfig::new(
            "task-abc".to_string(),
            &options(),
            PathBuf::from("/usr/local/bin/firecracker"),
        );
        let argv = config.argv(&[
            "--api-sock".to_string(),
            "/fc.sock".to_string(),
            "--id".to_string(),
            "task-abc".to_string(),
        ]);
        assert_eq!(
            argv,
            vec![
                "jailer",
                "--id",
                "task-abc",
                "--uid",
                "1234",
                "--gid",
                "1234",
                "--chroot-base-dir",
                "/srv/jailer",
                "--netns",
                "/var/run/netns/bt0",
                "--exec-file",
                "/usr/local/bin/firecracker",
                "--",
                "--api-sock",
                "/fc.sock",
                "--id",
                "task-abc",
            ]
        );
    }

    #[test]
    fn jailer_argv_with_daemonize_and_no_netns() {
        let mut opts = options();
        opts.netns = None;
        opts.daemonize = true;
        let config = JailerConfig::new(
            "task-xyz".to_string(),
            &opts,
            PathBuf::from("/usr/local/bin/firecracker"),
        );
        let argv = config.argv(&["--api-sock".to_string(), "/fc.sock".to_string()]);
        assert_eq!(
            argv,
            vec![
                "jailer",
                "--id",
                "task-xyz",
                "--uid",
                "1234",
                "--gid",
                "1234",
                "--chroot-base-dir",
                "/srv/jailer",
                "--daemonize",
                "--exec-file",
                "/usr/local/bin/firecracker",
                "--",
                "--api-sock",
                "/fc.sock",
            ]
        );
    }

    #[test]
    fn host_path_maps_jailed_socket() {
        let config = JailerConfig::new(
            "task-abc".to_string(),
            &options(),
            PathBuf::from("/usr/local/bin/firecracker"),
        );
        assert_eq!(
            config.host_path(Path::new(JAILED_API_SOCK)),
            PathBuf::from("/srv/jailer/task-abc/root/fc.sock")
        );
        assert_eq!(
            config.host_path(Path::new(JAILED_LOG_PATH)),
            PathBuf::from("/srv/jailer/task-abc/root/firecracker.log")
        );
    }

    #[test]
    fn jailed_paths_are_absolute() {
        assert!(Path::new(JAILED_API_SOCK).is_absolute());
        assert!(Path::new(JAILED_LOG_PATH).is_absolute());
    }
}
