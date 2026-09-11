//! `bigtop`: one binary. Server, agent, and CLI for the microVM orchestrator.

use anyhow::{Context, Result};
use bigtop_agent::{
    auto_runtime, run_agent, AgentConfig, FirecrackerConfig, FirecrackerRuntime, JailerOptions,
    ProcessRuntime, RuntimeKind,
};
use bigtop_core::{JobSpec, SnapshotState, SnapshotType, TaskSpec};
use bigtop_server::{serve_config, ServerConfig};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Default server URL for CLI commands.
const DEFAULT_SERVER: &str = "http://127.0.0.1:4667";

/// Column headers for `bigtop ps`.
const PS_HEADERS: [&str; 7] = ["TASK ID", "NAME", "STATE", "EXIT", "NODE", "IP", "JOB"];
/// Column headers for `bigtop nodes`.
const NODES_HEADERS: [&str; 5] = [
    "NODE ID",
    "NAME",
    "ALIVE",
    "CPU used/total",
    "MEM used/total MiB",
];

#[derive(Debug, Parser)]
#[command(
    name = "bigtop",
    version,
    about = "BigTop: the loud, fast, opinionated microVM orchestrator. One binary."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Which runtime the agent uses for tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RuntimeChoice {
    /// One Firecracker microVM per task.
    Firecracker,
    /// Plain child processes (dev/CI stand-in).
    Process,
    /// Firecracker when /dev/kvm exists, else processes.
    Auto,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run the `BigTop` server.
    Server {
        /// Port to listen on.
        #[arg(long, default_value_t = 4667)]
        port: u16,
        /// Address to bind.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        /// Pod network CIDR for per-task IPs (a /16, carved into /24s per node).
        #[arg(long, default_value = "172.28.0.0/16")]
        network_cidr: String,
    },
    /// Run the `BigTop` agent.
    Agent {
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
        /// Node name (defaults to $HOSTNAME).
        #[arg(long)]
        name: Option<String>,
        /// Which runtime runs tasks.
        #[arg(long, value_enum, default_value = "auto")]
        runtime: RuntimeChoice,
        /// Directory for per-VM state (Firecracker).
        #[arg(long, default_value = "/tmp/bigtop-vms")]
        vm_dir: PathBuf,
        /// The firecracker binary.
        #[arg(long, default_value = "firecracker")]
        firecracker_bin: String,
        /// Sandbox every microVM in `firecracker-jailer`.
        #[arg(long, default_value_t = false)]
        jailer: bool,
        /// The jailer binary.
        #[arg(long, default_value = "jailer")]
        jailer_bin: String,
        /// UID firecracker runs as inside the jail.
        #[arg(long, default_value_t = 1234)]
        jailer_uid: u32,
        /// GID firecracker runs as inside the jail.
        #[arg(long, default_value_t = 1234)]
        jailer_gid: u32,
        /// Jail chroot base dir (`<base>/<id>/root` becomes the jail's `/`).
        #[arg(long, default_value = "/srv/jailer")]
        chroot_base_dir: PathBuf,
        /// Network namespace path the jail joins (optional).
        #[arg(long)]
        netns: Option<PathBuf>,
    },
    /// Submit a job from a TOML file.
    Run {
        /// Job file.
        job_file: PathBuf,
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// List tasks.
    Ps {
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// List nodes.
    Nodes {
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// Show a task's logs.
    Logs {
        /// Task id.
        task_id: String,
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// `MicroVM` snapshots: create, list, restore.
    Snapshot {
        #[command(subcommand)]
        action: SnapshotCommands,
    },
}

/// Which kind of Firecracker snapshot to take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SnapshotKind {
    /// Complete guest memory dump; boots on its own.
    Full,
    /// Pages dirtied since the last snapshot.
    Diff,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommands {
    /// Ask the owning agent to snapshot a running task's microVM.
    Create {
        /// Task id.
        task_id: String,
        /// Full or incremental snapshot.
        #[arg(long, value_enum, default_value = "full")]
        kind: SnapshotKind,
        /// Destination for the guest memory file (agent default when omitted).
        #[arg(long)]
        mem_path: Option<String>,
        /// Destination for the snapshot state file (agent default when omitted).
        #[arg(long)]
        snap_path: Option<String>,
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// List a task's snapshots and their states.
    List {
        /// Task id.
        task_id: String,
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
    /// Boot a new task from a finished snapshot, pinned to the node
    /// holding the snapshot files.
    Restore {
        /// Task the snapshot belongs to.
        task_id: String,
        /// Snapshot id.
        snapshot_id: String,
        /// Server base URL.
        #[arg(long, default_value = DEFAULT_SERVER)]
        server: String,
    },
}

/// On-disk job file: `[[task]]` tables plus an optional `[task.vm]`.
#[derive(Debug, Deserialize)]
struct JobFile {
    name: String,
    #[serde(default)]
    task: Vec<TaskSpec>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server {
            port,
            bind,
            network_cidr,
        } => cmd_server(&bind, port, &network_cidr).await,
        Commands::Agent {
            server,
            name,
            runtime,
            vm_dir,
            firecracker_bin,
            jailer,
            jailer_bin,
            jailer_uid,
            jailer_gid,
            chroot_base_dir,
            netns,
        } => {
            cmd_agent(AgentOptions {
                server: &server,
                name,
                runtime,
                vm_dir: &vm_dir,
                firecracker_bin: &firecracker_bin,
                jailer,
                jailer_bin: &jailer_bin,
                jailer_uid,
                jailer_gid,
                chroot_base_dir: &chroot_base_dir,
                netns,
            })
            .await
        }
        Commands::Run { job_file, server } => cmd_run(&job_file, &server).await,
        Commands::Ps { server } => cmd_ps(&server).await,
        Commands::Nodes { server } => cmd_nodes(&server).await,
        Commands::Logs { task_id, server } => cmd_logs(&task_id, &server).await,
        Commands::Snapshot { action } => match action {
            SnapshotCommands::Create {
                task_id,
                kind,
                mem_path,
                snap_path,
                server,
            } => {
                cmd_snapshot_create(
                    &task_id,
                    kind,
                    mem_path.as_deref(),
                    snap_path.as_deref(),
                    &server,
                )
                .await
            }
            SnapshotCommands::List { task_id, server } => {
                cmd_snapshot_list(&task_id, &server).await
            }
            SnapshotCommands::Restore {
                task_id,
                snapshot_id,
                server,
            } => cmd_snapshot_restore(&task_id, &snapshot_id, &server).await,
        },
    }
}

async fn cmd_server(bind: &str, port: u16, network_cidr: &str) -> Result<()> {
    let addr: std::net::SocketAddr = format!("{bind}:{port}")
        .parse()
        .context("invalid bind address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("binding server socket")?;
    println!("bigtop server: listening on {addr} (loud and proud)");
    let config = ServerConfig {
        tick_interval: std::time::Duration::from_millis(500),
        network_cidr: network_cidr.to_string(),
    };
    serve_config(listener, config).await?;
    Ok(())
}

/// `agent` subcommand options (bundled: clippy caps function arity at seven).
struct AgentOptions<'a> {
    server: &'a str,
    name: Option<String>,
    runtime: RuntimeChoice,
    vm_dir: &'a Path,
    firecracker_bin: &'a str,
    jailer: bool,
    jailer_bin: &'a str,
    jailer_uid: u32,
    jailer_gid: u32,
    chroot_base_dir: &'a Path,
    netns: Option<PathBuf>,
}

async fn cmd_agent(opts: AgentOptions<'_>) -> Result<()> {
    let name = opts
        .name
        .unwrap_or_else(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".to_string()));
    let jailer_opts = opts.jailer.then(|| JailerOptions {
        bin: PathBuf::from(opts.jailer_bin),
        uid: opts.jailer_uid,
        gid: opts.jailer_gid,
        chroot_base_dir: opts.chroot_base_dir.to_path_buf(),
        netns: opts.netns,
        daemonize: false,
    });
    let fc_config = FirecrackerConfig {
        bin: PathBuf::from(opts.firecracker_bin),
        vm_dir: opts.vm_dir.to_path_buf(),
        jailer: jailer_opts,
        ..FirecrackerConfig::default()
    };
    let kind = match opts.runtime {
        RuntimeChoice::Firecracker => RuntimeKind::Firecracker(FirecrackerRuntime::new(fc_config)),
        RuntimeChoice::Process => RuntimeKind::Process(ProcessRuntime),
        RuntimeChoice::Auto => auto_runtime(Path::new("/dev/kvm"), fc_config),
    };
    match &kind {
        RuntimeKind::Firecracker(_) => {
            println!("bigtop agent '{name}': microVM runtime (firecracker)");
        }
        RuntimeKind::Process(_) => {
            println!("bigtop agent '{name}': process runtime (dev/CI stand-in)");
        }
    }
    if opts.jailer {
        println!("bigtop agent '{name}': jailer sandboxing enabled");
    }
    println!("bigtop agent '{name}': registering with {}", opts.server);
    run_agent(AgentConfig::new(opts.server.to_string(), name), kind).await?;
    Ok(())
}

async fn cmd_run(job_file: &Path, server: &str) -> Result<()> {
    let text = std::fs::read_to_string(job_file)
        .with_context(|| format!("reading job file {}", job_file.display()))?;
    let file: JobFile = toml::from_str(&text).context("parsing job TOML")?;
    let spec = JobSpec {
        name: file.name,
        tasks: file.task,
    };
    let client = reqwest::Client::new();
    let response: bigtop_core::SubmitJobResponse = client
        .post(format!("{server}/v1/jobs"))
        .json(&spec)
        .send()
        .await
        .context("submitting job")?
        .error_for_status()
        .context("server rejected job")?
        .json()
        .await
        .context("reading submit response")?;
    println!("submitted job {}", response.job_id);
    Ok(())
}

async fn cmd_ps(server: &str) -> Result<()> {
    let tasks: Vec<bigtop_core::Task> = get_json(server, "/v1/tasks").await?;
    let [task_id, name, state, exit, node, ip_hdr, job] = PS_HEADERS;
    println!("{task_id:<26} {name:<18} {state:<10} {exit:<6} {node:<26} {ip_hdr:<15} {job}");
    for task in tasks {
        let node = task
            .assigned_node
            .as_ref()
            .map_or_else(|| "-".to_string(), ToString::to_string);
        let exit = task
            .exit_code
            .as_ref()
            .map_or_else(|| "-".to_string(), ToString::to_string);
        let ip = task
            .network
            .as_ref()
            .map_or_else(|| "-".to_string(), |n| n.ip.to_string());
        println!(
            "{:<26} {:<18} {:<10} {:<6} {:<26} {:<15} {}",
            truncate(task.id.as_ref(), 26),
            truncate(&task.name, 18),
            task.state,
            exit,
            truncate(&node, 26),
            ip,
            task.job_id,
        );
    }
    Ok(())
}

async fn cmd_nodes(server: &str) -> Result<()> {
    let nodes: Vec<bigtop_core::NodeInfo> = get_json(server, "/v1/nodes").await?;
    let now = Utc::now();
    let [node_id, name, alive_hdr, cpu_hdr, mem_hdr] = NODES_HEADERS;
    println!("{node_id:<26} {name:<16} {alive_hdr:<7} {cpu_hdr:<22} {mem_hdr}");
    for node in nodes {
        let alive = if node.is_alive(now) { "yes" } else { "no" };
        let cpu = format!(
            "{}/{} millicores",
            node.used.cpu_millis, node.total.cpu_millis
        );
        let mem = format!("{}/{} MiB", node.used.mem_mb, node.total.mem_mb);
        println!(
            "{:<26} {:<16} {:<7} {:<22} {}",
            truncate(node.id.as_ref(), 26),
            truncate(&node.name, 16),
            alive,
            cpu,
            mem,
        );
    }
    Ok(())
}

async fn cmd_logs(task_id: &str, server: &str) -> Result<()> {
    let logs: bigtop_core::LogLinesResponse =
        get_json(server, &format!("/v1/tasks/{task_id}/logs")).await?;
    for line in logs.lines {
        println!("{line}");
    }
    Ok(())
}

async fn cmd_snapshot_create(
    task_id: &str,
    kind: SnapshotKind,
    mem_path: Option<&str>,
    snap_path: Option<&str>,
    server: &str,
) -> Result<()> {
    let request = bigtop_core::RequestSnapshotRequest {
        snapshot_type: match kind {
            SnapshotKind::Full => SnapshotType::Full,
            SnapshotKind::Diff => SnapshotType::Diff,
        },
        mem_file_path: mem_path.map(str::to_string),
        snapshot_path: snap_path.map(str::to_string),
    };
    let client = reqwest::Client::new();
    let response: bigtop_core::RequestSnapshotResponse = client
        .post(format!("{server}/v1/tasks/{task_id}/snapshot"))
        .json(&request)
        .send()
        .await
        .context("contacting server")?
        .error_for_status()
        .context("server rejected snapshot request")?
        .json()
        .await
        .context("reading response")?;
    println!(
        "snapshot {} requested for task {task_id}",
        response.snapshot_id
    );
    Ok(())
}

async fn cmd_snapshot_list(task_id: &str, server: &str) -> Result<()> {
    let records: Vec<bigtop_core::SnapshotRecord> =
        get_json(server, &format!("/v1/tasks/{task_id}/snapshots")).await?;
    println!(
        "{:<26} {:<12} {:<26} {:<6} FILES",
        "SNAPSHOT ID", "STATE", "NODE", "TYPE"
    );
    for record in records {
        let kind = match record.spec.snapshot_type {
            SnapshotType::Full => "full",
            SnapshotType::Diff => "diff",
        };
        let files = format!(
            "{} + {}",
            truncate(&record.spec.mem_file_path, 30),
            truncate(&record.spec.snapshot_path, 30)
        );
        println!(
            "{:<26} {:<12} {:<26} {:<6} {}",
            truncate(record.id.as_ref(), 26),
            format!("{:?}", record.state).to_lowercase(),
            truncate(record.node_id.as_ref(), 26),
            kind,
            files,
        );
    }
    Ok(())
}

async fn cmd_snapshot_restore(task_id: &str, snapshot_id: &str, server: &str) -> Result<()> {
    let tasks: Vec<bigtop_core::Task> = get_json(server, "/v1/tasks").await?;
    let task = tasks
        .iter()
        .find(|task| task.id.as_ref() == task_id)
        .with_context(|| format!("task {task_id} not found"))?;
    let records: Vec<bigtop_core::SnapshotRecord> =
        get_json(server, &format!("/v1/tasks/{task_id}/snapshots")).await?;
    let record = records
        .iter()
        .find(|record| record.id.as_ref() == snapshot_id)
        .with_context(|| format!("snapshot {snapshot_id} not found for task {task_id}"))?;
    anyhow::ensure!(
        record.state == SnapshotState::Done,
        "snapshot {snapshot_id} is {:?}, not done",
        record.state
    );
    let spec = record.restore_task_spec(task);
    let job = JobSpec {
        name: format!("restore-{}", task.name),
        tasks: vec![spec],
    };
    let client = reqwest::Client::new();
    let response: bigtop_core::SubmitJobResponse = client
        .post(format!("{server}/v1/jobs"))
        .json(&job)
        .send()
        .await
        .context("contacting server")?
        .error_for_status()
        .context("server rejected restore job")?
        .json()
        .await
        .context("reading response")?;
    println!(
        "restoring snapshot {snapshot_id} as job {}",
        response.job_id
    );
    Ok(())
}

async fn get_json<T>(server: &str, path: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let client = reqwest::Client::new();
    client
        .get(format!("{server}{path}"))
        .send()
        .await
        .context("contacting server")?
        .error_for_status()
        .context("server error")?
        .json()
        .await
        .context("reading response")
}

/// Truncate to `max_chars`, with an ellipsis when cut.
fn truncate(s: &str, max_chars: usize) -> String {
    let mut out: String = s.chars().take(max_chars).collect();
    if s.chars().count() > max_chars {
        out.push('…');
    }
    out
}
