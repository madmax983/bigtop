//! `bigtop`: one binary. Server, agent, and CLI for the microVM orchestrator.

use anyhow::{Context, Result};
use bigtop_agent::{
    auto_runtime, run_agent, AgentConfig, FirecrackerConfig, FirecrackerRuntime, ProcessRuntime,
    RuntimeKind,
};
use bigtop_core::{JobSpec, TaskSpec};
use bigtop_server::serve;
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Default server URL for CLI commands.
const DEFAULT_SERVER: &str = "http://127.0.0.1:4667";

/// Column headers for `bigtop ps`.
const PS_HEADERS: [&str; 6] = ["TASK ID", "NAME", "STATE", "EXIT", "NODE", "JOB"];
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
        Commands::Server { port, bind } => cmd_server(&bind, port).await,
        Commands::Agent {
            server,
            name,
            runtime,
            vm_dir,
            firecracker_bin,
        } => cmd_agent(&server, name, runtime, &vm_dir, &firecracker_bin).await,
        Commands::Run { job_file, server } => cmd_run(&job_file, &server).await,
        Commands::Ps { server } => cmd_ps(&server).await,
        Commands::Nodes { server } => cmd_nodes(&server).await,
        Commands::Logs { task_id, server } => cmd_logs(&task_id, &server).await,
    }
}

async fn cmd_server(bind: &str, port: u16) -> Result<()> {
    let addr: std::net::SocketAddr = format!("{bind}:{port}")
        .parse()
        .context("invalid bind address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("binding server socket")?;
    println!("bigtop server: listening on {addr} (loud and proud)");
    serve(listener).await?;
    Ok(())
}

async fn cmd_agent(
    server: &str,
    name: Option<String>,
    runtime: RuntimeChoice,
    vm_dir: &Path,
    firecracker_bin: &str,
) -> Result<()> {
    let name =
        name.unwrap_or_else(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".to_string()));
    let fc_config = FirecrackerConfig {
        bin: PathBuf::from(firecracker_bin),
        vm_dir: vm_dir.to_path_buf(),
        ..FirecrackerConfig::default()
    };
    let kind = match runtime {
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
    println!("bigtop agent '{name}': registering with {server}");
    run_agent(AgentConfig::new(server.to_string(), name), kind).await?;
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
    let [task_id, name, state, exit, node, job] = PS_HEADERS;
    println!("{task_id:<26} {name:<18} {state:<10} {exit:<6} {node:<26} {job}");
    for task in tasks {
        let node = task
            .assigned_node
            .as_ref()
            .map_or_else(|| "-".to_string(), ToString::to_string);
        let exit = task
            .exit_code
            .as_ref()
            .map_or_else(|| "-".to_string(), ToString::to_string);
        println!(
            "{:<26} {:<18} {:<10} {:<6} {:<26} {}",
            truncate(task.id.as_ref(), 26),
            truncate(&task.name, 18),
            task.state,
            exit,
            truncate(&node, 26),
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
