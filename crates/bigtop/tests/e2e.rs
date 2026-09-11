//! End-to-end: an in-process server plus an agent on the process runtime
//! run a 2-task job all the way to `Succeeded`, with logs.

use bigtop_agent::{run_agent, AgentConfig, ProcessRuntime, RuntimeKind};
use bigtop_core::{JobSpec, Resources, SnapshotPolicy, TaskSpec, TaskState, VmSpec};
use std::collections::HashMap;
use std::time::Duration;

fn echo_spec() -> TaskSpec {
    TaskSpec {
        name: "echo".to_string(),
        command: "sh".to_string(),
        args: vec!["-c".to_string(), "echo hello-from-$BIGTOP_TASK".to_string()],
        env: HashMap::new(),
        resources: Resources {
            cpu_millis: 10,
            mem_mb: 16,
        },
        count: 2,
        vm: VmSpec {
            kernel_image: String::new(),
            rootfs: String::new(),
            vcpu_count: 1,
            mem_mb: 128,
            boot_args: None,
            boot_snapshot: None,
        },
        node_affinity: None,
        snapshot_policy: SnapshotPolicy::None,
    }
}

#[tokio::test]
async fn two_task_job_runs_to_succeeded() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind server");
    let port = listener.local_addr().expect("local addr").port();
    let server_url = format!("http://127.0.0.1:{port}");

    let server = tokio::spawn(bigtop_server::serve_with_tick(
        listener,
        Duration::from_millis(50),
    ));
    let mut agent_config = AgentConfig::new(server_url.clone(), "e2e-agent".to_string());
    agent_config.heartbeat_interval = Duration::from_millis(200);
    agent_config.poll_interval = Duration::from_millis(200);
    let agent = tokio::spawn(run_agent(
        agent_config,
        RuntimeKind::Process(ProcessRuntime),
    ));
    let client = reqwest::Client::new();

    // Wait for the agent to register.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let nodes: Vec<bigtop_core::NodeInfo> = client
                .get(format!("{server_url}/v1/nodes"))
                .send()
                .await
                .expect("nodes request")
                .json()
                .await
                .expect("nodes json");
            if !nodes.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("node registration");

    // Submit a 2-task job.
    let spec = JobSpec {
        name: "e2e".to_string(),
        tasks: vec![echo_spec()],
    };
    let submitted: bigtop_core::SubmitJobResponse = client
        .post(format!("{server_url}/v1/jobs"))
        .json(&spec)
        .send()
        .await
        .expect("submit request")
        .json()
        .await
        .expect("submit json");
    assert!(submitted.job_id.to_string().starts_with("job-"));

    // Wait for both tasks to succeed.
    let tasks: Vec<bigtop_core::Task> = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let tasks: Vec<bigtop_core::Task> = client
                .get(format!("{server_url}/v1/tasks"))
                .send()
                .await
                .expect("tasks request")
                .json()
                .await
                .expect("tasks json");
            if tasks.len() == 2 && tasks.iter().all(|t| t.state == TaskState::Succeeded) {
                break tasks;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("tasks reached Succeeded");

    // Logs carry the real task output, including the injected env var.
    for task in &tasks {
        assert_eq!(task.exit_code, Some(0));
        let logs: bigtop_core::LogLinesResponse = client
            .get(format!("{server_url}/v1/tasks/{}/logs", task.id))
            .send()
            .await
            .expect("logs request")
            .json()
            .await
            .expect("logs json");
        assert!(
            logs.lines.iter().any(|l| l.contains("hello-from-task-")),
            "expected task output in logs, got: {:?}",
            logs.lines
        );
    }

    server.abort();
    agent.abort();
}
