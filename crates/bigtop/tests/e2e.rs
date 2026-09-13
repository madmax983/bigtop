//! End-to-end: an in-process server plus an agent on the process runtime
//! run a 2-task job all the way to `Succeeded`, with logs.

use bigtop_agent::{run_agent, AgentConfig, ProcessRuntime, RuntimeKind};
use bigtop_core::{
    JobSpec, NetworkSpec, Resources, ServiceSpec, SnapshotPolicy, TaskSpec, TaskState, VmSpec,
};
use std::collections::HashMap;
use std::time::Duration;

/// Port allocator for the e2e servers: one probe for the whole test
/// binary, then sequential ports. The `#[tokio::test]`s run in parallel
/// threads and must never share a port — and Autumn owns the listener and
/// `process::exit(1)`s on a bind conflict, so a collision would kill the
/// test binary outright, not just fail one test. A single probe keeps the
/// probe-drop race to one tiny window instead of one per test.
fn next_test_port() -> u16 {
    static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let base = *BASE.get_or_init(|| {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("probe bind")
            .local_addr()
            .expect("probe addr")
            .port()
    });
    base.wrapping_add(NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
}

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
        network: NetworkSpec::default(),
        service: ServiceSpec::default(),
    }
}

#[tokio::test(flavor = "local")]
async fn two_task_job_runs_to_succeeded() {
    // v0.5: the server owns its listener (binds the configured port), so
    // the test takes a port from the allocator and hands it to the server
    // via `ServerConfig`. Auth is on everywhere, including here.
    //
    // `serve_config`'s future is not `Send` (it holds Autumn's
    // config-loader future across an await), so the test runs on the
    // `local` flavor (a LocalRuntime) and uses `spawn_local`.
    const TOKEN: &str = "e2e-test-token";
    let port = next_test_port();
    let server_url = format!("http://127.0.0.1:{port}");

    let server =
        tokio::task::spawn_local(bigtop_server::serve_config(bigtop_server::ServerConfig {
            bind_host: "127.0.0.1".to_string(),
            bind_port: port,
            tick_interval: Duration::from_millis(50),
            api_token: Some("e2e-test-token".to_string()),
            network_cidr: "172.28.0.0/16".to_string(),
            data_dir: None,
            harvest_shadow: None,
        }));
    let mut agent_config = AgentConfig::new(server_url.clone(), "e2e-agent".to_string());
    agent_config.heartbeat_interval = Duration::from_millis(200);
    agent_config.poll_interval = Duration::from_millis(200);
    agent_config.api_token = Some(TOKEN.to_string());
    let agent = tokio::spawn(run_agent(
        agent_config,
        RuntimeKind::Process(ProcessRuntime),
    ));
    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {TOKEN}").parse().expect("token header"),
            );
            headers
        })
        .build()
        .expect("client");

    // Wait for the agent to register.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            // v0.5: Autumn owns the listener and binds it asynchronously
            // inside `app.run()`; the first requests can arrive before the
            // bind lands, so a refused connection just retries.
            let Ok(resp) = client.get(format!("{server_url}/v1/nodes")).send().await else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            // Regression guard for the v0.5 strangler: the typed `/v1`
            // handlers take `Extension<AppState>` from the scope's layer
            // stack. If that layer is missing, the handlers compile but
            // every request fails at runtime — this 200 is the proof the
            // state actually arrives.
            assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET /v1/nodes");
            let nodes: Vec<bigtop_core::NodeInfo> = resp.json().await.expect("nodes json");
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
    let resp = client
        .post(format!("{server_url}/v1/jobs"))
        .json(&spec)
        .send()
        .await
        .expect("submit request");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "POST /v1/jobs");
    let submitted: bigtop_core::SubmitJobResponse = resp.json().await.expect("submit json");
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

/// Spawn a server with auth on and wait until it answers on `/metrics`.
/// Must be called from a `local`-flavor test (the server future is not
/// `Send`, so it uses `spawn_local`).
async fn spawn_authed_server(
    token: &str,
) -> (
    u16,
    tokio::task::JoinHandle<Result<(), bigtop_server::ServerError>>,
) {
    let port = next_test_port();
    let server =
        tokio::task::spawn_local(bigtop_server::serve_config(bigtop_server::ServerConfig {
            bind_host: "127.0.0.1".to_string(),
            bind_port: port,
            api_token: Some(token.to_string()),
            tick_interval: Duration::from_millis(500),
            network_cidr: "172.28.0.0/16".to_string(),
            data_dir: None,
            harvest_shadow: None,
        }));
    let authed = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {token}").parse().expect("token header"),
            );
            headers
        })
        .build()
        .expect("client");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            // Fail fast if the server task died (e.g. panicked during
            // startup) instead of hanging until the timeout. Note: Autumn
            // owns the listener and `process::exit(1)`s on a bind
            // conflict, which would kill the test binary outright — the
            // port allocator above exists to make that impossible between
            // tests, and negligible otherwise.
            assert!(!server.is_finished(), "server task died during startup");
            if let Ok(resp) = authed
                .get(format!("http://127.0.0.1:{port}/metrics"))
                .send()
                .await
            {
                if resp.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("server ready");
    (port, server)
}

/// v0.5: every control-plane route — including the status page, metrics,
/// and the MCP envelope — requires the bearer token.
#[tokio::test(flavor = "local")]
async fn control_plane_rejects_unauthenticated_requests() {
    const TOKEN: &str = "e2e-auth-token";
    let (port, server) = spawn_authed_server(TOKEN).await;
    let base = format!("http://127.0.0.1:{port}");
    let plain = reqwest::Client::new();
    let wrong = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                "Bearer wrong-token".parse().expect("token header"),
            );
            headers
        })
        .build()
        .expect("client");

    for path in [
        "/",
        "/metrics",
        "/v1/nodes",
        "/v1/tasks",
        "/openapi.json",
        "/swagger-ui",
    ] {
        let status = plain
            .get(format!("{base}{path}"))
            .send()
            .await
            .expect("request")
            .status();
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED, "GET {path}");
        let status = wrong
            .get(format!("{base}{path}"))
            .send()
            .await
            .expect("request")
            .status();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "GET {path} with wrong token"
        );
    }
    // The API docs are part of the control plane: with a valid token they
    // serve normally.
    let authed = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {TOKEN}").parse().expect("token header"),
            );
            headers
        })
        .build()
        .expect("client");
    let status = authed
        .get(format!("{base}/openapi.json"))
        .send()
        .await
        .expect("openapi request")
        .status();
    assert_eq!(status, reqwest::StatusCode::OK, "GET /openapi.json");
    // The MCP envelope is gated too.
    let mcp_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "e2e", "version": "0" },
        },
    });
    let status = plain
        .post(format!("{base}/mcp"))
        .json(&mcp_body)
        .send()
        .await
        .expect("mcp request")
        .status();
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED, "POST /mcp");

    server.abort();
}

/// Helper for MCP JSON-RPC calls in tests.
async fn mcp_rpc(
    client: &reqwest::Client,
    url: &str,
    session: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    let mut req = client.post(url).json(&body);
    if let Some(session) = session {
        req = req.header("mcp-session-id", session);
    }
    req.send().await.expect("mcp rpc")
}

/// Parse an MCP Streamable-HTTP response body: SSE (`data: ` lines) or
/// plain JSON.
fn parse_mcp_payload(body: &str) -> serde_json::Value {
    if body.contains("data: ") {
        let last = body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .next_back()
            .expect("sse data line");
        serde_json::from_str(last).expect("sse json")
    } else {
        serde_json::from_str(body).expect("json body")
    }
}

/// Extract sorted tool names from a `tools/list` payload.
fn mcp_tool_names(payload: &serde_json::Value) -> Vec<String> {
    let mut tools: Vec<String> = payload
        .pointer("/result/tools")
        .expect("result.tools")
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| {
            t.pointer("/name")
                .expect("tool name")
                .as_str()
                .expect("name str")
                .to_string()
        })
        .collect();
    tools.sort();
    tools
}

/// Verify an authenticated `tools/call` dispatches through the real
/// router: the typed handler runs with its `Extension<AppState>` and
/// answers. This is the MCP half of the v0.5 strangler regression guard.
async fn assert_tools_call_ok(
    authed: &reqwest::Client,
    mcp: &str,
    session: Option<&str>,
    tool: &str,
) {
    let called = mcp_rpc(
        authed,
        mcp,
        session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": { "name": tool, "arguments": {} },
        }),
    )
    .await;
    assert_eq!(
        called.status(),
        reqwest::StatusCode::OK,
        "tools/call {tool}"
    );
    let body = called.text().await.expect("tools/call body");
    // Streamable HTTP may answer SSE or plain JSON.
    let payload = parse_mcp_payload(&body);
    assert!(
        payload.pointer("/result").is_some() && payload.pointer("/error").is_none(),
        "tools/call {tool} returned a result, got: {payload}"
    );
}

/// A tokenless `tools/call` is rejected at the envelope — auth cannot
/// be bypassed by skipping `initialize`.
async fn assert_tokenless_tools_call_rejected(plain: &reqwest::Client, mcp: &str) {
    let status = plain
        .post(mcp)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": { "name": "list_jobs", "arguments": {} },
        }))
        .send()
        .await
        .expect("tokenless tools/call")
        .status();
    assert_eq!(
        status,
        reqwest::StatusCode::UNAUTHORIZED,
        "tokenless tools/call"
    );
}

/// v0.5: exactly the ten deliberate tools are exposed over MCP — nine
/// read-only, plus `submit_job`, which is an explicit, documented,
/// bearer-authenticated mutation (not a smuggled one). No other mutating
/// routes, no HTML status page, no metrics.
#[tokio::test(flavor = "local")]
async fn mcp_exposes_only_the_allowlist() {
    const TOKEN: &str = "e2e-mcp-token";
    let (port, server) = spawn_authed_server(TOKEN).await;
    let mcp = format!("http://127.0.0.1:{port}/mcp");
    let plain = reqwest::Client::new();
    let authed = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {TOKEN}").parse().expect("token header"),
            );
            headers
        })
        .build()
        .expect("client");

    // initialize.
    let init = mcp_rpc(
        &authed,
        &mcp,
        None,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "e2e", "version": "0" },
            },
        }),
    )
    .await;
    assert_eq!(init.status(), reqwest::StatusCode::OK, "initialize");
    // Autumn 0.7's MCP endpoint is stateless: it answers 200 with no
    // `mcp-session-id`, and the follow-up calls simply omit the header too.
    let session: Option<String> = init
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // initialized notification.
    let notified = mcp_rpc(
        &authed,
        &mcp,
        session.as_deref(),
        serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    assert!(
        notified.status().is_success(),
        "initialized: {}",
        notified.status()
    );

    // tools/list -> the allowlist, nothing else.
    let listed = mcp_rpc(
        &authed,
        &mcp,
        session.as_deref(),
        serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
    )
    .await;
    assert_eq!(listed.status(), reqwest::StatusCode::OK, "tools/list");
    let body = listed.text().await.expect("tools/list body");
    // Streamable HTTP may answer SSE or plain JSON.
    let payload = parse_mcp_payload(&body);
    let tools = mcp_tool_names(&payload);
    let expected = [
        "assignments",
        "get_logs",
        "list_jobs",
        "list_nodes",
        "list_services",
        "list_snapshots",
        "list_tasks",
        "overlay_peers",
        "shadow_verdicts",
        "snapshot_requests",
        "submit_job",
    ];
    assert_eq!(tools, expected, "MCP tool allowlist");

    // A tokenless `tools/call` is rejected at the envelope — auth cannot
    // be bypassed by skipping `initialize`.
    assert_tokenless_tools_call_rejected(&plain, &mcp).await;

    // An authenticated `tools/call` dispatches through the real router: the
    // typed handler runs with its `Extension<AppState>` and answers. This
    // is the MCP half of the v0.5 strangler regression guard.
    assert_tools_call_ok(&authed, &mcp, session.as_deref(), "list_nodes").await;

    server.abort();
}
