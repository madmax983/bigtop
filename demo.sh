#!/usr/bin/env bash
# BigTop v0.6 live demo: server (with persistence + bearer auth, served
# through Autumn, plus the Harvest snapshot shadow) + agent (process
# runtime) + jobs. Exercises the v0.6 surface: --data-dir journaling,
# service discovery, /metrics, the status page, the bearer-token control
# plane, OpenAPI, the MCP allowlist, and the opt-in Harvest shadow
# (--harvest-shadow) auditing a process-runtime snapshot to an `agree`
# verdict.
# Usage: demo.sh — leaves server/agent running; kill them when done.
set -euo pipefail
cd "$(dirname "$0")"
BIN=./target/debug/bigtop
PORT=4667
SERVER="http://127.0.0.1:$PORT"
DATA_DIR=./demo-data
rm -rf "$DATA_DIR"
# v0.5: one bearer token for the whole control plane. The server, agent,
# CLI, and curl calls below all read it from BIGTOP_API_TOKEN.
export BIGTOP_API_TOKEN="demo-token-$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
AUTH=(-H "Authorization: Bearer $BIGTOP_API_TOKEN")

"$BIN" server --port "$PORT" --data-dir "$DATA_DIR" --harvest-shadow "$DATA_DIR/shadow.db" > demo-server.log 2>&1 &
SERVER_PID=$!
"$BIN" agent --server "$SERVER" --name demo-node --runtime process > demo-agent.log 2>&1 &
AGENT_PID=$!
echo "server pid $SERVER_PID, agent pid $AGENT_PID"

for _ in $(seq 1 50); do
    if "$BIN" nodes --server "$SERVER" 2>/dev/null | grep -q demo-node; then
        break
    fi
    sleep 0.2
done

echo '=== bigtop nodes ==='
"$BIN" nodes --server "$SERVER"
echo '=== bigtop run examples/hello.toml ==='
"$BIN" run examples/hello.toml --server "$SERVER"
sleep 3
echo '=== bigtop ps (should show running) ==='
"$BIN" ps --server "$SERVER"
TASK_ID=$(curl -s "${AUTH[@]}" "$SERVER/v1/tasks" | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")
echo '=== shadow: snapshot the running task (process runtime fails fast) ==='
"$BIN" snapshot create "$TASK_ID" --server "$SERVER"
echo '=== waiting for the agent report + shadow audit (1s driver, 5s polls) ==='
sleep 15
echo '=== GET /v1/shadow/snapshots (expect verdict "agree") ==='
curl -s "${AUTH[@]}" "$SERVER/v1/shadow/snapshots" | python3 -c "
import json, sys
tracks = json.load(sys.stdin)
assert tracks, 'no shadow tracks recorded'
for t in tracks:
    print(t['snapshot_id'], '->', t['verdict'], '| observations:', len(t['observations']))
bad = [t for t in tracks if t['verdict'] != 'agree']
if bad:
    print('FAIL: non-agree verdicts:', [(t['snapshot_id'], t['verdict']) for t in bad], file=sys.stderr)
    sys.exit(1)
print('shadow verdict: agree')
"
echo '=== shadow metrics ==='
curl -s "${AUTH[@]}" "$SERVER/metrics" | grep -E "^bigtop_harvest_shadow"
echo '=== unauthenticated request is rejected (401) ==='
curl -s -o /dev/null -w "%{http_code}\n" "$SERVER/v1/nodes"
echo '=== authenticated: list nodes ==='
curl -s "${AUTH[@]}" "$SERVER/v1/nodes" | head -c 200; echo
echo "=== bigtop logs $TASK_ID ==="
"$BIN" logs "$TASK_ID" --server "$SERVER"
echo '=== bigtop run examples/service.toml ==='
"$BIN" run examples/service.toml --server "$SERVER"
sleep 3
echo '=== bigtop services (should show db + api) ==='
"$BIN" services --server "$SERVER"
echo '=== GET /metrics ==='
curl -s "${AUTH[@]}" "$SERVER/metrics" | grep -E "^bigtop_(tasks|nodes_up|ipam_allocated)"
echo '=== GET / (status page) ==='
curl -s "${AUTH[@]}" "$SERVER/" | grep -o "<title>[^<]*</title>"
echo '=== GET /openapi.json (behind the token, like everything else) ==='
curl -s "${AUTH[@]}" "$SERVER/openapi.json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['info']['title'], d['info']['version'], len(d['paths']), 'paths')"
echo '=== MCP allowlist (tools/list over Streamable HTTP) ==='
# autumn-web 0.7 serves /mcp stateless: no mcp-session-id header is issued.
# Grab one if a session-capable build ever sends it, but never fail on it.
SID=$(curl -s -D - "${AUTH[@]}" -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"demo","version":"0"}}}' "$SERVER/mcp" -o /tmp/mcp-init.txt | grep -i '^mcp-session-id:' | tr -d '\r' | awk '{print $2}' || true)
SID_HDR=()
[ -n "$SID" ] && SID_HDR=(-H "mcp-session-id: $SID")
curl -s "${AUTH[@]}" -H 'Content-Type: application/json' "${SID_HDR[@]:-}" -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' "$SERVER/mcp" -o /dev/null -w "%{http_code}\n"
curl -s "${AUTH[@]}" -H 'Content-Type: application/json' "${SID_HDR[@]:-}" -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' "$SERVER/mcp" | grep -o '"name":"[a-z_]*"' | sort | tr '\n' ' '; echo
echo '=== waiting for tasks to finish (sleep 30) ==='
sleep 35
echo '=== bigtop ps (should show succeeded) ==='
"$BIN" ps --server "$SERVER"
echo '=== journal has ops ==='
wc -l "$DATA_DIR/journal.jsonl"
echo '=== clean shutdown (SIGTERM) compacts the journal ==='
kill -TERM "$SERVER_PID"
for _ in $(seq 1 50); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.2
done
ls -la "$DATA_DIR/snapshot.json"
echo '=== restart on the same data dir: state is remembered ==='
"$BIN" server --port "$PORT" --data-dir "$DATA_DIR" > demo-server2.log 2>&1 &
SERVER_PID=$!
sleep 1
"$BIN" ps --server "$SERVER" | grep -q succeeded && echo "tasks survived the restart"
echo "demo done; server=$SERVER_PID agent=$AGENT_PID still running"
