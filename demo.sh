#!/usr/bin/env bash
# BigTop v0.4 live demo: server (with persistence) + agent (process
# runtime) + jobs. Exercises the v0.4 surface: --data-dir journaling,
# service discovery, /metrics, and the status page.
# Usage: demo.sh — leaves server/agent running; kill them when done.
set -euo pipefail
cd "$(dirname "$0")"
BIN=./target/debug/bigtop
PORT=4667
SERVER="http://127.0.0.1:$PORT"
DATA_DIR=./demo-data
rm -rf "$DATA_DIR"

"$BIN" server --port "$PORT" --data-dir "$DATA_DIR" > demo-server.log 2>&1 &
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
TASK_ID=$(curl -s "$SERVER/v1/tasks" | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")
echo "=== bigtop logs $TASK_ID ==="
"$BIN" logs "$TASK_ID" --server "$SERVER"
echo '=== bigtop run examples/service.toml ==='
"$BIN" run examples/service.toml --server "$SERVER"
sleep 3
echo '=== bigtop services (should show db + api) ==='
"$BIN" services --server "$SERVER"
echo '=== GET /metrics ==='
curl -s "$SERVER/metrics" | grep -E "^bigtop_(tasks|nodes_up|ipam_allocated)"
echo '=== GET / (status page) ==='
curl -s "$SERVER/" | grep -o "<title>[^<]*</title>"
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
