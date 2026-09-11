#!/usr/bin/env bash
# BigTop v0.1 live demo: server + agent (process runtime) + hello job.
# Usage: demo.sh — leaves server/agent running; kill them when done.
set -euo pipefail
cd "$(dirname "$0")"
BIN=./target/debug/bigtop
PORT=4667
SERVER="http://127.0.0.1:$PORT"

"$BIN" server --port "$PORT" > demo-server.log 2>&1 &
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
echo '=== waiting for tasks to finish (sleep 30) ==='
sleep 35
echo '=== bigtop ps (should show succeeded) ==='
"$BIN" ps --server "$SERVER"
echo "demo done; server=$SERVER_PID agent=$AGENT_PID still running"
