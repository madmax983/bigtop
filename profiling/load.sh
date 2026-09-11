#!/usr/bin/env bash
# Synthetic API load for DHAT profiling: register a node, submit jobs,
# hammer /v1/tasks for ~30s. Usage: load.sh [port]
set -euo pipefail
PORT="${1:-4667}"
BASE="http://127.0.0.1:$PORT"

curl -sf -X POST "$BASE/v1/nodes/register" \
    -H 'Content-Type: application/json' \
    -d '{"name":"loadgen","addr":"127.0.0.1","total":{"cpu_millis":16000,"mem_mb":32768}}' \
    >/dev/null
echo "registered loadgen node"

for i in $(seq 1 10); do
    curl -sf -X POST "$BASE/v1/jobs" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"load-$i\",\"tasks\":[{\"name\":\"spin\",\"command\":\"true\",\"count\":20}]}" \
        >/dev/null &
done
wait
echo "submitted 10 jobs x 20 tasks"

end=$((SECONDS + 30))
while [ "$SECONDS" -lt "$end" ]; do
    curl -sf "$BASE/v1/tasks" >/dev/null
    sleep 0.1
done
echo "done polling"
