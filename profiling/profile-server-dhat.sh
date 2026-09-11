#!/usr/bin/env bash
# Run the BigTop server under DHAT while load.sh hammers the API.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v valgrind >/dev/null 2>&1; then
    echo "error: valgrind is not installed (try: apt install valgrind)" >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl is not installed" >&2
    exit 1
fi

echo "==> building server (release)"
cargo build --release -p bigtop

PORT=4667
echo "==> starting server under dhat"
valgrind --tool=dhat --log-file=profiling/dhat.log \
    ./target/release/bigtop server --port "$PORT" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

echo "==> waiting for server"
for _ in $(seq 1 50); do
    if curl -sf "http://127.0.0.1:$PORT/v1/nodes" >/dev/null; then
        break
    fi
    sleep 0.2
done

echo "==> applying load (~35s)"
profiling/load.sh "$PORT"

echo "==> stopping server"
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
trap - EXIT

echo "==> dhat tail:"
tail -n 40 profiling/dhat.log
if [ -f dh_view.html ]; then
    echo "==> open dh_view.html in a browser for the allocation-site breakdown"
else
    echo "==> (no dh_view.html generated; text summary above is the result)"
fi
