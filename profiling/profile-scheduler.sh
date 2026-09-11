#!/usr/bin/env bash
# Profile the scheduler hot path with callgrind.
#
# Builds the criterion scheduler bench in release, runs the bench binary
# under valgrind --tool=callgrind, and prints an annotated summary.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v valgrind >/dev/null 2>&1; then
    echo "error: valgrind is not installed (try: apt install valgrind)" >&2
    exit 1
fi

echo "==> building scheduler bench (release)"
cargo build --release --bench scheduler -p bigtop-server

BENCH_BIN="$(ls -t target/release/deps/scheduler-* | grep -v '\.d$' | head -n 1)"
echo "==> bench binary: $BENCH_BIN"
echo "==> running under callgrind (shortened criterion run to keep this sane)"
valgrind --tool=callgrind \
    --callgrind-out-file=profiling/callgrind.out \
    "$BENCH_BIN" --bench \
    --warm-up-time 0.2 --measurement-time 1 --sample-size 20

echo "==> hottest functions:"
callgrind_annotate --auto=yes profiling/callgrind.out 2>/dev/null | head -n 40
echo "==> full graph: kcachegrind profiling/callgrind.out"
