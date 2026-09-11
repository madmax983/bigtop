# BigTop profiling

Measure first, never guess. The scheduler tick is the hot path (it scans
every node and every pending task); the agent poll loop is second. The
server's HTTP handlers are thin and should stay that way.

## What's here

| Tool | Target | How |
|---|---|---|
| `criterion` bench `scheduler` | scheduler tick throughput | `cargo bench -p bigtop-server` |
| `profiling/profile-scheduler.sh` | scheduler under callgrind | builds bench, runs valgrind, annotates |
| `profiling/profile-server-dhat.sh` | server heap under DHAT | runs server under valgrind while `profiling/load.sh` hammers it |
| `profiling/load.sh` | synthetic API load | registers a node, submits 10×20 tasks, polls `/v1/tasks` ~30 s |

## Commands

```bash
# Scheduler throughput (native)
cargo bench -p bigtop-server

# Scheduler under callgrind (needs valgrind)
./profiling/profile-scheduler.sh
# then: kcachegrind profiling/callgrind.out

# Server heap under DHAT (needs valgrind)
./profiling/profile-server-dhat.sh
# then: open dh_view.html (written by DHAT on server exit) in a browser,
#       or read the text summary at the tail of profiling/dhat.log
```

## Reading results

- **callgrind**: `callgrind_annotate --auto=yes profiling/callgrind.out`
  (the script prints the top already); `kcachegrind`/`qcachegrind` for the
  full call graph. Look for the hottest function *below* the criterion
  harness — that's the code to optimize.
- **DHAT**: the text summary shows total bytes allocated and, more
  importantly, *bytes read/written* and peak live heap. `dh_view.html`
  breaks it down by allocation site. Suspects in this codebase: per-tick
  `Vec`/`HashMap` allocations in the scheduler, log line churn.

## Performance budget (v0.1 baselines — to be measured, not claimed)

| Metric | Target | Status |
|---|---|---|
| Scheduler decisions | ≥ 10k task-assignment decisions/sec (bench machine) | _to be measured_ |
| Server API p99 latency | < 5 ms in-process (submit/list on localhost) | _to be measured_ |
| Scheduler tick (100 nodes / 1k tasks) | < 10 ms | _to be measured_ |
| Agent poll loop overhead | negligible vs. 1 s poll interval | by inspection |

Baselines get recorded here once measured on a fixed machine. Numbers move
with hardware; always note the machine when updating them.

## v0.1 baseline numbers

_Unmeasured in this environment._ The sandbox VM runs with load averages
above 20 from other tenants' builds, so any timing taken here would be
noise, not a baseline — recording it would be worse than recording
nothing. Run this on a quiet machine and paste the results:

```bash
cargo bench -p bigtop-server
```

| Metric | Result |
|---|---|
| scheduler tick, 100 nodes / 1,000 tasks | _unmeasured_ |
| scheduler tick, 1,000 nodes / 10,000 tasks | _unmeasured_ |

The Callgrind (`profiling/profile-scheduler.sh`) and DHAT
(`profiling/profile-server-dhat.sh`) scripts ship with the repo but were
not exercised here: valgrind under this much contention would take hours
and the profiles would reflect the neighbors, not BigTop.
