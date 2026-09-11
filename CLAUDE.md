# CLAUDE.md — BigTop contributor notes

BigTop v0.1 "spark": a Firecracker-first orchestrator in Rust. One binary,
four crates. Read `SPEC.md` before touching behavior.

## Crates

- `bigtop-core` — domain types (`JobId`/`TaskId`/`NodeId` newtypes, never
  bare strings), wire DTOs, `thiserror` error enum.
- `bigtop-server` — axum REST API (`src/api.rs`), in-memory store
  (`src/state.rs`), scheduler (`src/scheduler.rs`, 500 ms tick).
- `bigtop-agent` — registration, 2 s heartbeat, 1 s assignment poll,
  `Runtime` trait with `ProcessRuntime` (dev/CI) and `FirecrackerRuntime`
  (real microVMs via the Firecracker REST API over a Unix socket).
- `bigtop` — the CLI binary (`anyhow` here, `thiserror` in libraries).

## Rules

- Rust standards per the project philosophy: `cargo fmt`, Clippy
  **pedantic + nursery** with zero warnings, no `unwrap`/`expect` in
  non-test code, Tokio for async.
- Terminal task states (`Succeeded`/`Failed`) are final — the server
  rejects transitions out of them with `409`.
- Scheduler accounting is recomputed from live tasks every tick
  (self-healing); never cache `used` resources across ticks.
- `#[serde(default)]` on `TaskSpec.vm` needs `Default for VmSpec` — keep
  the impl if you touch `VmSpec`.
- Firecracker changes need `/dev/kvm` to verify end to end; the fake-API
  socket tests in `firecracker.rs` cover the HTTP plumbing only. Say so
  honestly in commit messages and docs.

## Commands

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_HTTP_MULTIPLEXING=false   # sandbox proxy stalls on HTTP/2
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic -W clippy::nursery
cargo test --workspace
```

## Profiling

See `PROFILING.md`. Criterion bench: `cargo bench -p bigtop-server`.
Callgrind/DHAT scripts: `profiling/`. Abrash rules: measure first,
optimize only measured hot paths.
