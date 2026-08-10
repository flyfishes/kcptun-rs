<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-03 -->

# bench

## Purpose

Throughput and CPU-profile tooling for Rust vs Go kcptun. Go-compatible pprof profiling (CPU, heap, goroutine/deadlock), Go pprof export, and captured artifacts under `profiles/`.

## Key Files

| File | Description |
|------|-------------|
| `run_bench.sh` | Bench orchestration; labels are **Client → Server** (bulk stream direction) |
| `throughput.py` | Throughput measurement (loadgen → client listen port → server → echo) |
| `run_p99.sh` | Raw KCP layer P99/P999 cross-test (7 combos, open model) |
| `tunnel_p99.sh` | **Full tunnel-stack** P99/P999 test orchestrator with Go kcptun comparison, network impairment injection, payload/RPS sweep |
| `tunnel_latency.rs` | Rust example for tunnel-stack latency (crypto + KCP + SMUX + Snappy) with open-model fixed-rate measurement |
| `TUNNEL_TEST_MATRIX.md` | Test matrix documentation with TC-01 through TC-10 + Go comparison cases |
| `REPORT_TEMPLATE.md` | P99/P999 tunnel test report template with diagnostic rules |
| `profile_rust_go_pprof.sh` | Rust CPU → Go pprof protobuf (`make profile`) |
| `profile_go_pprof.sh` | Go side pprof helper |
| `PROFILE_RUNBOOK.md` | How to run and interpret profiles |
| `profiles/` | Artifacts: `HOTSPOTS.md`, `*.pb`, README |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `profiles/` | pprof outputs and hotspot notes |

## For AI Agents

### Working In This Directory

- Before speculative perf edits: skill `.claude/skills/flamegraph-perf/` + `PROFILE_RUNBOOK.md`.
- Prefer evidence from `profiles/HOTSPOTS.md` + re-bench over guesswork.
- One optimization class per change; keep wire compatibility; shared `encrypt_batch` paths.
- Root also has `bench_rust_vs_go.py` / `bench_results.json` for 3-way throughput.
- `run_bench.sh` path labels are always **Client → Server** (not server→client). `run_bench <label> <client_bin> <server_bin>`.

### Testing Requirements

- Not unit tests; validate by re-running profile/bench scripts after perf changes
- `make bench`, `make profile`, `make profile-rust-go`

### Common Patterns

- Env: `BENCH_DATA_MB`, `BENCH_FILTER`, `SKIP_PROFILE_REBUILD=1`
- `run_bench.sh` accepts `GO_{CLIENT,SERVER}` and
  `RUST_{TOKIO,SMOL}_{CLIENT,SERVER}` binary overrides for commit-to-commit
  comparisons without replacing workspace artifacts.
- Profiling profile: `make profiling-bins` (bakes in `force-frame-pointers=yes`)
- pprof HTTP endpoints: `--pprof 127.0.0.1:6060` (requires `--features pprof`)
- Deadlock detection: `--features pprof-deadlock` (adds overhead)

## Dependencies

### Internal

- Built `kcptun-client` / `kcptun-server` release or profiling bins

### External

- Go toolchain for pprof UI (`go tool pprof`)

<!-- MANUAL -->
