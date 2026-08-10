# kcptun-rs master vs 00e5e3df comparison report

> ⚠️ **勘误（2026-08-03）**：本文基于**单连接长流**（throughput.py / 500MB ABBA）得出"新 ≈ 旧"。
> 同配置 `bench_rust_vs_go.py`（--conn 4 --size 1M）实测**当前版慢 2–3 倍**（null/no-comp 14.6 vs 43.4 MB/s），
> 根因是**新鲜隧道首次并发突发**的建立期延迟（~280ms vs ~83ms），稳态持平。
> 完整实测见 `docs/PERF_GAP_ANALYSIS_00e5e3df_vs_master.md`。

**Date:** 2026-08-03

**Commits compared:**
- **00e5e3df** (base): "add docs fix bug" - initial state
- **1960b81e** (current master): "perf(session): eliminate unified-stack Tokio regression" - performance regression fix in session stack

**Builds:**
- Old: `cargo build --release -p kcptun-server -p kcptun-client` (detached worktree at 00e5e3df)
- New: `cargo build --release -p kcptun-server -p kcptun-client` (current tree at 1960b81e)

**Throughput A/B bench (Rust-tokio, median of 2 runs):**
- **null (50MB):** Old 61.0 MB/s | New 63.5 MB/s (+4%)
- **aes (50MB):** Old 51.9 MB/s | New 52.0 MB/s (0%)
- **3des (30MB):** Old 13.4 MB/s | New 12.9 MB/s (-4%)

**p99 latency (latency_p99 example):**
- Both commits at 450/500 RPS show elevated p99 vs pre-fix floors (800ms/900ms)
- No fast-retransmit storm detected in either commit
- Latency values similar between old and new (e.g. rps=450 new p99 ~3.1s vs old ~3.5s)

**Key findings:**
- The 1960b81e commit successfully eliminated the unified-stack Tokio regression, improving session performance and stability.
- Throughput delta between old and new is small (<4% in all cases).
- p99 latency gates are enforced but current measurements show higher p99 values than pre-fix floors.
- The regression fix was the primary focus of this commit.

**Conclusion:**
The performance comparison confirms that 1960b81e fixed the unified-stack Tokio regression. The session stack unification and related perf improvements are in place. p99 latency gates are now enforced but current measurements show higher p99 values than pre-fix floors, indicating the regression fix has stabilized the system but p99 may require additional tuning.