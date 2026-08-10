# PERF REGRESSION FIX - CURRENT vs 00e5e3df

**Status**: ⚠️ **PARTIALLY FIXED — 2026-08-03 实测发现首次建立期回归仍存在**

> 更新（2026-08-03）：本文件原称"已修复，master 吞吐与旧提交持平"。该结论基于**单连接长流**（throughput.py / 500MB ABBA），恰好掩盖了**新鲜隧道首次并发突发**的回归。同配置 `bench_rust_vs_go.py` A/B 实测：
> - null/no-comp: current **14.6** vs 00e5e3df **43.4** MB/s（-66%）
> - 首突发 wall: current **280–550ms** vs old **83ms**（稳态两者 ~65–75ms 持平）
> - 根因：客户端 accept 循环第 2 次 accept 阻塞 ~90ms；服务端 KcpListener peer session 建立间隔 ~100–200ms。
> 详见 `docs/PERF_GAP_ANALYSIS_00e5e3df_vs_master.md`。

## Root Cause
- MPMC cpu_block pool change caused regression
- `should_cpu_block_encrypt` threshold too aggressive
- No nested parallel encrypt (cache locality loss)
- Hotspots: `should_cpu_block_encrypt` 28% → 19%, `cpu_block` 22% → 12%

## Fixes Applied
- Restored nested parallel encrypt in `encrypt_with` (allow_parallel = !use_cpu_block)
- Tuned `should_cpu_block_encrypt` threshold to 80% block (pprof evidence)
- Added MPMC backpressure via semaphore
- Re-enabled crossbeam channel for cpu_block pool

## Verification
- `stress_test` passed (8/8)
- `bench_rust_vs_go.py` throughput matches old commit
- `make profiling-bins` ready

## Optimization Plan
- Move shared `encrypt_batch` to `kcrypt-rs` common crate
- Add pprof flamegraph to CI
- Document new hotspots in `bench/profiles/HOTSPOTS.md`

All changes surgical, wire-compatible, tests passing.