# Tunnel P99/P999 Performance Test Report

## 1. Test Environment

| Field | Value |
|-------|-------|
| **CPU** | AMD EPYC 7763 (8 vCPU) / Apple M1 Pro |
| **Kernel** | Linux 6.1.0 / macOS 26.6 |
| **SO_RCVBUF** | 4MB |
| **SO_SNDBUF** | 4MB |
| **Runtime** | tokio (Multi-thread) / smol (Lightweight) |
| **KCP Config** | Fast3 (nodelay=1, interval=10ms, resend=2, nc=1) |
| **MTU** | 1350 |
| **SMUX** | v2, sndwnd=1024, rcvwnd=1024 |
| **Cipher** | aes-128-cfb (or as tested) |
| **Compression** | Disabled (--nocomp) for clean latency measurement |
| **FEC** | Disabled (0/0) |
| **Network** | 127.0.0.1 loopback / tc netem for impairment |

## 2. Methodology

- **Open model** (Coordinated Omission safe): client sends at fixed rate independent of response timing
- **Warmup**: 30s excluded from all metrics (KCP SRTT convergence, buffer pool allocation, allocator warm-up)
- **Sampling**: 180s per test case (~54,000–180,000 samples per run)
- **Percentile computation**: Nearest-rank over all raw samples, sorted once, no batch averaging
- **Repeats**: 3 runs per test case, median reported
- **Outlier discard**: >2× median discarded

## 3. Test Matrix Results

### 3.1 Baseline: Clean Link (0% Loss) — Rust tokio vs Go kcptun

| Test | RPS | Payload | Streams | Implementation | P50(ms) | P90(ms) | P99(ms) | P999(ms) | Max(ms) | P99/P50 | P999/P99 | Verdict |
|------|-----|---------|---------|----------------|---------|---------|---------|----------|---------|---------|----------|---------|
| TC-01 | 10k | 128B | 1 | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-01 | 10k | 128B | 1 | Go kcptun | — | — | — | — | — | — | — | — |
| TC-02 | 10k | 1400B | 1 | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-02 | 10k | 1400B | 1 | Go kcptun | — | — | — | — | — | — | — | — |
| TC-03 | 5k | 1024B | 10 | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-03 | 5k | 1024B | 10 | Go kcptun | — | — | — | — | — | — | — | — |

### 3.2 Loss Scenarios — Rust tokio vs Go kcptun

| Test | RPS | Payload | Loss | Implementation | P50(ms) | P90(ms) | P99(ms) | P999(ms) | Max(ms) | P99/P50 | P999/P99 | Verdict |
|------|-----|---------|------|----------------|---------|---------|---------|----------|---------|---------|----------|---------|
| TC-04 | 5k | 1024B | 5% | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-04 | 5k | 1024B | 5% | Go kcptun | — | — | — | — | — | — | — | — |
| TC-05 | 5k | 1024B | 10% | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-05 | 5k | 1024B | 10% | Go kcptun | — | — | — | — | — | — | — | — |

### 3.3 Extreme Stress — Rust tokio vs Go kcptun

| Test | RPS | Payload | Conns | Loss | Implementation | P50(ms) | P90(ms) | P99(ms) | P999(ms) | Max(ms) | P99/P50 | P999/P99 | Verdict |
|------|-----|---------|-------|------|----------------|---------|---------|---------|----------|---------|---------|----------|---------|
| TC-06 | 5k | 1400B | 100 | 10% | kcptun-rs tokio | — | — | — | — | — | — | — | — |
| TC-06 | 5k | 1400B | 100 | 10% | Go kcptun | — | — | — | — | — | — | — | — |

### 3.4 Runtime Comparison (Rust only)

| Test | RPS | Payload | Loss | Runtime | P50(ms) | P90(ms) | P99(ms) | P999(ms) | Max(ms) | P99/P50 | P999/P99 | Verdict |
|------|-----|---------|------|---------|---------|---------|---------|----------|---------|---------|----------|---------|
| TC-01 | 10k | 128B | 0% | tokio | — | — | — | — | — | — | — | — |
| TC-01 | 10k | 128B | 0% | smol | — | — | — | — | — | — | — | — |
| TC-02 | 10k | 1400B | 0% | tokio | — | — | — | — | — | — | — | — |
| TC-02 | 10k | 1400B | 0% | smol | — | — | — | — | — | — | — | — |
| TC-04 | 5k | 1024B | 5% | tokio | — | — | — | — | — | — | — | — |
| TC-04 | 5k | 1024B | 5% | smol | — | — | — | — | — | — | — | — |
| TC-05 | 5k | 1024B | 10% | tokio | — | — | — | — | — | — | — | — |
| TC-05 | 5k | 1024B | 10% | smol | — | — | — | — | — | — | — | — |

## 4. Diagnostic Rules

### 4.1 Health Thresholds

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| P99 ≤ 3× P50 | ✅ | ⚠️ | 🔴 |
| P999 ≤ 3× P99 | ✅ | ⚠️ | 🔴 |
| P999 ≤ 10× P50 | ✅ | ⚠️ | 🔴 |
| Max ≤ 100× P50 | ✅ | ⚠️ | 🔴 |

### 4.2 Root Cause Analysis

#### Symptom: P999/P99 > 5 under packet loss

**Likely cause**: KCP ARQ retransmission wait time too long.

**Investigation**:
1. Check `ikcp_check` / timer interval — is it dense enough?
2. Check `resend` threshold — lower value = faster fast-retransmit
3. Check if `nc=1` (no congestion control) is set — with congestion control, window halving delays retransmission
4. Compare P999 at 5% vs 10% loss — linear growth is expected, exponential growth indicates timer issue

**Fix options**:
- Reduce `interval` (e.g., from 10ms to 5ms) for faster timer-driven check
- Lower `resend` from 2 to 1 for faster fast-retransmit
- Increase `sndwnd`/`rcvwnd` to reduce window-based throttling during recovery

#### Symptom: 0% loss but Max >> P999 (e.g., P999=2ms, Max=15ms)

**Likely cause**: Task queue blocking or epoll wake-up lag.

**Investigation**:
1. Check tokio work-stealing behavior — is one thread saturated?
2. Check `cpu_block` offload thresholds — is crypto offloading causing blocking?
3. Check SMUX flush loop lock contention — is the 4-phase flush holding locks too long?
4. Profile with `profile_under_load.sh` to identify CPU hotspots

**Fix options**:
- Increase tokio worker threads
- Tune `cpu_block` thresholds to avoid offloading small operations
- Shorten critical sections in flush loop

#### Symptom: P99 degrades significantly with more concurrent streams

**Likely cause**: SMUX lock contention or KCP session lock contention.

**Investigation**:
1. Check if `Stream` read/write locks (R4 model) are contended
2. Check KCP `snd_buf`/`rcv_buf` lock contention
3. Profile with `profile_under_load.sh` under multi-stream load

**Fix options**:
- Increase SMUX max frame size to reduce flush frequency
- Tune KCP window sizes to reduce ACK traffic
- Consider per-stream KCP sessions (multiple UDP sockets) instead of multiplexing

#### Symptom: smol P999 >> tokio P999 under high RPS

**Likely cause**: smol lacks work-stealing scheduler, causing task queue imbalance.

**Expected behavior**: tokio's work-stealing distributes load across cores, keeping P99 low. smol's cooperative scheduling can cause tail latency spikes when one task blocks.

**Investigation**:
1. Check if the bottleneck is CPU-bound (crypto/snappy) or I/O-bound (UDP send/recv)
2. Profile both runtimes under identical load
3. Check if `cpu_block` offloading is causing task stalls on smol

## 5. Per-Test Raw Data

### TC-01: Small-Packet Baseline (128B, 1 conn, 0% loss, tokio)

- RPS: 10000
- Payload: 128B
- Duration: 180s
- Samples: —
- P50: — µs
- P90: — µs
- P99: — µs
- P999: — µs
- Avg: — µs
- Min: — µs
- Max: — µs
- Notes: —

### TC-02: Large-Packet Baseline (1400B, 1 conn, 0% loss, tokio)

- RPS: 10000
- Payload: 1400B
- Duration: 180s
- Samples: —
- P50: — µs
- P90: — µs
- P99: — µs
- P999: — µs
- Avg: — µs
- Min: — µs
- Max: — µs
- Notes: —

(Repeat for each test case)

## 6. Comparison: Raw KCP vs Full Tunnel

| Metric | Raw KCP (run_p99.sh) | Full Tunnel (this test) | Overhead |
|--------|---------------------|------------------------|----------|
| P50 (tokio, 1KB) | — | — | — |
| P99 (tokio, 1KB) | — | — | — |
| P999 (tokio, 1KB) | — | — | — |
| P50 (smol, 1KB) | — | — | — |
| P99 (smol, 1KB) | — | — | — |
| P999 (smol, 1KB) | — | — | — |

The **tunnel overhead** is the difference between raw KCP latency and full-tunnel latency. This captures the cumulative cost of crypto + SMUX framing + Snappy compression + KCP ARQ.

## 7. Conclusions

- **Baseline latency**: P50/P99/P999 for clean link at various payload sizes
- **Loss sensitivity**: How P99/P999 degrade with 5%/10% packet loss
- **Runtime comparison**: tokio vs smol tail latency gap
- **Concurrency scaling**: How P99/P999 grow with concurrent streams
- **System limits**: At what RPS/loss/concurrency does the tunnel collapse
- **Recommendations**: Tunable parameters for production deployment

_Report generated by bench/tunnel_p99.sh and bench/REPORT_TEMPLATE.md_
