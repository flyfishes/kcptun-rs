# Tunnel P99/P999 Test Matrix

Professional-grade KCP tunnel latency test matrix for kcptun-rs.
Designed to measure P99/P999 latency through the full tunnel stack
(crypto + KCP + SMUX + Snappy) under controlled network conditions.

## Test Variables

| Variable | Values | Why |
|----------|--------|-----|
| Payload size | 128B, 1024B, 1400B | Small (game/IM/DNS) vs large (HTTP/gateway) |
| RPS | 1k, 5k, 10k, 50k | Light load vs saturation |
| Concurrent streams | 1, 10, 50, 100 | SMUX multiplexing contention |
| Packet loss | 0%, 5%, 10% | Tunnel anti-loss value proposition |
| Jitter | 0ms, 20ms, 50ms | Real-world network variation |
| Runtime | tokio, smol | Dual-backend comparison |
| Cipher | aes, sm4, xor, null | Crypto overhead impact |
| Implementation | kcptun-rs (tokio), kcptun-rs (smol), Go kcptun (kcp-go v5) | Cross-language comparison |

## Test Cases

### TC-01: Small-Packet Baseline (128B, 1 conn, 0% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 10,000 |
| Payload | 128 bytes |
| Connections | 1 |
| Loss | 0% |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Small-packet limit latency (game/IM/DNS profile). Excludes lock/jitter. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 10,000 |
| Payload | 128 bytes |
| Connections | 1 |
| Loss | 0% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for small-packet latency comparison |

### TC-02: Large-Packet Baseline (1400B, 1 conn, 0% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 10,000 |
| Payload | 1400 bytes (MTU-sized) |
| Connections | 1 |
| Loss | 0% |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Large-packet throughput ceiling. Measures bulk transfer tail latency. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 10,000 |
| Payload | 1400 bytes |
| Connections | 1 |
| Loss | 0% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for large-packet latency comparison |

### TC-03: Multi-Stream Contention (1KB, 10 streams, 0% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 (per stream = 50k total) |
| Payload | 1024 bytes |
| Streams | 10 (SMUX multiplexed over 1 KCP session) |
| Loss | 0% |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | SMUX lock contention and scheduler fairness under concurrent streams. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 per stream |
| Payload | 1024 bytes |
| Streams | 10 |
| Loss | 0% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for multi-stream contention comparison |

### TC-04: Loss Stress (1KB, 1 conn, 5% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 |
| Payload | 1024 bytes |
| Connections | 1 |
| Loss | 5% (random, tc netem) |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | KCP ARQ retransmission latency under moderate loss. P999/P99 ratio indicates retransmission wait time. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 |
| Payload | 1024 bytes |
| Connections | 1 |
| Loss | 5% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for loss recovery latency comparison |

### TC-05: Extreme Loss (1KB, 1 conn, 10% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 |
| Payload | 1024 bytes |
| Connections | 1 |
| Loss | 10% (random, tc netem) |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | System collapse红线. KCP window exhaustion, RTO backoff, and recovery behavior. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 |
| Payload | 1024 bytes |
| Connections | 1 |
| Loss | 10% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for extreme loss comparison |

### TC-06: Burst + Loss (1400B, 100 conn, 10% loss, tokio)

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 (per conn = 500k total) |
| Payload | 1400 bytes |
| Connections | 100 |
| Loss | 10% |
| Jitter | 0ms |
| Runtime | tokio |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Worst-case: high concurrency + large payload + severe loss. Measures buffer exhaustion and scheduler saturation. |

**Go kcptun baseline** (same parameters, Go kcptun binary):

| Parameter | Value |
|-----------|-------|
| RPS | 5,000 per conn |
| Payload | 1400 bytes |
| Connections | 100 |
| Loss | 10% |
| Runtime | Go kcptun (kcp-go v5) |
| Cipher | aes |
| Mode | fast3 |
| Purpose | Go baseline for burst + loss collapse comparison |

### TC-07–TC-10: smol Runtime Variants

Same as TC-01, TC-02, TC-04, TC-05 but with `--no-default-features --features smol`.
Measures the latency gap between tokio (work-stealing) and smol (lightweight) under tunnel load.

### TC-G1–TC-G3: Go kcptun Dedicated Comparison

These test cases run **only Go kcptun** (no Rust comparison) to establish the Go baseline under tunnel stack conditions:

| Test ID | Description | RPS | Payload | Loss | Purpose |
|---------|-------------|-----|---------|------|---------|
| TC-G1 | Go kcptun small-packet baseline | 10,000 | 128B | 0% | Go baseline for small-packet tunnel latency |
| TC-G2 | Go kcptun loss recovery | 5,000 | 1024B | 5% | Go baseline for ARQ retransmission latency |
| TC-G3 | Go kcptun extreme loss | 5,000 | 1024B | 10% | Go baseline for collapse behavior |

**Go kcptun binary**: `tests/kcptun-go/client` and `tests/kcptun-go/server`
**Go kcptun config**: `--mode fast3 --crypt aes --smuxver 2 --nocomp --sndwnd 1024 --rcvwnd 1024`

## Network Impairment Injection

Use `tc netem` on the loopback interface:

```bash
# 5% random loss
sudo tc qdisc add dev lo root netem loss 5%

# 10% loss + 20ms jitter
sudo tc qdisc add dev lo root netem loss 10% delay 0ms 20ms distribution normal

# Clean up
sudo tc qdisc del dev lo root netem
```

## Execution Order

Tests should be run in this order to avoid warm-up effects carrying across runs:

1. TC-01 (small packet, clean) — establish baseline
2. TC-02 (large packet, clean) — bulk ceiling
3. TC-07 (small packet, smol) — runtime comparison
4. TC-08 (large packet, smol) — runtime comparison
5. TC-03 (multi-stream) — contention
6. TC-04 (5% loss) — ARQ latency
7. TC-09 (5% loss, smol) — runtime comparison under loss
8. TC-05 (10% loss) — extreme stress
9. TC-10 (10% loss, smol) — runtime comparison under extreme loss
10. TC-06 (burst + 100 conn + 10% loss) — collapse test

Each test case should be run **3 times** and the median reported. Discard outliers (>2× median).

## Prerequisites

- Release binaries built: `make release` (tokio) and `make release-smol` (smol)
- `tc` (iproute2) installed for network impairment
- Root/sudo access for `tc qdisc` commands
- Loopback interface available (`lo`)
- At least 30 minutes per full matrix run (10 cases × ~3 min each)

## Notes

- All tests use **Fast3** KCP mode (nodelay=1, interval=10ms, resend=2, nc=1) — matches Go kcptun defaults
- SMUX version 2, 1024/1024 send/receive windows
- Snappy compression **disabled** (`--nocomp`) for clean latency measurement; re-enable for production-representative tests
- Crypto: AES-128-CFB (default) unless testing other ciphers
- All measurements are **open-model** (fixed-rate sends, Coordinated Omission safe)
- Warmup: 30 seconds (excluded from metrics)
- Measurement: 180 seconds per test case
