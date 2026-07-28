# Hotspot notes

## Bench matrix: Rust-tokio vs Go (from bench_results.json, 2026-07-27)

| Scenario | Rust-tokio (MB/s) | Go (MB/s) | Gap | Status |
|----------|-------------------|-----------|------|--------|
| 3des/comp | 15.5 | 13.4 | +16% | ✅ P0 done |
| none/no-comp | 65.7 | 46.2 | +42% | ✅ P1 done |
| aes-128-gcm/no-comp | 55.2 | 43.2 | +28% | ✅ P1 done |
| aes-128/no-comp | 35.4 | 39.8 | -11% | P3 |
| salsa20/comp | 33.0 | 36.9 | -11% | P3 |

Note: Rust-smol 3des/comp = 16.5 MB/s (also faster than Go).

---

## pprof 3des/comp analysis (2026-07-28, post ITIMER_REAL + frame-pointer)

- Build: `make profiling-bins` (force-frame-pointers, ITIMER_REAL, frame-pointer unwinding)
- Duration: 10s, Total samples = 8.89s (88.83% coverage)

### Top frames (wall-clock)

| Rank | Frame | flat % | cum % | Notes |
|------|-------|--------|-------|-------|
| 1 | `tokio::runtime::park::Inner::park` | 99.20% | 99.20% | I/O wait (correct for I/O-bound server) |
| 2 | `Inner::park_condvar` | 0.78% | 0.78% | tokio parking |
| 3 | `KcpServerSession::kcp_input_and_smux` | 0.01% | 0.02% | real work (filtered from park) |

### Interpretation

With ITIMER_REAL (wall-clock), I/O-bound servers show 99% in `Inner::park`.
This is **correct** — the server spends most wall time waiting for I/O.
To find CPU hotspots, filter: `go tool pprof -top -ignore="Inner::park" profile.pb.gz`

### Previous pprof issues (fixed)

- ~~`unix_madvise` 13% flat~~ — Fixed: ProfilingAllocator slow path now uses stack-allocated arrays
- ~~`__pthread_atfork_parent_handlers` 20% flat~~ — Fixed: frame-pointer unwinding stops at libc boundaries
- ~~`record_sample` 100% flat in heap profile~~ — Fixed: skip_profiling_frames() strips profiling internal frames
- ~~0.7% sample coverage~~ — Fixed: ITIMER_REAL instead of ITIMER_PROF gives ~90% coverage

---

## Previous pprof 3des/comp analysis (2026-07-27, pre-fixes)

| Rank | Frame | flat % | cum % | Notes |
|------|-------|--------|-------|-------|
| 1 | `UdpSocket::send_to` | 12.69% | 12.69% | UDP send syscall |
| 2 | `__pthread_atfork_parent_handlers` | 12.58% | 12.58% | ~~pprof signal artifact~~ (fixed) |
| 3 | `unix_madvise` | 6.40% | 6.40% | ~~ProfilingAllocator overhead~~ (fixed) |
| 4 | `Socket::recv_from_with_flags` | 4.30% | 4.30% | UDP recv syscall |
| 5 | `Selector::wake` | 3.42% | 3.42% | mio/tokio I/O driver |
| 6 | `encrypt_cfb` | 0.63% | 8.55% | 3DES CFB encrypt (cumulative) |

---

## Optimization history

### P0: Offload snappy to cpu_block even when has_encryption — ✅ DONE

Removed the `!has_encryption` guard in `should_cpu_block_compress` logic.

Result: 3des/comp tokio throughput improved from 6.9 → 15.5 MB/s (2.2×), surpassing Go (13.4 MB/s).

### P1: Channel-based blocking pool for tokio — ✅ DONE

Replaced `tokio::task::spawn_blocking` in `kio::cpu_block` with a persistent thread pool
+ `async_channel` job dispatch, mirroring smol's `BlockingPool`.

Result (bench, 10MB×4, macOS arm64, release LTO):

| Scenario | Before P1 (MB/s) | After P1 (MB/s) | Improvement |
|----------|-------------------|------------------|-------------|
| none/no-comp | ~31.6 | ~65.7 | +108% (2.1×) |
| null/no-comp | ~34.7 | ~54.4 | +57% |
| aes-128-gcm/no-comp | ~34.1 | ~55.2 | +62% |
| 3des/comp (post P0) | ~15.5 | ~15.5 | No regression |

### AES hardware acceleration — ✅ DONE

Evidence: L2 profiles showed `aes::soft::fixslice::*`; `nm` confirmed soft AES linked
despite Apple Silicon FEAT_AES.

Change: `.cargo/config.toml` sets `--cfg aes_armv8` for `aarch64-apple-darwin` and
`aarch64-unknown-linux-gnu`.

Before (soft): ~12–14 MB/s → After (armv8): ~66–85 MB/s (~5–6×).

### pprof-rs data quality fixes — ✅ DONE (2026-07-28)

1. **ITIMER_REAL** instead of **ITIMER_PROF**: I/O-bound servers get ~90% sample coverage
   vs <1%. ITIMER_REAL generates SIGALRM (signal handler updated).
2. **frame-pointer feature**: clean Rust-only backtraces, stops at libc/pthread boundaries.
3. **Empty backtrace filtering**: samples fired in libc (no frame pointers) are dropped.
4. **ProfilingAllocator hot path**: stack-allocated `[usize; 64]` + FNV-1a hash (zero alloc).
5. **Heap profile frame skipping**: `skip_profiling_frames()` strips `record_sample` etc.

## Remaining work

**P2: Reduce UDP send overhead on macOS**

On macOS, `send_batch_to` falls through to sequential `try_send_to` + `writable().await`.
Consider using a dedicated sender thread or batching smaller packets.

Expected impact: 5-10% improvement on macOS (not applicable on Linux where sendmmsg is used).

**P3: Close remaining throughput gaps**

- aes-128/no-comp: -11% vs Go
- salsa20/comp: -11% vs Go

These are secondary; P0+P1 already moved most scenarios past Go.

## Symbol map (pprof frame patterns)

| Frame pattern | Layer |
|---------------|--------|
| `encrypt_batch` / `should_cpu_block_encrypt` | Crypto batch |
| `CryptEngine` / cipher `encrypt` / CFB | Block crypt |
| `aes::armv8::*` / `aes::ni::*` | Hardware AES |
| `TripleDesCipher::encrypt_block` | 3DES |
| `KCP::flush` / `input` / `send` / `SegmentPool` | ARQ |
| `encode_header_into` / SMUX flush | Mux |
| snappy | Compression (off with `--nocomp`) |
| `send_batch` / `UdpSocket::send_to` | UDP I/O |
| `tokio::runtime::park::Inner::park` | I/O wait (filter with `-ignore`) |
| `cpu_block` | Scheduling / offload |

## Decision tree

1. **Profile shows 99% `Inner::park`** → I/O-bound; filter with `-ignore="Inner::park"`.
2. **Cipher inner loop (L2/L3)** → algorithm micro-opts; verify not residual `dyn`.
3. **Copy / Bytes churn (L1)** → ownership pipeline.
4. **Lock / mutex (L1/L4)** → shorten critical sections; never hold KCP lock across encrypt/snappy.
5. **Syscall / send** → batch send; Linux sendmmsg only if justified.
6. **No actionable ≥~5% leaf** → stop coding; document here.

Hard rules: wire compatibility; no congestion cheats; one class per change; shared `encrypt_batch`.
