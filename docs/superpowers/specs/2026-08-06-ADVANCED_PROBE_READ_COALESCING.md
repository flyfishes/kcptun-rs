# Spec: kcp-rs advanced-API latency probe + poll_read coalescing

| Field | Value |
|-------|-------|
| Created | 2026-08-06 |
| Status | implemented (probe + coalescing landed; acknodelay result evidence-gated) |
| Related | `docs/superpowers/plans/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY.md`; `bench/profiles/HOTSPOTS.md`; `bench/LATENCY_P99_REPORT.md` |
| Scope | `kcp-rs/examples/latency_p99.rs` (advanced-API re-implementation), `kcp-rs/src/conn.rs` (`poll_read_into` coalescing), `kcp-rs/tests/kcpconn_integrity.rs` (coalescing test), `bench/AGENTS.md` |

## 1. Motivation

The user requested: re-implement the raw-KCP P99/P999 test on the **kcp-rs
advanced async API** (`KcpConn`/`KcpListener` + TcpStream-aligned surface),
keeping the machine-readable `RESULT` output format identical to the previous
probe, and use **pprof + SNMP** to drive evidence-gated optimization of the
advanced implementation — targeting extreme throughput and stable low latency.

## 2. What changed

### 2.1 `kcp-rs/examples/latency_p99.rs` — advanced-API re-implementation

The probe now exercises the full async surface:

- **Builder knobs** (`conv`/`mode`/`mtu`/`sndwnd`/`rcvwnd`/`nodelay(n,i,r,nc)`/
  `acknodelay`/`stream`/`fec(ds,ps)`) shared between `KcpConn` and `KcpListener`
  builders via one local `apply_knobs!` macro.
- **Readiness-based client reads** (`readable()` + `read_shared()`) replacing the
  old per-read `kio::timeout(10s, read)` wrapper in the closed-loop path, so no
  fresh 10 s timer future is parked per response.
- **Coalescing echo server** (`read_shared` + `write_all_shared`) — the old
  split-half `read_exact`/`write_all` (via `poll_read_into`) drained one
  ~1.3 KB KCP segment per task wake; `read_shared` drains the whole buffered
  burst in one call.
- **Multi-connection fan-out** (`--conns N`) for throughput × tail Pareto on
  several parallel `KcpConn`s; single-connection measurement stays inline on
  the driving future to avoid an extra task hop perturbing the tail.
- **SNMP instrumentation**: `kcp_rs::snmp_enable()` + a new `SNMPRESULT` delta
  line (retrans / lost / fast / early / repeat / fallback / empty_flush /
  write_inline / write_flush / input_urgent / in+out pkts+segs+bytes) so a
  P999 spike can be correlated with a real protocol event. `--secstats` prints
  per-second sample count + max latency alongside per-second SNMP deltas to
  localize *when* a spike happened and whether it had a protocol cause.
- **`--raw FILE`** dumps raw microsecond latencies for offline percentile
  recomputation / A-B.
- **`--pprof ADDR`** unchanged (Go-compatible pprof HTTP server).

The `RESULT` line format is byte-for-byte unchanged, so `bench/run_p99.sh`
parsing works unmodified (verified end-to-end: all 10 steps incl. Go↔Rust
cross-interop produced parseable `RESULT` lines and a rendered report).

### 2.2 `kcp-rs/src/conn.rs` — `poll_read_into` coalescing

`poll_read_into` (backing `AsyncRead`/`read()`/split halves) previously popped
**one** `read_buf` entry per poll. It now drains the whole buffered burst into
the caller buffer (same loop as `read_shared`), so a multi-segment response
takes one task wake instead of one per ~1.3 KB segment. Semantics are unchanged
(any `n > 0` is legal for `AsyncRead`; `read_exact`/accumulating readers
handle partial reads), the read-timeout deadline path is untouched, and the
`read_buf` lock is held no longer than `read_shared` already held it.

### 2.3 `kcp-rs/tests/kcpconn_integrity.rs` — coalescing test

`kcpconn_read_coalesces_segments`: writes a 4-segment (4096 B) payload, lets
the peer buffer the full burst, then asserts a single `read()` returns the
entire payload. Fails if coalescing is reverted.

## 3. Evidence (pprof + SNMP)

Captured with the profiling binary (`--features pprof`, force-frame-pointers,
`bench/profile_p99_latency.sh` / direct `--pprof`) at 500 RPS / 26624 B:

- **CPU profile**: UDP syscalls dominate — `UdpSocket::send_to` 11.2%,
  `try_send` 8.6%, `try_recv` 7.4%, `try_recv_from` 5.3% (≈32% UDP syscalls;
  no sendmmsg on macOS), plus tokio scheduler (`notify_parked_local` 7.3%,
  `wake` 0.8%). **No kcp-rs leaf ≥ 1% flat** — the advanced implementation is
  syscall/scheduler-bound on macOS, consistent with `bench/profiles/HOTSPOTS.md`.
- **SNMP**: open-model tails occur with `retrans≈0`, `lost≈0`, `repeat≈0`,
  `fallback≈0` during spikes → spikes are scheduling/I-O, not protocol events.
- **Probe-level A/B (short runs)**: coalescing (probe) vs one-entry-per-poll
  (old split-half echo) showed ~5× p99 tail difference at 500 RPS / 26624 B —
  the motivation for §2.2. Formal P999 requires ≥10 min per the plan.

## 4. acknodelay finding (evidence-gated)

`--acknodelay` consistently improved p90–p99 at 500 RPS / 26624 B in a first
pass (p99 ≈ 400 µs vs ≈ 800 µs). Whether this is a stable, publishable
latency lever needs a clean multi-rep paired A/B (machine-idle, ≥30 s runs);
the async conn already flushes ACKs per input burst via `flush_input_batch`,
so the mechanism is under investigation (see §7 follow-ups).

## 5. Gates

`make gate` (fmt --check + cargo test --workspace + clippy -D warnings):
**✅ all passed**. kcp-rs async-tokio + async-smol test suites pass (incl. the
new coalescing test, 11 tests each in `kcpconn_integrity`).

## 6. Not changed / rejected

- No wire-format, KCP, FEC, crypto, or SMUX changes.
- No change to the SNMP counter gating (still opt-in via `snmp_enable`).
- Direct-Inline-Send for tokio stays rejected (plan-documented 66% p99 median
  regression); only the read path coalescing was applied.
- macOS syscall bound (no sendmmsg) is out of scope; Linux `sendmmsg`/
  `recvmmsg` remain the real throughput lever (already in kio, verified).

## 7. Follow-ups

- Clean paired A/B for `--acknodelay`; if confirmed, consider documenting
  ACK-nodelay as a latency tuning in kcp-rs README / kcptun config guidance.
- 10+ minute formal P999 runs for the report (per plan §5.1).
- Re-verify read-path coalescing under `make e2e` (Go interop) on a Linux box.
