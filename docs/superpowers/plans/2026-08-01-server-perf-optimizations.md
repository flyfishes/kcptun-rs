# kcptun-server Performance Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the per-session / per-packet fixed costs in `kcptun-server` identified in the performance audit: idle-session 2ms wakeup, double DashMap lookup + per-packet KCP lock, `drain_new_streams` full-HashSet clone, per-datagram `String`/`Vec` clones, and the blocking (thread-sleeping) `RateLimiter`.

**Architecture:** All changes are scheduler/accounting-layer optimizations inside existing hot loops. KCP/SMUX/FEC/crypto wire formats and the KCP state machine are untouched. Rate limiting stays per-session (as in Go) but becomes non-blocking so a tokio worker thread is never held by `std::thread::sleep`.

**Tech Stack:** Rust workspace, tokio (default) + smol runtimes via `kio-rs`, `parking_lot`, `dashmap`.

## Global Constraints

1. **Wire compatibility is absolute.** Do not change any KCP segment header, SMUX frame header, FEC frame, crypto header (`[nonce 16B][CRC32 4B]` / AES-GCM `[nonce 12B][tag]`), or padding byte. Zero changes to `kcp-rs`, `smux-rs`, `kcrypt-rs`, `qpp-rs` wire code.
2. **No KCP/congestion-cheat timing changes.** The KCP state machine (`kcp.rs`) behaves identically; only the Rust flush-loop *sleep cadence* and the rate-limiter *waiting mechanism* change.
3. **`make gate` must pass:** `cargo fmt --all -- --check` (zero diff), `cargo test --workspace` (zero failures), `cargo clippy --workspace -- -D warnings` (zero warnings).
4. **Server integrity for any task touching `kcptun-server/src/main.rs`:** `cargo build --release` then `cargo test --release --package kcptun-server --test stress_test -- --nocapture --test-threads=1` passes.
5. **SNMP stays gated.** All counter updates go through `kcp_rs::snmp_add` (AtomicBool-gated, zero cost when disabled). Do not add unconditional counters on hot paths.
6. **Keep shared paths.** Continue to use the existing `encrypt_batch` / `cpu_block` / `send_batch_to` / `prepare_outbound_into` helpers; do not create client/server drift.
7. **No new `#[allow(...)]`.** Existing crate-level kcp-rs/smux-rs clippy allows are pre-existing and stay; don't add new suppressions.
8. **One optimization per change.** Each task is one class of change; keep the diff surgical.
9. **`RateLimiter` stays per-session** (client and server each construct their own, matching Go's per-connection `rate.Limiter`). Do not introduce a global/process-wide limiter.

---

### Task 1: Flush-loop idle backoff (stop 500 Hz/session idle wakeup)

**Files:**
- Modify: `kcptun-server/src/main.rs` — const block (~line 45-51), flush-loop tail (~lines 961-968), health-check comment (~lines 767, 785-786)

**Interfaces:**
- Consumes: `KCP::flush() -> u32` (already returns the KCP interval when idle — verified `kcp-rs/src/kcp.rs:922` starts `let mut next_update = self.interval;`), `KCP::wait_send() -> i32` (already used in the same block).
- Produces: new module const `MAX_IDLE_UPDATE_MS: u64`. No public API change.

**Problem.** The flush loop forces an idle session to wake every 2 ms:
```rust
next_update = kcp_guard.flush() as u64;                  // idle ⇒ returns interval (10-40 ms)
if had_outbound || kcp_guard.wait_send() > 0 {
    next_update = 1;                                     // busy: 1 ms (correct)
} else {
    next_update = next_update.clamp(1, KCP_UPDATE_INTERVAL_MS);  // ⚠️ idle: clamps 30 ms → 2 ms
}
```
`flush()` already returns the right idle value (the mode interval), but the `clamp(1, 2)` discards it. Every idle session therefore wakes 500×/s and each wake re-locks the SMUX stream map ~4× and the KCP mutex once (all no-op work). Go wakes idle sessions at the KCP `interval` (10-40 ms) — 5-15× fewer wakes.

- [ ] **Step 1: Add the new constant.**

Near `const KCP_UPDATE_INTERVAL_MS: u64 = 2;` (`main.rs:51`) add:
```rust
/// Upper bound on the idle flush-loop sleep. Idle sessions back off to the
/// KCP interval (mode-dependent, 10-40ms) but are capped so the Phase 1a
/// stream reaper and keepalive health checks still run on a bounded schedule.
const MAX_IDLE_UPDATE_MS: u64 = 100;
```

- [ ] **Step 2: Change the idle branch of the wake-time computation.**

Replace only the `else` arm of the `next_update` computation (~`main.rs:966-968`):
```rust
    } else {
        // Idle: flush() already returns the KCP interval (mode-dependent,
        // 10-40ms). Back off to it instead of forcing a 2ms wakeup per idle
        // session — 500Hz × session count was the dominant idle CPU cost.
        // Bounded so Phase 1a reaping and keepalive checks run on time.
        next_update = next_update.clamp(1, MAX_IDLE_UPDATE_MS);
    }
```
Leave the `had_outbound || wait_send() > 0` → `next_update = 1` arm untouched.

- [ ] **Step 3: Fix the now-stale calibration comment.**

The comment above `let mut health_checks_left: u32 = 0;` (~`main.rs:767`) and the reset site (`main.rs:786`) say `// ~100ms at 2ms update interval`. Change to reflect reality: health checks now run every `health_checks_left` (50) flush cycles, which at idle cadence is ~50×interval (≈1.5 s for mode `fast`). Note that this keeps SMUX keepalive (default 10 s interval, 30 s timeout) and KCP dead_link detection well inside the 4-10 s acceptance envelope; the *client* still discovers dead peers via its own 20-retransmit dead_link.

- [ ] **Step 4: Verify.**

Run:
```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo build --release
cargo test --release --package kcptun-server --test stress_test -- --nocapture --test-threads=1
```
Expected: all pass (no new tests needed — this changes cadence only; stress test confirms no data-integrity regression).

- [ ] **Step 5: Commit.**

```bash
git add kcptun-server/src/main.rs
git commit -m "perf(kcptun-server): back off idle flush loop to KCP interval (was 2ms clamp)"
```

---

### Task 2: `get_or_create_session` single lookup + drop per-packet KCP lock in `is_dead`

**Files:**
- Modify: `kcptun-server/src/main.rs` — `KcpServerSession::is_dead` (~lines 723-736), `get_or_create_session` (~lines 1292-1299)

**Interfaces:**
- Consumes: existing `self.dead: Arc<AtomicBool>`, `self.smux.is_closed()`, `DashMap::get/remove`.
- Produces: none (internal refactor). Behavior identical except `is_dead` no longer locks KCP.

**Problem.** Every inbound datagram does two DashMap shard lookups and one KCP mutex lock before the session's own `feed_data_mut` locks KCP again:
```rust
let need_evict = sessions.get(peer).is_some_and(|s| s.is_dead()); // lookup ① (+ KCP lock)
if need_evict { sessions.remove(peer); }
if let Some(s) = sessions.get(peer) { return s.clone(); }         // lookup ②
```
And `is_dead` locks the KCP mutex on **every** call:
```rust
if self.kcp.lock().is_dead() {   // per-packet KCP lock — contended with the flush loop
    self.dead.store(true, Ordering::Release);
    return true;
}
```

- [ ] **Step 1: Remove the KCP lock from `is_dead`.**

Replace the body of `KcpServerSession::is_dead` (~`main.rs:723-736`) with:
```rust
    fn is_dead(&self) -> bool {
        if self.dead.load(Ordering::Acquire) {
            return true;
        }
        if self.smux.is_closed() {
            return true;
        }
        // KCP dead_link detection is owned by the flush loop's health check
        // (it stores `dead` when kcp.is_dead()). Checking KCP here would lock
        // the KCP mutex on every datagram — the flush loop already marks the
        // session dead on kcp.is_dead() / SMUX keepalive timeout, which is the
        // only signal get_or_create_session needs to evict. Detection latency
        // stays well inside the 4-10s reconnect-acceptance envelope.
        false
    }
```

- [ ] **Step 2: Collapse the double lookup in `get_or_create_session`.**

Replace (~`main.rs:1292-1299`):
```rust
    let need_evict = sessions.get(peer).is_some_and(|s| s.is_dead());
    if need_evict {
        sessions.remove(peer);
    }

    if let Some(s) = sessions.get(peer) {
        return s.clone();
    }
```
with:
```rust
    if let Some(s) = sessions.get(peer) {
        if s.is_dead() {
            // Drop the DashMap read guard before remove() — holding it while
            // calling remove() can deadlock on the shard lock (read vs write).
            drop(s);
            sessions.remove(peer);
        } else {
            return s.clone();
        }
    }
```
Keep the surrounding comment about "do not hold the get() guard while calling remove()" — it now documents the explicit `drop(s)`.

- [ ] **Step 3: Verify** (same commands as Task 1 Step 4).

- [ ] **Step 4: Commit.**

```bash
git add kcptun-server/src/main.rs
git commit -m "perf(kcptun-server): single DashMap lookup per packet; is_dead no longer locks KCP"
```

---

### Task 3: `drain_new_streams` — eliminate per-packet full-HashSet clone

**Files:**
- Modify: `kcptun-server/src/main.rs` — `KcpServerSession::drain_new_streams` (~lines 1230-1262)

**Interfaces:**
- Consumes: `self.handled_streams: Arc<Mutex<HashSet<u32>>>`, `self.smux.streams() -> Arc<Mutex<HashMap<u32, Arc<Stream>>>>`, `Stream::is_ready()`, `Stream::available()`.
- Produces: unchanged signature `Vec<(u32, Arc<smux_rs::stream::Stream>)>`.

**Problem.** On every datagram the function clones the **entire** handled-stream-id set and then walks every stream, even when nothing is new — O(total streams) per packet with one heap clone:
```rust
let handled = self.handled_streams.lock().clone();   // O(N) heap clone, every datagram
let streams = self.smux.streams();
let stream_map = streams.lock();
let new_streams: Vec<_> = stream_map.iter()
    .filter(|(&id, s)| !handled.contains(&id) && (s.is_ready() || s.available() > 0))
    .map(|(&id, s)| (id, s.clone()))
    .collect();
drop(stream_map);
{ let mut h = self.handled_streams.lock(); for (id, _) in &new_streams { h.insert(*id); } }
```

- [ ] **Step 1: Replace with a single-pass no-clone scan under a consistent lock order.**

Replace the whole body of `drain_new_streams` with:
```rust
    fn drain_new_streams(&self) -> Vec<(u32, Arc<smux_rs::stream::Stream>)> {
        // Single pass, no HashSet clone. Lock order is streams → handled,
        // identical to the flush loop's Phase 1a reaper, so there is no
        // lock-order inversion. Accept-and-mark happen atomically under the
        // handled lock.
        let streams = self.smux.streams();
        let stream_map = streams.lock();
        let mut handled = self.handled_streams.lock();
        let mut new_streams = Vec::new();
        for (&id, s) in stream_map.iter() {
            if handled.contains(&id) {
                continue;
            }
            // Accept streams that are ready (SYN received) OR have data
            // buffered. A FIN may arrive before the server reads the data, so
            // also accept streams with pending data even if state is
            // FinReceived.
            if s.is_ready() || s.available() > 0 {
                new_streams.push((id, s.clone()));
                handled.insert(id);
            }
        }
        new_streams
    }
```

- [ ] **Step 2: Verify** (same commands as Task 1 Step 4).

- [ ] **Step 3: Commit.**

```bash
git add kcptun-server/src/main.rs
git commit -m "perf(kcptun-server): drain_new_streams single-pass, no per-packet HashSet clone"
```

---

### Task 4: Remove per-datagram `target_str` / `qpp_key` clones in the UDP recv loop

**Files:**
- Modify: `kcptun-server/src/main.rs` — the `process_datagram` closure in the UDP recv task (~lines 1783-1817)

**Interfaces:**
- Consumes: `target_str: String`, `qpp_key: Vec<u8>` captured by the non-`move` `process_datagram` closure.
- Produces: none.

**Problem.** Two heap allocations per inbound datagram, used only when a *new* stream is being spawned:
```rust
let target_str = target_str.clone();   // String alloc, every datagram
let qpp_key = qpp_key.clone();         // Vec<u8> alloc, every datagram
session.feed_data_mut(data);
session.flush_notify.notify_one();
for (stream_id, smux_stream) in session.drain_new_streams() {
    ...
    let target = target_str.clone();   // ← already re-clones inside the loop
    let qpp_key = qpp_key.clone();
    ...
}
```

- [ ] **Step 1: Delete the two outer clones.**

Remove exactly these two lines (the ones immediately after `let session = get_or_create_session(...);` and before `session.feed_data_mut(data);`, ~`main.rs:1784-1785`):
```rust
        let target_str = target_str.clone();
        let qpp_key = qpp_key.clone();
```
The clones **inside** the `for (stream_id, smux_stream) in session.drain_new_streams()` loop stay — they are what the spawned `'static` task requires. The closure is non-`move` and borrows `target_str`/`qpp_key`, so borrowing-and-cloning-on-demand is valid. Note: the TCP-mode loop (`spawn_tcp_recv_loop`, ~`main.rs:1442-1443`) already clones inside its loop — leave it.

- [ ] **Step 2: Verify** (same commands as Task 1 Step 4).

- [ ] **Step 3: Commit.**

```bash
git add kcptun-server/src/main.rs
git commit -m "perf(kcptun-server): stop cloning target/qpp_key on every datagram"
```

---

### Task 5: Non-blocking per-session `RateLimiter` (async pacing)

**Files:**
- Modify: `kcptun-common/src/ratelimit.rs` — `acquire` contract + unit tests
- Modify: `kcptun-server/src/main.rs` — flush-loop call site (~lines 1035-1039)
- Modify: `kcptun-client/src/main.rs` — flush-loop call site (~lines 1200-1203) and lib-KCP write path (~line 1655)

**Interfaces:**
- Consumes: existing `RateLimiter` struct + `Inner::refill()`.
- Produces: `acquire` keeps its signature `pub fn acquire(&self, n: usize) -> Duration` but becomes **non-blocking**: returns `Duration::ZERO` when tokens were consumed (no wait needed), otherwise returns the wait duration **without sleeping and without consuming tokens**. Call sites loop `{ wait = acquire(n); if wait.is_zero() { break } sleep(wait).await }`.

**Problem.** `RateLimiter::acquire` calls `std::thread::sleep` / `yield_now` in a busy-wait loop (`ratelimit.rs:73-90`) from inside async flush tasks. With `--ratelimit > 0` this blocks a tokio worker thread (a per-connection flush loop is `async`). Default `--ratelimit 0` already short-circuits, but enabled rate limiting must not stall the event loop. Rate limiting stays **per-session** (each session owns its `RateLimiter`, matching Go).

- [ ] **Step 1: Make `acquire` non-blocking.**

Replace `RateLimiter::acquire` body (`ratelimit.rs:50-105`) with:
```rust
    /// Reserve tokens for sending `n` bytes under the rate limit, without
    /// blocking. Returns `Duration::ZERO` if `n` bytes were granted
    /// immediately (tokens consumed). Otherwise returns the time the caller
    /// must wait and does **not** consume tokens — the caller should sleep
    /// asynchronously (e.g. `kio::sleep(wait).await`) and re-call `acquire`.
    /// `--ratelimit 0` (rate == 0) always grants immediately.
    pub fn acquire(&self, n: usize) -> Duration {
        let nf = n as f64;
        if nf == 0.0 {
            return Duration::ZERO;
        }
        let mut inner = self.inner.lock();
        if inner.rate <= 0.0 {
            return Duration::ZERO;
        }
        inner.refill();
        if inner.tokens >= nf {
            inner.tokens -= nf;
            Duration::ZERO
        } else {
            // Compute the wait to top up the deficit; do not sleep and do
            // not consume — the caller paces asynchronously and re-acquires.
            let deficit = nf - inner.tokens;
            Duration::from_secs_f64(deficit / inner.rate)
        }
    }
```
Update the struct-level doc comment (`ratelimit.rs:9-13`, "A rate limiter that blocks the caller...") to describe the non-blocking contract above.

- [ ] **Step 2: Update the server call site** (`kcptun-server/src/main.rs`, ~`1035-1039`).

Replace:
```rust
                    {
                        let total_bytes: usize = encrypted.iter().map(|b| b.len()).sum();
                        ratelimiter.acquire(total_bytes);
                    }
```
with:
```rust
                    {
                        let total_bytes: usize = encrypted.iter().map(|b| b.len()).sum();
                        loop {
                            let wait = ratelimiter.acquire(total_bytes);
                            if wait.is_zero() {
                                break;
                            }
                            kio::sleep(wait).await;
                        }
                    }
```

- [ ] **Step 3: Update the client flush-loop call site** (`kcptun-client/src/main.rs`, ~`1200-1203`).

Replace:
```rust
                        let total_bytes: usize = encrypted.iter().map(|b| b.len()).sum();
                        rate_limiter2.acquire(total_bytes);
```
with the same non-blocking loop (`let wait = rate_limiter2.acquire(total_bytes); if wait.is_zero() { break; } kio::sleep(wait).await;`).

- [ ] **Step 4: Update the client lib-KCP write path** (`kcptun-client/src/main.rs`, ~`1655`).

Replace:
```rust
                rate_limiter.acquire(to_send.len());
```
with the same non-blocking loop.

- [ ] **Step 5: Update the unit tests** (`kcptun-common/src/ratelimit.rs`, `mod tests`).

The non-blocking contract changes the meaning of the returned `Duration` in `test_rate_limit_enforces_wait` (it now returns the *computed* wait without sleeping). Keep assertions that still hold, and add one that pins the non-consuming behavior:
```rust
    #[test]
    fn rate_limit_enforces_wait() {
        let lim = RateLimiter::new(1_000_000); // 1 MB/s
        // Drain the bucket.
        assert_eq!(lim.acquire(1_000_000), Duration::ZERO);
        // Not enough tokens — returns the wait needed, WITHOUT consuming.
        let w1 = lim.acquire(100_000);
        assert!(w1 >= Duration::from_millis(80), "wait was {:?}", w1);
        assert!(w1 <= Duration::from_millis(500), "wait was {:?}", w1);
        // Tokens were NOT consumed by the failed acquire: an immediate
        // re-acquire still sees the same deficit (no free lunch).
        let w2 = lim.acquire(100_000);
        assert!(
            w2 >= Duration::from_millis(80),
            "deficit must not be consumed on a non-granting acquire (w2={:?})",
            w2
        );
    }
```
Do not change `test_zero_rate_is_unlimited`, `test_small_burst_passes_immediately`, `test_set_rate_dynamically`, or `test_zero_n_returns_immediately` unless the new contract breaks them (it should not — verify).

- [ ] **Step 6: Verify.**

Run:
```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo build --release
cargo test --release --package kcptun-server --test stress_test -- --nocapture --test-threads=1
```
Also run the rate limiter unit tests specifically:
```bash
cargo test -p kcptun-common ratelimit -- --nocapture
```
Expected: all pass.

- [ ] **Step 7: Commit.**

```bash
git add kcptun-common/src/ratelimit.rs kcptun-server/src/main.rs kcptun-client/src/main.rs
git commit -m "perf(ratelimit): non-blocking per-session acquire; async pacing in client/server flush loops"
```

---

## Out of scope (deferred P3)

- Merging the flush cycle's repeated SMUX stream-map lock acquisitions (`prepare_outbound_into` + Phase 1a + still-pending check) — touches `smux-rs` API, higher risk.
- Linux `recvmmsg` slot `Vec::with_capacity(2048)` per-packet allocation in `kio-rs` — small, cross-crate.
- macOS per-packet `to_vec()` in the batch recv path — known, recorded in `bench/profiles/HOTSPOTS.md`.
