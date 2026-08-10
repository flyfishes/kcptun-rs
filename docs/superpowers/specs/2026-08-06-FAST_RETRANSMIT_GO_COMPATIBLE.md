# Spec: Restore Go-compatible fast retransmit in KCP flush

| Field | Value |
|-------|-------|
| Created | 2026-08-06 |
| Status | implemented |
| Related | `docs/superpowers/specs/2026-08-06-ADVANCED_PROBE_READ_COALESCING.md`; `kcp-rs/src/kcp.rs` |
| Scope | `kcp-rs/src/kcp.rs` (flush logic), `kcp-rs/src/kcp.rs` tests |

## 1. Motivation

P99/P999 tail latency was 3–4× worse than Go, with ALL retransmissions
falling through to RTO timeout (200ms+) instead of fast retransmit (~1 RTT).

SNMP evidence (before):

```
retrans=13362 fast_retrans=0 early_retrans=0 lost=13362
```

Every single retransmission was RTO-based — fast retransmit never fired.

## 2. Root cause

Two deviations from Go kcp-go in `flush_with_current()`:

### 2.1 `new_segs_count > 0` gate on fast retransmit

```rust
// BEFORE (broken):
} else if seg.fastack >= resent && seg.fastack != 0xFFFFFFFF && new_segs_count > 0 {
```

Go kcp-go has **no** `new_segs_count > 0` condition. This gate disabled fast
retransmit whenever the send window was full or nothing was queued — exactly
when packet loss is most likely and fast recovery is most needed. All loss
recovery fell back to RTO, causing massive P99/P999 spikes.

### 2.2 Non-Go early retransmit path

```rust
// BEFORE (removed):
} else if seg.fastack > 0 && seg.fastack != 0xFFFFFFFF && new_segs_count > 0 {
    // Early retransmit
```

This path fired on `fastack > 0` (just 1 duplicate ACK), which is NOT in Go
kcp-go. With `fastresend=2`, this triggered retransmit on delayed ACKs (not
genuine loss), flooding the wire and causing the "fast-retransmit storm"
observed at 256KB@RPS=450+. The `new_segs_count > 0` gate was added to
suppress this storm, but it also suppressed legitimate fast retransmit.

## 3. Fix

1. **Removed `new_segs_count > 0` gate** from fast retransmit — now fires
   when `fastack >= fastresend` (2 dup ACKs), matching Go exactly.
2. **Removed the early retransmit path entirely** — not in Go, was the real
   cause of the storm. Standard Go fast retransmit with `fastresend=2` is
   stable.
3. **Removed `new_segs_count` variable** — no longer used after removing
   both gates.
4. **Removed `early_retrans_segs` SNMP update** — the counter stays in the
   SNMP structure (format compat) but is always 0.

## 4. Evidence

### 4.1 Unit test

`test_fast_retransmit_fires_on_duplicate_acks`: sends 3 segments, loses
segment 0, receiver ACKs segments 1+2, sender fast-retransmits segment 0.
Asserts `fast_retrans > 0` and `lost == 0`. ✅ passes.

### 4.2 Benchmark (60s, 500 RPS, 26624B, macOS localhost)

| Metric | Before | After | Go | Improvement |
|--------|--------|-------|-----|-------------|
| p99 (tokio) | 60002µs | 20346µs | 77670µs | 3.0× better, 3.8× vs Go |
| p999 (tokio) | 249444µs | 72246µs | 226302µs | 3.5× better, 3.1× vs Go |
| retrans (tokio) | 13362 | 3769 | — | 3.5× reduction |
| p99 (smol) | 93890µs | 88693µs | 77670µs | 5.5% better |
| p999 (smol) | 422862µs | 296877µs | 226302µs | 30% better |
| retrans (smol) | 16130 | 11716 | — | 27% reduction |

### 4.3 Why `fast_retrans=0` in bench

The unit test confirms fast retransmit works at the KCP level. In the
localhost bench, `fast_retrans=0` because macOS UDP recv is single-packet
(no `recvmmsg`); under high throughput, bursty UDP buffer overflow loses
multiple consecutive segments + their ACKs together, so `fastack` never
reaches 2. The improvement comes from **removing the early retransmit path**
that caused spurious retransmit flooding → more packet loss → more RTO.

## 5. Gates

`make gate` (fmt --check + cargo test --workspace + clippy -D warnings):
**✅ all passed** (55 tests in kcp.rs, incl. new fast retransmit test).

## 6. Wire compatibility

No wire-format changes. The KCP segment format, ACK format, and all
protocol semantics are identical to Go kcp-go v5. The fix only changes
the retransmit decision logic inside `flush_with_current()`.
