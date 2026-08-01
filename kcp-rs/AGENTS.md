<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-07-31 (session.rs removed; KcpListener multi-peer accept; listen/connect tests) -->

# kcp-rs

## Purpose

KCP ARQ (Automatic Repeat-reQuest) reliable UDP protocol state machine — port of Go `github.com/xtaci/kcp-go/v5`. Ordered, reliable delivery over UDP with congestion control, Reed-Solomon FEC, and atomic SNMP counters. **All crypto lives in `kcrypt-rs`** — kcp-rs has **no** dependency on it and **no** crypto re-exports; depend on `kcrypt-rs` directly for `BlockCrypt` / `CryptEngine` / `CryptoBuf` / wire packing.

Async surface (optional): `KcpConn` is a tokio-TCP-shaped `AsyncRead`/`AsyncWrite` over UDP with optional FEC. **No encryption inside KcpConn** — crypto is an external `PacketTransport` (see `kcptun-common::CryptoTransport`).

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Deps: `bytes`, `parking_lot`, `crossbeam`, `reed-solomon-erasure`, `crc32fast`; optional `kio-rs`. **No** `kcrypt-rs` dependency |
| `src/lib.rs` | Crate root; large intentional `#![allow(clippy::…)]` list — do not "fix" |
| `src/kcp.rs` | Core `KCP` state machine: windows, RTO, flush, input, NoDelay |
| `src/segment.rs` | 24-byte LE wire header, `Command` enum, `SegmentPool` (SegQueue) |
| `src/fec.rs` | `FecEncoder` / `FecDecoder` / `fec_expand_packets` / `fec_kcp_from_recovered`; header types `0x00f1` / `0x00f2` / `0x00f3` |
| ~~`src/crypto_buf.rs`~~ | **Removed** (B2) — moved to `kcrypt-rs::wire`; see `../kcrypt-rs/AGENTS.md` |
| `src/conn.rs` | (feature `async-*`) `KcpConn` + `KcpConnBuilder` + `KcpListener` (multi-peer accept) + `KcpConn::connect_tcp` + `KcpTcpListener` (Linux raw-TCP, 1 conn = 1 session) + `PacketTransport` |
| `src/config.rs` | **Always-on** `KcpConfig` / `KcpMode`; `KCP::apply` / `set_mode` (B1) |
| `src/snmp.rs` | Global `DEFAULT_SNMP` atomic counters; `snmp_enable` / `snmp_add` / `snmp_store` |
| `README.md` | User-facing usage guide: sync + async API, wire format, config, testing |
| `test.sh` | Standalone test runner: sync (default) + `async-tokio` + `async-smol` |
| `tests/data_correctness.rs` | Sync reliability + FEC data-correctness over in-memory flaky channel |
| `tests/kcpconn_integrity.rs` | Async `KcpConn` integrity over localhost UDP (tokio / smol) |
| `tests/kcpconn_listener.rs` | Server listen / client connect: accept echo, multi-peer demux, serve-after-close |

## Features

| Feature | Effect |
|---------|--------|
| *(default)* | Sync KCP only — no async deps |
| `async` | Alias → `async-tokio` |
| `async-tokio` | `KcpConn` + `PacketTransport` via `kio-rs/tokio` |
| `async-smol` | Same over `kio-rs/smol` (mutually exclusive with tokio at kio) |

## Async API sketch (`async-*`)

```rust
// Raw UDP, optional FEC; crypto is NOT here
let conn = KcpConn::connect("1.2.3.4:29900")
    .mtu(1400)
    .fec(10, 3)
    .mode(KcpMode::Fast3)
    .build()
    .await?;

// Or plug a custom PacketTransport (e.g. CryptoTransport from kcptun-common)
let conn = KcpConn::with_transport(transport, cfg).await?;
```

- `PacketTransport`: `send` / `recv` / `local_addr` / `remote_addr` (+ optional batch).
- FEC: `.fec(datashard, parityshard)` on builder; encode on flush, decode on input.
- `KcpListener`: `bind` → `accept() -> (KcpConn, SocketAddr)`. One bound UDP socket; demux by source addr
  via per-peer queue-backed `PeerTransport`. Reconnect = fresh client session (KCP SN continuity blocks
  same-stream reuse after a server-side close).
- Production `kcptun-client` / `kcptun-server` binaries still use **legacy** KCP+SMUX+Snappy flush loops; this stack is **library-ready**.

## For AI Agents

### Working In This Directory

- **Wire compatibility with kcp-go v5 is the primary constraint.** Control flow mirrors Go; crate-level clippy allows exist for that reason.
- `KCP::input()` must queue ACKs for **every** received Push segment.
- `snd_buf` cleanup: ACKed segments removed from the **front** in `flush()` (Go `k.snd_buf = k.snd_buf[1:]`).
- Constants (`IKCP_RTO_*`, `IKCP_PROBE_*`, `KCP_DEFAULT_WND=32`, cmds 81–84) must match Go.
- Crypto is entirely in `kcrypt-rs` — depend on it directly. kcp-rs has no crypto API / re-exports.
- `kcrypt_rs::wire::CryptoBuf` nonce is **not** the CFB IV (IV is fixed `GO_CFB_IV`); nonce is `[counter 8B][session_id 8B]`.
- SNMP collection is **opt-in** (`snmp_enable`) so hot paths stay free when unused.
- **Do not put crypto inside `KcpConn`.** Use `PacketTransport` wrappers (`CryptoTransport`).
- Snappy stays **outside** KcpConn (session-level over KCP user data in binaries / common).

### Testing Requirements

- Standalone runner: `bash kcp-rs/test.sh` (sync default → `async-tokio` → `async-smol`)
- Sync data-correctness: `cargo test -p kcp-rs --test data_correctness`
- Async integrity: `cargo test -p kcp-rs --features async-tokio --test kcpconn_integrity` (or `async-smol`)
- Listener / connect: `cargo test -p kcp-rs --features async-tokio --test kcpconn_listener` (or `async-smol`)
- In-module unit tests where present
- Async: `cargo test -p kcp-rs --features async-tokio`
- Interop: `bash test_e2e.sh` after segment/KCP/FEC changes
- Stress: `make stress` for flush/lock behavior under load

### Common Patterns

- Output callback: `Box<dyn FnMut(bytes::Bytes) + Send>` on `KCP`
- NoDelay modes applied by binaries via `nodelay/interval/resend/nc`
- FEC optional at **session / KcpConn layer** (`FecEncoder`/`FecDecoder`); no core-KCP FEC API
- Recovered FEC payload: `fec_kcp_from_recovered` (Go `r[2:sz]`); reconstruct present-flag is `true` = present
- Public API: `KCP`, `KcpConfig`/`KcpMode`, FEC + SNMP helpers. Crypto types (`BlockCrypt`, `CryptEngine`, `CryptoBuf`, `encrypt_batch`, wire helpers) live in `kcrypt-rs` — **not** re-exported here.
- With `async-*`: also `KcpConn`, `KcpConfig`, `KcpMode`, `PacketTransport`, `KcpListener`
- Wire packing / encrypt / offload heuristics (`CryptoBuf`, `encrypt_batch`, `decrypt_cfb_in_place`, `should_cpu_block_*`, `OffloadProfile`, …): see `../kcrypt-rs/AGENTS.md` — all live in `kcrypt_rs::wire`.

## Dependencies

### Internal

- None — crypto lives in `kcrypt-rs` (not a dependency of this crate)

### External

- `bytes`, `crossbeam`, `parking_lot`, `reed-solomon-erasure`, `crc32fast`
- optional `kio-rs` via features `async` / `async-tokio` / `async-smol`

<!-- MANUAL: -->
