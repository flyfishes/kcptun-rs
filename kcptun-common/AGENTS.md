<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-28 | Updated: 2026-07-31 (Task 7: library-ready KcpSession) -->

# kcptun-common

## Purpose

Shared helpers for `kcptun-client` and `kcptun-server` so wire-compatible logic is not duplicated. Also hosts the **library-ready** encrypted KCP session path (`CryptoTransport` + `kcp_session*`) built on `kcp-rs::KcpConn`.

## Key Files

| File | Description |
|------|-------------|
| `src/lib.rs` | Crate root; feature-gated re-exports |
| `src/key.rs` | PBKDF2-HMAC-SHA1 key derive (`salt = b"kcp-go"`) |
| `src/mode.rs` | KCP mode profiles (`normal`/`fast`/`fast2`/`fast3`) applied to bare `KCP` |
| `src/kcp_config.rs` | `kcp_config_from` / `KcpCliParams` → `kcp_rs::KcpConfig` (runtime feature) |
| `src/snappy_frame.rs` | Session-level Snappy framing stream decoder |
| `src/snappy_pipe.rs` | `SnappyPipe<T>` — `AsyncRead+AsyncWrite` Snappy session codec wrapping any transport (M0.4) |
| `src/pipe.rs` | Idle-timeout bidirectional pipe (`kio::copy_bidirectional_idle`) |
| `src/snmp_log.rs` | Periodic SNMP CSV logger |
| `src/session.rs` | `CryptoTransport` (`PacketTransport`), `kcp_session*`, `dial_kcp_session`, `accept_kcp_peer` |
| `src/qpp_port.rs` | QPP stream wrapper (feature `qpp`) |

## Features

| Feature | Effect |
|---------|--------|
| `tokio` (default) | `kio-rs/tokio` + `kcp-rs/async-tokio` — pipe / snmp / CryptoTransport / kcp_session / kcp_config |
| `smol` | `kio-rs/smol` + `kcp-rs/async-smol` — same helpers, smol backend |
| `qpp` | `qpp-rs` + `QPPPort` |

Binaries must forward their runtime feature:

```toml
tokio = [..., "kcptun-common/tokio"]
smol  = [..., "kcptun-common/smol"]
qpp   = ["dep:qpp-rs", "kcptun-common/qpp"]
```

## KcpSession path (library-ready; production cut-over deferred)

| Helper | Role |
|--------|------|
| `CryptoTransport` | `PacketTransport` that encrypt/decrypt wraps UDP (CFB/AEAD/null via `kcrypt_rs::wire` + `CryptEngine`) |
| `kcp_session` / `kcp_session_with_socket` | Build `KcpConn` over `CryptoTransport` + FEC from `KcpConfig` |
| `kcp_config_from` / `KcpCliParams` | Map CLI-shaped params → `kcp_rs::KcpConfig` |
| `dial_kcp_session` | Client-shaped dial helper (tests / optional path) |
| `accept_kcp_peer` | Single-peer accept helper (not multi-peer `KcpListener`) |

**Status (Tasks 1–7):**

- **Done:** CryptoTransport, kcp_session, kcp_config_from, dial/accept helpers; unit-tested.
- **Production binaries still use legacy** KCP+SMUX+Snappy flush loops — full cut-over is follow-up.
- **Snappy stays outside KcpConn** (session-level over KCP user data).
- **Not done:** multi-peer server demux via `KcpListener`; production client/server rewrite.

## For AI Agents

- Prefer extending this crate over copying helpers into binaries.
- `pipe` is **idle** timeout (Go `closeWait`), not total duration.
- Do not change Snappy framing or PBKDF2 parameters (wire / key interop).
- QPP AsyncRead/Write has separate tokio (`ReadBuf`) and smol (`&mut [u8]`) impls.
- When wiring binaries to `KcpConn`, map CLI via `KcpCliParams` / `kcp_config_from`;
  do **not** put Snappy or SMUX inside KcpConn.
- Compose: `KcpSession` (this crate) → optional Snappy → `SmuxConn::connect` / `serve`.

## Dependencies

- Always: `kcp-rs`, `pbkdf2`, `sha1`, `snap`, `log`
- With runtime feature: `kio-rs`, `anyhow`, `bytes`, `parking_lot`
- Optional: `qpp-rs`

<!-- MANUAL: -->
