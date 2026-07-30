<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-28 | Updated: 2026-07-28 (phase-2 pipe/snmp/qpp) -->

# kcptun-common

## Purpose

Shared helpers for `kcptun-client` and `kcptun-server` so wire-compatible logic is not duplicated.

## Key Files

| File | Description |
|------|-------------|
| `src/lib.rs` | Crate root; feature-gated re-exports |
| `src/key.rs` | PBKDF2-HMAC-SHA1 key derive (`salt = b"kcp-go"`) |
| `src/mode.rs` | KCP mode profiles (`normal`/`fast`/`fast2`/`fast3`) |
| `src/snappy_frame.rs` | Session-level Snappy framing stream decoder |
| `src/pipe.rs` | Idle-timeout bidirectional pipe (`kio::copy_bidirectional_idle`) |
| `src/snmp_log.rs` | Periodic SNMP CSV logger |
| `src/qpp_port.rs` | QPP stream wrapper (feature `qpp`) |

## Features

| Feature | Effect |
|---------|--------|
| `tokio` (default) | `kio-rs/tokio` — enables `pipe` / `snmp_logger` |
| `smol` | `kio-rs/smol` — same helpers, smol backend |
| `qpp` | `qpp-rs` + `QPPPort` |

Binaries must forward their runtime feature:

```toml
tokio = [..., "kcptun-common/tokio"]
smol  = [..., "kcptun-common/smol"]
qpp   = ["dep:qpp-rs", "kcptun-common/qpp"]
```

## For AI Agents

- Prefer extending this crate over copying helpers into binaries.
- `pipe` is **idle** timeout (Go `closeWait`), not total duration.
- Do not change Snappy framing or PBKDF2 parameters (wire / key interop).
- QPP AsyncRead/Write has separate tokio (`ReadBuf`) and smol (`&mut [u8]`) impls.

## Dependencies

- Always: `kcp-rs`, `pbkdf2`, `sha1`, `snap`, `log`
- With runtime feature: `kio-rs`, `anyhow`, `bytes`, `parking_lot`
- Optional: `qpp-rs`

<!-- MANUAL: -->
