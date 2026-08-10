<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-03 (CLI module extraction) -->

# kcptun-client

## Purpose

kcptun client binary: local TCP listen → SMUX over KCP/UDP or KCP/raw-TCP to remote server. Both transports use `kcptun_common::KcptunSession`; the binary owns CLI, socket acquisition, stream forwarding, QPP, SNMP log, and optional pprof.

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Features `tokio`/`smol`/`qpp`/`pprof`; deps kcp/kcrypt/**common**(pipe/snmp/QPP)/smux/kio |
| `build.rs` | Build-time glue |
| `src/cli.rs` | Clap `Cli`, JSON `Config`, and deterministic CLI/config merge rules |
| `src/main.rs` | Runtime entry and orchestration: UDP/raw-TCP socket acquisition, `KcptunSession` pool, stream forwarding, logging, and pprof startup |

## Subdirectories

None (flat binary crate).

## For AI Agents

### Working In This Directory

- Stack: local TCP → (optional QPP) → SMUX stream → Snappy session → KCP → BlockCrypt → UDP.
- **Unified session path**: UDP and `--tcp` differ only in how they obtain a
  `kio::DatagramSocket`; both call `KcptunSession::connect` with a
  `KcptunConfig`. There is no binary-local session wrapper or dispatch trait.
  The connection pool stores `Vec<KcptunSession>` directly.
  A flaky tail-loss on large transfers was fixed via the SMUX EOF grace in `smux_rs::Stream::read`
  (see CHANGELOG / production migration plan §0.3).
- Shared: `kcptun_common::{KcptunConfig, KcptunSession, derive_key, pipe, snmp_logger, QPPPort?}`.
- Global allocator: `mimalloc`.
- Prefer `kio::*` for async; dual impl blocks for tokio/smol on AsyncRead/Write wrappers.
- SNMP logger only meaningful when SNMP collection is enabled in kcp-rs.
- Crypto, FEC, KCP input/flush, and Snappy scheduling belong to the common/KCP
  layers; do not reintroduce them in this binary.

### Testing Requirements

- `cargo test -p kcptun-client`
- `make e2e` / `bash test_e2e.sh` after client path changes
- `make stress` (server-side) still validates client interop under load when used together

### Common Patterns

- Config: `cli.rs` owns CLI + optional JSON (`deny_unknown_fields`); keep flag defaults and merge precedence wire-compatible with Go behavior
- Multi-port remote parse: `host:min-max` / `host:port`

## Dependencies

### Internal

- `kcp-rs`, `kcrypt-rs`, `kcptun-common`, `smux-rs`, `kio-rs`

### External

- `clap`, `serde`/`serde_json`, `parking_lot`, `socket2`, `mimalloc`, optional `pprof`

<!-- MANUAL: pprof feature is optional and off by default (keeps ARM release bins small). Enable with --features pprof. -->
