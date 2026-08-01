<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-07-22 (inbound CFB/null less-copy) -->

# kcptun-client

## Purpose

kcptun client binary: local TCP listen → SMUX over KCP/UDP to remote server. Single-file `src/main.rs` owns CLI, `KcpConn` flush loop, optional QPP, SNMP log, optional pprof. Shared helpers (`derive_key`, `apply_mode`, `SnappyStreamDecoder`) live in `kcptun-common`.

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Features `tokio`/`smol`/`qpp`/`pprof`; deps kcp/kcrypt/**common**(pipe/snmp/QPP)/smux/kio |
| `build.rs` | Build-time glue |
| `src/main.rs` | Entire binary: `Cli`, `KcpConn`, `QPPPort`, `handle_client`, `snmp_logger`, `run_pprof` |

## Subdirectories

None (flat binary crate).

## For AI Agents

### Working In This Directory

- Stack: local TCP → (optional QPP) → SMUX stream → Snappy session → KCP → BlockCrypt → UDP.
- Flush loop is **4-phase** to minimize KCP mutex hold; keep crypto/snappy outside the lock.
- **Experimental lib path (M1-A)**: `--experimental-lib-kcp` (default off) routes connections through
  `LibKcpConn` (`kcp_rs::KcpConn` via `dial_kcp_session`) instead of the inlined KCP loop. The accept
  loop / scavenger dispatch through the `SessionHandle` trait (`Vec<Box<dyn SessionHandle>>`).
  A flaky tail-loss on large transfers was fixed via the SMUX EOF grace in `smux_rs::Stream::read`
  (see CHANGELOG / production migration plan §0.3). Do not enable by default until M2 flips the flag.
- Session cipher: `Arc<kcrypt_rs::CryptEngine>` (no separate `Arc<dyn AeadCrypt>`).
- Shared: `kcptun_common::{derive_key, apply_mode, SnappyStreamDecoder, SnappyPipe, pipe, snmp_logger, QPPPort?}`.
- Global allocator: `mimalloc`.
- Prefer `kio::*` for async; dual impl blocks for tokio/smol on AsyncRead/Write wrappers.
- SNMP logger only meaningful when SNMP collection is enabled in kcp-rs.
- UDP reader: CFB decrypts **in place** (`decrypt_cfb_in_place`); large non-FEC datagrams may `cpu_block` via `should_cpu_block_decrypt`.
- ACK encrypt uses dedicated `ack_crypto_buf` + `CryptoBuf::encrypt_packet` / `seal_aead` (salsa/xor must be headerless — never bare `encrypt_cfb`).

### Testing Requirements

- `cargo test -p kcptun-client`
- `make e2e` / `bash test_e2e.sh` after client path changes
- `make stress` (server-side) still validates client interop under load when used together

### Common Patterns

- Config: CLI + optional JSON (`deny_unknown_fields`)
- Multi-port remote parse: `host:min-max` / `host:port`

## Dependencies

### Internal

- `kcp-rs`, `kcrypt-rs`, `kcptun-common`, `smux-rs`, `qpp-rs`, `kio-rs`

### External

- `clap`, `serde`/`serde_json`, `snap`, `pbkdf2`/`sha1`, `parking_lot`, `socket2`, `mimalloc`, optional `pprof`

<!-- MANUAL: pprof feature is optional and off by default (keeps ARM release bins small). Enable with --features pprof. -->
