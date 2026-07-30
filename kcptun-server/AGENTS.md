<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-07-22 (feed_data_mut in-place inbound) -->

# kcptun-server

## Purpose

kcptun server binary: UDP/KCP accept → SMUX → target TCP. `KcpServerSession` per peer, DashMap session table, optional QPP, SNMP log, optional pprof. Shared helpers live in `kcptun-common`. Stress tests in `tests/stress_test.rs` (no AGENTS under `tests/`).

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Features `tokio` (default) / `smol`; optional `pprof`; + `dashmap` / `kcptun-common` |
| `build.rs` | Build-time glue |
| `src/main.rs` | Entire binary: `Cli`, `KcpServerSession`, `handle_stream`, `pipe` idle timeout, pprof |
| `tests/stress_test.rs` | Multi-connection stress / data integrity (run via `make stress`) |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `tests/` | Integration stress tests only — **no AGENTS.md** |

## For AI Agents

### Working In This Directory

- Stack: UDP → decrypt/FEC → KCP → Snappy → SMUX → (optional QPP) → target TCP.
- `pipe` uses **idle** timeout (`closewait`), not total duration — matches Go; do not convert to hard total timeout.
- Session cipher: `Arc<kcrypt_rs::CryptEngine>`; large non-FEC inbound may `feed_data_owned` on `cpu_block` (`should_cpu_block_decrypt`).
- Inbound default: `feed_data_mut` — CFB/null in place, then FEC + `KCP::input` + SMUX.
- Flush loop 4-phase like client; keep lock short.
- Shared: `kcptun_common::{derive_key, apply_mode, SnappyStreamDecoder, pipe, snmp_logger, QPPPort?}`.
- Known open issue history: proxy SMUX stream leak (`bugs/BUGREPORT_PROXY_MEMORY_GROWTH.md`).

### Testing Requirements

```bash
make stress
cargo test --release --package kcptun-server --test stress_test -- --nocapture --test-threads=1
make e2e
```

### Common Patterns

- `DashMap` for concurrent session lookup
- Log rotation helper for file logs

## Dependencies

### Internal

- `kcp-rs`, `kcrypt-rs`, `kcptun-common`, `smux-rs`, `qpp-rs`, `kio-rs`

### External

- Same family as client + `dashmap`; optional `pprof`

<!-- MANUAL: pprof feature is optional and off by default (keeps ARM release bins small). Enable with --features pprof. -->
