<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-03 (CLI module extraction) -->

# kcptun-server

## Purpose

kcptun server binary: UDP or raw-TCP KCP accept → SMUX → target TCP. Shared UDP uses `kcptun_common::KcptunListener`; accepted raw-TCP sockets connect directly to the same `KcptunSession` implementation. Optional QPP, SNMP log, and pprof are supported. Stress tests live in `tests/stress_test.rs` (no AGENTS under `tests/`).

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Features `tokio` (default) / `smol`; optional `pprof`; shared stack from `kcptun-common` |
| `build.rs` | Build-time glue |
| `src/cli.rs` | Clap `Cli`, JSON `Config`, and deterministic CLI/config merge rules |
| `src/main.rs` | Runtime entry and orchestration: UDP/raw-TCP listener setup, unified session stream loop, stream forwarding, and pprof startup |
| `tests/stress_test.rs` | Multi-connection stress / data integrity (run via `make stress`) |
| `tests/autoexpire_multi_port_test.rs` | Functional: combined multi-port (`-l`/`-r` range) + client `--autoexpire` scavenger |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `tests/` | Integration stress tests only — **no AGENTS.md** |

## For AI Agents

### Working In This Directory

- Stack: UDP → decrypt/FEC → KCP → Snappy → SMUX → (optional QPP) → target TCP.
- `pipe` uses **idle** timeout (`closewait`), not total duration — matches Go; do not convert to hard total timeout.
- Default UDP inbound is demultiplexed once by `KcptunListener`, then each peer queue is wrapped by `CryptoTransport`; never let peer `KcpConn`s call `recv()` on the shared socket.
- Raw-TCP inbound sockets are already per-peer and use
  `KcptunSession::serve_transport`; UDP and raw TCP then share the same stream
  accept/forward lifecycle.
- Shared: `kcptun_common::{KcptunConfig, KcptunListener, KcptunSession, derive_key, pipe, snmp_logger, QPPPort?}`.
- Known open issue history: proxy SMUX stream leak (`bugs/BUGREPORT_PROXY_MEMORY_GROWTH.md`).

### Testing Requirements

```bash
make stress
cargo test --release --package kcptun-server --test stress_test -- --nocapture --test-threads=1
make e2e
```

### Common Patterns

- One `KcptunListener` demultiplexer per UDP listen socket
- `cli.rs` owns every flag/default/config-file merge rule; do not duplicate CLI interpretation in runtime modules
- Log rotation helper for file logs

## Dependencies

### Internal

- `kcp-rs`, `kcrypt-rs`, `kcptun-common`, `smux-rs`, `qpp-rs`, `kio-rs`

### External

- Same family as client; optional `pprof`

<!-- MANUAL: pprof feature is optional and off by default (keeps ARM release bins small). Enable with --features pprof. -->
