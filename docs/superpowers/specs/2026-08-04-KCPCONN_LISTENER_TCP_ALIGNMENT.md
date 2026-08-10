# Spec: KcpListener ↔ std::net::TcpListener 接口对齐 — implementation record

> **Canonical path (git):** `docs/superpowers/specs/2026-08-04-KCPCONN_LISTENER_TCP_ALIGNMENT.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-04 |
| All commits | single session (uncommitted working tree) |
| Bug report | — |

## 改动的文件列表（Files changed）

| File | What & why |
|------|-----------|
| `kcp-rs/src/listener.rs` | `KcpListener`:`accept_timeout` + `try_accept` + `take_error` + `last_error` 字段(demux reader recv 错误经 `spawn_listener_reader` 存储);`KcpTcpListener`:`accept_timeout` + `take_error` + `last_error` 字段(accept 错误);两个 listener builder 加 `IntoFuture` |
| `kcp-rs/src/conn.rs` | `KcpConnBuilder` 加 `IntoFuture`(与 listener 保持一致) |
| `kcp-rs/tests/kcpconn_listener.rs` | 新增 5 测试:`listener_bind_into_future`、`kcpconn_connect_into_future`、`listener_accept_timeout`、`listener_try_accept`、`listener_take_error_initial_none` |
| `kcp-rs/AGENTS.md` | listener.rs Key File 行补 TcpListener-aligned surface |
| `kcp-rs/README.md` | KcpListener API 表补 `accept_timeout`/`try_accept`/`take_error` + bind IntoFuture 说明 |

## 测试结果（Test results）

- `cargo fmt --all -- --check` — clean
- `cargo test --workspace` — 23 test groups, 0 failures
- `cargo clippy --workspace -- -D warnings` — clean
- `cargo test -p kcp-rs --features async-tokio` / `async-smol` — 全绿(含 5 个新测试)
- **Go↔Rust e2e(`make e2e`):138 passed, 0 failed, 0 skipped** —— 全部 cipher × mode × smuxver × nocomp × FEC 组合与 Go kcptun 互通,wire 兼容确认

## 修订记录（Revision history）

| Date | Change |
|------|--------|
| 2026-08-04 | 初始实现:listener TCP 对齐(accept_timeout/try_accept/take_error/IntoFuture)+ 测试 + e2e 验证 |
