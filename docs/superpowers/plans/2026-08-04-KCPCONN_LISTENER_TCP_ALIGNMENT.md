# Plan: KcpListener ↔ std::net::TcpListener 接口对齐

> **Canonical path (git):** `docs/superpowers/plans/2026-08-04-KCPCONN_LISTENER_TCP_ALIGNMENT.md`

| Field | Value |
|-------|-------|
| Status | implemented |
| Created | 2026-08-04 |
| Scope | `kcp-rs` `KcpListener`/`KcpTcpListener` — 让监听器接口与 `tokio::net::TcpListener` / `std::net::TcpListener` 靠齐,与既有 `KcpConn`↔`TcpStream` 对齐形成闭环 |
| Out of scope | `incoming()`(方案 A:跳过,async 惯用法 `loop { accept().await }` 已覆盖);`KcpTcpListener::try_accept`(raw-TCP accept 阻塞,tokio 表面也无此方法);socket 选项(`set_ttl`/`set_only_v6`/`set_nonblocking`,与 KcpConn 已忽略的 ttl/keepalive 同类);wire 层改动 |
| Related | `kcp-rs/src/listener.rs`, `kcp-rs/src/conn.rs`, `kcp-rs/AGENTS.md`, `docs/superpowers/specs/2026-08-04-KCPCONN_TCPSTREAM_ALIGNMENT.md` |

## Problem

`KcpConn` 已对齐 `TcpStream`;`KcpListener` 只实现了 `bind(builder)/accept/local_addr/close`,缺
`accept_timeout`、`try_accept`、`take_error`,且 `bind` 必须显式 `.build().await`(与
`TcpListener::bind(addr).await` 写法不一致)。

## 方案（Solution）

1. **builder `IntoFuture`**:`KcpConnBuilder`/`KcpListenerBuilder`/`KcpTcpListenerBuilder` 实现
   `std::future::IntoFuture`,让 `KcpListener::bind(addr).await?` / `KcpConn::connect(addr).await?`
   直接可用;`.build().await` 保留。(`KcpTcpListenerBuilder::build` 是同步的,用 async 块包裹。)
2. **`accept_timeout(Duration)`**:`kio::timeout(t, self.accept())`,超时返 `TimedOut`。KcpListener + KcpTcpListener。
3. **`try_accept()`**:非阻塞弹 pending 队列,空返 `Ok(None)`;`closed` 时返 `ConnectionAborted`。仅 KcpListener(UDP demux)。KcpTcpListener 跳过(raw-TCP `accept` 阻塞,tokio 表面无此方法)。
4. **`take_error()`**:`KcpListener` 存 demux reader 的 recv 错误(`last_error` 经 `spawn_listener_reader` 传递);`KcpTcpListener` 存 accept 错误。`take_error` 返回并清除。

## 实施顺序（Implementation order）

1. `listener.rs`:KcpListener/KcpTcpListener 新增方法 + `last_error` 字段。
2. `conn.rs`:KcpConnBuilder IntoFuture;`listener.rs`:两个 listener builder IntoFuture。
3. 测试(accept_timeout/try_accept/take_error/IntoFuture)+ AGENTS/README。
4. `make gate` + Go↔Rust e2e。

## 验收（Acceptance）

- `make gate` 全绿;`cargo test -p kcp-rs --features async-tokio/async-smol` 全绿(含 5 个新测试)。
- Go↔Rust e2e(`make e2e`)全过,**138 passed, 0 failed** —— 功能扩展且 wire 兼容 Go。
