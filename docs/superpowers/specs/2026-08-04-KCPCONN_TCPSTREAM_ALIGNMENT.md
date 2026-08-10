# Spec: KcpConn ↔ std::net::TcpStream 接口对齐 — implementation record

> **Canonical path (git):** `docs/superpowers/specs/2026-08-04-KCPCONN_TCPSTREAM_ALIGNMENT.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-04 |
| All commits | single session (uncommitted working tree) |
| Bug report | — |

## 改动的文件列表（Files changed）

| File | What & why |
|------|-----------|
| `kcp-rs/src/kcp.rs` | 新增 `KCP::request_probe()`（`probe |= KCP_ASK_SEND`），connect_timeout 强制首探的基础 |
| `kcp-rs/src/conn.rs` | 核心改动（+658 行）：`KcpConnShared` 新增 timeout/deadline/`last_error`/`nodelay`/`write_closed`/`read_closed`/`first_inbound` 字段，`acknodelay`→`AtomicBool`；KCP setter 重命名 `set_kcp_*`；新增 set_nodelay(bool)/nodelay/take_error/peek/shutdown/read·write timeout/split/into_split/readable/writable + builder `.connect_timeout`；`poll_shutdown`/`poll_close` 改为写半关闭；`poll_read`/`do_poll_write` 支持半关闭+deadline 超时；后台循环写 `last_error`；`feed_inbound_batch` 置 `first_inbound` |
| `kcp-rs/tests/kcpconn_integrity.rs` | 新增 `into_split` echo、`shutdown(Write)` 拒写+送达、`read_timeout` TimedOut、TCP 面（nodelay/peek/take_error/readable/writable）测试；`read_exact_timeout` 泛型化以支持 halves |
| `kcp-rs/tests/kcpconn_listener.rs` | 新增 `connect_timeout` 活监听成功 + 死端口超时测试 |
| `kcp-rs/AGENTS.md` | conn.rs Key File 行 + Async API sketch 补充 TcpStream 对齐面、`set_kcp_*` 约定、`poll_shutdown` 语义、connect_timeout 语义 |
| `kcp-rs/README.md` | `KcpConn (async)` API 表扩充 |

## 修复的故障路径（Fixed failure paths）

1. **read/write timeout 死锁**：`if let Some(dl) = *mutex.lock()` 的临时 `MutexGuard` 生命周期
   延伸到整个 `if let` 块，块内再次 `mutex.lock()` 清除 deadline → parking_lot 不可重入死锁
   （`kcpconn_read_timeout` 测试挂死 30min）。修复：先 `let x = *mutex.lock();` 拷贝值，guard
   在语句末尾释放后再重锁。
2. **`poll_shutdown` 语义**：原来 = 全关 `close()`；tokio 语义应为只关写方向。改为置
   `write_closed` + 唤醒 flush。生产栈（`kcptun_session`）显式 `close()`，已验证无回归。
3. **半关闭 + 无 wire FIN**：KCP 无 FIN 段，`shutdown(Write)` 只能本地拒写 + flush，无法让对端感知
   EOF（peer-aware 半关闭属 SMUX 层）。文档明确此限制。

## 测试结果（Test results）

- `cargo fmt --all -- --check` — clean
- `cargo test --workspace` — 23 test groups, 0 failures
- `cargo clippy --workspace -- -D warnings` — clean
- `cargo test -p kcp-rs --features async-tokio` — 全绿（含新测试）
- `cargo test -p kcp-rs --no-default-features --features async-smol` — 全绿（含新测试）

## 修订记录（Revision history）

| Date | Change |
|------|--------|
| 2026-08-04 | 初始实现：对齐面 + 重命名 + connect_timeout + 语义修正 + 测试/文档 |
