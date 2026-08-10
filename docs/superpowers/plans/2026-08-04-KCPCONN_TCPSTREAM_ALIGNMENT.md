# Plan: KcpConn ↔ std::net::TcpStream 接口对齐

> **Canonical path (git):** `docs/superpowers/plans/2026-08-04-KCPCONN_TCPSTREAM_ALIGNMENT.md`

| Field | Value |
|-------|-------|
| Status | implemented |
| Created | 2026-08-04 |
| Scope | `kcp-rs` `KcpConn` — 让异步 KCP 可靠流接口与 `std::net::TcpStream` / `tokio::net::TcpStream` 靠齐，降低学习成本 |
| Out of scope | `set_ttl`/`ttl`、`set_keepalive`/`keepalive`（用户明确忽略）；UDP socket buffer size 直通（`set_recv_buffer_size`/`set_send_buffer_size`，与 ttl/keepalive 同类，低价值）；wire 层 FIN（KCP 无 FIN 段） |
| Related | `kcp-rs/src/conn.rs`, `kcp-rs/src/kcp.rs`, `kcp-rs/AGENTS.md`, `docs/superpowers/specs/2026-07-30-KCP_NETCONN_ABSTRACTION_ANALYSIS.md` |

## Problem

`KcpConn` 已有 `kio::AsyncRead`/`AsyncWrite`（数据面已对齐），但固有便捷方法缺失：没有
`peer_addr` 等固有便捷方法缺失：没有
`shutdown` 半关闭、读/写超时、`split`/`into_split`、`connect_timeout`、`peek`、
`take_error`；且 KCP 特有 setter（`set_nodelay(4参)` 等）占用裸 `set_*` 名字，与 TCP 语义的
`set_nodelay(bool)` 冲突。目标：用完整个 TCP 常用 API 面，KCP 调优接口加 `set_kcp_*` 前缀避让。

## 方案（Solution）

1. **重命名**：`KcpConn` 实例上的 KCP 特有 setter 统一加 `set_kcp_` 前缀
   （`set_nodelay(4参)`→`set_kcp_nodelay`，`set_window_size`→`set_kcp_window_size`，
   `set_mtu`→`set_kcp_mtu`，`set_stream_mode`→`set_kcp_stream_mode`），新增
   `set_kcp_acknodelay`。裸 `set_*` 留给 TCP 对齐方法。爆炸半径为零（无库外调用者）。
   `KCP` 状态机方法与 builder 方法被类型名隔离，不改。
2. **TCP 对齐新增**（`KcpConn`）：
   - `set_nodelay(bool)` + `nodelay()`（bool→Fast3/Normal KCP 参数）
   - `set_read_timeout`/`set_write_timeout` + getters（`TimedOut` 返回）
   - `shutdown(std::net::Shutdown)` 半关闭（Read/Write/Both；`Both`≈`close`）
   - `peek(&mut [u8])`（非阻塞，`WouldBlock` 当空）
   - `take_error() -> io::Result<Option<io::Error>>`（后台循环写错）
   - `split()`/`into_split()`（借用 + 自有 halves；`Lifecycle` 引用计数最后一个 half drop 时 `close`）
   - `readable()`/`writable()`（现有 notify 驱动）
   - builder `.connect_timeout(Duration)`：强制 `WASK` 探活 → 等首个 conv 合法入包（`WINS`/ACK）
3. **语义修正**：`poll_shutdown`/`poll_close` 从"全关"改为 tokio 语义的**只关写方向**
   （生产栈用显式 `close()`，已验证无回归）。`poll_read` 在 `read_closed`/`closed` 时返 EOF(0)。
4. **timeout 实现**：`read_shared`/`write_all_shared` 用 `kio::timeout` 包 notify 等待；
   `poll_read`/`poll_write` 用 mono-ms deadline + 一次性定时唤醒任务。

## 实施顺序（Implementation order）

1. `kcp.rs` 新增 `request_probe()`（`probe |= ASK_SEND`）→ 供 connect_timeout 强制首探。
2. `conn.rs` `KcpConnShared` 新增字段：read/write timeout、deadline、`last_error`、`nodelay`、
   `write_closed`/`read_closed`、`first_inbound`；`acknodelay` 改 `AtomicBool`。
3. 重命名 setter + 新增 TCP 对齐方法 + halves/split + builder connect_timeout + poll 语义。
4. 后台循环写 `last_error`；`feed_inbound_batch` 置 `first_inbound`。
5. 测试 + AGENTS/README 同步。

## 验收（Acceptance）

- `make gate`（fmt + `cargo test --workspace` + clippy -D warnings）全绿。
- `cargo test -p kcp-rs --features async-tokio` 与 `--no-default-features --features async-smol` 全绿。
- 新增测试：`into_split` echo、`shutdown(Write)` 拒写+数据送达、`read_timeout` TimedOut、
  TCP 面（nodelay/peek/take_error/readable/writable）、`connect_timeout` 活监听成功 + 死端口超时。
