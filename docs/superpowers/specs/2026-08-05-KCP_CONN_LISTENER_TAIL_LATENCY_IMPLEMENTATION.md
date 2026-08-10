# Spec: kcp-rs conn/listener 尾延迟优化 — implementation record

> **Canonical path (git):** `docs/superpowers/specs/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY_IMPLEMENTATION.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-05 |
| All commits | `ae765aa7`, `94ee0b21`, `b2a21b44`, `1e2f9b64`, `2ed978f3`, `e1c3ebae`, `4287a9bf`, `f7c6aa39` |
| Plan | `docs/superpowers/plans/2026-08-05-KCP_CONN_LISTENER_TAIL_LATENCY.md` |

## 改动的文件列表（Files changed）

| File | Change |
|------|--------|
| `kcp-rs/src/conn.rs` | waker 锁外唤醒（clone-in-lock + wake-outside）；`RawPacketQueue` pending/spare 双缓冲；`read_fallback_timeout` 计数；input loop 100ms timeout → `kio::race(recv, cancel_token.cancelled())` |
| `kcp-rs/src/listener.rs` | drop-tail/admission-drop buffer 回收；`RouteOutcome`→直接返回 spare；RECV_BATCH 冷启动预分配；affected HashSet O(1) 去重；`process_builds` 三阶段 + `max_build_time_per_wakeup`；reader 100ms timeout → race |
| `kcp-rs/src/transport.rs` | `push_and_reuse` 返回 `(Vec<u8>, bool)`，drop-tail 回收入站 buffer |
| `kcp-rs/src/fec.rs` | `recover()` 删除死 `new_buffers` clone |
| `kcp-rs/src/snmp.rs` | Rust-only `read_fallback_timeout` 计数器（默认关，不入 Go CSV） |
| `kio-rs/src/sync/cancel.rs` | **新增** `CancellationToken`（基于 permit-storing `Notify`）+ 运行时无关 `race(a, b)` |
| `kio-rs/src/sync/mod.rs`, `lib.rs` | cancel 模块导出 |
| `kio-rs/AGENTS.md`, `kcp-rs/AGENTS.md` | 可取消 recv 语义 + sync/cancel 文档同步 |

## 修复的故障路径（Fixed failure paths）

- **`kcpconn_read_timeout` 无限挂起**（自己引入并修复）：waker `take()` 使 `waiter_changed` 恒真 → deadline 被清 → 定时任务无限重挂。改用 clone-in-lock + wake-outside，waker 留在槽位以保留 deadline 语义。
- **close 延迟依赖 100ms tick**：可取消 recv 使 close() 立即唤醒 input/reader task，消除每空闲连接 ~10 Hz timer churn。
- **drop 路径每包重新分配 MAX_DATAGRAM slot**：admission/queue drop 回收被丢弃 datagram 的 buffer。
- **raw 输出每 drain 锁内分配**：pending/spare swap 锁内零分配 + 容量回收。
- **连接风暴下 build 锁次数随 peer 线性增长**：三阶段批量降至 ~2 锁/batch。

## 测试结果（Test results）

- `make gate`（fmt + `cargo test --workspace` + `cargo clippy --workspace -- -D warnings）：全绿 ×8`
- 新增测试：waker 回归（kcpconn_read_timeout / write_shared_timeout / readable_timeout）、`push_drop_tail_recycles_buffer`、`route_inner_admission_drop_recycles_buffer`、`raw_packet_queue_recycles_capacity` / `_caps_retained_batch`、`route_inner_dedups_affected_per_peer`、kio `cancelled_*` / `race_returns_second_when_cancelled`（tokio+smol）
- kcp-rs async 集成：40 passed（integrity + listener × tokio/smol）
- `make e2e`（Go↔Rust 互操作）：**138 passed / 0 failed**

### 最终延迟数字（`latency_p99 --mode self --rps 500 --size 26624`，3s warmup + 15s，7500 样本）

| runtime | p50 | p90 | p99 | p999 |
|---------|-----|-----|-----|------|
| tokio | 242µs | 287µs | **396µs** | **506µs** |
| smol | 309µs | 365µs | **454µs** | **579µs** |

对比此前 spec（2026-08-05）基线：tokio-multi P99 6720µs / tokio-single 557µs / smol 8626µs。
两 runtime P99 均压到亚毫秒，tokio 优于旧 tokio-single 最优值。

### 重新 profile 结论（`rust-server-null-20260805-233001.pb`）

kcp-rs 内部工作已接近零（listener 路由 cum ~0.2%、spawn_input_loop 出 top、mimalloc ~10%→~3%）。
服务器 72% CPU 是 macOS UDP syscall（send_to 43% + recv_from 28%，无 sendmmsg）。达到计划 §17 停止条件。

## 修订记录（Revision history）

| Date | Note |
|------|------|
| 2026-08-05 | 8 项优化落地；Phase 5/7/8 与 Phase 6 余项按证据门控延期（见计划 §19）；重 profile 确认 kcp-rs 至 syscall 绑定极限 |
