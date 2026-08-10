# KCP 单发送者修复 — 回环乱序重传风暴根因与修复（2026-08-02）

## 问题

256 KiB @ RPS≥450 的裸 `kcp-rs` KcpConn 单任务 echo 基准下：

- **fast 重传风暴** 14–20K / 2s（RPS=450/500）
- 接收端 `rcv_nxt` 缺口：`gap` 8–11K / 2s，`gmax` ≈ 511（缺口横跨整个 512 窗口）
- `in ≈ out`（差 <0.1%）→ **无丢包**，纯重排

**关键判断：loopback 是 FIFO 链路，乱序到达只能说明发送端把段发乱序了**——这是代码缺陷，不是网络现象。

## 根因：共享发送队列被 3 个 task 并发 drain + 发送

`raw_packets`（共享发送 FIFO）有三个并发 drainer，各自走自己的发送 sink，wire 顺序无保证：

| drainer | 发送路径 |
|---------|---------|
| 写路径 `do_poll_write`（AsyncWrite）| `try_send_batch` 内联 |
| lib 隧道写路径 `write_all_shared` | `send_packets_with_fec` |
| 主 flush loop | `send_packets_with_fec` |

三个 task 各自 drain 到自己的批次、异步发送 → 批次在 wire 上交错 → 后发的高 sn 段先到 → 接收端 `rcv_nxt` 卡在缺头段 → 对端对在途段 fastack 累计 → fast 重传风暴。

此外 **ACK 乱序**：`flush_with_current` 从共享 `acklist` 取 ACK，被 4 条路径调用（写路径内联、主 flush loop、input loop、`update()`）——Go 只有单 goroutine 调 flush，无此竞态；我们把 flush 拆成多路径时引入。

## 修复：单发送者（single-drainer）+ 单 ACK 生产者

1. **`raw_packets` 只允许 flush loop drain + 发送**。`do_poll_write`、`write_all_shared`、input loop 全部改为生产后 `flush_notify.notify_one()`，不再内联发送。wire 顺序 = flush 顺序。
2. **`flush_data_only()`（`flush_with_current(current, flush_acks=false)`）**：写路径内联 flush 和主 flush loop 不再发 ACK。ACK 只由 input loop 的 deferred flush（`flush_acks=true`）发出——单 ACK 生产者。
3. 删除过时的 `urgent_pending` / `urgent_sending` 机制（单 drainer 后不再需要）。
4. 补 `in_pkts` / `out_pkts` 计数（之前恒为 0，无法做 `in≈out` 丢失对照）。

单元测试 `test_ack_emission_single_owner` 验证：`flush_data_only` 不清 acklist、`flush` 清。

## 验证结果

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| fast 重传 @RPS=450/500 | **14–20K / 2s** | **0**（确定性）|
| RPS=450 干净态 p50/p99 | 445ms / 672ms | **4.5ms / 18ms** |
| 隧道（lib）p50/p99 @RPS=500 | ~6.8ms | **6.2ms / 8.5ms，100%** |
| Go↔Rust 双向互操作 | — | ✅ 通过 |
| `make gate`（fmt+test+clippy）| — | ✅ 全过 |

回归脚本 `bench/run_p99_regression.sh`：主要断言 **fast 重传 < 3000/2s**（确定性 bug 守卫），次要断言 p99 低于修复前基线。

## 最终 p99 测试矩阵（256 KiB 单任务 echo，Fast3，窗口 512/512）

环境：macOS 26.6，load ≈ 2.3–3.3。**RPS≥450 是饱和边缘，双稳态**（同一配置有时干净有时崩，与负载无关）。

| RPS | p50 | p90 | p99 | 备注 |
|-----|-----|-----|-----|------|
| 100 | 3.4ms | 4.0ms | 4.9ms | 干净 |
| 200 | 2.8ms | 3.5ms | 6.7ms | 干净 |
| 300 | 2.9ms | 3.8ms | 7.1ms | 干净 |
| 400 | 3.1ms | 4.4ms | 42ms | 干净（尾部抖动）|
| 450 | 4.5–108ms | 12–260ms | 18–279ms | **双稳态** |
| 475 | 476ms | 687ms | 744ms | 崩塌 |
| 500 | 427ms | 637ms | 657ms | 崩塌 |

隧道对照（双任务 `copy_bidirectional`，同一 KcpConn）：**RPS=500 稳定 6.2ms / p99 8.5ms**，100% 成功——KCP 机制本身没有吞吐问题。

## 遗留问题

1. **RPS≥450 单任务 echo 双稳态崩塌（非 KCP bug）**：即便 fast=0、零重传、零丢包、in≈out，RPS≥450 时单任务 echo 基准仍间歇性进入数百毫秒。机制：client 的 `write_all` 阻塞在满窗口时停读 → echo 累积 → 延迟被记录成 oldest-send 时间；任何瞬态停顿（调度、flush loop 忙、socket 背压）都可能触发，且自增强。隧道（读写独立任务）不受影响。**修复方向**：基准结构（独立读任务）或写唤醒延迟调优（`WAIT_FALLBACK_MS`）；均需谨慎——用户明确单任务才是对 Go 公平的比较。
2. **基准负载敏感**：loadavg > ~4 时 p99 膨胀（40–300ms 波动），数字不可跨负载比较。回归脚本需在 loadavg < 3.5 时运行。
3. **macOS 无公开 `sendmmsg`/`recvmmsg`**（libSystem 无符号），批量收发需裸 syscall（未实施）。Linux 已有 `mmsg.rs`。

## 无效尝试（记录以免重试）

| 方案 | 结果 |
|------|------|
| 非对称窗口（rcv_wnd=2048）| 无变化，证伪 wnd=0 死锁主因 |
| `rmt_wnd==0` 抑制重传 | 无变化 |
| 完全禁用 fast/early 重传 | 更差 4× |
| ackOnly input flush | 更差 5× |
| ACK 独立 task（all-or-nothing）| 风暴 3× 更糟 |
| ACK 累积到 flush loop 批量发 | 更差 |
| **数据全走 flush loop（含 ACK）** | 风暴=0 但延迟回归（RTO 累积），因 flush loop 阻塞发送成瓶颈 → 改为"仅数据单 drainer + ACK 也走 flush loop 但非阻塞优先"最终方案 |
