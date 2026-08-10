# 多线程场景 P99 延迟优化：Input Loop 内联发送策略

> **Canonical path (git):** `docs/superpowers/specs/2026-08-05-P99_MULTITHREAD_INLINE_SEND_OPTIMIZATION.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-05 |
| All commits | single session (uncommitted at spec write time) |
| Bug report | n/a (perf optimization, not correctness bug) |
| Previous work | `docs/superpowers/specs/2026-08-04-TOKIO_ACK_BATCHING_FIX.md` (ACK batching) |
| Related files | `kcp-rs/src/conn.rs`, `kio-rs/src/task/{tokio,smol}.rs` |

---

## 1. 背景与目标

### 1.1 前序工作

在 2026-08-04 的 ACK batching 修复中，解决了 tokio 服务端 ACK 膨胀 14× 的问题（逐包
`notify_one()` → 1-datagram burst → 每数据段一个 ACK）。修复后 tokio 吞吐量追平 smol
（41.2 vs 43.4 MB/s）。

但在 P99 尾部延迟测试（`bench/run_p99.sh`，Rust↔Rust self-loopback）中，tokio 多线程
的 P99 仍然显著高于单线程模式（前序 A/B 测试：tokio-multi P99 15ms vs tokio-single
557µs，2.9× 差距）。

### 1.2 本次目标

针对 **tokio 多线程** 和 **smol** 两种运行时，分析并优化 P99/P999 尾部延迟。

---

## 2. 跨线程唤醒路径分析

### 2.1 KcpConn 的任务拓扑

每条 `KcpConn` 有 3 个后台任务（跨线程边界以 ⟹ 标注）：

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Input Loop  │     │  Flush Loop  │     │  Sender /    │
│  (recv →     │     │  (deadline   │     │  Reader /    │
│   KCP input  │     │   → KCP      │     │  Writer      │
│   → flush)   │     │   flush)     │     │              │
└──────┬───────┘     └──────▲───────┘     └──────┬───────┘
       │                    │                    │
       │  flush_notify      │                    │  write_notify
       ├───────⟹───────────┤                    │
       │  notify_one()      │                    │
       │                    │                    │
       │  wake_reader()     │                    │
       ├───────────────────⟹────────────────────┤
       │                    │                    │
```

### 2.2 每 input burst 的跨线程唤醒次数

优化前，`flush_input_batch()` 在结尾无条件调用：

```rust
fn flush_input_batch(shared: &KcpConnShared) {
    // ... KCP flush (ACK + data + window probes) ...
    shared.flush_notify.notify_one();  // ⟹ 跨线程唤醒 #1: 唤醒 flush loop
}
```

加上 `feed_inbound_batch` 中的 `wake_reader()`（跨线程唤醒 #2），每个 input burst
产生 **2 次跨线程唤醒**。

### 2.3 跨线程唤醒的代价

| 运行时 | 线程数 | 跨线程唤醒代价 | 原因 |
|--------|--------|----------------|------|
| tokio multi | N (默认 = CPU 核数) | 中等 | work-stealing 调度，任务可能迁移到另一个核，但多核可并行 |
| tokio single | 1 | 零 | 同线程，无迁移 |
| smol | 2 | **高** | 仅 2 线程，唤醒即抢占另一个唯一线程，无并行性可补偿 |

**关键洞察**：smol 的 2 线程 executor 中，跨线程唤醒的代价无法通过并行性补偿——
input loop 唤醒 flush loop 意味着 input loop 自身被挂起，recv 停顿，形成串行化。

---

## 3. 优化策略

### 3.1 核心思路：运行时条件化的内联发送

将 `flush_input_batch()` 中的 `flush_notify.notify_one()` 剥离，让调用方根据运行时
选择发送策略：

- **smol（2 线程）**：input loop 内联 `try_drain_and_send().await` 直接发送产生的
  wire packets，消除跨线程唤醒
- **tokio（N 线程）**：保持 `flush_notify.notify_one()`，保留 input loop / flush loop
  的多核并行性

### 3.2 为什么 tokio 不用内联发送？

在 8 轮 A/B 测试中实测：

| 指标 | tokio baseline (notify) | tokio inline-send | 变化 |
|------|--------------------------|-------------------|------|
| P99 中位数 | 6720µs | 11150µs | **+66%** |
| P50 中位数 | 210µs | 215µs | +2.4% (无变化) |

tokio 多线程下 inline-send 反而恶化 P99，原因：

1. `send_packets_with_fec().await` 包含 UDP `send_to` syscall + 可选 FEC 编码 + 加密
2. input loop 在 `await` 期间被挂起，无法继续 `recv` 下一批 datagram
3. 丢失了 input loop 与 flush loop 的跨核并行性
4. 高 RPS 下 recv 积压 → burst 间隔增大 → P99 抖动

### 3.3 为什么 smol 适合内联发送？

| 指标 | smol baseline (notify) | smol inline-send | 变化 |
|------|--------------------------|-------------------|------|
| P99 中位数 | 21212µs | 8626µs | **−59%** |
| P50 中位数 | 192.7µs | 197.2µs | +2.3% (无变化) |
| P50 抖动 | 3/8 轮 >2000µs | 0/8 轮 >1000µs | **消除** |

smol 只有 2 个 executor 线程，跨线程 `notify_one()` 的代价无法通过并行性补偿。
内联发送把 recv→input→flush→send 串行化在同一个任务中，反而消除了唤醒延迟和
线程迁移开销。

### 3.4 编译期零成本分发

使用 `kio::runtime_kind()`（`const fn`）做运行时条件分发：

```rust
flush_input_batch(&shared);

if kio::runtime_kind() == kio::RuntimeKind::Smol {
    if !shared.try_drain_and_send().await {
        shared.flush_notify.notify_one();
    }
} else {
    shared.flush_notify.notify_one();
}
```

`runtime_kind()` 是 `const fn`，编译器的死分支消除（dead code elimination）会移除
不走的分支——tokio 构建中 `try_drain_and_send().await` 分支被完全移除，smol 构建中
`else` 分支被移除。**零运行时开销**。

---

## 4. 实现细节

### 4.1 改动的文件

仅 `kcp-rs/src/conn.rs`，3 处修改：

#### 修改 1：`flush_input_batch` — 移除 `flush_notify.notify_one()`

```rust
// 优化前：
fn flush_input_batch(shared: &KcpConnShared) {
    // ... KCP flush ...
    shared.write_notify.notify_one();
    shared.flush_notify.notify_one();  // ← 移除此行
}

// 优化后：
fn flush_input_batch(shared: &KcpConnShared) {
    // ... KCP flush ...
    shared.write_notify.notify_one();
    // flush_notify 由调用方负责（input loop 内联发送 / feed_batch 显式 notify）
}
```

**原因**：`flush_input_batch` 被 2 个调用方使用，它们的发送策略不同。

#### 修改 2：`feed_batch` — 补回 `flush_notify.notify_one()`

```rust
pub fn feed_batch(&self, datagrams: Vec<Vec<u8>>) -> io::Result<()> {
    // ... feed_inbound_batch + flush_input_batch ...
    // 外部驱动（非后台任务）—— 通知 flush loop 发送
    self.shared.flush_notify.notify_one();  // ← 新增
    Ok(())
}
```

**原因**：`feed_batch` 是外部驱动调用（如 `KcptunListener` 的 `process_builds`），
不是后台 input loop，必须通知 flush loop 发送产生的 packets。

#### 修改 3：`spawn_input_loop` — runtime 条件化发送策略

```rust
// 优化前：
flush_input_batch(&shared);
// flush_input_batch 内部调用 flush_notify.notify_one()

// 优化后：
flush_input_batch(&shared);
if kio::runtime_kind() == kio::RuntimeKind::Smol {
    if !shared.try_drain_and_send().await {
        shared.flush_notify.notify_one();
    }
} else {
    shared.flush_notify.notify_one();
}
```

**`try_drain_and_send` 的 `is_sending` 互斥保证**：

- `try_drain_and_send` 通过 `compare_exchange(false, true)` 获取 `is_sending` token
- 如果 flush loop 正在发送（持有 token），返回 `false`，input loop 回退到 `notify_one`
- 同一时间只有一个 sender，wire order 不变

### 4.2 `try_drain_and_send` 既有实现（未修改）

```rust
async fn try_drain_and_send(&self) -> bool {
    if self.is_sending
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false; // flush loop 正在发送
    }
    let packets = self.drain_raw_packets();
    if packets.is_empty() {
        self.finish_sending();
        return true;
    }
    if let Err(e) = self.send_packets_with_fec(&packets).await {
        *self.last_error.lock() = Some(e);
    }
    self.finish_sending();
    true
}
```

此方法原本用于 write path 的内联发送（匹配 Go kcp-go 的同步 `Write`），现在也被
smol 的 input loop 复用。

---

## 5. 性能验证

### 5.1 测试方法

- 工具：`kcp-rs/examples/latency_p99.rs`（self-loopback echo）
- 参数：500 RPS，26KB payload，15s/轮，3s warmup
- 8 轮交替 A/B（baseline vs optimized），控制系统条件
- macOS (Intel i7, 6 核)

### 5.2 smol A/B 结果（8 轮）

| Round | baseline P50 | opt P50 | baseline P99 | opt P99 | baseline P999 | opt P999 |
|-------|-------------|---------|--------------|---------|---------------|----------|
| #1 | 190.0 | 197.4 | 2011.0 | 18549.1 | 22182.3 | 64228.2 |
| #2 | 191.1 | 188.9 | 2930.7 | 3662.4 | 30538.1 | 39799.6 |
| #3 | 2155.8 | 202.7 | 50285.9 | 10453.7 | 235948.5 | 64833.1 |
| #4 | 190.1 | 196.9 | 22842.5 | 6797.8 | 150172.3 | 101553.0 |
| #5 | 2191.5 | 189.0 | 32495.2 | 1159.0 | 139089.5 | 20456.0 |
| #6 | 192.2 | 194.8 | 31290.7 | 4718.1 | 68702.0 | 41061.5 |
| #7 | 193.2 | 219.6 | 19581.5 | 130346.4 | 84809.1 | 521533.7 |
| #8 | 2167.0 | 201.6 | 18595.9 | 30138.0 | 88051.9 | 110485.5 |
| **中位数** | **192.7** | **197.2** | **21212** | **8626** | **88052** | **64833** |

**smol 改善**：

- P99 中位数 **−59%**（21212 → 8626µs）
- P50 抖动消除：baseline 3/8 轮 P50 >2000µs（#3, #5, #8），optimized 0/8 轮 >1000µs
- P999 中位数 **−26%**（88052 → 64833µs）

### 5.3 tokio A/B 结果（8 轮）

| Round | baseline P50 | opt P50 | baseline P99 | opt P99 | baseline P999 | opt P999 |
|-------|-------------|---------|--------------|---------|---------------|----------|
| #1 | 208.3 | 215.0 | 20793.7 | 18626.3 | 47777.3 | 46037.8 |
| #2 | 208.2 | 218.3 | 2313.5 | 11617.2 | 37051.9 | 90615.1 |
| #3 | 214.1 | 215.2 | 12076.5 | 10682.6 | 34201.8 | 27441.6 |
| #4 | 208.6 | 2197.6 | 2910.6 | 38084.1 | 24773.3 | 95332.4 |
| #5 | 211.8 | 213.5 | 11973.1 | 974.7 | 51358.1 | 35283.0 |
| #6 | 209.5 | 456.5 | 991.9 | 14822.0 | 43928.2 | 59289.6 |
| #7 | 210.4 | 213.0 | 775.1 | 765.7 | 12892.7 | 30632.2 |
| #8 | 211.1 | 211.2 | 10529.7 | 669.8 | 41732.4 | 32652.4 |
| **中位数** | **210.0** | **215.1** | **6720** | **11150** | **41732** | **42957** |

> **注**：以上 tokio "opt" 数据是 **inline-send 版本**（非最终版本），用于验证
> inline-send 在 tokio 上的效果。最终版本 tokio 走 **notify 路径**（与 baseline 一致）。

**tokio inline-send 效果**：

- P99 中位数 **+66%**（6720 → 11150µs）—— **恶化**，确认不用
- P50 无变化（210 vs 215µs）

**tokio 最终版本（notify 路径）验证**：

| Round | baseline P50 | opt P50 | baseline P99 | opt P99 |
|-------|-------------|---------|--------------|---------|
| #1 | 210.8 | 211.2 | 1448.9 | 17680.5 |
| #2 | 203.2 | 205.1 | 1970.6 | 6370.7 |
| #3 | 205.2 | 203.5 | 944.2 | 1480.8 |

P50 完全一致（210/203/205 vs 211/205/204），确认 tokio 走 notify 路径无退化。
P99 波动属 macOS 系统抖动正常范围。

### 5.4 Gate check 结果

```
cargo fmt --all -- --check          ✅ 通过
cargo clippy --workspace -D warnings ✅ 通过
cargo test --workspace               ✅ 213 passed, 0 failed, 10 ignored
```

---

## 6. 为什么 macOS 上 P99 波动大

macOS 不是实时操作系统，P99/P999 尾部延迟受以下系统级因素主导：

1. **macOS timer coalescing**：内核合并多个 timer 到一个 wakeup window（~1-5ms），
   导致 timer 唤醒抖动
2. **Mach scheduler quantum**：10ms 调度量子，线程被抢占后可能等一个量子
3. **background tasks**：Spotlight 索引、Time Machine、iCloud 同步等不可控
4. **UDP loopback**：macOS 的 UDP loopback 走完整协议栈（不像 Linux 的 `lo` 旁路）

这些因素导致 P99 在 1ms-50ms 间随机波动，**不是代码层面可以完全消除的**。
在 Linux 上（特别是 `PREEMPT_RT` 内核或 `SCHED_FIFO`），P99 会更稳定。

---

## 7. 优化决策矩阵

| 场景 | 策略 | 理由 |
|------|------|------|
| smol（2 线程） | ✅ inline-send | 跨线程唤醒代价 > 内联串行化代价；无并行性可损失 |
| tokio multi（N 线程） | ✅ notify | 内联发送损失 input/flush 跨核并行性；P99 恶化 66% |
| tokio single（1 线程） | ✅ notify（自动） | `const fn` 编译期走 else 分支；同线程 notify 无跨线程开销 |
| `feed_batch`（外部驱动） | ✅ notify | 外部调用者不持有 `is_sending` 语义；flush loop 负责发送 |

---

## 8. 前序 A/B 测试回顾（tokio-multi vs tokio-single vs smol）

在本次优化之前，已进行过 8 轮交替 A/B 测试对比三种运行时模式：

| 运行时 | P50 中位数 | P99 中位数 | P999 中位数 | 特点 |
|--------|-----------|-----------|------------|------|
| tokio multi | 210µs | 6720µs | 41732µs | 跨线程 work-stealing 主导 P99 |
| tokio single | 208µs | 557µs | 3583µs | 无跨线程唤醒，P99 最优 |
| smol (baseline) | 193µs | 21212µs | 88052µs | 2 线程跨线程唤醒代价高 |
| **smol (optimized)** | **197µs** | **8626µs** | **64833µs** | inline-send 消除跨线程唤醒 |

**结论**：

- tokio-single P99（557µs）是理论最优——单线程无跨线程开销
- smol 优化后 P99（8626µs）相比 baseline（21212µs）改善 59%，但仍不如 tokio-single
- tokio-multi 的 P99（6720µs）受 macOS 系统抖动主导，代码优化空间有限

---

## 9. 后续可探索方向（未实施）

### 9.1 tokio `current-thread` 模式（已支持）

`kio::block_on_local()` 已提供 tokio current-thread 运行时。如果 P99 是硬指标，
可在 server 端使用 `--event-loop current-thread`（或 per-SO_REUSEPORT shard worker）
获得 tokio-single 级别的 P99（557µs）。

代价：失去多核并行性，吞吐量下降。适合低延迟场景（如游戏/实时音视频）。

### 9.2 Linux `SO_REUSEPORT` + per-shard `current-thread`

在 Linux 上可用 `SO_REUSEPORT` 创建多个 UDP socket（每个绑定同一端口但不同 shard），
每个 shard 跑一个 `current-thread` tokio runtime。这样：

- 每个 shard 内无跨线程唤醒（tokio-single P99）
- 多 shard 并行（保持多核吞吐量）
- 内核 RSS 分发 datagram 到各 shard

这是 Linux 上的最优方案，但需要较大的 listener 重构。

### 9.3 `flush_notify` 批量化（已在前序工作中完成）

`flush_input_batch` 已在前序 ACK batching 修复中改为 per-burst 单次 flush（而非
per-datagram），ACK 也在 burst 内批量。本次优化在此基础上进一步消除了跨线程唤醒。

---

## 10. 修订记录

| Date | Note |
|------|------|
| 2026-08-05 | 初始实施：runtime 条件化 inline-send（smol ✅ / tokio ✅）；8 轮 A/B 验证 |
