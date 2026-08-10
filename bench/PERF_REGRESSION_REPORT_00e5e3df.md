# 性能退化分析报告 — commit 00e5e3df

> 分析日期：2026-08-03
> 测试方法：`bash bench/run_bench.sh`（200MB 吞吐量，crypt=aes, nocomp, sndwnd=1024, smuxver=2）
> 基线：commit 00e5e3df 的父版本（`00e5e3df^`）

---

## 隔离测试结果

对 commit 00e5e3df 中的三项关键改动分别做 revert，构建 release 并跑 bench 测试。

| 配置 | 原始 (master) | Revert 1<br>read_shared 超时 | Revert 2<br>feed_inbound 旧版 | Revert 3<br>tokio::sync::Notify |
|------|:-------------:|:---------------------------:|:----------------------------:|:-------------------------------:|
| Go→Go | 55.13 | 49.17 | **57.94** | 37.77 |
| **Tokio→Tokio** | **73.64** | 59.19 | **69.84** | 54.18 |
| **Smol→Smol** | **82.18** | 70.24 | **81.31** | 53.06 |
| **Tokio→Go** | **83.20** | 77.55 | **84.75** | 51.44 |
| **Go→Tokio** | **60.99** | 63.47 | **62.44** | 46.69 |
| **Tokio→Smol** | **87.79** | 72.78 | **77.65** | 69.58 |
| **Smol→Tokio** | **50.12** | 47.69 | 46.70 | 45.39 |

---

## 结论

### 主因：`feed_inbound_batch` 数据包克隆（Revert 2 恢复效果最明显）

Revert 2（恢复旧的 per-datagram `feed_inbound`）把绝大多数指标恢复到接近原始值：

| 关键指标 | 原始 | Revert 2 | 恢复率 |
|----------|:---:|:--------:|:------:|
| Tokio→Tokio | 73.64 | **69.84** | 95% |
| Smol→Smol | 82.18 | **81.31** | 99% |
| Tokio→Go | 83.20 | **84.75** | 102% |
| Go→Tokio | 60.99 | **62.44** | 102% |

**退化机制**：

`kcp-rs/src/conn.rs` 中的 `spawn_input_loop` 函数被修改为先将所有数据包收集到 `Vec<Vec<u8>>`，再批量处理。

旧代码（每数据包零拷贝处理）：
```rust
// 零拷贝，直接传入 &[u8] 切片
loop {
    if n > 0 {
        feed_inbound(&shared, &buf[..n]);  // 传切片
        shared.wake_reader();
    }
    match shared.transport.try_recv(&mut buf) {
        Ok(m) if m > 0 => n = m,
        _ => break,
    }
}
```

新代码（先克隆到 Vec）：
```rust
// 每包先 to_vec() 克隆
let mut burst: Vec<Vec<u8>> = Vec::with_capacity(8);
if n > 0 {
    burst.push(buf[..n].to_vec());  // ← 克隆！
}
loop {
    match shared.transport.try_recv(&mut buf) {
        Ok(m) if m > 0 => burst.push(buf[..m].to_vec()),  // ← 克隆！
        _ => break,
    }
}
let produced_user_data = feed_inbound_batch(&shared, &burst);
```

**影响量化**：
- 每 128KB chunk 产生约 90 个 UDP 数据包（MSS=1460）
- 每包 2048 字节的 `to_vec()` 分配+拷贝
- 每 chunk 多出约 **180KB 的额外堆分配 + memcpy**
- 200MB 吞吐测试中，额外分配总量约 **280MB**（200MB ÷ 128KB × 180KB）

### 次因：`read_shared` 丢失 10ms 超时安全网

Revert 1 恢复了 `read_shared` 中 `notified()` 的 10ms 超时包装，但恢复效果有限（Tokio→Tokio 从 73.64 降到 59.19，只恢复到 59.19）。说明超时丢失不是主因，但仍有一定影响——在 Notify 唤醒延迟波动时，10ms 超时提供了保底。

### 不相关：自定义 `Notify` 替换

Revert 3 恢复 `tokio::sync::Notify` + `event_listener::Event` 后，所有指标反而下降（Go→Go 从 55.13 降到 37.77）。说明自定义 `Notify` 的单 waiter 设计在性能上并没有问题，甚至在 macOS 上优于 `event_listener`。

### 未解决的问题：`Smol→Tokio` 跨运行时不对称

所有 Revert 都未能修复 Smol→Tokio（45-50 MB/s）与 Tokio→Smol（70-78 MB/s）之间的不对称。这个不对称是 `copy_bidirectional` 实现差异造成的：

- **tokio 版本**：使用 `tokio::select!` 随机轮询两个方向
- **smol 版本**：使用 `poll_fn` 顺序轮询（先写待发数据，再读新数据）

这不是 commit 00e5e3df 引入的，而是两个后端 `kio-rs/src/lib.rs` 中 `cfg_copy_bidirectional` 的历史差异。

---

## 修复建议

### P0：恢复 `feed_inbound` 零拷贝方式

将 `spawn_input_loop` 改回每数据包处理，避免 `to_vec()` 克隆。保留 `feed_inbound_batch` 的 FEC 解码优化（FEC 模式下仍需在外部分解码），但非 FEC 模式用零拷贝路径。

### P1：恢复 `read_shared` 的 10ms 超时

重新添加 `kio::timeout(Duration::from_millis(WAIT_FALLBACK_MS), ...)` 包装，防止 Notify 丢失时的永久阻塞。

### P2（后续任务）：统一 `copy_bidirectional` 实现

将 tokio 和 smol 的 `cfg_copy_bidirectional` 统一为 `poll_fn` 顺序轮询模式，消除跨运行时不对称。