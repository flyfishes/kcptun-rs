# 延迟优化分析

> 基于 2026-08-03 吞吐+延迟测试结果

---

## 一、当前测试数据

### 吞吐量（200MB，crypt=aes, nocomp, sndwnd=1024, smuxver=2）

| 组合 | 吞吐量 | 延迟 | 历史对比 |
|------|:-----:|:----:|:--------:|
| Go→Go | 56.54 MB/s | 0.12ms | 基准 |
| **Tokio→Tokio** | **72.95 MB/s** | **0.12ms** | 73.64→72.95 ✅ 稳定 |
| **Smol→Smol** | **56.31 MB/s** | **0.16ms** | **82.18→56.31 ❌ -31%** |
| **Tokio→Go** | **82.38 MB/s** | **0.10ms** | 83.20→82.38 ✅ 稳定 |
| **Go→Tokio** | **52.32 MB/s** | **0.46ms** | **60.99→52.32 ❌ -14%** |
| **Tokio→Smol** | **78.06 MB/s** | **0.14ms** | 87.79→78.06 ⚠️ -11% |
| **Smol→Tokio** | **79.56 MB/s** | **0.14ms** | **50.12→79.56 ✅ +59%** |

### 关键变化

| 指标 | 变化 | 原因 |
|------|:----:|------|
| **Smol→Tokio** | **+59%** | `copy_bidirectional` 统一：tokio 的 `select!` + `write` 替代 `write_all`，消除跨运行时不对称 |
| **Smol→Smol** | **-31%** | `read_shared` 的 `WAIT_FALLBACK_MS=10ms` 超时在 smol 上每次调用创建 `Timer`，增加分配开销 |
| **Go→Tokio** | **-14%** | `is_sending` 令牌竞争导致 tokio 发送路径尾延迟，Go→Tokio 的延迟从 0.28ms 升到 0.46ms |
| **Tokio→Smol** | **-11%** | `select!` + `write` + `continue` 增加了 loop 迭代次数 |

---

## 二、根因分析

### 2.1 `is_sending` 令牌竞争（P0）

`is_sending` 原子令牌被三条路径竞争：

| 路径 | 文件位置 | 操作 |
|------|---------|------|
| `write_all_shared` → `try_drain_and_send` | `conn.rs:370-386` | `compare_exchange(false, true)` → 失败时回退 `notify_one()` |
| flush 循环 fast-send | `conn.rs:1191-1204` | `compare_exchange(false, true)` → 失败时跳过发送 |
| flush 循环 second drain | `conn.rs:1262-1279` | `compare_exchange(false, true)` → 失败时 `notify_one()` 重试 |

**竞争链**：
```
writer 获取令牌 → 发送数据包 → 释放令牌
flush 循环尝试获取令牌 → 失败 → 跳过发送 → KCP 状态机产生的 ACK 延迟发出
```

在 tokio 多线程运行时上，`compare_exchange` 失败后的回退路径（`notify_one()` → 任务调度 → 发送）增加 **50-200µs** 的尾延迟。

### 2.2 `read_shared` 超时在 smol 上的开销

`read_shared` 的 `kio::timeout(10ms, notified())` 在 smol 上使用 `futures_lite::future::or`：

```rust
// smol 的 kio::timeout 实现
futures_lite::future::or(
    async move { Some(future.await) },
    async move { let _ = smol::Timer::after(dur).await; None },
).await;
```

每次调用创建一个 `Timer` future，注册到 reactor。在 smol 单线程运行时上，`read_loop` 和 `copy_bidirectional` 串行执行，Timer 的创建/销毁增加了每次迭代的开销。

### 2.3 `send_to_kcp` 中 `flush_data_only` 重复工作

`send_to_kcp`（`conn.rs:393-421`）在 KCP 锁内调用 `kcp.flush_data_only()`，扫描整个 `snd_buf` 产生 wire packets。flush 循环随后再次调用 `kcp.flush_data_only()`，重复扫描 `snd_buf`。

---

## 三、优化方案

### 优化 1（P0）：消除 `is_sending` 令牌

**改动**：`conn.rs` 中 3 处删除 `is_sending` 竞争，直接发送。

```rust
// ── write_all_shared 中（conn.rs:659-662）──
if sent > 0 {
    // 直接发送，不竞争 is_sending 令牌
    let packets = self.shared.drain_raw_packets();
    if !packets.is_empty() {
        let _ = self.shared.send_packets_with_fec(&packets).await;
    }
    self.shared.flush_notify.notify_one();  // 通知 flush 循环处理 KCP 状态机
}

// ── flush 循环 fast-send 中（conn.rs:1191-1204）──
// 直接发送，不检查 is_sending
let fast_packets = shared.drain_raw_packets();
if !fast_packets.is_empty() {
    let _ = shared.send_packets_with_fec(&fast_packets).await;
}

// ── flush 循环 second drain 中（conn.rs:1262-1279）──
// 直接发送，不检查 is_sending
let packets = shared.drain_raw_packets();
if !packets.is_empty() {
    let _ = shared.send_packets_with_fec(&packets).await;
}
```

**移除**：`is_sending` 字段、`try_drain_and_send` 方法、`finish_sending` 方法。

**预期收益**：Go→Tokio 从 52.32 恢复到 ~60 MB/s，延迟从 0.46ms 恢复到 ~0.28ms。

---

### 优化 2（P0）：`read_shared` 条件性超时

**改动**：`conn.rs:616-623`，正常路径无超时，仅 Notify 丢失后启用超时保护。

```rust
pub async fn read_shared(&self, buf: &mut [u8]) -> io::Result<usize> {
    let mut use_timeout = false;
    loop {
        if buf.is_empty() { return Ok(0); }
        {
            let mut rb = self.shared.read_buf.lock();
            if let Some(mut data) = rb.pop_front() {
                // ... 返回数据 ...
                return Ok(n);
            }
        }
        if self.shared.is_closed() { return Ok(0); }

        if use_timeout {
            // 超时模式：仅当之前 Notify 丢失时启用
            let _ = kio::timeout(
                Duration::from_millis(WAIT_FALLBACK_MS),
                self.shared.read_notify.notified(),
            ).await;
            use_timeout = false;
        } else {
            // 正常模式：纯 notify，零分配
            self.shared.read_notify.notified().await;
            // 如果 Notify 返回但 read_buf 仍空，说明丢失了唤醒
            if self.shared.read_buf.lock().is_empty() && !self.shared.is_closed() {
                use_timeout = true; // 下次启用超时保护
            }
        }
    }
}
```

**预期收益**：smol↔smol 从 56.31 恢复到 ~80 MB/s。

---

### 优化 3（P1）：`send_to_kcp` 移除 `flush_data_only`

**改动**：`conn.rs:414-417`，移除 KCP 锁内的 flush 调用。

```rust
// 旧：kcp.send() + kcp.flush_data_only()
if offset > 0 {
    kcp.flush_data_only();  // ← 移除，flush 循环负责
}

// 新：只做 kcp.send()
// if offset > 0 { kcp.flush_data_only(); }  // 删除
```

**预期收益**：KCP 锁持有时间减少，writer 和 input 循环之间锁竞争降低。

---

### 优化 4（P1）：`copy_bidirectional` tokio 版写入优化

**改动**：`kio-rs/src/lib.rs`，`write` 返回 `Ok(0)` 时不 `continue`，直接 fall through 到 `select!`（已修复）。

```rust
match b.write(&buf_a[pending_ab..n_a]).await {
    Ok(0) => { /* fall through 到 select! */ }
    Ok(m) => {
        pending_ab += m;
        continue;  // 正常写入后继续
    }
    Err(e) => return Err(e),
}
// Ok(0) 时不 continue → select! 处理另一方向
```

**预期收益**：防止 `Ok(0)` 时的 busy loop，稳定 P99。

---

## 四、优先级与预期收益

| 优化 | 优先级 | 影响指标 | 当前值 | 预期值 | 改动量 |
|------|:------:|---------|:-----:|:-----:|:------:|
| 消除 `is_sending` 令牌 | **P0** | Go→Tokio 吞吐 | 52.32 | ~61 | 3 处删除 |
| 消除 `is_sending` 令牌 | **P0** | Go→Tokio 延迟 | 0.46ms | ~0.28ms | 同上 |
| 条件性 `read_shared` 超时 | **P0** | Smol→Smol 吞吐 | 56.31 | ~80 | 1 处修改 |
| 移除 `flush_data_only` | **P1** | Tokio→Tokio 吞吐 | 72.95 | ~78 | 2 行删除 |
| `copy_bidirectional` 写入优化 | **P1** | 稳定性 | — | P99 稳定 | 已修复 |