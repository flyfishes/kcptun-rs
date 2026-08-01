# Rate Limit 分析：kcptun-rs vs kcptun (Go)

> 分析日期：2026-07-30

## 一、结构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          kcptun-rs (Rust)                               │
│                                                                         │
│  kcptun-client/src/main.rs           kcptun-server/src/main.rs          │
│  ┌────────────────────────────┐    ┌──────────────────────────────┐     │
│  │ KcpConn                                    │ KcpServerSession              │
│  │   rate_limiter: Arc<RateLimiter>           │   ratelimiter: Arc<RateLimiter>│
│  │                                          │                              │
│  │  flush loop → encrypt → acquire(n) →    │  flush loop → encrypt →       │
│  │                socket.send_batch()       │    acquire(n) →                │
│  └────────────────────────────┘             │    socket.send_batch_to()      │
│                                              └──────────────────────────────┘
│         ▲ 应用层 flush loop 中直接限速 ▲
│                                                                         │
│  kcp-rs/src/session.rs                                                  │
│    UDPSession::set_rate_limit() → NO-OP  ⚠️ 从未被应用层调用
│                                                                         │
│  kcptun-common/src/ratelimit.rs                                         │
│    RateLimiter { Mutex<{ rate, burst, tokens, last }> }                 │
│      acquire(n):  token bucket / blocking / 1ms spin-wait               │
│      set_rate(), rate()                                                 │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          kcptun (Go)                                    │
│                                                                         │
│  kcptun server/client                                                   │
│    conn.SetRateLimit(uint32(config.RateLimit))                          │
│         │                                                               │
│         ▼                                                               │
│  vendored kcp-go/v5/sess.go                                             │
│    UDPSession {                                                         │
│      rateLimiter atomic.Value  // stores *rate.Limiter                   │
│    }                                                                    │
│    SetRateLimit(bytesPerSec):                                           │
│      burst = maxBatchSize(64) * mtuLimit(1500) = 96000                  │
│      limiter = rate.NewLimiter(rate.Limit(bytesPerSec), 96000)          │
│      rateLimiter.Store(limiter)                                         │
│                                                                         │
│    postProcess goroutine (flush pipeline):                              │
│      for {                                                              │
│        collect packets → compute bytesToSend                            │
│        → if limiter exists: limiter.WaitN(ctx, bytesToSend)             │
│        → s.tx(txqueue)  // UDP send                                     │
│      }                                                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## 二、结论：当前 `--ratelimit` 是否生效？

| 组件 | 状态 | 说明 |
|------|------|------|
| 客户端 `--ratelimit` | ✅ **生效** | `main.rs:1229` `rate_limiter2.acquire(total_bytes)` 在 UDP send 前调用 |
| 服务端 `--ratelimit` | ✅ **生效** | `main.rs:1053` `ratelimiter.acquire(total_bytes)` 在 UDP send 前调用 |
| CLI 参数解析 | ✅ | `--ratelimit` 默认 0（无限速），可配合 JSON config |
| `kcp-rs::UDPSession::set_rate_limit` | ⚠️ **空实现** | `_bytes_per_sec` 前置下划线明确忽略，但 **应用层不使用此方法**，因此不影响 |
| JSON config merge | ✅ | CLI 非 0 覆盖文件值；CLI 为 0 则用文件值，语义正确 |

**总体：`--ratelimit` 在 Rust 版中实际是工作的。** 和 Go 的关键区别在实现层次而非功能有无。

## 三、与 Go 实现的实质性差异

### 3.1 限速所在层

| 项目 | 限速层 | 代码位置 |
|------|--------|----------|
| Go | kcp-go 库层 (UDPSession) | `sess.go` postProcess goroutine |
| Rust | 应用层 (KcpConn / KcpServerSession) | flush loop, encrypt → send 之间 |

两者都实现"每连接每 KCP session 的出口字节限速"，效果等价。Rust 选择了在应用层实现而非修改 kcp-rs 库，这样不侵入底层 KCP 逻辑。

### 3.2 Burst 大小差异 ⚠️ **最关键的实质差异**

```
Go:   burst = maxBatchSize(64) * mtuLimit(1500) = 96,000 bytes (固定)
Rust: burst = rate (= bytes_per_sec, 例如 1MB/s → burst = 1,000,000 bytes)
```

**影响**：
- **低速率下**（如 `--ratelimit 100000`）: Rust burst=100KB ≈ Go burst=96KB → 行为接近
- **高速率下**（如 `--ratelimit 1048576`）: Rust burst=1MB vs Go burst=96KB → Rust 允许 10× 更大的突发流量
- **初始连接时**: Rust 允许立即发满 1 秒的数据量；Go 只允许 ~96KB。之后稳定态速率相同

**Rust 代码注释说 "matching Go" 是不准确的** — 注释在 `ratelimit.rs:33`。这是文档和实际行为的不一致。

### 3.3 限速精度

| 项目 | 精度机制 | 等待开销 |
|------|---------|---------|
| Go `x/time/rate` | Go runtime timer (微妙级) | 低 — 使用 `Timer` channel |
| Rust `RateLimiter` | 1ms spin-wait loop | 中等 — 每次 acquire 可能 busy-wait |

Go 使用 `time/rate.Limiter.WaitN(ctx, n)` 阻塞在 goroutine 的 timer 上，精度高且无 CPU 空转。但 Rust 用来做 async 的 1ms spin-wait 也足够用于限速场景。

### 3.4 动态配置

| 项目 | 运行时修改 | 机制 |
|------|-----------|------|
| Go | 通过 `SetRateLimit()` 随时修改 | `atomic.Value.Store` — 无锁、线程安全 |
| Rust | 有 `set_rate()` 方法 | `Mutex` 保护的内核更新 |

当前 Rust 没有暴露运行时动态修改机制（如 SIGHUP 重载或信号处理）。

### 3.5 限速参考点

```
数据流: KCP flush → output callback → encrypt → RATE LIMIT → UDP send
                                                        ↑
                                             Go 和 Rust 都在这里限速
```

两者限速的都是 **发送到 UDP 的加密后字节数**（而非原始应用数据字节数）。

## 四、改进方案

### P0 — 行为对齐（推荐实现）

#### 4.1 Burst 大小对齐 Go

文件：`kcptun-common/src/ratelimit.rs`

```rust
const KCP_MTU_LIMIT: u32 = 1500;
const KCP_MAX_BATCH_SIZE: u32 = 64;
const KCP_RATE_BURST: u32 = KCP_MAX_BATCH_SIZE * KCP_MTU_LIMIT;  // = 96000

pub fn new(bytes_per_sec: u32) -> Self {
    let rate = bytes_per_sec as f64;
    let burst = KCP_RATE_BURST as f64;
    RateLimiter {
        inner: parking_lot::Mutex::new(Inner {
            rate,
            burst,
            tokens: burst,  // start full (matching Go's filled bucket)
            last: Instant::now(),
        }),
    }
}
```

影响：burst 从动态(1s容量)改为固定96KB，匹配 Go 行为。允许的短时突发流量变小。

#### 4.2 修复注释

`ratelimit.rs:33` 注释从 `"matching Go"` 改为准确描述。
`kcp-rs/src/session.rs:88-91` 在 `set_rate_limit` 中补充说明应用层已实现。

### P1 — 推荐优化

#### 4.3 移除 KCP session 层的误导性空实现

文件：`kcp-rs/src/session.rs`

选项 A：实现真实的 `set_rate_limit`，让 kcp-rs 库自带限速能力（完全对齐 Go）
选项 B：添加更详细的注释说明应用层已覆盖此功能

推荐选项 B（最小侵入），因为当前架构下 kcp-rs 不持有 UDP socket，无法在 output callback 中直接限速。

#### 4.4 支持运行时动态调整

类似 Go 的 SIGHUP 或控制接口，让 Ratelimit 可运行时调整而不重启进程。当前优先级低。

#### 4.5 添加单测覆盖 ratelimit 在 flush loop 中的集成

新增集成测试，用实际 KCP session + RateLimiter 验证限速效果。

## 五、检查清单

### 5.1 代码层面检查

- [x] 客户端 `--ratelimit` CLI 参数存在并解析
- [x] 服务端 `--ratelimit` CLI 参数存在并解析
- [x] `RateLimiter` 在 `kcptun-common` 中实现
- [x] 客户端 `KcpConn` 持有 `rate_limiter: Arc<RateLimiter>`
- [x] 服务端 `KcpServerSession` 持有 `ratelimiter: Arc<RateLimiter>`
- [x] 客户端 flush loop 在 send_batch 前调用 `acquire(total_bytes)` (L1229)
- [x] 服务端 flush loop 在 send_batch_to 前调用 `acquire(total_bytes)` (L1053)
- [x] JSON config `"ratelimit"` 字段支持 (server Config.ratelimit)
- [x] merge 逻辑正确处理 CLI vs 文件优先级 (server L276-283)
- [x] merge 逻辑正确处理 CLI vs 文件优先级 (client L272-276)
- [ ] ⚠️ Go: `ratelimit < 0 check` (Rust 用 `u32` 天然禁止负数)
- [ ] ⚠️ Go: burst = 96000 固定值 (Rust burst = rate, 需对齐)

### 5.2 未覆盖的边界

- 当 `--ratelimit` 特别小时（如 100），1ms spin-wait 的分辨率可能不够精确
- `acquire` 的返回值（实际等待时间）未被日志记录或用于 SNMP
- 没有运行时动态 reconfig 机制（Go 也没有在 kcptun 层提供，只限于 kcp-go 库）

## 六、实现步骤

### Step 1: 对齐 Burst 大小
```
文件: kcptun-common/src/ratelimit.rs
变更: burst = KCP_RATE_BURST (96000) 而非 burst = rate
```

### Step 2: 添加常量及注释修正
```
- 在 ratelimit.rs 添加 KCP_MTU_LIMIT, KCP_MAX_BATCH_SIZE 常量
- 修正 "matching Go" 为准确说明
- 在 kcp-rs/src/session.rs set_rate_limit 补充说明
```

### Step 3: 编译+测试验证
```bash
cargo build --workspace
cargo test --workspace
```

### Step 4 (可选): 添加限速集成测试
```
在 kcptun-common 或 kcptun-server/tests 中添加
  test_ratelimit_limits_throughput()
模拟实际 KCP flush loop 验证限速效果
```
