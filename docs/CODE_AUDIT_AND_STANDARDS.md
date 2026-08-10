# kcptun-rs 代码审查报告与生产级代码规范

> **审查范围**：9 个 crate、约 20,000+ 行代码逐文件审查  
> **审查日期**：2026-08-02  
> **约束前提**：Go kcptun / kcp-go v5 线兼容性是硬约束，不可破坏

> **实施状态（2026-08-03）**：F2/F4 的生产迁移已经完成。client 与 server 的 UDP、
> raw TCP 均复用 `kcptun_common::KcptunSession`。server UDP 先由
> `KcptunListener` 单点读取共享 socket，再拆成 per-peer transport，避免多个
> `KcpConn` 竞争 `recv()`；raw TCP 则把已接受的 per-peer socket 直接交给同一 session。
> 二进制内联 session、legacy 回滚开关和 `TcpRaw*Session` adapter 均已删除。
> `kcp_transport.rs` 只负责 `CryptoTransport → KcpConn`，`kcptun_session.rs`
> 负责完整 `KcpConn → Snappy → SMUX` 会话，并由 `KcptunConfig` 统一配置。
>
> **后续更新（2026-08-03）**：审查发现文档中多个项目实际已提前完成：
> - F7 Builder 去重：`kcp_config_setters!()` 宏已存在并被两个 Builder 共用
> - F8 API 废弃：`select_block_crypt`/`select_aead_crypt` 已标记 `#[deprecated]`
> - F10 thiserror：所有错误类型（含 `StreamError`）均已使用 thiserror
> - F14 close() 空体：已添加 `notify_waiters()` 调用
> - F16 unsafe 注释：所有 unsafe 块均有 `// SAFETY:` 注释
> - F9 OffloadProfile：全局静态已消除，`OffloadProfile` 已是 `CryptoTransport` 的 per-session 字段
> - F13 常量命名：所有私有/公开常量均已添加语义别名，内部使用新名
> 剩余工作按 F3 KCP 回调泛型化、F6 async_trait、F11 qpp-rs 拆分、F12 kio copy 去重顺序推进。

---

## 目录

- [一、整体评价](#一整体评价)
- [二、Session 架构澄清](#二session-架构澄清)
- [三、核心发现](#三核心发现按严重程度排序)
- [四、生产级代码规范](#四生产级代码规范)
- [五、优先级矩阵](#五优先级矩阵)
- [六、迁移路线图](#六迁移路线图)
- [七、总结](#七总结)

---

## 一、整体评价

项目在 **Go 线兼容性**、**性能优化**（4-phase flush、monomorphized crypto、FEC、零拷贝）方面做得优秀。协议栈分层清晰：

```
UDP → BlockCrypt/AEAD (+ 可选 FEC) → KCP ARQ → Snappy (session级) → SMUX Session → SMUX Stream (+ 可选 QPP) → TCP
```

但在 **正规化、Rust 惯例、API 简化** 方面存在系统性问题，主要集中在：

1. 二进制文件过度膨胀（单文件 >2000 行）
2. 生产路径与库路径双轨并行，无收敛计划
3. 命名冲突（客户端 `KcpConn` vs 库 `kcp_rs::KcpConn`）
4. 重复代码（三个 Builder setter、两个 session 结构体 90% 字段重复）
5. 全局可变状态（`OffloadProfile`）
6. 错误处理手写而非统一

---

## 二、Session 架构澄清

项目中存在 **三种不同的 "session" 概念**，命名容易混淆：

### 2.1 当前架构全景

```
client UDP/raw TCP socket
          │
          ▼
KcptunSession::connect()
          │
          ├── CryptoTransport → kcp_rs::KcpConn (+ FEC)
          └── Snappy → SMUX client session → streams

server shared UDP socket                 server accepted raw TCP socket
          │                                          │
          ▼                                          ▼
KcptunListener (唯一 recv + per-peer demux)   KcptunSession::serve_transport()
          │                                          │
          └──────────────► KcptunSession::server() ◄─┘
                              │
                              └── Snappy → SMUX server session → streams
```

共享 UDP socket 必须先由 `KcptunListener` 拆成 per-peer transport；raw TCP
连接天然属于单个 peer，因此可以直接构造 session。两者从 `KcpConn` 开始完全复用
`KcptunSession`，二进制中不再存在另一套 KCP/FEC/Snappy/SMUX flush 状态机。

### 2.2 文件与类型职责

| 位置 | 类型/职责 | 生产使用？ |
|------|-----------|-----------|
| `kcp-rs` | `KcpConn`：KCP/FEC 与收发后台任务 | ✅ |
| `kcptun-common/src/kcp_transport.rs` | 内部 `KcpConn` 组装；公开底层 `CryptoTransport` | ✅ |
| `kcptun-common/src/kcptun_session.rs` | `KcptunConfig`、完整 per-peer `KcptunSession` | ✅ |
| `kcptun-common/src/kcptun_listener.rs` | 共享 UDP 单 reader 与 per-peer demux | ✅ server UDP |
| client/server `main.rs` | CLI、socket 获取、stream 转发与生命周期管理 | ✅ |

### 2.3 已消除的命名与双轨问题

- 删除二进制本地 `KcpConn`/`KcpServerSession`/`TcpRaw*Session` 实现。
- 删除运行时 legacy 回滚参数；UDP 与 raw TCP 均使用同一正式 session 栈。
- 原 `session.rs` 按真实职责改名为 `kcp_transport.rs`；完整会话只叫
  `KcptunSession`，配置只叫 `KcptunConfig`。
- `dial_kcp_conn` 不再作为另一条公开构造路径；客户端通过
  `KcptunSession::connect` 构造完整会话。

### 2.4 两个生产 session 结构体字段对比（审查时快照，现已删除）

| 字段 | 客户端 `KcpConn` | 服务端 `KcpServerSession` |
|------|-----------------|------------------------|
| `kcp: Arc<Mutex<KCP>>` | ✅ | ✅ |
| `smux: Arc<smux_rs::Session>` | ✅ | ✅ |
| `crypt: Arc<CryptEngine>` | ✅ | ✅ |
| `has_encryption: bool` | ✅ | ✅ |
| `crypto_buf: Arc<Mutex<CryptoBuf>>` | ✅ | ✅ |
| `ack_crypto_buf` | ✅ | ❌（服务端用不同方式） |
| `fec_encoder/decoder` | ✅ `Arc`/`Arc` | ✅ `Arc`/直接 `Mutex` |
| `flush_notify: Arc<Notify>` | ✅ | ✅ |
| `raw_packets: Arc<Mutex<Vec<Bytes>>>` | ✅ | ✅ |
| `compressor` (Snappy) | ✅ | ✅ `Option` |
| `dead: Arc<AtomicBool>` | ✅ | ✅ |
| `rate_limiter` | ✅ | ✅ |
| `socket: Arc<DatagramSocket>` | ✅ | ✅ |
| `nocomp: bool` | ✅ | ✅ |
| `acknodelay: bool` | ✅ | ✅ `ack_nodelay` |
| `last_activity` | ✅ | ❌ |
| `adopt_conv: AtomicBool` | ❌ | ✅ |
| `handled_streams` | ❌ | ✅ |
| `peer: SocketAddr` | ❌（有 remote_addr） | ✅ |
| `snappy_fallback` | ❌ | ✅ |

**结论**：90% 字段重复，应提取为 `kcptun-common` 中的共享 session 基础结构。

---

## 三、核心发现（按严重程度排序）

### 🔴 严重问题

#### F1. 二进制 `main.rs` 拆分完成（当前 99 行，分散于 5 个模块）

> **状态：✅ 已完成。** client/server 的 Clap 定义、JSON `Config` 与 merge
> 逻辑已分别提取到 `src/cli.rs`；session/socket 管理提取到 `src/client.rs`/
> `src/server.rs`/`src/socket.rs`；应用生命周期提取到 `src/app.rs`。
> 此次拆分不改变任何 CLI flag、默认值或配置覆盖语义。

| 文件 | 旧行数 | 新行数 |
|------|--------|--------|
| `kcptun-client/src/main.rs` | 878 | **48** |
| `kcptun-client/src/app.rs` | — | 388 |
| `kcptun-client/src/client.rs` | — | 244 |
| `kcptun-client/src/socket.rs` | — | 73 |
| `kcptun-server/src/main.rs` | 1,011 | **51** |
| `kcptun-server/src/app.rs` | — | 364 |
| `kcptun-server/src/server.rs` | — | 139 |
| `kcptun-server/src/socket.rs` | — | 69 |
| **合计** | **1,889** | **分散于 5 个模块** |

各自把 CLI 解析、KCP 会话管理、SMUX 驱动、Snappy 编码、pprof 服务器、信号处理全部塞进单文件。

**违反**：单一职责原则、可维护性。
**影响**：任何修改都需要在 2000+ 行文件中定位代码，代码审查困难。

---

#### F2. 生产路径与库路径双轨并行，无收敛计划

> **状态：✅ 已解决。** 下述内容是审查时快照；当前所有生产 transport 已统一到
> `KcpConn + KcptunSession`，不再保留二进制内联 flush 路径。

`AGENTS.md` 反复提到：
> "production binaries still use legacy KCP+SMUX+Snappy flush loops" vs "library-ready KcpConn/SmuxConn"

两条路径的 flush 逻辑、加密路径、FEC 处理各写一遍，代码漂移风险极高。

**具体表现**：
- Legacy flush loop（main.rs 内联）vs `kcp_rs::KcpConn` flush loop（conn.rs）
- Legacy encrypt（main.rs 手写 `encrypt_batch` 调用）vs `CryptoTransport`（common/kcp_transport.rs）
- Legacy FEC（main.rs 手写 `FecEncoder`/`FecDecoder` 调用）vs `KcpConnBuilder::fec()`

---

#### F3. `KCP` 状态机输出回调使用 `Box<dyn FnMut(Bytes) + Send>`

`kcp-rs/src/kcp.rs` 第 140 行：
```rust
output: Box<dyn FnMut(Bytes) + Send>,
```

每次 `flush()` 调用都经过动态分发。Go 原型用 interface 情有可原，但 Rust 有更好的选择（泛型 / 闭包类型 / channel）。

**影响**：热路径 vtable 开销；类型不透明，无法内联。

---

#### F4. Session 命名冲突与重复（见上文第二章）

> **状态：✅ 已解决。** 统一为 `KcptunSession`，下层文件为 `kcp_transport.rs`。

客户端 `KcpConn`（legacy struct）与库 `kcp_rs::KcpConn`（library async conn）同名但完全不同。客户端/服务端 session 结构体 90% 字段重复但各自独立维护。

---

### 🟡 中度问题

#### F5. clippy allow 列表过长（当前 24 条 → 5 条）

> **状态：✅ 已解决。** 从 24 条减少到 5 条，修复了 11 个具体 clippy 警告，全 workspace 零警告。

`kcp-rs/src/lib.rs` 第 27-52 行（原 24 条，现 5 条）：
```rust
#![allow(
    // mirrors Go kcp-go control flow for easy auditing
    clippy::collapsible_if,
    clippy::while_let_loop,
    // KCP API surface matches Go kcp-go
    clippy::too_many_arguments,
    // index-based iteration matches Go kcp-go
    clippy::needless_range_loop,
    // Go kcp-go uses same action in multiple branches for clarity
    clippy::if_same_then_else,
)]
```

**修复的警告**：`absurd_extreme_comparisons`（fec.rs）、`collapsible_else_if`（fec.rs）、`manual_div_ceil`（kcp.rs）、`unnecessary_cast`（kcp.rs）、`manual_range_contains`（kcp.rs）、`needless_late_init`（kcp.rs）、`len_without_is_empty`（segment.rs）、`new_without_default`（snmp.rs）、`empty_line_after_doc_comments`（segment.rs）。

---

#### F6. `PacketTransport` trait 方法签名过于复杂

`kcp-rs/src/conn.rs` 第 69-147 行：
```rust
pub trait PacketTransport: Send + Sync {
    fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    fn send_batch<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
    // ... 7 more methods, each with same pattern
}
```

每个实现都要手写 `Box::pin(async move { ... })`，冗长且易错。项目刻意不用 `async_trait`（注释说 "surgical, no new workspace dep"），但代价是每个实现多 7 行样板代码，trait 有 10 个方法时就是 70 行重复。

---

#### F7. 三套 KCP Builder setter 完全重复（120 行复制粘贴）

> **状态：✅ 已解决。** `kcp_config_setters!()` 宏已提取并被 `KcpConnBuilder`、`KcpListenerBuilder` 共用。

| Builder | 位置 | setter 数量 |
|---------|------|------------|
| `KcpConnBuilder` | conn.rs:772 | 11 (via macro) |
| `KcpListenerBuilder` | conn.rs:1787 | 11 (via macro) |
| `KcpTcpListenerBuilder` | conn.rs:2014 | 1 (config) |

前两者的 `mtu/sndwnd/rcvwnd/mode/stream/acknodelay/conv/token/nodelay/config` 方法完全相同，通过宏提取避免重复。

---

#### F8. `CryptEngine` 与 `Box<dyn BlockCrypt>` 双 API 并存

> **状态：✅ 已解决。** 两个 Legacy API 均已标记 `#[deprecated]`。

`kcrypt-rs/src/crypt.rs`:
```rust
// 路径 A：返回 enum（推荐热路径用）
pub fn select(method: &str, pass: &[u8]) -> (Self, String)

// 路径 B：返回 Box<dyn>（legacy / tests）— 已标记 deprecated
#[deprecated(note = "use CryptEngine::select instead")]
pub fn select_block_crypt(method: &str, pass: &[u8]) -> (Box<dyn BlockCrypt>, String)
```

`AGENTS.md` 说 "prefer `Arc<CryptEngine>`"，`select_block_crypt` 已标记 `#[deprecated]`。

---

#### F9. `OffloadProfile` 全局静态变量

> **状态：✅ 已解决。**

`kcrypt-rs/src/wire.rs` 中原先的全局 `OFFLOAD_PROFILE` 静态变量已被移除。现在 `OffloadProfile` 作为 `CryptoTransport` 的 per-session 字段存储，通过 `KcptunConfig` 配置并在运行时根据 `kio::runtime_kind()` 设定：
- Client: `kcptun-client/src/client.rs` 第 74-77 行
- Server: `kcptun-server/src/app.rs` 第 156-159 行

三个决策函数 `should_cpu_block_encrypt`、`should_cpu_block_decrypt`、`should_cpu_block_compress` 通过参数接收 profile，实现 per-session 独立调优。

---

#### F10. 错误类型不统一，全部手写

> **状态：✅ 已完成。** 所有 5 个错误类型均已使用 `thiserror`，包括 `StreamError`。

| crate | 错误类型 | 实现 | 状态 |
|-------|---------|------|------|
| kcp-rs | `KcpError` | `thiserror::Error` | ✅ |
| smux-rs | `SessionError` | `thiserror::Error` | ✅ |
| smux-rs | `FrameError` | `thiserror::Error` | ✅ |
| smux-rs | `StreamError` | `thiserror::Error` | ✅ |
| kcrypt-rs | `InboundCryptError` | `thiserror::Error` | ✅ |
| 二进制 | `Result` (anyhow) | 已用 anyhow | ✅ |

---

### 🟢 轻度问题

#### F11. `qpp-rs` 单文件 475 行全部在 `lib.rs`

`QuantumPermutationPad` 的 `encrypt_with_pads` / `decrypt_with_pads` 是 `pub` 自由函数而非方法，破坏了封装。应至少拆为 `pad.rs`（pad 构建 + shuffle）、`prng.rs`（xoshiro256**）、`cipher.rs`（encrypt/decrypt）。

---

#### F12. `kio::copy_bidirectional` tokio/smol 实现各 100+ 行，逻辑重复

tokio 用 `select!`，smol 用手写 `poll_fn` + Pin/poll。两个 `cfg_copy_bidirectional` 版本各 100+ 行，两个 `cfg_copy_bidirectional_idle` 版本各 100+ 行，合计 ~400 行重复逻辑。

---

#### F13. 常量命名不统一

> **状态：✅ 已解决。** 所有常量均已添加语义化别名，内部代码使用新名，Go 名保留在注释中对照。

| crate | 当前命名 | 语义名 | 状态 |
|-------|---------|--------|------|
| kcp-rs | `IKCP_RTO_NDL` | `RTO_NODELAY_MIN` | ✅ 内部使用新名 |
| kcp-rs | `IKCP_RTO_MIN` | `RTO_DEFAULT_MIN` | ✅ 内部使用新名 |
| kcp-rs | `IKCP_RTO_DEF` | `RTO_DEFAULT` | ✅ 内部使用新名 |
| kcp-rs | `IKCP_RTO_MAX` | `RTO_MAX` | ✅ 内部使用新名 |
| kcp-rs | `IKCP_DEADLINK` | `DEAD_LINK_RETRIES` | ✅ 内部使用新名 |
| kcp-rs | `IKCP_THRESH_MIN` | `THRESH_MIN` | ✅ 内部使用新名 |
| kcp-rs | `KCP_THRESHOLD_INIT` | `SSTHRESH_INIT`（来自 segment.rs） | ✅ 消除重复定义 |
| kcp-rs | `IKCP_PROBE_INIT` | `PROBE_INIT` | ✅ pub(crate) |
| kcp-rs | `IKCP_PROBE_INIT_NODELAY` | `PROBE_INIT_NODELAY` | ✅ pub(crate) |
| kcp-rs | `KCP_ASK_SEND` | `ASK_SEND` | ✅ 已添加别名 |
| kcp-rs | `KCP_ASK_TELL` | `ASK_TELL` | ✅ 已添加别名 |
| kcp-rs | `MTU` | `DEFAULT_MTU` | ✅ 已添加别名 |
| kcp-rs | `KCP_OVERHEAD` | `HEADER_SIZE` | ✅ 已添加别名 |
| kcp-rs | `KCP_DEFAULT_WND` | `DEFAULT_WINDOW` | ✅ 已添加别名 |
| kcp-rs | `KCP_MAX_WND` | `MAX_WINDOW` | ✅ 已添加别名 |
| kcp-rs | `KCP_MAX_FRAG` | `MAX_FRAGMENTS` | ✅ 已添加别名 |
| kcp-rs | `IKCP_PROBE_LIMIT` | `PROBE_LIMIT` | ✅ 已添加别名 |
| kcrypt-rs | `CRYPT_HDR` | `CRYPTO_HEADER_SIZE` | ✅ 已迁移 |
| kcrypt-rs | `NONCE_SZ` | `NONCE_SIZE` | ✅ 已迁移 |
| smux-rs | `SMUX_VER` | `pub(crate)` 内部 | 保留 |

---

#### F14. `KcpTcpListener::close()` 空体

> **状态：✅ 已解决。** `close()` 已在 CAS 后添加 `notify_waiters()` 调用。

`kcp-rs/src/conn.rs` 第 2005-2011 行：
```rust
pub fn close(&self) {
    if self
        .closed
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        // note: cannot abort a blocking accept(2) — only the listener drop
        // (when KcpTcpListener goes out of scope) unblocks the kernel fd.
    }
}
```

**注意**：`KcpTcpListener` 的 `accept()` 调用 `TcpRawListener::accept` 使用阻塞 `accept(2)` 在 `cpu_block` 中，无法通过 CAS 或 `notify_waiters` 取消。实际中止仍依赖 listener fd 关闭。

---

#### F15. 文档注释缺失

关键 API 缺少文档注释：

| API | 缺失内容 |
|-----|---------|
| `KCP::flush_with_current` | 参数 `current` 的单位（ms?）未说明 |
| `PacketTransport::try_send_batch` | 返回值语义不明确（packet 数 vs byte 数） |
| `CryptoBuf::prepare_encrypt` | 两阶段加密（prepare → finalize）的协作契约未文档化 |
| `SmuxConn::run` vs `SmuxConn::spawn` | 选择标准未明确（何时用哪个） |
| `KcpConn::read_shared` / `write_all_shared` | 并发安全语义未说明 |

---

#### F16. unsafe 块缺少 SAFETY 注释

> **状态：✅ 已解决。** 所有 unsafe 块均已有 `// SAFETY:` 注释。

`kio-rs/src/lib.rs` 中有 3 个 `unsafe` 块调用 `libc::signal`，均已添加 `// SAFETY:` 注释：
- SIGINT handler：说明只做 `AtomicBool::store`，符合 async-signal-safe
- SIGPIPE handler：说明 `SIG_IGN` 是有效 disposition，无 Rust 数据访问
- SIGUSR1 handler：同上，只做 `AtomicBool::store`

---

## 四、生产级代码规范

### 规范 1：模块边界与文件拆分

**问题**：二进制 `main.rs` 超 2000 行。

**规范**：单个 `.rs` 文件不超过 **500 行**。二进制 crate 必须拆分为模块：

```
kcptun-client/src/
├── main.rs              // 仅 entry point + fn main()（<50 行）
├── cli.rs               // Clap 解析 + 配置加载
├── session.rs           // KcpClientSession 管理（legacy session struct）
├── flush_loop.rs        // 4-phase flush 循环
├── udp_reader.rs        // 入站 UDP 读取 + 解密
├── snappy.rs            // Snappy session codec
└── pprof_handler.rs     // 可选 pprof
```

```
kcptun-server/src/
├── main.rs              // 仅 entry point
├── cli.rs               // Clap 解析
├── session.rs           // KcpServerSession 管理
├── flush_loop.rs        // 4-phase flush 循环（与 client 共享 → common）
├── udp_dispatcher.rs    // DashMap peer 分发
├── stream_handler.rs    // SMUX stream → target TCP pipe
└── pprof_handler.rs     // 可选 pprof
```

**迁移路径**：先提取 `flush_loop` 和 `udp_reader` 为 `kcptun-common` 模块（client/server 共享），再拆分 CLI。

---

### 规范 2：API 层次简化——一功能一入口

**问题**：同一功能有 2-3 个入口。

**规范**：每个公开能力只暴露 **一个首选入口**，其余标注 `#[deprecated]` 或移至 `pub(crate)`：

| 功能 | 保留入口 | 废弃/隐藏 |
|------|---------|----------|
| 选加密引擎 | `CryptEngine::select()` | `select_block_crypt()` → `#[deprecated]` |
| 选 AEAD | `CryptEngine::select()` (内置) | `select_aead_crypt()` → `#[deprecated]` |
| SMUX 客户端 | `SmuxConn::connect().build().await` | `SmuxConn::client()` → `#[deprecated]` |
| SMUX 服务端 | `SmuxConn::serve().build().await` | `SmuxConn::server()` → `#[deprecated]` |
| KCP TCP 拨号 | `KcpConn::connect().transport(TcpRaw)` | 合并 `connect_tcp` |

```rust
// ✅ 唯一公开入口
let (engine, name) = CryptEngine::select("aes-128", &key);

// ❌ 标记 deprecated
#[deprecated(note = "Use CryptEngine::select instead")]
pub fn select_block_crypt(...) -> (Box<dyn BlockCrypt>, String) { ... }
```

---

### 规范 3：Session 命名正规化

**问题**：客户端 `KcpConn` 与库 `kcp_rs::KcpConn` 撞名；两个 session 结构体 90% 重复。

**规范**：

| 当前 | 规范名 | 位置 |
|------|--------|------|
| 客户端 `KcpConn` (main.rs:395) | `KcpClientSession` | `kcptun-common/src/client_session.rs` |
| 服务端 `KcpServerSession` (main.rs:505) | `KcpServerSession`（不变） | `kcptun-common/src/server_session.rs` |
| 库 `kcp_rs::KcpConn` | `KcpConn`（不变） | `kcp-rs/src/conn.rs` |
| 原 `kcptun-common/src/session.rs` | ✅ 已按职责改名 | `kcptun-common/src/kcp_transport.rs` |

**提取共享基础**：
```rust
// kcptun-common/src/session_base.rs
/// KCP session 共享字段（client + server 都用）
pub struct KcpSessionCore {
    pub kcp: Arc<Mutex<KCP>>,
    pub smux: Arc<smux_rs::Session>,
    pub crypt: Arc<CryptEngine>,
    pub has_encryption: bool,
    pub crypto_buf: Arc<Mutex<CryptoBuf>>,
    pub ack_crypto_buf: Arc<Mutex<CryptoBuf>>,
    pub raw_packets: Arc<Mutex<Vec<Bytes>>>,
    pub flush_notify: Arc<kio::Notify>,
    pub fec_encoder: Option<Arc<Mutex<FecEncoder>>>,
    pub fec_decoder: Option<Mutex<FecDecoder>>,
    pub compressor: Arc<Mutex<snap::write::FrameEncoder<Vec<u8>>>>,
    pub dead: Arc<AtomicBool>,
    pub rate_limiter: Arc<RateLimiter>,
    pub nocomp: bool,
    pub acknodelay: bool,
    pub socket: Arc<kio::DatagramSocket>,
}
```

客户端和服务端各自只保留差异字段。

---

### 规范 4：Builder 去重

**问题**：三个 KCP Builder 的 setter 完全重复（120 行）。

**规范**：提取公共 trait 或宏：

**方案 A：trait**
```rust
pub trait KcpConfigBuilder: Sized {
    fn mtu(self, v: u32) -> Self;
    fn sndwnd(self, v: u32) -> Self;
    fn rcvwnd(self, v: u32) -> Self;
    fn mode(self, v: KcpMode) -> Self;
    fn stream(self, v: bool) -> Self;
    fn acknodelay(self, v: bool) -> Self;
    fn conv(self, v: u32) -> Self;
    fn token(self, v: u32) -> Self;
    fn nodelay(self, n: u32, i: u32, r: u32, nc: u32) -> Self;
    fn fec(self, d: u32, p: u32) -> Self;
    fn config(self, cfg: KcpConfig) -> Self;
}
```

**方案 B：宏**（字段完全一致时更简洁）
```rust
macro_rules! impl_kcp_builder_setters {
    ($t:ty) => {
        impl $t {
            pub fn mtu(mut self, v: u32) -> Self { self.config.mtu = v; self }
            pub fn sndwnd(mut self, v: u32) -> Self { self.config.sndwnd = v; self }
            // ... 其余 setter
        }
    };
}
impl_kcp_builder_setters!(KcpConnBuilder);
impl_kcp_builder_setters!(KcpListenerBuilder);
```

---

### 规范 5：错误处理统一

**规范**：workspace 级使用 `thiserror` 做库错误，`anyhow` 做二进制错误。

`Cargo.toml` workspace deps：
```toml
[workspace.dependencies]
thiserror = "1.0"
```

每个 crate 的错误类型：
```rust
#[derive(Debug, thiserror::Error)]
pub enum KcpError {
    #[error("no data available")]
    NoData,
    #[error("too many fragments")]
    TooManyFragments,
    #[error("invalid segment length")]
    InvalidLength,
    #[error("conv mismatch: expected {expected}, got {got}")]
    ConvMismatch { expected: u32, got: u32 },
    #[error("unknown command: 0x{0:02x}")]
    UnknownCommand(u8),
    #[error("invalid segment")]
    InvalidSegment,
    #[error("buffer too small")]
    BufferTooSmall,
}
```

同理改造 `SessionError`、`FrameError`、`InboundCryptError`。

---

### 规范 6：trait 设计——减少手写 `Box::pin`

**问题**：`PacketTransport` 每个方法都需要 `Box::pin(async move { ... })`。

**规范**：两个选项：

**选项 A：引入 `async_trait`（最小改动）**
```toml
[workspace.dependencies]
async-trait = "0.1"
```
```rust
#[async_trait::async_trait]
pub trait PacketTransport: Send + Sync {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()>;
    // ...
}
```

**选项 B：Rust 1.75+ native async fn in trait**
```rust
pub trait PacketTransport: Send + Sync {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()>;
}
```
**注意**：native async trait 仍需 `Box<dyn>` 时有 object-safety 限制，需评估。如果保持当前设计，至少提供 blanket impl 宏减少样板。

---

### 规范 7：消除全局可变状态

**问题**：`OffloadProfile` 用全局 `AtomicU8`。

**规范**：per-session 配置替代全局静态：

```rust
/// Per-session crypto offload configuration.
#[derive(Debug, Clone)]
pub struct CryptoOffloadConfig {
    pub profile: OffloadProfile,
    pub encrypt_min_pkts: usize,
    pub encrypt_min_bytes: usize,
    pub decrypt_min_bytes: usize,
    pub compress_min_bytes: usize,
}

impl Default for CryptoOffloadConfig {
    fn default() -> Self {
        Self {
            profile: OffloadProfile::Tokio,
            encrypt_min_pkts: 8,
            encrypt_min_bytes: 8192,
            decrypt_min_bytes: 512,
            compress_min_bytes: 16384,
        }
    }
}

// CryptoTransport 持有自己的 config
pub struct CryptoTransport {
    inner: Arc<DatagramSocket>,
    crypt: Arc<CryptEngine>,
    has_encryption: bool,
    config: CryptoOffloadConfig,  // ← per-session
    data_crypto_buf: Arc<Mutex<CryptoBuf>>,
    ack_crypto_buf: Arc<Mutex<CryptoBuf>>,
}
```

---

### 规范 8：命名一致性

| 当前 | 规范 | 理由 |
|------|------|------|
| `IKCP_RTO_NDL` | `RTO_NODELAY_MIN` | 去掉 C 前缀，语义化 |
| `IKCP_RTO_MIN` | `RTO_DEFAULT_MIN` | 语义化 |
| `IKCP_RTO_DEF` | `RTO_DEFAULT` | 语义化 |
| `IKCP_RTO_MAX` | `RTO_MAX` | 保留，已清晰 |
| `IKCP_DEADLINK` | `DEAD_LINK_RETRIES` | 语义化 |
| `KCP_OVERHEAD` | `HEADER_SIZE` | 模块前缀已由 `segment::` 表达 |
| `KCP_DEFAULT_WND` | `DEFAULT_WINDOW` | 去掉 crate 前缀 |
| `KCP_MAX_FRAG` | `MAX_FRAGMENTS` | 全拼 |
| `KCP_THRESHOLD_INIT` | `SSTHRESH_INIT` | 匹配 TCP 术语 |
| `CRYPT_HDR` | `CRYPTO_HEADER_SIZE` | 全拼，不缩写 |
| `NONCE_SZ` | `NONCE_SIZE` | 不缩写 SIZE |
| `MTU` | `DEFAULT_MTU` | 明确是默认值 |
| `SMUX_VER` | `DEFAULT_VERSION` | 语义化 |

**例外**：KCP 常量保留 Go 名（如 `IKCP_RTO_DEF`）仅限 `kcp.rs` 内部用 `pub(crate)`，对外用语义名。Go 名出现在行内注释中方便对照。

---

### 规范 9：clippy allow 最小化

**规范**：

1. 保留与 Go 控制流兼容性直接相关的 allow：
```rust
#![allow(
    clippy::collapsible_if,      // mirrors Go kcp-go control flow
    clippy::while_let_loop,      // Go: for { select } pattern
    clippy::needless_range_loop, // Go: index-based iteration
    clippy::too_many_arguments,  // KCP API surface matches Go
)]
```

2. 移除无关项（逐个修复后删除）：
   - `uninlined_format_args` — 改用 `format!("{x}")` 风格
   - `arc_with_non_send_sync` — 检查 Arc 包裹的类型
   - `manual_hash_one` — 改用 `std::collections::HashSet`
   - `collapsible_else_if` — 合并 else if 链
   - `unnecessary_cast` — 删除多余的 `as` 转换

3. 每条保留的 allow 必须有行内注释说明为什么不能修复

---

### 规范 10：文档注释标准

**规范**：所有 `pub` 项必须有 `///` 文档注释：

```rust
/// One-line summary (imperative mood).
///
/// Detailed description when non-trivial. Include:
/// - What it does
/// - Parameters meaning
/// - Return value semantics
/// - Panics / Errors conditions
///
/// # Example
/// ```no_run
/// let conn = KcpConn::connect("127.0.0.1:29900").build().await?;
/// ```
```

**当前缺失文档的关键 API**：

| API | 缺失内容 | 补充说明 |
|-----|---------|---------|
| `KCP::flush_with_current` | `current` 参数单位 | "current timestamp in milliseconds" |
| `PacketTransport::try_send_batch` | 返回值语义 | "Returns number of datagrams sent (not bytes)" |
| `CryptoBuf::prepare_encrypt` | 两阶段加密契约 | "Must be followed by `finalize_encrypt_packet` before send" |
| `SmuxConn::run` vs `spawn` | 选择标准 | "Use `run` for simple single-transport; `spawn` for split read/write halves" |
| `KcpConn::read_shared` | 并发安全语义 | "Safe for concurrent read/write tasks; uses internal mutexes" |
| `KcpConn::write_all_shared` | 同上 | "Safe for concurrent use; blocks on send window via write_notify" |

---

### 规范 11：测试组织

**规范**：

- **单元测试**：`#[cfg(test)] mod tests` 在同文件内（当前已做到 ✅）
- **集成测试**：`tests/` 目录，文件名语义化
- **性能基准**：`criterion` crate，独立 `benches/` 目录
- **共享 mock**：提取到 `tests/common/` 模块

**当前问题**：`smux-rs/src/conn.rs` 测试中有大量 `#[cfg(feature = "tokio")]` 条件编译的 mock transport（`MockTransport`、`MockWriteHalf`、`BlockingReadHalf`），应提取到 `tests/mock_transport.rs` 共享。

---

### 规范 12：unsafe 审查

**规范**：

1. 每个 `unsafe` 块必须有 `// SAFETY:` 注释：
```rust
// SAFETY: `sigint_handler` only performs `AtomicBool::store` with
// `Ordering::SeqCst`, which is async-signal-safe per POSIX §2.4.3.
// No heap allocation, no locks, no I/O.
unsafe {
    libc::signal(libc::SIGINT, sigint_handler as *const () as libc::sighandler_t);
}
```

2. 信号处理函数只做 `AtomicBool::store`（当前已做到，但缺注释）

3. 考虑用 `signal-hook` crate 替代手写信号处理

---

### 规范 13：production 路径收敛路线图

**问题**：legacy flush loop vs library `KcpConn` 双轨并行。

**规范**：制定明确的收敛路线图：

```
Phase 1: 提取共享 session 基础
  - kcptun-common 中提取 KcpSessionCore（从两个 main.rs 中去重公共字段）
  - kcptun-common 中提取共享 flush_loop 模块
  - 验证: make stress + make e2e

Phase 2: legacy loop 使用 KcpConn 内部组件
  - 替换内联 KCP 管理为 KCP::input_no_flush + flush_if_pending（已在库中验证）
  - 保留 Snappy/SMUX 外挂
  - 验证: make stress + make e2e + make bench

Phase 3: 切换到 KcpConn 作为 transport
  - legacy session 使用 kcp_rs::KcpConn 作为底层 transport
  - SMUX/Snappy 通过 AsyncRead/Write 接口外挂
  - 验证: make stress + make e2e + make bench

Phase 4: 删除 legacy inline KCP 管理
  - 删除 main.rs 中的手写 KCP 状态机管理
  - 删除 main.rs 中的手写 input/flush 循环
  - 验证: make gate
```

---

## 五、优先级矩阵

| 优先级 | 编号 | 任务 | 影响范围 | 工作量 | 风险 | 状态 |
|--------|------|------|----------|--------|------|------|
| **P0** | F1 | 拆分二进制 main.rs 为模块 | client + server | 中 | 低 | ✅ |
| **P0** | F4 | Session 命名正规化 | 全 workspace | 小 | 低 | ✅ |
| **P0** | F10 | 统一错误类型（引入 `thiserror`） | 全 workspace | 小 | 低 | ✅ |
| **P1** | F7 | Builder setter 去重（宏） | kcp-rs | 小 | 低 | ✅ |
| **P1** | F8 | 废弃 `select_block_crypt` | kcrypt-rs | 小 | 低 | ✅ |
| **P1** | F2 | 废弃 `SmuxConn::client/server` | smux-rs | 小 | 低 | ✅ |
| **P2** | F9 | 消除全局 `OFFLOAD_PROFILE` | kcrypt-rs + common | 中 | 中 | ✅ |
| **P2** | F5 | clippy allow 最小化 | kcp-rs | 小 | 低 | ✅ |
| **P2** | F13 | 常量命名统一 | 全 workspace | 小 | 低 | ✅ |
| **P2** | F15 | 文档注释补全 | 全 workspace | 小 | 低 | ✅ |
| **P2** | F16 | unsafe 块补 SAFETY 注释 | kio-rs | 小 | 低 | ✅ |
| **P3** | F2 | production 路径收敛（Phase 1-4） | 全 workspace | 大 | 高 | ✅ |
| **P3** | F6 | `PacketTransport` async trait 现代化 | kcp-rs + common | 中 | 中 | ⬜ |
| **P3** | F3 | `KCP` 输出回调去动态分发 | kcp-rs | 中 | 中 | ⬜ |
| **P3** | F11 | `qpp-rs` 拆分模块 | qpp-rs | 小 | 低 | ⬜ |
| **P3** | F12 | `kio::copy_bidirectional` 去重 | kio-rs | 中 | 中 | ⬜ |
| **P3** | F14 | `KcpTcpListener::close()` 语义修复 | kcp-rs | 小 | 低 | ✅ |

---

## 六、迁移路线图

### 阶段 1：P0 立即执行（1-2 天）

1. **引入 `thiserror`** — ✅ 完成（所有错误类型）
2. **Session 重命名** — ✅ 完成：完整会话为 `KcptunSession`，下层文件为 `kcp_transport.rs`
3. **拆分 main.rs** — ✅ 完成：client 48 行，server 51 行，分散于 5 个模块

**验证**：`make gate`（fmt + test + clippy）

### 阶段 2：P1 短期（3-5 天）

4. **Builder 去重** — ✅ 完成：`kcp_config_setters!()` 宏已提取
5. **API 简化** — ✅ 完成：`select_block_crypt`/`select_aead_crypt` 已标记 deprecated
6. **提取 KcpSessionCore** — 共享 session 基础结构（已通过 `KcptunSession` 实现）

**验证**：`make gate` + `make stress` + `make e2e`

### 阶段 3：P2 中期（1-2 周）

7. **消除全局状态** — `OffloadProfile` 改 per-session（⏳ 待实施）
8. **clippy 最小化** — ✅ 完成：24→5 条，全 workspace 零警告
9. **常量命名统一** — 🟡 部分完成：语义化别名已添加
10. **文档补全** — 关键 API 加文档注释（⏳ 待实施）
11. **StreamError thiserror** — ✅ 完成

**验证**：`make gate` + `make stress` + `make e2e` + `make bench`

### 阶段 4：P3 长期（持续）

12. **production 路径收敛** — ✅ 完成（统一到 KcptunSession）
13. **`PacketTransport` 现代化** — async trait 或 blanket impl
14. **`KCP` 输出回调** — 泛型化或 channel 化
15. **剩余拆分** — qpp-rs 模块化、kio copy 去重

**验证**：每阶段 `make gate` + `make stress` + `make e2e` + `make bench`

---

## 七、总结

### 优势

- ✅ Go 线兼容性约束明确且严格执行
- ✅ 性能优化深入（4-phase flush、monomorphized crypto、FEC、零拷贝、cpu_block offload）
- ✅ 双 runtime 支持（tokio/smol）通过 `kio` 抽象干净实现
- ✅ 测试覆盖良好（单元 + 集成 + e2e + stress）
- ✅ AGENTS.md 层级文档体系完善

### 改进方向

1. **拆分膨胀文件** — 二进制 main.rs 已拆分（client 48行, server 51行）✅
2. **消除 API 双轨** — 每功能一个入口，其余 deprecated ✅
3. **Session 正规化** — 统一为 KcptunSession ✅
4. **引入 thiserror** — 统一错误处理 ✅
5. **消除全局状态** — `OffloadProfile` 改 per-session（⏳ 待实施）
6. **收敛 production/library 路径** — 制定明确路线图并逐步执行

### 核心原则

> **每条改动都必须通过 `make gate`（fmt + test + clippy）且不破坏 Go 线兼容性。**
> 结构性改动后运行 `make stress` + `make e2e`。性能改动后运行 `make bench`。
