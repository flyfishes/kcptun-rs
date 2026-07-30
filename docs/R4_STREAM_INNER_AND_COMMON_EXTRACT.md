<!-- Created: 2026-07-28 | Status: R4 implemented; common phase-2 still design -->

# R4 StreamInner 多锁合并 + common 二期抽取 + 验证计划

> **状态：** **R4 已实现**；**common 二期已实现**（`pipe` / `snmp_logger` / `QPPPort` + runtime features）。  
> **分支：** `perf/p0-cryptengine-and-common`。  
> **硬约束：** 不改变 SMUX v1/v2 wire、FIN/UPD/peer window 语义、Go 互通行为。  
> **关联：** `PERF_OPTIMIZATION_PLAN.md` §5.4 R4；P0 CryptEngine + common 一/二期。

---

## 0. 文档范围

| 项 | 本文是否覆盖 | 说明 |
|----|--------------|------|
| R4 SMUX `StreamInner` 多锁合并 | ✅ 主设计 | 最高风险，先文档后改 |
| `QPPPort` / `pipe` / `snmp_logger` 抽 common | ✅ 二期编码 | 行为 bit-identical |
| e2e / stress / bench 全量 | ✅ 验收门禁 | 需 release +（e2e）Go bins |
| 入站 decrypt offload 排序 bug | ❌ 仅引用 | master 未提交修复；R4 不依赖 |
| CryptEngine / `kcptun-common` 一期 | ❌ 已完成 | commit `45599de` |

**实施顺序（严格）：**

```text
1. 本文评审通过
2. R4 实现 + smux 单测 + stress（无 e2e 也可先做）
3. common 二期抽取（QPPPort/pipe/snmp）— 可与 R4 分 PR
4. e2e（Go bins 齐）+ smoke + bench — 等 master 相关 bug 稳定后更稳
```

冒烟 / e2e **可等 master 入站排序修复合入后再跑全量**；R4 与 common 抽取的单测/stress 不阻塞文档落地。

---

## 1. R4 — SMUX `StreamInner` 多锁合并

### 1.1 现状（代码事实）

`smux-rs/src/stream.rs` 中 `Stream` 持有多把独立锁 / 原子：

| 字段 | 同步原语 | 用途 |
|------|----------|------|
| `state` | `Arc<Mutex<StreamState>>` | 状态机 |
| `recv_buf` | `Arc<Mutex<BytesMut>>` | **legacy** 连续收缓冲（`push_data` 已绕开） |
| `recv_buf_bytes` | `Arc<Mutex<VecDeque<Bytes>>>` | 零拷贝收队列（热路径） |
| `send_buf` | `Arc<Mutex<VecDeque<Bytes>>>` | 待 flush 发送队列 |
| `read_waker` / `write_waker` | `Mutex<Option<Waker>>` | poll 唤醒 |
| `local_closed_at` | `Mutex<Option<Instant>>` | linger 收割 |
| `send_buf_bytes` / `recv_buf_bytes_avail` | `AtomicUsize` | 无锁长度查询 |
| `local_closed` / `remote_closed` / `fin_sent` / `opened` | `AtomicBool` | 半关闭 / FIN |
| `incr` / `upd_*` / `peer_*` | `AtomicU32` / `AtomicBool` | v2 流控 |
| `ch_reader_wakeup` / `ch_write_wakeup` | `kio::Notify` | async 等待 |

**读路径问题（`read`）：**

1. 先锁 `recv_buf_bytes` 排空  
2. 再锁 `recv_buf` legacy 回退  
3. 期间多次读 atomics（`remote_closed`、`bytes_read`、`incr`…）  
4. 与 `push_data_bytes`（写 recv）争用同一把 `recv_buf_bytes` 锁 — 这是必要的；真正税是 **双缓冲 + state/waker 另锁**

**写路径（`write_bytes` / `drain_send_max`）：** 主要争 `send_buf`；`peer_send_window` 全原子，较好。

**`clear_buffers` / `close`：** 依次锁 recv×2 + send + state，窗口大、易与 poll 交错。

### 1.2 目标（功能不变）

1. **收端单锁：** `state` + `recv` 队列 + `read_waker`（+ 可选 `local_closed_at`）合并为 `StreamInner`。  
2. **废除 legacy `recv_buf: BytesMut`**（`push_data` 已转 `push_data_bytes`；`read` 的 fallback 分支删除或断言空）。  
3. **写端独立锁：** `send_buf` + `write_waker` 留在 `SendHalf` 或第二把锁，**避免读推数据与 flush drain 互堵**。  
4. **对外 API 不变：** `Stream` 仍是 `Arc` 共享、方法签名不变；`Session`/`SmuxConn`/`SmuxIo` 无需改调用约定。  
5. **Atomics 保留** 在热查询上（`pending_send`、`available`、`peer_send_window`、半关闭标志），避免为 `available()` 抢 inner 锁。

### 1.3 目标结构（建议）

```rust
/// 收端 + 状态 + 读 waker（单锁）
struct RecvInner {
    state: StreamState,
    /// 唯一收队列（原 recv_buf_bytes）；不再维护 legacy BytesMut
    recv: VecDeque<Bytes>,
    read_waker: Option<Waker>,
    local_closed_at: Option<Instant>,
}

/// 写端缓冲 + 写 waker（单锁，与 RecvInner 分离）
struct SendInner {
    send: VecDeque<Bytes>,
    write_waker: Option<Waker>,
}

pub struct Stream {
    id: u32,
    max_recv_buf: usize,
    recv: Mutex<RecvInner>,          // 不再 Arc<Mutex<…>> 套娃；Stream 已 Arc 共享
    send: Mutex<SendInner>,
    // 以下保持 Atomic* / Notify，语义同现网
    send_buf_bytes: AtomicUsize,
    recv_buf_bytes_avail: AtomicUsize,
    bytes_read: AtomicU32,
    bytes_written: AtomicU32,
    opened: AtomicBool,
    remote_closed: AtomicBool,
    local_closed: AtomicBool,
    fin_sent: AtomicBool,
    incr: AtomicU32,
    upd_consumed: AtomicU32,
    pending_upd: AtomicBool,
    peer_consumed: AtomicU32,
    peer_window: AtomicU32,
    ch_reader_wakeup: kio::Notify,
    ch_write_wakeup: kio::Notify,
}
```

**命名说明：** 计划里的 `StreamInner` 在实现上拆成 `RecvInner` + `SendInner` 两把锁，比「万物一锁」更符合 R4 原文「send 侧可保留独立锁，避免读写互堵」。文档与代码可称 **R4 = 双 half 合并**，对外仍叫 StreamInner 方案。

### 1.4 锁序与不变式（防死锁）

| 规则 | 内容 |
|------|------|
| **L1** | 若同时需要 `recv` 与 `send`，**永远先 `recv` 后 `send`**（仅 `close` / `clear_buffers` 需要双锁） |
| **L2** | 持锁期间 **禁止** `.await`、禁止再调可能重入 `Stream` 的 session 方法 |
| **L3** | `wakeup_reader` / `wakeup_writer`：先在锁内 `take` waker，**放锁后再** `wake()` + `Notify`（避免 waker 回调重入死锁） |
| **L4** | `push_data_bytes` 只锁 `recv`；`drain_send_max` 只锁 `send` + 读 atomics |
| **L5** | atomics 与锁内队列长度：**更新队列时同步改 atomic**（与现逻辑一致）；允许短暂与 `len()` 统计偏差仅在 clear 路径需一起清零 |

### 1.5 方法迁移表

| 方法 | 现锁 | 目标 |
|------|------|------|
| `state` / `set_state` / `is_closed` / `is_ready` | `state` | `recv` 锁内 `state` |
| `push_data` / `push_data_bytes` | `recv_buf_bytes` | `recv`；删 legacy 路径 |
| `read` / `read_async` / `poll_read` 共享逻辑 | `recv_buf_bytes` 然后 `recv_buf` | 仅 `recv` 一把 |
| `write` / `write_bytes` | `send_buf` | `send` |
| `drain_send_max` / `drain_send` | `send_buf` | `send` |
| `register_read_waker` / `wakeup_reader` / `fin_event` | `read_waker` | `recv` |
| `register_write_waker` / `wakeup_writer` | `write_waker` | `send` |
| `mark_local_closed` / `local_closed_elapsed` / `force_local_closed_at` | `local_closed_at` | `recv.local_closed_at` + atomics |
| `clear_buffers` | 三把 | `recv` 再 `send`（L1） |
| `close` | state + clear | `recv`（state+clear recv）再 `send`；atomics；放锁 wake |
| `apply_peer_update` / `peer_send_window` / `take_upd` | 仅 atomics | **不变** |
| `recv_buf_capacity` | `recv_buf.capacity()` | 改为 `max_recv_buf` 或 `recv` 队列估算；**检查调用方** — 若仅测试/诊断，可返回 `max_recv_buf` 或 `recv_buf_bytes_avail` 相关语义并改测试 |

### 1.6 删除 / 保留决策

| 项 | 决策 | 理由 |
|----|------|------|
| `recv_buf: BytesMut` | **删除** | 热路径已不写入；减少双路径与 `clear` 成本 |
| `push_data` | 保留 API，内部只 `copy_from_slice` → `push_data_bytes` | 兼容 session 若仍调 `push_data` |
| `Arc` 包在每个 Mutex 上 | **去掉内层 Arc** | `Stream` 已是 `Arc<Stream>` 共享；少一层间接 |
| v2 atomics | **保留** | 与 Go 窗口算法一致；`poll_write` 高频读 |
| `Notify` | **保留** | `read_async` / 背压依赖 |

### 1.7 风险与缓解

| 风险 | 等级 | 缓解 |
|------|------|------|
| FIN 时序：`mark_remote_closed` 与 `read` 竞态 | 高 | 保持「先 atomic remote_closed，再 wakeup」；单测 `read_async` FIN+data |
| 双锁 `close` 死锁 | 高 | 强制 L1 锁序；单测与 loom 可选（不做 loom 默认） |
| `wake()` 持锁回调 | 高 | L3 放锁再 wake |
| peer window 假死 | 中 | 不改 `peer_send_window` 公式；stress + smuxver=2 e2e |
| 性能回退（一锁过大） | 中 | 读写分锁；bench/stress 对比 RSS 与吞吐 |
| `recv_buf_capacity` 语义变 | 低 | 全局 rg 调用方后改 |

### 1.8 实现步骤（代码阶段，本文通过后）

```text
R4.1  引入 RecvInner/SendInner，字段迁移，new/with_buffer 编译通过
R4.2  迁移 push/read/waker/state；删除 legacy recv_buf
R4.3  迁移 send/drain/write/close/clear；统一锁序
R4.4  cargo test -p smux-rs（tokio + smol feature）
R4.5  make stress（release）
R4.6  更新 smux-rs/AGENTS.md + PERF 勾选 R4
```

**单 PR 纪律：** 仅 `smux-rs`（+ 必要 AGENTS/PERF）；不混 common 二期。

### 1.9 R4 验收标准

- [ ] `cargo test -p smux-rs`（default tokio）全绿  
- [ ] `cargo test -p smux-rs --no-default-features --features smol` 全绿  
- [ ] 现有 stream 单测全部保留且语义不变（含 peer_window、read_async FIN、clear_buffers 不伪造 remote_closed）  
- [ ] `make stress` 8/8  
- [ ] clippy `-D warnings`（smux-rs）  
- [ ] （可选待 Go bins）`test_e2e.sh` smuxver=1 与 2  
- [ ] 无新增持锁 `.await`

---

## 2. common 二期 — QPPPort / pipe / snmp_logger

### 2.1 动机

一期已抽：`derive_key` / `apply_mode` / `SnappyStreamDecoder`。  
二期目标：client/server 仍重复的会话周边 helper，**行为完全一致拷贝合并**。

### 2.2 现状体量（约）

| 符号 | client | server | 说明 |
|------|--------|--------|------|
| `QPPPort` + AsyncRead/Write×2 | ~1480–1670 | ~398–590 | tokio + smol 双 impl；依赖 `qpp` feature |
| `pipe` | ~1671–1770 | ~592–700 | idle timeout 语义；server 注释更全 |
| `snmp_logger` | ~1773–1819 | ~1477–1521 | 周期写 SNMP 文件 |

### 2.3 目标落点

```text
kcptun-common/
  src/key.rs              # 已有
  src/mode.rs             # 已有
  src/snappy_frame.rs     # 已有
  src/pipe.rs             # 新增：idle pipe
  src/snmp_log.rs         # 新增：snmp_logger
  src/qpp_port.rs         # 新增：feature = "qpp"
```

### 2.4 依赖与 feature

| 模块 | 新增依赖 | feature |
|------|----------|---------|
| `pipe` | `kio-rs`（`AsyncRead`/`AsyncWrite`/`timeout`）、`anyhow` 或 `std::io` | 需 **tokio \| smol** 与 binary 对齐 → **common 必须加 runtime features** |
| `snmp_log` | `kcp-rs`（SNMP）、`kio` sleep、`log` | 同上 runtime |
| `qpp_port` | `qpp-rs`、`kio`、parking_lot | `qpp` + runtime |

**设计选择（推荐）：**

```toml
# kcptun-common/Cargo.toml（二期）
[features]
default = ["tokio"]
tokio = ["kio-rs/tokio"]
smol = ["kio-rs/smol"]
qpp = ["dep:qpp-rs"]
```

一期 common **无 runtime**；二期引入 feature 后：

- binaries：`kcptun-common/tokio` 或 `kcptun-common/smol` 与自身 feature 同步  
- Makefile `RT_PKGS` 视需要纳入 common（若 common 仅 lib 被 binary 拉起，可不单独进 RT_PKGS）

**备选（更小风险）：** `pipe`/`QPPPort` 仍放 binary，只抽 **纯 snmp 格式化** 或 snmp_logger 的「写文件逻辑」— 收益低，不推荐。

### 2.5 语义对齐要点

| 组件 | 必须保持 |
|------|----------|
| `pipe` | **idle** 超时（任一侧 `idle_secs` 无读写则结束），**不是**总时长；返回 `(up_bytes, down_bytes)` |
| `snmp_logger` | period、stop flag、与 `DEFAULT_SNMP` 字段格式一致 |
| `QPPPort` | 与 Go QPP 流混淆字节兼容；enc/dec 分 PRNG；tokio `ReadBuf` vs smol `&mut [u8]` |

**合并前 diff：** 以 client 为基准，server 多出的注释/小差异写入 common 文档；行为取已通过 stress 的一侧，禁止「顺手优化」pipe 超时。

### 2.6 实施步骤

```text
C2.1  common 加 features + kio 依赖；抽 snmp_logger（依赖最少）
C2.2  抽 pipe；双端改 use；跑 client/server 单测
C2.3  抽 QPPPort（feature qpp）；无 qpp 构建仍绿
C2.4  更新 kcptun-common/AGENTS + client/server AGENTS
C2.5  clippy-both + test-both
```

**与 R4 关系：** 分 PR；common 二期不改 smux。

### 2.7 验收

- [ ] client/server 无本地 `struct QPPPort` / `fn pipe` / `fn snmp_logger` 重复定义  
- [ ] `cargo test -p kcptun-client -p kcptun-server`（tokio）  
- [ ] smol feature 构建 + 相关 test  
- [ ] `qpp` off 时 ARM/smol 路径可编译（与现 optional qpp 一致）

---

## 3. 全量验证计划（e2e / stress / bench）

### 3.1 前置

| 产物 | 命令 | 备注 |
|------|------|------|
| tokio release | `make release` | stress / smoke / bench |
| smol release | `make release-smol` | e2e / smoke smol 组 |
| Go kcptun bins | `tests/kcptun-go/` 可执行文件 | **当前仓库仅 go.mod+README，缺 bin** → e2e 阻塞直到补齐或改路径 |

### 3.2 门禁矩阵

| 阶段 | 命令 | 何时跑 |
|------|------|--------|
| 单测 | `cargo test -p smux-rs` + common + bins | 每个 PR |
| clippy | `make clippy` 或改动 crate `-D warnings` | 每个 PR |
| stress | `make release && make stress` | R4、flush、session 锁 |
| smoke Rust↔Rust | `bash smoke_test_rust_rust.sh` | master 入站排序修复后优先；R4 后建议再跑 |
| e2e Go↔Rust | `make e2e` 或 `bash test_e2e.sh` | 有 Go bins；crypt/smuxver/nocomp/FEC |
| bench | `make bench` / `BENCH_DATA_MB=50 …` | 性能 PR；对比不回退 bulk |

### 3.3 已知干扰（勿与 R4 混为一谈）

1. **入站 `should_cpu_block_decrypt` 乱序**（master 工作区已倾向 `should_offload = false` 或仅 `nocomp && !fec`）：会导致 aes/comp 下 md5/短读；**R4 不修复此问题**，全量 smoke 前应先合入或 rebase 该修复。  
2. **e2e 缺 Go 二进制**：需从上游 kcptun 构建放入 `tests/kcptun-go/` 或文档化替代路径。  
3. 历史 smoke 在 p0 上曾出现部分全双工/Wave 失败 — 复现时先区分「入站 offload」vs「SMUX 锁改」。

### 3.4 建议时间线

```text
现在     → 本文评审
接着     → R4 实现 + smux test + stress
并行/后  → common C2（snmp → pipe → qpp）
master 入站修复合入 p0 后 → smoke 全量
Go bins 就绪 → e2e
可选     → bench 对照 45599de / master
```

---

## 4. 明确不做（本轮）

| 不做 | 原因 |
|------|------|
| 把 peer window 也塞进 Mutex | 热路径原子已够；易引入 lock 放大 |
| 读写同一把大锁 | 违背 R4「避免读写互堵」 |
| 改 SMUX 帧格式 / 默认窗口 | wire / 公平 |
| R4 PR 内大重构 session.rs | 范围膨胀 |
| 默认恢复无序 inbound cpu_block | 正确性优先于 offload 收益 |
| 在 `tests/` 下新增 AGENTS.md | 项目约定 |

---

## 5. 文档与代码同步清单（实现后）

- [ ] `smux-rs/AGENTS.md` — Stream 锁模型、RecvInner/SendInner  
- [ ] `PERF_OPTIMIZATION_PLAN.md` — R4 勾选完成  
- [ ] `kcptun-common/AGENTS.md` — pipe/snmp/qpp 模块  
- [ ] 根 `AGENTS.md` — 若 common feature 成为 workspace 约定  
- [ ] 本文状态改为 `implemented` + 日期与 commit

---

## 6. 开放问题（实现前需确认或默认）

| # | 问题 | 默认建议 |
|---|------|----------|
| Q1 | `Stream` 内 Mutex 是否去掉 `Arc` 包装？ | **是**（Stream 已 Arc 共享） |
| Q2 | legacy `recv_buf` 是否直接删？ | **是**，单测覆盖 push/read |
| Q3 | common 二期是否引入 tokio/smol feature？ | **是**，与 kio 对齐 |
| Q4 | R4 与 common 二期同一 PR？ | **否**，两个 PR |
| Q5 | 全量 smoke 是否阻塞 R4 合并？ | **不阻塞**；stress+smux test 足够合 R4；smoke 跟 master 修复 |

若默认建议可接受，实现阶段按本文 §1.8 / §2.6 执行，不再扩大范围。

---

## 7. 一句话

> **R4：收发分锁、废除 legacy 双收缓冲、放锁再 wake；common 二期：pipe/snmp/QPP 进带 runtime feature 的 common；全量 e2e/smoke 等 Go bins 与 master 入站修复，不挡 R4 单测与 stress。**

## 8. 收尾补全（2026-07-28）

| 项 | 状态 |
|----|------|
| salsa20/xor 入站无 20B 头 | ✅ client/server |
| 入站 offload 默认关闭（排序安全） | ✅ + `decrypt_offload_skipped` |
| `encrypt_batch_into` / raw_packets capacity | ✅ |
| `SessionConfig` 收敛构造参数 | ✅ client/server |
| SNMP `encrypt_inline`/`encrypt_offload` | ✅ Rust-only |
| e2e/smoke/bench | 仍依赖 Go bins / master 稳定后 |
