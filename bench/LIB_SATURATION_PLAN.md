# LIB 路径高并发饱和 — 诊断与优化计划

- 日期: 2026-08-01
- 状态: **P0/P1/P3/P4 已实施（P2 定位中，另见 bugs/）**
- 范围: `kcp_rs::KcpConn`（lib 路径，`KCPTUN_USE_LIB_KCP=1`）

---

## 1. 问题症状

128KB payload 下 lib 路径高并发饱和：

| RPS | 探针 | lib P50 | lib P99 | lib P999 | 备注 |
|----:|------|--------:|--------:|---------:|------|
| 100 | asyncio CONC=8 | 4408µs | 7258µs | 10263µs | 干净，100% 成功 |
| 500 | sync（无界） | 3414µs | 5106µs | 13517µs | **22% 请求 >5s 超时** |
| 500 | asyncio CONC=8 | 6329µs | 113882µs | 351447µs | 尾部爆炸（114ms p99） |

对照：**legacy 在 sync 探针下 99.98% 成功**（曾误判为 lib 独有问题），但 asyncio CONC=8 下 legacy 同样恶化（p99=212ms）。→ 部分是**所有实现的共性瓶颈**，部分是 lib 特有。

## 2. 取证证据（已确认）

### 2.1 CPU profile（饱和负载下，14s 采样）

**lib 客户端**（~1.5 核，远未 CPU 饱和）：
| 热点 | flat | 归属 |
|------|-----:|------|
| `DatagramSocket::send_batch` | 23.2% | 逐包 async UDP 发送 |
| `CryptoTransport::try_recv` | 17.4% | 收包解密 |
| `_mi_arenas_try_find_free` | 14.3% | **内存分配** |
| `tokio time::Driver::park_internal` | 12.6% | **2ms 定时器轮询** |
| `event_listener::Task::wake` + `wake_by_val` | 5.7% + 2.3% | 任务唤醒 |
| mutex lock/unlock_slow | ~3% | 锁竞争 |
| `lib_flush_loop`（cum） | 36.5% | 写路径，其中 **send_batch 占 75.6%** |

**legacy 客户端**（对照）：`send_batch` 29%、alloc 15%、timers 10%、mutex 3.6% —— **profile 形状与 lib 相同**。
**服务端**（legacy）：`start_flush_loop` 34% flat，1.75 核，能扛住。

### 2.2 SNMP（饱和负载）
- **双方 KCP 队列均空**（RingBufferSndQueue=0, RcvQueue=0）→ 非队列积压
- `FastRetrans=273`、服务端 `FECRecovered=1321` → **loopback 在 64MB/s 下丢包**，靠 FEC/重传恢复
- 16MB sockbuf 下尾部仍坏（p99=114ms）→ **缓冲溢出非主因**

### 2.3 send_batch 实现（kio-rs tokio 非 Linux 路径）
已用 `try_send` 快路径（仅 WouldBlock 时 `writable().await`），23-29% 是 46k 包/s 的逐包 syscall + async 包装成本。**macOS 无 sendmmsg**，此块收益有限。

## 3. 根因分析

### 已确认
1. **非 CPU 瓶颈**：lib 客户端仅 ~1.5 核，延迟型瓶颈。
2. **共性瓶颈**：128KB × 8 并发 = 1MB 在途 ≈ KCP 窗口（1024×1326B=1.35MB）上限，所有实现在此饱和（~230-270 RPS 有效）。多毫秒尾部来自丢包触发的 KCP 重传（RTO 恢复）。
3. **I/O + 调度开销是主要 CPU 消耗**：逐包 async send（23-29%）、分配（14%）、2ms 定时器（10-13%）、唤醒（5-8%）。
4. **lib 特有弱项**：sync（无界）探针下 lib 有 22% 请求 >5s（bimodal 延迟），legacy 无。机制未完全追踪。

### 待确认
- lib 5s 停滞的确切机制（KCP RTO 链 vs SMUX 背压 vs 探针假象）—— 需在无界负载下用 SNMP/日志定位单请求时间线。

## 4. 优化方案（按优先级）

### P0: 减少分配 churn（14% CPU，风险低，收益明确）
- **机制**：`BytesMut` / 段缓冲区在热路径反复分配；mimalloc `_mi_arenas_try_find_free` 高。
- **方案**：
  - `feed_inbound` 的 `read_buf.push_back(d)` 已零拷贝（`split_to().freeze()`）；检查 `kcp.input` 的段 data `extend_from_slice` 是否可复用（`BytesMut::split` 复用底层 buffer）。
  - 输入循环的 `try_recv` 复用 `MAX_DATAGRAM` buffer（已做）；检查 `maybe_fec_expand` / `send_batch` 路径的临时 `Vec` 分配。
- **验证**：分配占比 <5%。

### P1: 降低 2ms 定时器/唤醒 churn（~18% CPU，风险中）
- **机制**：`read_shared` / `write_all_shared` / `lib_flush_loop` 都 `kio::timeout(2ms, notified())` —— 每个等待都创建一个 tokio 定时器，计时器轮 + 唤醒反复触发。
- **方案**：单读者假设下，`read_shared` 改用纯 `notified()`（无 2ms 超时），靠 notify permit 语义；close/数据到达必唤醒。`write_all_shared` 同理。**需保留一个安全网**（如 10ms 兜底而非 2ms）防极端丢失唤醒。
- **风险**：多读者竞争时 notify permit 被其他读者消费 → 需确认隧道场景单读者。

### P2: lib 特有 bimodal 停滞（>5s 请求，风险中，需先定位）
- **机制待确认**（见 §3）。候选：无界负载下 `write_all_shared` 的 2ms 等待循环 + send_batch 阻塞 → SMUX 背压级联。
- **方案**：先加单请求时间线日志（入口→SMUX→KCP→UDP→echo→返回）定位停滞点，再针对性改。

### P3: 逐包 send 开销（23-29%，收益有限）
- macOS 无 sendmmsg；Linux 已用。可尝试：更大的 UDP send buffer 减少 WouldBlock、或加密/发送更紧的批处理（已部分做）。
- **2026-08-01 评估结论：不改代码。** tokio 非 Linux 路径已用 `try_send` 快路径（仅 WouldBlock 时 `writable().await`），每个包最多一次 syscall；socket 缓冲已是 4MB（`SOCK_BUF`），且 §2.2 显示 16MB sockbuf 尾部仍坏 → 缓冲溢出非主因，加大缓冲无收益。无 sendmmsg 的前提下，逐包 syscall 是平台上限，属已知边界。保留为最后手段。

### P4（配置层，非代码）: KCP 窗口
- 128KB 大数据高并发时，`--sndwnd/--rcvwnd 1024` 的 1.35MB 窗口限制在途。提高窗口（如 4096）可提升饱和点。**同适用于 legacy**，非 lib 特有。
- **窗口取值指导**：KCP 窗口按**在途 KCP 段数**计（每段 ≈ MSS = MTU − 24B 头；默认 MTU 1350 → MSS 1326）。
  在途段数 ≈ `并发流数 × 单流平均在途字节 / MSS`，建议取：
  ```
  window ≥ ceil(并发流数 × 单流在途字节 / MSS) × 1.5~2（留头部余量，含 FEC/重传突发）
  ```
  示例：
  | 并发流数 × 单流 | 估算在途段数 | 建议 `--sndwnd/--rcvwnd` |
  |---|---:|---:|
  | 8 × 128KB | ~790 | 1024~2048 |
  | 16 × 128KB | ~1580 | 2048~4096 |
  | 32 × 128KB | ~3160 | 4096~8192 |
  默认 128 只适合 ≤1 条 128KB 流；`KCP_MAX_WND=32768` 是硬上限。
- **注意**：窗口只提升饱和点，不解决丢包重传尾部（loopback 64MB/s 下的 FastRetrans/FECRecovered 仍靠 RTO 恢复）；配合 `--nc`（nocwnd）关闭拥塞窗口上限、以及足够大的 socket 缓冲使用。

## 5. 验证计划

1. P0/P1 改后：跑 asyncio CONC=8 三档（RPS 100/300/500）对比 p50/p99/p999 + 成功率。
2. P0/P1 后重抓 profile：alloc 占比、timer 占比应显著下降。
3. P2 定位后：无界 sync 探针复测，lib 22% 丢失应消除或大幅下降。
4. **gate 必须过**：fmt / test / clippy。
5. 回归：RPS=100 干净负载下 p50 不应退化（≤ 现在 4408µs ± 10%）。

## 6. 决策点（需你拍板）

| # | 决策 | 选项 |
|---|------|------|
| 1 | 先做 P0 还是 P1？ | P0（低风险高收益）先做，P1 次之 |
| 2 | P2（lib bimodal）是否值得深挖？ | 需要先跑时间线日志定位（~30min）再决定 |
| 3 | 是否提高默认窗口？ | 属配置/使用层，可单独立项 |
| 4 | 是否接受"lib 与 legacy 在极端负载下都饱和"作为已知边界？ | 若接受，优化聚焦 P0/P1 的全局开销 |

---
_本计划基于 2026-08-01 的 profile + SNMP 取证。P0（分配）/P1（2ms→10ms 兜底）/P4（窗口文档）已实施于 `kcp-rs/src/conn.rs`；P3 评估为无清晰收益（见报告）。P2 待定位。_

---

## 7. P2 根因调查结果（2026-08-02）

**结论：P2 的「22% 请求 >5s」在受控、清洁条件下不可复现；它是偶发停滞事件（或探针/进程污染）的瞬时表现，不是 lib 路径的稳态缺陷。** 可稳定复现的是 CONC=8 饱和下的尾部恶化（p99≈150ms），但 lib 与 legacy 表现相同，属 KCP 层共性瓶颈（计划 §3.2 已预判）。计划 §2.3 的 CPU profile 与「22% >5s」之间没有建立因果链，本节给出复现实验、结构性差异与残余风险。

### 7.1 复现实验与现象

受控复现（macOS，tokio release 二进制，`--crypt aes --mode fast3 --sndwnd/--rcvwnd 1024 --nocomp --smuxver 2`，FEC 默认 10/3）：

| 实验 | 负载 | lib 结果 | legacy 结果 |
|------|------|----------|-------------|
| 同步探针（顺序，新 TCP/请求） | RPS=500, 128KB, 45s | **5155/5155 ok（100%）**，p50=3.8ms p99=6.6ms max=22.7ms，SNMP 重传=0 | — |
| 同步探针（顺序） | RPS=100, 128KB, 3s | 300/300 ok，p50=3.9ms | — |
| **并发探针 CONC=8** | RPS=500, 128KB, 12s | 5155/5155 ok（0 失败），**p50=7.8ms p99=151ms p999=481ms** | 5019/5019 ok，p50=7.9ms p99=163ms p999=551ms |
| 原始 KCP go→smol server（改名二进制避开 pkill 干扰） | RPS=200, 1KB, 20 轮 | 0 hang；仅 1 轮 p999=3ms 尾部尖峰 | — |

- **22% >5s 无法复现**：在 lib 隧道同步探针（RPS=500，128KB，45s）与并发 CONC=8（RPS=500，128KB）下均为 0 失败。计划 §1 表格中「500 sync（无界）22% 超时」与其同表的 p99=5.1ms 自相矛盾——若 22% 请求在 5s 超时，成功样本的 p99 不应只有 5ms。**该行更像测量窗口内发生了 1~2 次数秒级停滞（期间所有在途请求超时），而非稳态 bimodal**。
- **CONC=8 尾部恶化可复现**：与计划「asyncio CONC=8 p99=113ms」吻合，且 legacy 同样恶化（p99=163ms），证明这是窗口饱和共性瓶颈，非 lib 特有。

### 7.2 根因分析

#### 7.2.1 稳态饱和机制（CONC=8，可复现，属共性）

128KB×8 并发 ≈ 1MB 在途 ≈ KCP 窗口上限（1024×1326B=1.35MB）。饱和时：

1. 发送窗口填满 → `kcp.flush()` 中 `new_segs_count == 0`（snd_queue 无新段可移入 snd_buf）。
2. KCP 的 **early-retransmit 路径**被反复触发（`seg.fastack > 0 && new_segs_count == 0`）。SNMP 实测：`EarlyRetransSegs` 累计 19850、`FastRetransSegs` 2039、`RetransSegs` 21889，而 `LostSegs=0`、`FECRecovered=0`、队列全空（SndQueue=0/RcvQueue=0）。
3. 大量重复传输 → 收端重复 ACK → 更多 fastack → 更多 early-retransmit，形成 churn，抬高尾部延迟（p99≈150ms），但**不丢数据、不失败**。

这是 kcp-go 语义下 KCP 状态机在窗口上限的行为，lib 与 legacy 共享，不是 lib 特有的 bug。提高 `--sndwnd`（P4）直接缓解。

#### 7.2.2 结构性差异（lib vs legacy，已确认）

| 维度 | lib（`kcp_rs::KcpConn`） | legacy（binary 自带 session） |
|------|--------------------------|-------------------------------|
| 写路径背压 | `write_all_shared` 在 `wait_send >= snd_wnd` 时**阻塞**，2ms 轮询 `write_notify` | Task 2 无窗口检查，`kcp.send()` 无条件入 snd_queue，flush loop 后台排空 |
| KCP 锁竞争方 | 3 个（input loop / flush loop / `write_all_shared` 调用方） | 2 个（reader / flush loop） |
| ACK 发送 | input loop 突发批量后**内联** `send_urgent_packets().await` | reader 内联 `send_batch().await` |

lib 的背压更靠前（KCP 窗口级），legacy 靠 SMUX 流缓冲（256KB/流）+ 无界 snd_queue 吸收。这解释了 legacy 在短时停滞时更「能扛」（写路径不阻塞，数据排队等窗口），但也意味着 legacy 在极端停滞下会无界积压。**CONC=8 实测两者 p99 相当，说明该差异不构成 22% 丢失的来源。**

#### 7.2.3 偶发 >5s 停滞的残余机制（未完全确认）

`BUGREPORT_P99_STEP7_HANG.md` 记载的协议级机制仍是最可能的 >5s 停滞源：**收端 `rcv_queue` 填满 → ACK 通告 `wnd=0` → 发端 `rmt_wnd` 缓存为 0 → 窗口重开依赖 WIns/WAsk 握手**。若收端 input loop 短暂停滞（如 `send_urgent_packets().await` 因 UDP 发送缓冲满而阻塞），ACK/WIns 停顿数秒，发端 `write_all_shared` 的 2ms 轮询持续看到窗口满 → 在途请求超时。2ms 兜底使多数停滞在毫秒级自愈（实测 CONC=8 为 150ms 级尾部），但极端下可放大到秒级。

**重要修正**：`BUGREPORT_P99_STEP7_HANG` 的「1/25 挂死」及用户 100 轮猎挂脚本捕获的 4 次「HANG」（RUN 47/48/51/55），经核实**全部为误报**——4 个 RUN 的 `p99_run_*.log` 均完成全部 7 步（无 FAILED 行）。误报来源：`/tmp/p99_100_runs.sh` 每轮结尾执行 `pkill -f "latency_p99.*server"` 与 `pkill -f "kcp-go-latency"`，该 pkill 会误杀**任何并发的 `latency_p99* --mode server` 进程**（包括本调查的复现进程），而 watchdog 的 `pgrep -f "kcp-go-latency client"` 会匹配残留/包装进程。原始 bug report 的「both alive ~20% CPU」需在**无 pkill 干扰**环境下重新验证。

### 7.3 为什么「legacy 无此问题」的表象

计划 §1 的对照（lib 22% vs legacy 99.98%）很可能源于：
1. **探针差异**：计划自己标注「不同探针（sync vs asyncio）尾部有 ~10% 波动」；sync 无界探针的 5s 超时会把一次数秒停滞计入「失败」，而 legacy 写路径不阻塞、缓冲更深，同一停滞下 probe 侧 TCP 写不一定超时。
2. **进程污染**：若该行测量与用户猎挂循环重叠，`pkill` 会杀掉服务器使客户端挂死。

CONC=8 下 lib 与 legacy 的 p99 几乎相同（151 vs 163ms），不支持「lib 独有稳态缺陷」的结论。

### 7.4 修复建议（按风险排序）

| 优先级 | 建议 | 函数/机制 | 风险 | 评估 |
|--------|------|-----------|------|------|
| 1 | **提高默认/建议窗口**（P4 落地） | `--sndwnd/--rcvwnd 4096` | 低 | 直接缓解 CONC=8 饱和；lib/legacy 通用。实测 1024 窗口在 128KB×8 并发下必然饱和 |
| 2 | **抑制 early-retransmit churn** | `kcp.rs flush()` 的 early-retransmit 分支（`new_segs_count == 0 && fastack>0`） | 中 | 饱和时 19850 次早重传纯属 churn；可加「窗口满时降低早重传频率」节流。需保持与 Go kcp-go 线格式兼容（重传内容不变量，仅触发频率） |
| 3 | **input loop ACK 发送防阻塞** | `spawn_input_loop` 的 `send_urgent_packets().await` | 中 | 若 UDP 发送缓冲满，ACK 发送阻塞会停掉整个 input loop（含 `feed_inbound`）。可改为 try-send + 超时或交由 flush loop 兜底 |
| 4 | **写路径背压降频** | `write_all_shared` 的 2ms 轮询 | 低 | 2ms→5~10ms 兜底（P1 已提），降低窗口满时的锁轮询开销；CONC=8 下实测 lib 与 legacy 相当，收益有限 |
| 5 | **去除猎挂脚本 pkill 误杀** | `/tmp/p99_100_runs.sh` 的 `pkill -f "latency_p99.*server"` | 低 | 改为精确 PID（`kill_wait`），避免误杀并发进程；这是本次调查中污染复现的最大来源 |

### 7.5 待确认项

1. **原始 step-7 双活挂死是否真实存在**：需在无 pkill 干扰、单独跑 `run_p99.sh`（RPS=200 WARMUP=1 DURATION=2）≥50 次，挂死时 `sample` 服务器取栈。本调查的 20 轮干净复现未捕获双活挂死。
2. **>5s 停滞的单请求时间线**：若重现，需在 `write_all_shared` 阻塞点、input loop ACK 发送点、`feed_inbound` 打时间戳，确认停滞发生在写路径等待还是 ACK 路径阻塞。
3. **early-retransmit churn 是否可被 P0/P1 缓解**：P0/P1 改后重测 CONC=8，看 `EarlyRetransSegs` 是否下降。
4. **smol 后端 `notify_waiters` 语义**：kio `sync/smol.rs` 的 `notify_waiters = event.notify(usize::MAX)` 与 tokio 的「不存 permit」语义存在细微差异，需确认在窗口满时不会导致写路径忙轮询放大锁竞争。

### 7.6 对验证计划的修正

- 计划 §5.3「P2 定位后复测 lib 22% 丢失」——**建议改为**：先按 §7.4 排除探针/进程污染（修 pkill），再以 CONC=8 复测 p99（目标：lib p99 不高于 legacy），最后长时（≥5min）同步探针确认无 >5s 停滞。
- 计划 §6 决策 #2「P2 是否值得深挖」——证据表明 **P2 大概率是偶发事件+测量污染，不值得单独排期深挖**；把预算转给 §7.4 的 #1（窗口）与 #3（ACK 防阻塞），两者对 CONC=8 尾部有直接收益。

_本报告基于 2026-08-02 的受控复现（lib/legacy 隧道 CONC=8 对比、clean go→smol 20 轮、SNMP 取证）与源码分析。结论倾向「P2 非稳态缺陷」，残余 >5s 停滞机制见 §7.2.3。_
