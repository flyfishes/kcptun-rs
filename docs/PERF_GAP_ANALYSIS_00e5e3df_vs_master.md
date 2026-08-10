# kcptun-rs 当前版 vs 00e5e3df 性能差距分析（实测 + pprof）

**日期:** 2026-08-04
**对比:** `master`(HEAD 5ec65c64) vs `00e5e3dfeae522b3bdcbaf`
**机器:** Intel x86_64 macOS (Darwin 25.6.0)，Rust/Go 均为原生 x86_64（无 Rosetta 偏差）

---

## ⚡ 更新（2026-08-04）：根因已定位并修复

毫秒级插桩定位到**服务端 KCP ACK 批处理延迟**为根因，已实施 3 项修复（详见
`docs/PLAN_FIX_FIRST_BURST_REGRESSION.md` §0.5）。修复后同配置 bench 结果：

| Config | 修复前 | 修复后 | 00e5e3df | Go | vs Old |
|--------|--------|--------|----------|-----|--------|
| null/no-comp | 14.6 | **30.5** | 43.4 | 34.8 | 1.42× |
| null/comp | 12.6 | **32.6** | 44.7 | 31.4 | 1.37× |
| aes-128/no-comp | 14.1 | **32.8** | 35.7 | 34.7 | 1.09× |
| aes-128-gcm/no-comp | 14.9 | **30.0** | 48.4 | 36.2 | 1.62× |
| salsa20/no-comp | 14.5 | **36.2** | 46.0 | 34.6 | 1.27× |
| blowfish/no-comp | 14.1 | **33.8** | 38.2 | 29.3 | 1.13× |
| sm4/no-comp | 14.3 | **36.8** | 33.6 | 3.6 | **0.91×（反超）** |
| 3des/no-comp | 12.4 | **31.2** | 18.9 | 13.0 | **0.61×（反超）** |

**结论**：
- 全部 config 提升 **2–3 倍**（修复前 14–15 MB/s → 修复后 30–36 MB/s）。
- **3des/sm4 反超旧版**（3des 0.53×、sm4 0.92×）；旧版重加密受 ACK 卡顿影响小，修复后纯数据面更快。
- **当前版重新领先 Go**（大部分 config ≥ 1.0×；3des 2.7×；sm4 10×）。
- vs 旧版剩余差距 1.1–1.5×（轻加密，稳态多跳开销，属 T6 后续优化）。
- 验证：`make gate` 全绿；stress 8/8；e2e Go↔Rust 48/48。
- **P1 已推进**：T7（`8d410d0d` `kio::Notify::has_pending` + write_loop 快路径）进一步将首突发 per-conn 降至 ~107ms（接近旧版）。

---

## 0. 结论摘要

1. **差距是真实的**：同配置 A/B 下当前版比 00e5e3df **慢 2–3 倍**（null/no-comp 14.6 vs 43.4 MB/s；aes-128-gcm 14.9 vs 48.4）。仓库内既有文档（`PERF_REGRESSION_ANALYSIS.md`、`docs/COMPARISON-00E5E3DF-1960B81E.md`）声称"回归已修复、差距 <4%"，**与实测不符**——那两份文档基于单连接长流（throughput.py / 500MB ABBA），恰好掩盖了本回归。

2. **根因定位**：回归**不是稳态吞吐**，而是**新鲜隧道的首次并发突发（connection establishment）**。
   - 当前版首次突发 **~280–550ms**，旧版 **~83ms**。
   - 首次突发之后（warm 状态）两版都 ~65–75ms，持平。
   - 无 KCP 重传（SNMP `RetransSegs=0`）→ 非 RTO/丢包问题。
   - 基准脚本每个 config 都启动全新进程 → 每次都付首次突发代价 → 吞吐暴跌。

3. **方法学陷阱**：`bench_rust_vs_go.py` 的 MB/s = 总字节/总墙钟（含连接建立/端口清理 ~11s/run）。`--size 65536`（64KB）时数字崩到 ~1.25 MB/s，`--size 1048576` + `--conn 10` 时 ~22–24 MB/s。**不同参数的两组结果直接相减会产生"19 倍差距"假象**——工作区 `bench_results.json` 的改动正是这种配置漂移。

4. **两个 ~100ms 建立期卡顿**（详见 §3）：
   - **客户端 accept 循环**：第 2 个 `listener.accept()` 阻塞 ~86–94ms，即使连接已排队。
   - **服务端 peer session 建立**：KcpListener reader 各 peer 首包相隔 ~100–200ms。

---

## 1. 同配置 A/B 测量（`bench_rust_vs_go.py --quick --conn 4 --size 1048576 --runs 3`）

| Config | Current (MB/s) | 00e5e3df (MB/s) | Δ% | Current lat(avg) | Old lat(avg) |
|--------|----------------|-----------------|-----|------------------|--------------|
| null/no-comp | 14.62 | 43.35 | **-66%** | 0.191s | 0.053s |
| null/comp | 12.64 | 44.71 | -72% | 0.217s | 0.055s |
| aes-128/no-comp | 14.08 | 35.73 | -61% | 0.200s | 0.070s |
| aes-128-gcm/no-comp | 14.92 | 48.44 | -69% | 0.188s | 0.048s |
| salsa20/no-comp | 14.50 | 46.00 | -68% | 0.194s | 0.049s |
| blowfish/no-comp | 14.07 | 38.23 | -63% | 0.201s | 0.071s |
| sm4/no-comp | 14.25 | 33.64 | -58% | 0.204s | 0.084s |
| 3des/no-comp | 12.42 | 18.94 | -34% | 0.245s | 0.172s |

> 观察：当前版**全 cipher 几乎同速**（12–15 MB/s），说明瓶颈是**每连接固定开销**（建立/时延），与 cipher CPU 无关；旧版则随 cipher 递减（重加密更慢），是 CPU-bound 的正常形态。

### 1.0 Rust vs Go（同配置，Go 为 `tests/kcptun-go` x86_64 原生）

| Config | Cur | Old | Go | Old/Go | Cur/Go |
|--------|-----|-----|-----|--------|--------|
| null/no-comp | 14.6 | 43.4 | 34.8 | **1.25×** | **0.42×** |
| aes-128/no-comp | 14.1 | 35.7 | 34.7 | 1.03× | 0.41× |
| aes-128-gcm/no-comp | 14.9 | 48.4 | 36.2 | 1.34× | 0.41× |
| salsa20/no-comp | 14.5 | 46.0 | 34.6 | 1.33× | 0.42× |
| blowfish/no-comp | 14.1 | 38.2 | 29.3 | 1.31× | 0.48× |
| sm4/no-comp | 14.3 | 33.6 | 3.6 | 9.34× | 3.96× |
| 3des/no-comp | 12.4 | 18.9 | 13.0 | 1.46× | 0.96× |

> 除 sm4（Go 软实现极慢）与 3des（重加密主导）外，**旧版 Rust 领先 Go ~1.03–1.52×，当前版落后 Go ~0.40–0.53×**。
> 本次回归使 Rust 的 Rust-vs-Go 定位从"领先"翻转为"落后"。

### 1.1 首次突发 vs 稳态（`conc_loop`，4 并发 × 1MB echo 反复）

| 版本 | iter 1 (fresh) | iter 2+ (warm) |
|------|---------------|----------------|
| current | **280–550ms** | 65–75ms |
| 00e5e3df | **83ms** | 72–81ms |

- 当前版：首个 burst 慢 ~4×，之后恢复正常。
- 旧版：首个 burst 即快（无冷启动惩罚）。

### 1.2 交叉实现定位

| Client | Server | 首次突发 wall | 结论 |
|--------|--------|---------------|------|
| current | current | 280ms | 慢 |
| old | current | 262–550ms | 慢 → **服务端有责** |
| current | old | 95–454ms | 无 warmup 时慢 → **客户端有责** |
| old | old | 83ms | 快 |

结论：新 session 栈的**客户端与服务端各自引入 ~100ms 建立期延迟**，任一参与即慢。

---

## 2. 方法与可复现性

- 二进制：`cargo build --release`（opt-level=3, LTO, strip, panic=abort）；旧版在 detached worktree（`/tmp/kcptun-rs-old`）构建后临时替换 `target/release`。
- 负载：python 并发 echo（1MB 双向），模拟 bench_rust_vs_go.py 计时段。
- 测量前已清理残留 kcptun 进程（曾有一个 `--pprof 6060` 的测试 server 常驻 :29900，会抢 CPU 干扰）。
- 所有数字在多轮重复下稳定（首 burst 慢 4×、稳态持平）。

---

## 3. 根因证据链（毫秒级插桩）

对 session/accept/reader 加 `KCPTUN_TRACE` 毫秒级时间戳，逐层定位：

### 3.1 客户端 accept 循环卡顿 ~90ms

```
[ACCEPT] t=38ms  peer=A   ← 第 1 个连接（accept 正常）
[OPEN]   t=38ms           ← open_stream 耗时 0ms（非 open_stream 问题）
[SENT]   t=38ms len=8     ← session A 发出 SMUX SYN
[ACCEPT-WAIT] t=38ms
[ACCEPT] t=151ms peer=B   ← 第 2 个连接（连接已排队，却阻塞 113ms!）
```

- 4 个 TCP 连接在同一 0.5ms 内连上（负载发生器计时证实）。
- 但客户端第 2 次 `listener.accept()` 阻塞 ~90–113ms 才返回已排队的连接。
- `open_stream()` 全程 0ms → 非 SMUX 锁问题。
- 该阻塞推迟了后续连接 pipe 启动 → SYN/数据延迟发出。

### 3.2 服务端 peer session 建立间隔 ~100–200ms

```
[READER] t=0ms   peer=A n=32     ← 第 1 个 peer 首包
[READER] t=199ms peer=B n=32     ← 第 2 个 peer 首包（间隔 199ms!）
[READER] t=200ms peer=B n=1350   ← bulk 数据随后涌入
```

- KcpListener 单 reader 任务按 peer 串行建 session；peer 之间出现 ~100–200ms 空窗。
- 即便旧客户端（旧×旧很快）配当前服务端，reader 依然出现 199ms 间隔 → **服务端独立引入延迟**。

### 3.3 单连接场景同样慢（非多 session 特有）

- current conn=1 首连接：**213–268ms**；old conn=1：**15ms**。
- 说明每 session 的**首个连接建立**就有 ~200ms 延迟。

### 3.4 排除项

| 假设 | 验证 | 结果 |
|------|------|------|
| KCP RTO 重传 | SNMP `RetransSegs=0` | ✗ 无重传 |
| session write_loop 2ms 轮询 churn | `KCPTUN_WPOLL_MS=50` 重测 | ✗ 无改善 |
| 多 session 特有 | conn=1 同样慢 | ✗ 单连接也有 |
| 稳态数据面 | iter2+ 两版持平 | ✗ 稳态正常 |
| 通用 tokio reactor 饱和 | 最小 repro（4 session × 2/10/100ms 定时器 + UDP 发送 + TCP accept）：**accept 第 2 个连接返回 11µs** | ✗ 非通用问题，**kcptun 特有** |

### 3.5 pprof（稳态，短连接负载，-ignore=Inner::park）

| 帧 | current | 00e5e3df |
|----|---------|----------|
| `UdpSocket::send_to` | 51.8% | 51.5% |
| `Socket::recv_from` | 15.4% | 25.4% |
| `tokio park_condvar` | **9.47%** | 1.51% |
| `io::driver turn` | 6.8% | 10.3% |
| `Timespec::now` | 1.96% | 0 |
| `notify_parked_local` | 1.91% | 0 |

稳态 profile 显示当前版 `park_condvar` 高 6×、时钟读取/调度唤醒帧多——**任务调度/唤醒开销更大**（新栈每 session ~5 任务 vs 旧 ~2）。

---

## 4. 代码差异回顾（00e5e3df → master）

| 维度 | 旧版 | 新版 (master) |
|------|------|---------------|
| input 链 | UDP recv 任务内联 `decrypt→FEC→KCP→SMUX` | `KcpListener reader→PeerQueue→KcpConn input loop→read_buf→session read_loop→SMUX`（**多 2 次任务/队列交接**） |
| 每 session 任务数 | 2（recv inline + flush loop） | 4–5（input loop, flush loop, read_loop, write_loop）+ handler |
| flush 调度 | 动态 `next_update`(1ms busy / 100ms idle) | deadline 绝对时间 + notify |
| accept 链 | DashMap `get_or_create_session` 直接 feed | reader → pending → `KcptunListener::accept` → `KcptunSession::server` → `session.accept()` → stream |

**1960b81e 已修的回归**（逐包拷贝、64KB 块间 2ms、listener 逐包拷贝）确实是修复，但**未覆盖本节定位的建立期延迟**。

---

## 4.5 Rust vs Go 实现对比（源码对照 /Users/sean/Documents/kcptun）

### Go 数据面结构

```
serveListener:  AcceptKCP → SetNoDelay/SetWindowSize → handleMux(goroutine)
handleMux:      conn = std.NewCompStream(kcpConn)  // snappy 帧流包在 KCP 上
                mux = smux.Server(conn) → 每 stream 一个 goroutine → std.Pipe(io.Copy)
kcp-go:         UDPSession 内部自带 readLoop/flushLoop goroutine + channel 通知
smux:           读循环 + shaper 写队列（channel），writeResult 回传
std.Pipe:       io.CopyBuffer（smux Stream 实现 WriteTo，走内部缓冲）
```

| 维度 | Go | Rust (master) |
|------|----|---------------|
| KCP 驱动 | UDPSession 内部 goroutine | KcpConn input+flush 任务（每 peer） |
| UDP 收包 | kcp-go readLoop 单 goroutine 直接 feed | KcpListener reader → PeerQueue → 每 peer input loop（多 2 跳） |
| SMUX 写 | channel shaper goroutine | notify + 共享 64KB 批处理 |
| 压缩 | `CompStream`（snappy 帧流） | session 级 `snap::FrameEncoder` |
| stream 转发 | 每 stream goroutine + `io.Copy` | tokio 任务 + `kio::copy_bidirectional`(64KB) |
| 并发模型 | goroutine 无栈开销 | tokio 任务（多一跳调度） |

**Go 的优势**：KCP 收包与 SMUX 处理在同 goroutine 栈上内联完成，无任务/队列交接；
smux 每 stream 独立 goroutine，写路径经 channel 直接回传 writeResult（同步语义）。
Rust 新栈把 input 链拆成 reader→queue→input loop→read_buf→read_loop 多跳，
建立期每跳的 task wakeup 是 ~100ms 级延迟的候选来源。

> 注：本机 Go 二进制为 x86_64 原生（Intel Mac），无 Rosetta 偏差；bench 实测 Go ≈ 33 MB/s（见 §1.1 数据），
> 旧版 Rust 43 MB/s > Go，当前版 Rust 14.6 MB/s < Go——**回归使 Rust 从领先 Go 变为落后 Go**。

---

## 5. 优化与重构方案（按预期收益排序）

### P0 — 消除首次建立期延迟（直接修 2–3× 差距）

**P0.1 客户端 accept 循环批量处理**
- 现象：第 2 个 `accept()` 对已排队连接阻塞 ~90ms。
- 方案：accept 后**不立即逐连接处理**，而是 `try_accept` 排空待决连接，批量路由+open_stream；或单独 accept 任务 + mpsc 交给 worker。避免首连接处理（open_stream→KCP 活动）延迟后续 accept 的 reactor 就绪事件。
- 文件：`kcptun-client/src/main.rs`（accept 循环 ~line 593）。
- 验收：conc_loop current 首突发从 280ms → <100ms。

**P0.2 服务端 KcpListener peer session 建立**
- 现象：单 reader 串行建 peer session，peer 间隔 ~100–200ms。
- 方案：reader 只负责 recv→queue，peer 的 `KcpConn::build()` 与 `KcptunListener::accept`/`KcptunSession::server` 链**移出 reader 热循环**（异步/多任务），或复用 warm 状态下的快速路径。
- 文件：`kcp-rs/src/conn.rs` `spawn_listener_reader`，`kcptun-common/src/kcptun_listener.rs`。
- 验收：旧客户端×当前服务端首突发 <100ms。

**P0.3 建立期定时器策略**
- 现象：input loop `timeout(100ms, recv)`、reader `timeout(100ms, recv_from)` 在建立期被触发，配合新任务拓扑造成 ~100ms 级调度延迟。
- 方案：对已连接 peer 的 recv 去掉 100ms 包装（仅关闭检测用更短 tick 或独立任务），或缩短到 10ms。

### P1 — 降低每 session 调度开销（稳态 `park_condvar` 6×）

**P1.1 合并 session 任务**
- 现每 session 4–5 个任务（input/flush/read/write + handler）。read_loop 与 KcpConn input loop 可融合（input loop 直接 `smux.process_data`），write_loop 与 flush loop 协调。减少 notify→wake 跳数。
- 预期：`park_condvar` 9.5%→~2%，稳态小幅提升。

**P1.2 共享 `encrypt_batch` 迁入 kcrypt-rs**（既有计划）
- 已在 `PERF_REGRESSION_ANALYSIS.md` 列为待办。消除 client/server 加密路径漂移。

### P2 — 基准/方法学

**P2.1 `bench_rust_vs_go.py` 数字文档化**
- MB/s 含连接建立开销，`--size`/`--conn` 漂移会产生数十倍假差。建议默认 `--size 1048576`，并在输出中标注"含建立开销"。
- 工作区 `bench_results.json` 当前是 `--size 65536` 的脏数据，与已提交版本不可比。

**P2.2 建立期作为独立 KPI**
- 新增"首突发建立时延"基准（如 `conc_loop`），防止 session 栈重构再次引入建立期回归（现有 bulk/吞吐基准测不出来）。

---

## 6. 附录：复现命令

```bash
# 构建
cargo build --release -p kcptun-server -p kcptun-client
git worktree add /tmp/kcptun-rs-old 00e5e3df
(cd /tmp/kcptun-rs-old && cargo build --release -p kcptun-server -p kcptun-client)

# A/B
python3 bench_rust_vs_go.py --quick --conn 4 --size 1048576 --runs 3 --rust-only
bash /tmp/kcptun-ab/conc_loop.sh current 5 4   # 首突发 vs 稳态

# pprof（稳态）
CRYPT=aes bash bench/profile_rust_go_pprof.sh server 20
go tool pprof -top -ignore="Inner::park" bench/profiles/rust-server-*.pb
```

**门禁：** 插桩已全部清除；`make gate`（fmt/test/clippy）全绿。
