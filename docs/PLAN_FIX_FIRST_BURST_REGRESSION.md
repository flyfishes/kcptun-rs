# 实施计划：修复首次连接建立期回归（P0）

**状态:** 实施中（根因已定位并修复）
**日期:** 2026-08-04
**来源:** `docs/PERF_GAP_ANALYSIS_00e5e3df_vs_master.md`（2026-08-03 实测）

---

## 0. 背景

当前 master 比 00e5e3df **慢 2–3 倍**（bench_rust_vs_go.py：null/no-comp 14.6 vs 43.4 MB/s）。
根因：**新鲜隧道的首次并发突发**建立期延迟（当前 280–550ms vs 旧 83ms）。稳态持平（65–75ms）。

## 0.5 根因定位（2026-08-04 实测，毫秒级插桩）

**主因：服务端 KCP ACK 批处理延迟。**
- `input_no_flush` 的 `pending_flush` 只在 `flush_segments!=0`（UNA 推进/fastack）或 `acklist>=45` 或 `acknodelay` 时置位。
- 客户端 `cwnd = min(snd_wnd, rmt_wnd)`，`rmt_wnd` 初始 32（KCP_DEFAULT_WND），随 ACK 增长。
- 客户端发 ~32 段后 cwnd 卡死，等服务端 ACK。服务端收 32 段但 `acklist=32<45` 且 `acknodelay=false` → **ACK 不发** → 客户端等 ~200ms（RTO 重传循环）。
- 旧版 flush loop 每周期 `kcp.flush()`（flush_acks=true）→ ACK 及时，无此问题。

**修复（已实现）**：
1. **`flush_input_batch` 每 burst 有 ACK/pending 即 flush**（`pending_flush_flag()!=0 || acklist_len()>0`），ACK 及时发出，保持单生产者（write path / flush loop 仍 flush_acks=false）。
2. **客户端 accept 循环 try_accept 排空**（`kio::TcpListener::try_accept` 用 `poll_accept`+noop waker 实现），避免连接突发串行在 reactor wakeup 后。
3. **`read_shared` 合并 read_buf 条目**（一次填满 64KB），减少逐段 lock/pop/process_data 开销。

**效果**：首突发 287ms → **~107ms**（旧版 ~106ms）；`make gate` 全绿；stress 8/8；e2e 48/48 通过。

## 0.6 P1 进展（2026-08-04）

**T7 已完成**（`8d410d0d`）：`kio::Notify::has_pending()` + write_loop 快路径 —— 有 pending 通知时直接 await，空闲才建定时器，减少定时器 wheel churn。
首突发 per-conn ~115-130ms → ~107ms（接近旧版 ~99-108ms）。

**T6（合并 session read_loop 到 KcpConn input loop）**：剩余稳态差距（per-conn ~75-100ms vs 旧 ~51-61ms）主要来自新架构多跳任务调度（pprof `park_condvar` 9.5% vs 1.5%）。
需 kcp-rs↔smux-rs 解耦（trait 注入数据消费者），较大重构，列为后续。
已定位两个 ~100ms 级建立期卡顿：
1. **客户端** accept 循环：第 2 个 `listener.accept()` 对已排队连接阻塞 ~90ms（kcptun 特有，裸 tokio 不复现）。
2. **服务端** KcpListener reader：各 peer session 建立间隔 ~100–200ms。

## 1. 目标与成功标准

**目标：** 消除首次建立期延迟，使同配置 bench 回归到旧版水平，同时保持 wire 兼容与稳态性能。

| 指标 | 当前 | 目标 | 验收命令 |
|------|------|------|----------|
| conc_loop 首突发 wall（current×current） | 280–550ms | **<100ms** | `bash /tmp/kcptun-ab/conc_loop.sh current 5 4` |
| conc_loop 首突发 wall（old-client×current-server） | 262–550ms | **<100ms** | 同上交叉 |
| bench null/no-comp（同配置） | 14.6 MB/s | **>35 MB/s** | `bench_rust_vs_go.py --quick --conn 4 --size 1048576 --runs 3 --rust-only` |
| 单连接首连接（conn=1） | 213–268ms | **<50ms** | `conc_loop.sh current 3 1` |
| 稳态（iter 2+） | 65–75ms | 不回退 | 同上 iter 2+ |
| `make gate` | — | 全绿 | `make gate` |
| `make stress` / `make e2e` | — | 全绿 | 见 Makefile |
| wire 兼容 | — | 不变 | e2e + 无协议字节改动 |

## 2. 任务分解

### T1 — 建立期基准工具（先行，作为回归门禁）

**内容**：把 `conc_loop.sh` 沉淀为仓库内基准脚本 `bench/first_burst_bench.sh`，支持 current/old 参数、输出首突发 vs 稳态。

**为什么先行**：后续每个改动都要用它验收；没有它，"建立期回归"会再次被 bulk 基准掩盖。

**验收**：脚本可在 current 上复现首突发 280ms / 稳态 70ms；`--runs` 取中位数。

---

### T2 — 客户端 accept 循环批量处理（P0.1）

**现象**：`kcptun-client/src/main.rs` accept 循环（~line 593–698），第 2 次 `kio::timeout(500ms, listener.accept())` 对已排队连接阻塞 ~90ms。裸 tokio repro 不复现 → kcptun 特有。

**假设（实现时验证）**：第 1 个连接的处理（`open_stream` → KCP SYN 发送 → session 活动）与 accept future 的 reactor 就绪事件存在交互；或 accept 循环逐连接 await 的模式引入调度间隙。

**方案（择一，以实测为准）**：
- **A**：accept 后先 `try_accept` 排空待决连接，批量路由 + `open_stream`，再统一 spawn handler。
- **B**：accept 循环只负责 accept 并投递到 mpsc；连接处理（open_stream + spawn）由独立 worker 消费，accept 循环保持热。

**文件**：`kcptun-client/src/main.rs`（`spawn_session_stream_loop` / accept 循环）。

**风险**：round-robin 语义保持；`conns.lock()` 不跨 await；不引入新任务风暴。

**验收**：`conc_loop current 5 4` 首突发 <100ms；`conn=1` 首连接 <50ms。

---

### T3 — 服务端 KcpListener peer 建立移出 reader 热循环（P0.2）

**现象**：`kcp-rs/src/conn.rs` `spawn_listener_reader`（~line 1851）单任务 `recv_from`→按 peer 建 `KcpConn`（含 `.build().await`）→push pending。peer 之间出现 ~100–200ms 间隔。

**假设**：reader 内联的 peer 建立（build/accept 链）阻塞了对后续 peer 首包的 recv；或 reader 的 `timeout(100ms, recv_from)` 在建立间隙触发。

**方案**：
- reader 只负责 demux + `PeerQueue.push`；peer session 的 `KcpConn::build()` + `KcptunListener::accept`/`KcptunSession::server` 链交给独立 accept 任务，不阻塞 reader。
- 检查并去除 reader 内联的 `.build().await` 慢路径。

**文件**：`kcp-rs/src/conn.rs`、`kcptun-common/src/kcptun_listener.rs`。

**风险**：多 peer 并发建立时的资源上限；accept 顺序保证（KCP conv 采纳不受影响）。

**验收**：`old-client × current-server` 首突发 <100ms。

---

### T4 — 建立期定时器策略（P0.3）

**现象**：`spawn_input_loop` 每 recv 包 `timeout(100ms, recv_vec)`；`spawn_listener_reader` 每 recv `timeout(100ms, recv_from)`。建立期间隙触发后产生 ~100ms 级调度延迟。

**方案**：
- 对已建立 peer 的 recv 去掉 100ms 包装（仅关闭检测用独立短 tick 任务）；或把超时降到 10ms。
- 保持 `close()` 能打断 recv 的语义（`Notify::notify_waiters` 或 fd 关闭）。

**文件**：`kcp-rs/src/conn.rs`（`spawn_input_loop`、`spawn_listener_reader`）。

**风险**：关闭检测回归（silent peer 挂死 socket）；`kio::timeout` 语义。

**验收**：首连接 <50ms；silent peer 关闭仍生效（现有 conn.rs 测试覆盖）。

---

### T5 — 验证与文档（收尾）

**内容**：
1. `make gate`（fmt/test/clippy）全绿。
2. `make stress`（10–100 连接数据完整性）。
3. `make e2e`（Go interop，smux v1/v2 + 全 crypt）。
4. 重新跑 A/B + conc_loop + pprof，确认热点迁移。
5. 更新 `docs/PERF_GAP_ANALYSIS_00e5e3df_vs_master.md` 状态、`bench/profiles/HOTSPOTS.md`、`bench/PERF_REGRESSION_ANALYSIS.md`。
6. 更新 `docs/PERF_OPTIMIZATION_PLAN.md` 的剩余项（R6 等如有触及）。

**验收**：§1 全部指标达标；文档状态更新。

---

## 2.1 P1 — 稳态调度与任务合并（P0 之后，可选但推荐）

> 来源：pprof 稳态对比 `park_condvar` 当前 **9.47%** vs 旧 1.51%（6×），
> 且当前版多出 `Timespec::now` / `notify_parked_local` 帧——新 session 栈任务/定时器开销更大。
> 不影响首突发（P0 已修），但影响稳态 CPU 与峰值吞吐。

### T6 — 合并 session read_loop 到 KcpConn input loop（减少任务跳数）

**现状**：读路径 `KcpConn input loop → read_buf → session read_loop → snappy → smux.process_data`，
2 次任务切换 + 1 次 read_buf 队列交接（kcptun_session.rs `read_loop` line 193；conn.rs `spawn_input_loop`）。
旧版是 UDP recv 任务内联完成 decrypt→FEC→KCP→SMUX，无中间跳。

**方案（择一）**：
- **A**：KcpConn 暴露可注入的"用户数据消费者"回调（trait / 闭包），`feed_inbound_batch` 产出 user data 后**同任务**喂给 SMUX（snappy 解码 + `process_data`），省去 read_buf + read_loop 跳。
- **B**：保留分层，但 input loop 直接 `notify` 并传 `Bytes` 引用（去 read_buf 拷贝），read_loop 只做 snappy+SMUX。

**注意**：方案 A 需在 **kcp-rs 与 smux-rs 之间解耦**（kcp-rs 不依赖 smux-rs）——用 trait 注入，避免 crate 反向依赖；1960b81e 的 burst 批处理语义必须保留。

**文件**：`kcptun-common/src/kcptun_session.rs`、`kcp-rs/src/conn.rs`、`kcp-rs/src/lib.rs`。

**预期**：稳态 `park_condvar` 9.5%→~2%；稳态吞吐小幅提升。

**风险**：层依赖反转；read 顺序/backpressure 语义；smol runtime 也要同步。

**验收**：稳态 iter2+ 不回退；pprof `park_condvar` 下降；`make gate` + stress + e2e（双 runtime）全绿。

---

### T7 — 稳态调度 churn 优化（空闲定时器降频）

**现状**：每 session 的 `write_loop` 固定 `timeout(2ms, notify)` 空闲轮询（kcptun_session.rs line 246），
+ input loop 100ms + flush deadline。4 session ≈ 500 次/s 定时器唤醒。pprof `park_condvar` 6× 的来源。

**方案**：
- `write_loop` 空闲时用较长 poll（如 50–100ms，对齐旧 `MAX_IDLE_UPDATE_MS`），**有 pending 数据时**才 2ms；用 notify 保证有数据立即醒。
- 与 T4 协调（T4 管建立期，T7 管稳态），避免重复改动同一处。

**文件**：`kcptun-common/src/kcptun_session.rs`（write_loop）。

**预期**：稳态 CPU 下降；`park_condvar` → ~2%。

**风险**：keepalive/健康检查时序（health check 每 50 循环）；SMUX FIN/stream 清理延迟。

**验收**：pprof `park_condvar` 降；稳态吞吐不降；keepalive 超时仍生效（e2e）。

> **P1.3（原分析 §5 P1.2）已落地，无需新任务**：共享 `encrypt_batch` 已在 `kcrypt-rs/src/wire.rs:462`，
> client/server 均经 `kcptun-common::CryptoTransport` 共用同一路径（`kcp_transport.rs`）。T5 收尾时确认对称性即可。

---

## 3. 实施顺序与依赖

```
P0:  T1（基准工具）→ T2（客户端 accept）→ T3（服务端 reader）
                        ↘ T4（定时器）可并行
P1:  T6（read_loop 合并）→ T7（空闲定时器降频）   ← P0 验收后再动
收尾: T5（验证与文档）
```

- **P0 先行**（修 2–3× 首突发差距）；**P1 在 P0 验收后**再动（改稳态调度，避免与 P0 归因混淆）。
- T2/T3/T4 相互独立，每步用 T1 验收。
- **一次一类优化**：每 PR 只动一个假设，用 T1 量前后，避免多个改动混淆归因。

## 4. 硬约束

- **wire 兼容**：不改变 KCP/SMUX/Snappy/加密字节。
- **不破坏 1960b81e 已修项**：burst 批处理、64KB notify permit 保留、listener 缓冲池。
- **kio::Notify 单 waiter 语义**：不引入同一 Notify 双 waiter。
- **`CLAUDE.md` 手术式纪律**：只改目标行；不顺手重构。

## 5. 遗留疑问（实现阶段需实测）

1. 客户端 accept 阻塞的确切机制（tokio-console 或插桩 accept future 的 poll/wake）。
2. 服务端 peer 间隔是 reader 阻塞还是客户端迟发（需 reader 内时间戳 vs 客户端 SYN 发送时间戳对齐）。

## 6. 参考

- 实测分析：`docs/PERF_GAP_ANALYSIS_00e5e3df_vs_master.md`
- 复现脚本：`/tmp/kcptun-ab/`（`conc_loop.sh`、`conc.sh`、`diag.sh`）
- 旧版 worktree：`/tmp/kcptun-rs-old`（00e5e3df）
