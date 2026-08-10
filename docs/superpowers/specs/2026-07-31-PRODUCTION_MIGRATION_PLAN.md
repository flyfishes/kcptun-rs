# 生产路径迁移计划（KcpConn / Smux 库栈 → kcptun binaries）

**日期**: 2026-07-31  
**状态**: ✅ **M0 + M1-A 已实施**（2026-08-01）；M1-B+ 待定  
**分支**: `feat-refactor_kcp_and_smux`  
**前置**: 库侧 SDD Tasks 1–7 已完成（`aa64678d` … `455ed53f`）

---

## 0.1 §6 讨论议题决议（2026-08-01 拍板）

按用户指示，§6 七项全部采用计划推荐方案，已落地：

1. **M1-A 先保留 binary SMUX 循环** — ✅ 采纳（M1-A 只换 KCP 传输，SMUX+Snappy 调度仍在 binary）。
2. **Snappy 做成 AsyncRead+AsyncWrite 包装** — ✅ 采纳（M0.4 `SnappyPipe`，`kcptun-common`）。
3. **Server demux 独立交付物** — ✅ 采纳（`kcp_rs::KcpListener` 已由 `4689cef8` 落地，M3 再接）。
4. **M0.2 cpu_block 作为默认切换硬门禁** — ✅ 采纳（M0.2 已把 offload 移进 `CryptoTransport`）。
5. **Feature flag 默认 off → M2 on** — ✅ 采纳（`--experimental-lib-kcp` + `KCPTUN_USE_LIB_KCP=1`，默认 off）。
6. **tcpraw / multi-port 排除在 M1 外** — ✅ 采纳（M1-A 固定单 UDP；tcpraw 走 legacy）。
7. **迁移期允许短暂双栈** — ✅ 采纳。

---

## 0.2 M0 实施状态（2026-08-01 完成）

| 项 | 状态 | 说明 |
|----|------|------|
| M0.1 背压 | ✅ | `KcpConn::do_poll_write` 新增 `window_bytes`（snd_wnd×MSS）硬上限 + 部分写入；`backpressure_relieved()` 唤醒条件含 write_buf 水位。单测 `kcp_conn_write_backpressure_bounds_inflight` 通过（死 peer 下 5 连跑稳定）。 |
| M0.2 CryptoTransport offload | ✅ | `encrypt_data/urgent` 改 async，`use_cpu_block` 分支真正走 `kio::cpu_block`（原注释称"binary concern"已废）；CryptoBuf 改 `Arc<Mutex<>>`。 |
| M0.3 配置黄金测试 | ✅ | `golden_mode_curves_legacy_vs_config` / `golden_manual_mode` / `golden_unknown_mode`：每种 CLI mode 与 `apply_mode` 曲线 + interval/窗口逐项等价。 |
| M0.4 Snappy 适配器 | ✅ | `kcptun-common/src/snappy_pipe.rs`：`SnappyPipe<T>` 泛型 AsyncRead+AsyncWrite，compress/passthrough 双模式；KcpConn 往返单测（压缩 + 直通）。 |

**库新增 API（M0.1/M1-A 附带）：**
- `kcp_rs::KcpConn::{read_shared, write_all_shared}` — `&self` 并发读写（内部状态已线程安全），使 reader/writer 双任务无需 Mutex。
- `kcp_rs::KcpConn::is_dead()`, `rcv_wnd()`, `read_notify`（内部）。

---

## 0.3 M1-A 实施状态（2026-08-01，原型）

**flag**: `--experimental-lib-kcp`（默认 off）+ 环境变量 `KCPTUN_USE_LIB_KCP=1`。tcpraw 自动回退 legacy。

**结构**: client 新增 `SessionHandle` trait + `LibKcpConn`（`dial_kcp_session` → `kcp_rs::KcpConn`）+ `lib_read_loop` / `lib_flush_loop` 双任务（reader 用 `read_shared` ACK，writer 用 `write_all_shared` 写 SMUX+Snappy）。accept loop / scavenger 改为 `Vec<Box<dyn SessionHandle>>`。

**验收结果（本地 echo 200KB/1MB，Rust legacy server）：**

| 场景 | flag off (legacy) | flag on (lib) |
|------|-------------------|---------------|
| null / nocomp | ✅ 3/3 | ✅ 3/3 |
| aes + FEC 10/3 + nocomp | ✅ | ⚠️ **flaky**（约 4/5 全过；1/5 尾部丢 ~20KB） |
| aes + FEC 10/3 + comp | ✅ | ⚠️ flaky（同上） |
| null + comp | ✅ | ⚠️ flaky |
| 1MB null/nocomp | ✅ | ⚠️ flaky（~1/3 丢 ~18KB） |

**已知缺陷（M1-A 原型 gap，M1-B/M2 必修）：**
> 大传输尾部偶发丢 ~15–60 段。根因：server 的 FIN 帧（小段，窗口有空位就发出）**先于**被窗口积压的 echo 尾段送达 client → client stream 收 FIN 后 remote_closed → 尾部被 SMUX 丢弃。加大两端窗口不消除 → 与窗口大小无关，属 **FIN-vs-tail 排序竞态**，且与 client ACK 时机相关（legacy client ACK 及时，几乎不触发；lib 路径 ACK 有 ~毫秒级滞后）。已排查排除：非 client teardown（无 TEARDOWN 触发）、非 client rcv_wnd（=512 正确）、非 auto-close（已移除 lib 内 auto-close，dead 检测交给 binary）。

**2026-08-01 深挖后的细化诊断（已修复）：**
- 服务端确认发送全部 echo（flush 日志 200114 bytes，`kcp.send` 无错误）；`--nc 1 --sndwnd 1024` 仍丢尾 → **排除服务端拥塞窗口**（`cwnd=32` 只是 `self.cwnd` 字段，`--nc` 后 move gate 用 `min(snd_wnd, rmt_wnd)=512`）。
- **根因（逐层打点确认）**：服务端 SMUX stream 在 echo 未排空前就 `local_closed`（实测 121480/200000）→ FIN 提前发出 → client 收到 [echo..][FIN][echo tail..]（FIN 在尾段之前）→ client stream `remote_closed` 时 pipe 读 EOF → 尾部丢失。触发点是 **SMUX `Stream::read` 在 `remote_closed && 空缓冲` 时立即返回 EOF**，而尾部仍在途（client 拥塞窗口慢启动导致 100-300ms 到达）。
- **修复**：`smux-rs::Stream::read` 增加 **EOF grace 期**（`EOF_GRACE_MS=300`）：`remote_closed && 空` 时返回 `WouldBlock`（Pending）并调度 300ms 后重醒 reader，期间尾部到达即可被读出；期满才返回 EOF。同时保留 **CryptoTransport ACK/urgent 路径 force_inline**（FEC 展开后 ACK 批不触发 cpu_block）。
- **验证**：本地 echo 全矩阵通过 —— null/nocomp、aes+FEC 10/3（±comp）、null+comp、1MB 均稳定 200000/1000000 全量（修复前约 1/5 丢尾）。
- **代价**：EOF grace 给连接关闭增加最多 300ms 延迟（正常连接也在远端 FIN 后等待 grace 才 EOF）。对 M1-A 原型可接受；M2 若用 `SmuxConn` 统一驱动可缩短或去掉。

**不实施 auto-close 的决定**: `KcpConn` 内部 flush loop 的 dead 探针在突发负载下会误报 `is_dead()`（某段 xmit≥20 即永久 dead），auto-close 会杀健康连接。已移除；binary 用 SMUX keepalive timeout + `is_dead()` 自决。

**下一步（建议）**: M1-B 用 `SmuxConn` 统一驱动（其 teardown 语义正确），或先在 M1-A 修 FIN 竞态（server 侧不发 FIN 直到 KCP 发完 / client 侧 FIN 后延迟关 stream）。

---

## 0. 目的

把 **生产** `kcptun-client` / `kcptun-server` 从「二进制内联 KCP+FEC+crypto+SMUX flush」迁到已落地的库栈，同时：

- 保持 Go kcptun / kcp-go v5 **wire 兼容**
- 不一次大爆炸删掉 ~2000 行热路径
- 明确 **当前设计里不合理 / 缺口**，先讨论再改计划

---

## 1. 现状对照

### 1.1 生产路径（现状）

```
client local KcpConn (~900 行 loops):
  UDP recv → decrypt → FEC → KCP.input → [Snappy] → smux.process_data
  smux.prepare_outbound → [Snappy] → KCP.send/flush → FEC → encrypt → UDP

server KcpServerSession + DashMap<peer>:
  shared UDP recv → demux peer → per-session feed (同上)
  flush 同上 + send_batch_to(peer)
```

另有：rate limit、dead_link/SMUX keepalive、ack_crypto_buf、cpu_block、snmp、tcpraw、QPP、pprof 等。

### 1.2 库路径（已就绪）

```
UDP → CryptoTransport → KcpConn(+FEC) → AsyncRead/Write
SmuxConn::connect/serve(transport).build() → open_stream → Stream/SmuxIo
helpers: kcp_config_from, dial_kcp_session, accept_kcp_peer (单 peer)
```

**未就绪 / stub：**

| 缺口 | 影响 |
|------|------|
| `KcpListener` 多 peer demux | 不能直接替 server accept 表 |
| 库栈 **不含 Snappy** | 必须在 SMUX↔KcpConn 之间另加一层 |
| 生产仍维护 `apply_mode` + 手写 nodelay | 与 `kcp_config_from` 双轨 |
| `CryptoTransport` 重加密无 `cpu_block` | 性能可能回退 |
| Task 5 去掉 client SMUX 对 KCP `wait_send` 背压 | 迁到 KcpConn 前，client 写 SMUX 可能打爆 KCP |
| `SmuxConn` Builder 默认 `run()` 单任务 10ms poll | 与当前「SMUX drain + KCP flush 紧耦合」语义不同 |
| `accept_kcp_peer` 不能绑在共享 listen socket 上多会话 | server 必须 demux 后再 per-peer transport |

### 1.3 目标生产栈（推荐语义）

```
                    ┌─ Snappy session codec ─┐
TCP/QPP ↔ Smux Stream ↔ (encode/decode) ↔ KcpConn ↔ CryptoTransport ↔ UDP/TcpRaw
                         ▲                      ▲
                    流压缩（会话级）         可靠传输+FEC+加密
```

**硬约束（与 Go 一致）：**

1. 加密包住 **整帧 FEC**（`header_offset=0`）  
2. Snappy 在 **KCP 用户数据** 上，不在 SMUX 帧头外、不在 UDP 明文层  
3. Server 按 **peer SocketAddr**（或 conv 策略）分会话  
4. `--key/--crypt/--mode/--nocomp/--datashard/--parityshard` 行为不变  

---

## 2. 迁移原则（计划层）

1. **先 client 后 server**（client 单会话、socket 已 connect，风险更低）  
2. **每阶段可回滚**：feature flag 或 `legacy_kcp_loop` 编译期/运行时开关  
3. **每阶段验收**：`cargo test` 相关包 → 定向 e2e（null/aes ± FEC ± nocomp）→ stress  
4. **不把 Snappy 塞进 KcpConn**（保持库「纯传输」边界）  
5. **不把 SMUX 塞进 kcp-rs**  
6. **配置单源**：生产最终只走 `KcpCliParams` / `kcp_config_from`  

---

## 3. 分阶段计划（建议）

### Phase M0 — 补齐迁移前置（库，不切生产）

**目标：** 关掉「迁了就会立刻回归」的已知洞。

| 项 | 说明 | 验收 |
|----|------|------|
| M0.1 恢复/迁移背压 | 在 `KcpConn::poll_write` 已有 wait_send 背压的前提下，保证 **SMUX→KcpConn** 路径能感知；若仍用 legacy SMUX flush 写 `kcp.send`，需显式策略 | 大窗口压测无无界 `snd_queue` 涨死 |
| M0.2 CryptoTransport offload | 重 cipher 路径接回 `kio::cpu_block` + 现有 `should_cpu_block_encrypt` | cast5/3des 等吞吐不低于基线太多（定义阈值） |
| M0.3 配置对齐表 | 文档化 CLI → `KcpCliParams` → `KcpConfig` 与旧 `apply_mode` 逐项等价 | 单测覆盖 mode 矩阵 |
| M0.4 Snappy duplex 适配器设计 | 明确 `SnappyPipe` / `SnappySession`：`AsyncRead+AsyncWrite` 包在 `KcpConn` 外 | 设计+单测 roundtrip |

**产出：** 短设计补丁（可并入本文件 §5）+ 库补丁 PR，**仍不改 main 热路径**。

---

### Phase M1 — Client 旁路原型（双路径）

**目标：** 新栈能跑通 **一条** client 会话，旧栈默认。

```
env/flag: KCPTUN_USE_LIB_KCP=1 (或 --experimental-lib-kcp)
```

**建议结构（逻辑，非最终 API）：**

```
dial socket (UDP/tcpraw 保持现有)
  → dial_kcp_session / kcp_session_with_socket
  → optional Snappy wrapper around KcpConn
  → either:
       A) 低阶 Session + 自写 read/flush 任务（更接近现状，易对照）
       B) SmuxConn::connect(snappy_kcp).build()（更干净，但驱动模型变了）
  → handle_client 仍 pipe TCP↔SmuxIo
```

**推荐先走 A 再 B：**

- **M1-A**：用库 `KcpConn` 替换「UDP↔crypto↔FEC↔KCP」；**SMUX+Snappy 调度仍在 binary**（读 `KcpConn` / 写 `KcpConn`）。  
- **M1-B**：再换成 `SmuxConn` 驱动，删 binary 内 SMUX prepare 循环。

**删除范围（M1-A）：** client `start_background_loops` 里 decrypt/FEC/KCP/encrypt 大段；**保留** snappy + smux process/prepare。  
**不删：** CLI、QPP、snmp、重连逻辑（仅改连接构造）。

**验收：**

1. 本地 null/nocomp 与 Go server 互通  
2. aes + FEC 10/3  
3. 重连 / dead_link 行为与旧路径对比  
4. 默认 flag off 时行为与 master 一致  

---

### Phase M2 — Client 默认切新栈 + 删旧 client KCP 循环

**目标：** 去掉 client 内联 KCP 状态机与 crypto/FEC 重复。

- 默认 `USE_LIB_KCP`  
- 删除 client 私有 `struct KcpConn` 中与库重复的字段/任务  
- 统一配置：`kcp_config_from`  
- 文档 / AGENTS：client 热路径改为库栈  

**验收：** `make e2e` client 矩阵 + 快速 bench 抽样。

---

### Phase M3 — Server 单 peer 试点

**目标：** 在 **不** 改全局 demux 的前提下，验证 per-peer `KcpConn`。

选项：

- **M3-a（稳）：** 共享 UDP 上 demux 后，每个 peer 一个 `CryptoTransport` 适配「只收该 peer 的队列」+ `KcpConn`（需要 **peer-scoped PacketTransport**，今天还没有）。  
- **M3-b（险）：** 每 peer `connect` 回去（NAT/对称问题，通常 **不可行**）。  

**计划默认 M3-a**，并承认：**必须新增 `PeerDatagramMux` / `DemuxTransport` 一类组件**——这是当前计划里最容易被低估的一块。

**验收：** 单 client 连 server 新路径；多 client 至少 2 会话不串包。

---

### Phase M4 — Server 生产 demux + 删 `KcpServerSession` 内联 KCP

**目标：**

```
UDP listen
  → demux by peer
  → per peer: CryptoTransport(view) → KcpConn → Snappy → SMUX
  → handle_stream 不变
```

- 实现真正的 multi-peer accept（可命名 `KcpListener` 或 server 私有 demux）  
- 迁移 rate limit / snmp / tcpraw 分支  
- 删除 server 内联 KCP/crypto/FEC  

**验收：** `make stress` + `make e2e` 全矩阵 + 多连接。

---

### Phase M5 — 可选：SmuxConn 统一驱动

在 client/server 都稳定后：

- binary 内 SMUX prepare/process 循环 → `SmuxConn::connect/serve`  
- 评估 `run()` 10ms vs 现网 flush_notify 紧耦合；必要时给 SmuxConn 加 **双任务 spawn 默认**  

**验收：** 行为与 M4 无差异；延迟/CPU 可接受。

---

### Phase M6 — 扫尾

- 删 feature flag  
- 删 deprecated `UDPSession` 若无引用  
- CHANGELOG / AGENTS / 性能基线对比  
- 可选：`cpu_block` / monomorphize `PacketTransport`  

---

## 4. 建议的里程碑与门禁

| 里程碑 | 最少门禁 |
|--------|----------|
| M0 | 库单测 + clippy 触及包 |
| M1-A | 手工/脚本 e2e：null、aes、fec、nocomp 各至少 1 |
| M2 | `make e2e`（client 相关） |
| M3 | 2 clients × 1 server 数据完整性 |
| M4 | `make stress` + `make e2e` |
| M5–M6 | 全量 gate + bench 抽样 |

**回滚策略：** M1–M2 用 flag；M3–M4 用 git revert 单 commit 串；避免「半切 demux」。

---

## 5. 当前计划中 **不合理 / 需先讨论** 的点

> 下面这些是「原 Phase 4 一刀切」和「现状库 API」之间的张力，**请优先拍板**。

### 5.1 「一次删 1200 行 binary」不现实

原设计 Phase 4 把 client+server 热路径一次换掉。  
**问题：** server demux、Snappy、SMUX 调度、tcpraw、ratelimit 全缠在一起。  
**建议：** 接受 M1→M4 渐进；Task 4 已选 safer path 是对的，生产迁移不要退回大爆炸。

### 5.2 把 Snappy 留在 binary 却假设 `SmuxConn::connect(KcpConn)` 直接可用

`SmuxConn` 吃的是 **字节流** transport。若 Snappy 在 SMUX 与 KCP 之间：

- 正确：`SmuxConn` 的 transport = `Snappy(KcpConn)`  
- 错误：`SmuxConn` 直接包 `KcpConn` 再在外面 snappy SMUX 帧（wire 会与 Go 不一致）

**必须先有 Snappy Async 适配器设计**，再谈 M1-B / M5。

### 5.3 Server 不能「`kcp_session` 一下」

`kcp_session` / `dial_kcp_session` 是 **已 connect 的点对点** 模型。  
Server 是 **一 socket 多 peer**。  
`accept_kcp_peer` 文档已写明不能多 peer 共享 listen socket。  

**缺口：Peer demux transport 是独立交付物**，不应假装 `KcpListener` stub 填一下就行。

### 5.4 去掉 SMUX 层 KCP 背压后的空窗

Task 5 删除了 `with_backpressure`。Legacy client 写 SMUX 不再看 `wait_send`。  
库 `KcpConn::poll_write` 有背压，但 **只有** 数据经 `AsyncWrite` 进 KcpConn 才生效。  
M1-A 若仍 `kcp.send` 直接打状态机，会 **再次绕过** 背压。

**迁移时写路径必须统一：** 只通过 `KcpConn.write` / 受控 API，禁止旁路 `kcp.lock().send`。

### 5.5 `SmuxConn::run` 与现网 flush 模型不匹配

现网：SMUX 有数据 → `flush_notify` → 尽快 KCP flush。  
`SmuxConn::run`：读超时 10ms 轮询，偏 standalone。  

**若 M5 直接默认 Builder.run，可能增加尾延迟。**  
计划应写清：生产路径优先 `spawn` 双任务或增强 Builder，而不是默默用 10ms `run`。

### 5.6 配置双轨

`apply_mode(&mut KCP, mode)` vs `KcpConfig`/`KcpMode`。  
未知 mode 时两边语义需锁死（Task 4 已有一版 Manual 映射）。  
**生产切换前做一次「配置黄金测试」**（每种 CLI mode 对比 nodelay/interval/resend/nc/窗口）。

### 5.7 性能：`PacketTransport`  dyn + 无 cpu_block

Box future per I/O + 重加密同步，可能让 aes/cast5 回归。  
**M0.2 应视为 client 默认切换的门禁，而不是扫尾优化。**

### 5.8 tcpraw / 多 listen / 端口范围

Client 多 port、server 多 listen、tcpraw 与 UDP 分支，都比「单 UDP dial」复杂。  
**M1 应固定：单 remote UDP**；tcpraw 与 multi-port 放到 M2 之后单独列。

### 5.9 重连与 dead_session

Client 在 server 重启时的重连逻辑依赖现有 session/smux 生命周期。  
换库 `KcpConn` 后 **close/dead_link/通知链** 要对表（已有 bug 文档可挂过来）。  
计划里应有 **显式回归用例**，不要只靠 e2e 碰巧覆盖。

### 5.10 「库完成 = 生产完成」的错觉

Tasks 1–7 完成的是 **可组合库**。  
生产迁移的主成本在 **Snappy 适配 + Server demux + 行为/性能门禁**，不是再写一个 KcpConn。

---

## 6. 讨论议题（请你拍板）

请按优先级表态（同意 / 反对 / 改方案）：

1. **是否接受 M1-A 先保留 binary SMUX 循环**，而不是一步 `SmuxConn::connect(KcpConn)`？  
2. **Snappy** 是否同意做成 `AsyncRead+AsyncWrite` 包装 `KcpConn`（唯一推荐）？  
3. **Server demux** 是否单独立项为 `PeerDatagramMux`（或等价），而不是硬塞进现在的 `KcpListener` stub？  
4. **M0.2 cpu_block** 是否作为 client 默认切换的硬门禁？  
5. **Feature flag** 名称与默认（建议默认 off → M2 再 default on）？  
6. **tcpraw / multi-port** 是否明确排除在 M1 之外？  
7. **是否允许** 在迁移期短暂双栈（二进制体积/复杂度换安全）？  

---

## 7. 非目标（本生产迁移仍不做）

- 改变 Go wire  
- 把 crypto 搬进 `kcp-rs`  
- 重写 QPP / pprof / snmp 语义  
- 一次 PR 删光 client+server 旧循环  

---

## 8. 成功标准（最终）

- [ ] client/server 生产热路径不再内联 KCP 状态机 / FEC / 批量加密  
- [ ] `make e2e` + `make stress` 绿  
- [ ] 抽样 bench：null/aes ± comp 相对迁移前无不可接受回退（阈值你定，例如 p50 不低于 90%）  
- [ ] AGENTS 与代码一致：无「legacy loop」双描述  
- [ ] 配置只从 `kcp_config_from`（或其后继）流出  

---

## 9. 下一步

1. 你审阅本文，标出仍不合理处  
2. 我们改计划（尤其 §5 / §6）  
3. 冻结 M0/M1 范围后再开实施 SDD  

**本文仅计划，不包含实施。**
