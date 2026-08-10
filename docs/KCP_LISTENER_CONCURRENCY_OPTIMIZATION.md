# KcpListener 高并发优化方案（审核修订版）

> 版本：v3，2026-08-04。审核对象以当前工作区代码为准：
> `kcp-rs/src/listener.rs`、`transport.rs`、`conn.rs`，以及直接依赖的
> `kio-rs/src/net/*`、`kio-rs/src/task/*`、
> `kcptun-common/src/kcp_transport.rs`、`kcptun-server/src/app.rs`。
>
> 目标：保持 Go kcptun / kcp-go v5 wire compatibility，同时让监听层能作为网络游戏服务器
> 和网络隧道的数据面基础。本文是实施方案，不把尚未实测的性能估计当作结论。

---

## 0. 审核结论

原 v2 文档的主方向部分正确，但不能按原优先级直接实施。必须修正以下问题：

| 原建议 | 审核结论 | 修正 |
|---|---|---|
| Linux reader 用 `recvmmsg` | 方向正确，但当前 `try_recv_batch_from` 并非稳态零分配 | 先改成“调用者持有 payload 槽位 + 复用地址数组”的 API，再接入 listener |
| burst 按 peer `sort_unstable_by_key` | 不建议 | unstable sort 不保证同 peer 到达顺序；游戏负载常为 `D≈B`，排序未必减少工作 |
| `PeerQueue::push_batch` | 隧道/单热点 peer 有价值，游戏多 peer burst 收益不确定 | 先做一次 sessions 锁/每 burst；根据 `D/B` 和 profile 决定是否稳定分组 |
| input loop 批量出队 | 合理 | 必须连同 `CryptoTransport`、坏包压缩、批量上限和公平调度一起实现 |
| reader 为每个新 peer spawn build task | 不合理 | `build()` 在该路径基本是同步分配；无界 spawn 会把连接风暴变成任务风暴 |
| `sessions` 改 `RwLock` | 不建议先做 | 单 reader 没有读读并发；先修执行器归属、锁粒度和资源上限 |
| 100 ms tick 改 500 ms | 不解决根因 | 应增加可取消 receive/close wakeup，去掉海量连接的周期 timer churn |
| 生产 shard 内锁无竞争 | 与当前代码不符 | listener 在主运行时构建，reader/input/flush 并未保证运行在 shard current-thread 上 |
| 固定写出 `+20–60%`、`1.5–3x` | 证据不足 | 改为可证伪假设；只有复现实测后才写入收益数字 |

此外，v2 遗漏了高并发库最重要的四项：

1. `sessions`、`pending` 和 `PeerQueue::packets` 当前均无容量上限；
2. 任意来源的首个 UDP datagram 都会创建 `KcpConn`，无 admission limit；
3. 未被应用 `accept()` 的连接没有清理路径，会长期留在 `pending`/`sessions`；
4. 每连接两个后台任务、100 ms receive timeout 和 64 KiB 初始 write buffer 不适合直接扩展到数万游戏会话。

因此正确的优先级是：

> **资源有界 + 执行器归属 → 零分配批量收包 → 批量路由/解密/KCP 输入 → 建连与内存优化 → 游戏专用 shard endpoint**

在 P0 完成前，本 listener 可以用于受控环境和兼容性实验，但不应宣称适合公网高连接数服务。

---

## 1. 当前实现的真实并发模型

### 1.1 收包和连接路径

`spawn_listener_reader` 当前执行：

```text
recv_from 首包
  → try_recv_from 循环排空 socket
  → 每 datagram 查询 sessions
  → 新 peer 同步构造 KcpConn，并 spawn input/flush task
  → datagram 移入 PeerQueue
  → 每个 affected peer notify 一次
```

“整次 socket drain 后再 notify”是正确设计，已有实测证明它能避免 Tokio 多线程下 input loop
过早醒来造成 ACK datagram 膨胀；任何后续优化都必须保留以下语义：

- 同一 peer 的入站顺序不应被主动打乱；
- 一次 reader burst 内每 peer 最多 notify 一次；
- peer input loop 尽可能一次取完 burst，再执行一次 KCP lock/flush；
- listener 只做 demux，不把 crypto、FEC 或 KCP 状态放入全局临界区。

### 1.2 当前 `try_recv_batch_from` 的隐藏分配

Linux 下 `kio::UdpSocket::try_recv_batch_from` 确实调用 `recvmmsg`，但收到每个 datagram 后会：

```rust
let v = std::mem::take(&mut packet_bufs[i]);
packet_bufs[i] = Vec::with_capacity(...);
out.push((v, peer));
```

此外，`mmsg::recvmmsg_from` 还会为返回的元数据创建一个 `Vec`。因此当前 API 虽减少 syscall，
却可能重新引入接近每包一次的 2 KiB buffer 分配，破坏刚完成的 listener buffer recycle 优化。
在 null/AES 等轻 CPU 路径上，分配器和 cache miss 可能抵消 syscall 批量化收益。

非 Linux fallback 还会 `slot[..n].to_vec()`，所以不能直接无条件替换当前 listener 的
`try_recv_from + mem::take` 路径。

### 1.3 当前 shard 注释与任务归属不一致

`kcptun-server/src/app.rs` 先在主 `kio::block_on` 运行时调用
`KcpListener::build().await`，随后才把 `serve_udp_shard(listener, ...)` 移入新的
`block_on_local` 线程。结果是：

- listener reader 在构建它的主运行时 spawn；
- 新 peer 的 KCP input/flush task 又由 reader 所在运行时 spawn；
- shard 线程主要运行 accept、SMUX 和 TCP pipe；
- Tokio 下 `spawn_task = tokio::spawn`，任务可在主多线程 runtime 的 worker 间迁移；
- smol 下 `spawn_task` 固定使用 process-global executor，`block_on_local` 也不提供真正的 shard-local spawn。

所以当前代码不能据此声称“一个 shard fd 只被一个 worker 触碰”或“listener 锁在生产中无竞争”。
SO_REUSEPORT 仍能把不同 peer 分到不同 socket，但单个 socket 的 async task affinity 尚未被保证。

### 1.4 资源生命周期风险

当前首包路径在验证 KCP/crypto 有效性前就创建并放入：

- `sessions: HashMap<SocketAddr, Arc<PeerQueue>>`；
- `pending: VecDeque<(KcpConn, SocketAddr)>`；
- 一个 `KcpConn`，其初始 `write_buf` capacity 为 64 KiB；
- input/flush 后台任务；启用 FEC 时还有每会话 FEC 状态和 shard buffers。

如果应用未及时 `accept()`，或者攻击者不断伪造源地址发送垃圾 UDP 包，这些对象没有自动过期路径。
`PeerQueue::packets` 也无上限。对公网服务而言，这是在吞吐优化前必须解决的内存与任务 DoS 问题。

---

## 2. 两类目标负载不能用同一组默认值

| 维度 | 网络游戏服务器 | 网络隧道 |
|---|---|---|
| 会话数 | 高，常见数千到数万 | 通常较少，单会话可承载很多 SMUX stream |
| 包大小/频率 | 小包、固定 tick、fan-in 明显 | 大流、持续 burst、单 peer 连续包多 |
| 首要指标 | P99/P999 延迟、抖动、公平性、内存/连接 | 吞吐、CPU/Gbps、syscall/packet |
| burst 的 peer 分布 | 常见 `D≈B`（每包来自不同玩家） | 常见 `D≪B`（一个隧道占多个包） |
| 合适架构 | shard-owned endpoint/actor | 当前 stream API + 批量收发可继续优化 |
| 过载策略 | 快速丢包/拒绝新会话，保护已连接玩家 | 有界排队，允许较大单 peer burst |

建议最终提供两个显式 profile，而不是暗中根据运行时猜测：

- `Game`: 小 receive batch、严格 drain time budget、小 peer queue、低 initial buffer、强公平性；
- `Tunnel`: 较大 batch/queue、允许连续处理同 peer、较大 socket/KCP window、启用跨包批处理。

profile 只能给出起始配置。最终值必须由连接数、MTU、KCP window、丢包率和可用内存共同决定。

---

## 3. P0：先让系统在过载下保持有界

### 3.1 增加 listener limits 和 admission 状态

建议为 `KcpListenerBuilder` 增加组合配置，而不是继续堆独立 setter：

```rust
pub struct KcpListenerLimits {
    pub max_sessions: usize,
    pub max_building: usize,
    pub max_pending_accepts: usize,
    pub max_peer_queue_packets: usize,
    pub max_peer_queue_bytes: usize,
    pub max_total_queue_bytes: usize,
    pub pending_timeout: Duration,
    pub new_sessions_per_second: u32,
}
```

初始数值必须从内存预算反推，不能把上述字段写成任意“大常量”。至少要满足：

```text
max_sessions × 单会话稳态内存
+ max_total_queue_bytes
+ max_pending_accepts × 未接受会话内存
< 进程允许的数据面内存预算
```

会话表应显式保存生命周期，避免同一 peer 重复 build：

```rust
enum PeerState {
    Building { generation: u64, queue: Arc<PeerQueue>, created_at: Instant },
    Ready    { generation: u64, queue: Arc<PeerQueue> },
}
```

新 peer 的正确顺序：

1. 在一次 `sessions` 临界区中检查 session/rate/queue/pending 限额；
2. 先插入 `Building`，再释放锁，防止后续包重复创建连接；
3. 首包进入有界 queue；
4. 把 build 请求放入**有界**队列或有并发上限的 worker；
5. build 成功后原子地把相同 generation 变成 `Ready` 并放入 pending；
6. build 失败/超时只删除相同 generation，不能误删后来重连的新会话；
7. pending 满时关闭该连接并回收 session，而不是继续增长。

不建议“一 peer 一 spawn”。它没有 admission backpressure，而且当前 listener 路径的
`KcpConnBuilder::build()` 没有真正异步等待，主要做同步分配、FEC 初始化和 task spawn。

### 3.2 UDP 队列只能丢，不能等待

reader 不能等待 peer 消费队列，否则一个慢 peer 会阻塞整个 socket。达到限额时应使用明确策略：

- 默认 drop-tail：丢新 datagram，保留已排队顺序；KCP 负责重传；
- 记录 `peer_queue_drop_packets/bytes`；
- 连续超限可关闭或短期拉黑该 peer；
- 全局 queue budget 耗尽时优先拒绝新/未认证 peer，保护已建立会话；
- 不在 reader 热路径打印逐包日志。

对于游戏服务器，可以把 queue 上限设得较小来限制抖动；对于隧道，应同时限制 packet 数和
byte 数，避免大量小包或少量大包绕过单一维度的限制。

### 3.3 Admission 的安全边界

保持 Go wire compatibility 意味着不能在协议里强制增加 cookie handshake。仍可做：

- 每源 IP/网段和全局新会话 token bucket；
- 最小/最大 datagram 长度检查；
- 可选 `admission_filter(peer, first_datagram)` hook；
- 在应用鉴权完成前使用更小的 queue、window 和 idle timeout；
- Linux 公网部署配合 nftables/eBPF/XDP 做明显伪造流量和速率过滤。

crypto 校验目前位于每 peer 的 `CryptoTransport` 中，listener 在构建连接前无法确认密文有效。
若需要“先验密再建会话”，应在 `kcptun-common` 增加可复用的 pre-admission decrypt/validate
层，而不是把 crypto 重新塞进 `kcp-rs`。需避免首包解密两次，并保持 `kcp-rs` 无 crypto 依赖。

### 3.4 修正执行器/worker 归属

短期必须先把契约写清：`KcpListener` 的任务运行在哪个 executor，由创建/注入的 spawner 决定，
不能从持有它的 `serve_udp_shard` future 所在线程推断。

Tokio 的临时修复思路是让 socket 注册、listener build、reader、KCP task 和 session task 都在
同一个 shard runtime 内创建。但 `tokio::net::UdpSocket` 与 reactor 绑定，不能简单在主 runtime
创建后再假设迁移安全；应把原始 `std::net::UdpSocket`/fd 或 socket factory 移进 shard 后再包装。

跨 tokio/smol 的长期方案二选一：

1. 给 `KcpListener`/`KcpConn` 注入明确的 `Spawner`/executor handle；或
2. 建立 shard-owned `KcpEndpoint`，由一个 worker 同时拥有 demux、KCP state、timer 和 send。

验收必须用 thread-id trace 或调度指标证明 reader/input/flush 实际归属，不能只依赖代码注释。

---

## 4. P1：零分配批量接收和公平 drain

### 4.1 先修 kio batch API

推荐增加不转移 payload ownership 的接口；payload slots 由 caller 长期持有，地址数组复用容量：

```rust
fn try_recv_batch_from_into(
    &self,
    packet_bufs: &mut [Vec<u8>],
    peers: &mut Vec<SocketAddr>,
) -> io::Result<usize>;
```

约束：

- 返回后 `packet_bufs[..n]` 直接包含 payload，不为每个 slot 创建替换 Vec；
- `peers.clear()` 后复用既有 capacity；
- `mmsg` helper 把元数据写入调用者/线程本地 scratch，不返回新分配 Vec；
- listener 路由时 `mem::take(packet_bufs[i])`，再用 `PeerQueue` 返回的 spare 填回该 slot；
- warm-up 后验证每 packet allocation 接近 0；
- 非 Linux fallback 逐包 syscall 但仍原地填 slot，不再 `to_vec()`。

可以先用当前 `try_recv_batch_from` 做独立原型验证 syscall 假设，但不能把它直接作为最终实现合入。

### 4.2 批量大小和 drain budget

一次 wakeup 一直 drain 到 `WouldBlock` 会在持续入流时饿死 KCP/SMUX/accept 等任务。批量收包后
更容易出现这种问题，因此需要两个不同的上限：

- `recv_batch_size`：一次 `recvmmsg` 最多取多少包；
- `max_drain_packets` 或 `max_drain_time`：一次调度轮最多处理多少包/多少微秒。

达到 budget 时记录 counter，并 `yield_now`/重新等待可读，保证协作式公平。建议从以下范围做 A/B，
不把它们当固定默认值：

- Game：batch 16–32，单轮 64–128 包或约 50–100 µs；
- Tunnel：batch 32–64，单轮 128–512 包或约 100–250 µs。

选择标准是 P99/P999 延迟和吞吐的联合 Pareto 前沿，而不是单看峰值 pps。

### 4.3 接收批量化的正确收益表述

假设是：当 `burst > 1` 且 recv syscall 在 profile 中占可行动热点时，`recvmmsg` 可降低
syscalls/datagram。实际整机收益取决于：

- burst size 分布，而不是配置的最大 batch；
- crypto/FEC/SMUX 占用；
- allocator 是否已退出热路径；
- reader 与 peer task 是否跨核迁移；
- 游戏负载的公平 budget 是否频繁命中。

只有同时观察到 `recv_syscalls / recv_datagrams` 下降、allocation 不回升、P99 不恶化，才能判定成功。

---

## 5. P1：批量路由、队列和 KCP 输入

### 5.1 不使用 unstable sort

原方案的 `sort_unstable_by_key(peer)` 不保证相同 peer 的原始到达顺序。KCP能处理网络乱序，
但 listener 没有理由主动制造额外乱序；这会增加 ACK/FEC 行为的不确定性。

推荐分两步实施：

1. **先把 `sessions.lock()` 从每 datagram 一次降为每 burst 一次**，仍按到达顺序路由；
2. profile 证明 queue lock 是热点后，再加入保持同 peer 顺序的稳定分组。

稳定分组可以用跨 wakeup 复用 capacity 的 `HashMap<SocketAddr, group_index> + Vec<Group>`，
扫描 burst 时按到达顺序 append 到各 group。不要每轮创建 `BTreeMap` 或新的大 HashMap。

负载差异必须反映在决策中：

- Tunnel：`D≪B`，一次 `push_batch` 可明显减少 queue lock；
- Game：`D≈B`，每 peer 本来只有一个包，分组不会减少 queue lock，额外 hash/group 可能回归；
- 小 burst：保留直接路径，避免 batch bookkeeping 大于收益。

### 5.2 `PeerQueue::pop_batch_into`

server 的 `PeerTransport` 应支持一次 queue lock 弹出多个 payload，并把消费者旧 buffer 批量归还
spare pool。接口需同时返回包数并维持两个上限：

- 单次最多 `MAX_INPUT_BATCH`；
- queue retained spare 仍受 packets/bytes 限制，不能因瞬时 burst 永久保留峰值内存。

批量实现不得把 `Vec<Vec<u8>>` 重新复制成新的 payload Vec。

### 5.3 `CryptoTransport` batch 必须配套

只给 `PeerTransport` 实现 batch 不够，因为生产服务在外层包了 `CryptoTransport`。需要：

1. `supports_recv_batch()` 透传 inner；
2. `try_recv_batch()` 先从 inner 取一批；
3. 原地逐槽 decrypt + truncate；
4. CRC/AEAD 失败的槽位稳定压缩掉，保持有效包相对顺序；
5. 坏包 buffer 回收，不泄漏容量；
6. null cipher 走最小分支；重型 crypto 是否 offload 由 profile 决定，不能在 reader 全局串行解密。

### 5.4 `KcpConn` input loop 批处理

input loop 当前阻塞取首包后用 `try_recv_vec` 一直排空。改造时：

- batch transport 一次填充剩余预分配 slots；
- 每轮有 packet/time budget，避免一个高吞吐隧道独占 worker；
- 继续用一次 `feed_inbound_batch` + 一次 deferred flush；
- 保持 FEC decode 在 KCP mutex 外；
- batch slots 和 `Cow` scratch 跨轮复用 capacity；
- direct connected UDP 客户端也覆盖到 `recvmmsg_connected` 路径。

测试必须专门覆盖：同 peer 顺序、跨 peer 隔离、坏密文夹在有效包之间、FEC 恢复、queue overflow、
close 与 batch 同时发生。

---

## 6. P1/P2：取消 timer tick，而不是把 100 ms 改成 500 ms

listener reader 和每个 KCP input loop 都用 100 ms timeout 包裹 receive，以便检查 close。对少量隧道
影响不大，但数万游戏连接会长期创建/唤醒大量 timer。

正确方向是为跨 runtime I/O 增加显式取消能力，例如：

- `PacketTransport::cancel_recv()` / close notification；或
- `kio` 提供 runtime-agnostic `race(recv, close_notify)`；或
- shard endpoint 由单一 event loop 处理关闭，不为每 session 创建 receive timer。

要求：

- `KcpConn::close()` 能立即唤醒阻塞的 `PeerTransport::recv_vec`；
- listener `close()` 能立即唤醒 shared socket reader；
- 无数据时不产生固定 10 Hz × connections 的 timer 工作；
- shutdown latency 不依赖 100/500 ms polling interval。

仅把 tick 拉长会降低 timer 频率，但增加资源回收和关闭延迟，不能作为最终优化。

---

## 7. P2：连接构建和单会话内存

### 7.1 有界 build，而非无界 spawn

连接风暴期间 reader 不应连续执行大量同步 build，也不应创建无限 task。可选实现：

- current-thread/shard actor：每轮只 build 固定数量，其余留在有界 `Building` queue；
- multi-thread executor：用 semaphore 限制并发 build，并限制等待队列；
- 超出上限直接 admission drop，不阻塞 socket reader。

无论哪种实现，reader 都必须有 drain budget 和显式 yield，否则 socket 持续可读时 build task 仍可能
得不到调度。

### 7.2 延迟分配大 buffer

`KcpConn` 当前每会话创建 `BytesMut::with_capacity(64 * 1024)`。这对少量大吞吐隧道合理，
对数万空闲/小包游戏连接成本过高。建议：

- initial write/read buffer capacity 进入 profile/config；
- Game profile 从小容量或 lazy allocation 起步；
- Tunnel profile 可保留较大预留，减少扩容；
- FEC state 仅在启用时创建，统计启用 FEC 后的真实 bytes/session；
- heap profile 分别记录 accepted、pending、idle、busy 会话内存。

不要只看 Rust struct 的 `size_of`；Vec/BytesMut capacity、FEC shard cache、crypto buffers 和任务分配
才是主要内存。

### 7.3 自动清理

listener 自身至少应清理：

- `Building` timeout；
- `pending` accept timeout；
- queue closed 且无 owner 的 sessions entry；
- build/accept 失败的 generation；
- listener close 后尚未交给应用的 pending KcpConn。

已被应用接收的活跃 session idle policy 可以继续由上层 `KcptunSession` 管理，但底层必须提供
可观测的 `last_activity` 和显式 `remove_peer`/close 语义。

---

## 8. P3：面向游戏服务器的 shard-owned `KcpEndpoint`

即使完成上述 P0–P2，当前“一连接一个 input task + 一个 flush task”的 TcpStream 风格 API 仍更适合
隧道和中等连接数。若目标是数万游戏会话和稳定 P999，建议新增 endpoint 模式，而不是继续微调锁：

```text
每个 shard 一个 UDP socket
  → 一次 recvmmsg
  → shard-local session table
  → 批量 feed 多个 KCP state
  → shard timer wheel 驱动 update/flush
  → 汇总不同 peer 的 outbound
  → 一次 sendmmsg_to
```

特性：

- session state 单 owner，大部分状态无需跨线程 mutex；
- 一个 shard 一个 receive loop 和 timer wheel，不是每连接一个 receive timer/task；
- 可跨 session 聚合 `sendmmsg_to`，解决共享 fd `writable()` 惊群和小 ACK batch；
- 每轮同时有 per-session 和 per-shard budget，避免热点玩家/隧道饿死其他连接；
- 应用通过有界 channel/回调收发消息，明确 backpressure；
- Linux 用 SO_REUSEPORT 做 kernel 4-tuple sharding；非 Linux 用单 acceptor + 有界 channel 哈希到 worker；
- 可复用现有 `background_input(false)` / `feed_batch` 思路，但还需 external flush/timer 驱动接口。

这是一项新 API/架构，不应与 listener 的 P1 微优化混在同一提交。TcpStream 风格 `KcpConn` 继续保留，
服务隧道和易用性场景；`KcpEndpoint` 服务高连接游戏场景。

---

## 9. 暂不建议的优化

- **直接把 `Mutex<HashMap>` 换成 `RwLock`/DashMap**：没有解决 task affinity、无界资源和批量粒度；
- **单 fd 多 reader**：会增加同一 peer 并发路由、顺序和 session 创建竞态；横向扩展优先 SO_REUSEPORT；
- **`sort_unstable_by_key` 分组**：主动改变同 peer 顺序，且 `D≈B` 时无收益；
- **用无界 channel 替代 `PeerQueue`**：只是把无界内存从 VecDeque 换到 channel；
- **为每个新 peer spawn task**：攻击流量可直接制造任务风暴；
- **把 timer 从 100 ms 调到 500 ms**：只是在性能与回收延迟之间移动问题；
- **先动 KCP core mutex**：listener batch/资源/调度问题未证实前，不破坏 Go 对齐的 KCP 控制流；
- **仅用平均吞吐作为成功标准**：游戏服务器的 P999、公平性和过载内存更重要。

---

## 10. 可观测性和基准矩阵

### 10.1 必须增加的低开销指标

默认关闭或采样，避免给热路径强加全局原子开销：

- `recv_syscalls`、`recv_datagrams`、实际 batch size histogram；
- `drain_budget_hits`、单轮处理时间；
- `sessions_ready/building/pending`；
- session build 成功/失败/限流/超时；
- peer/global queue packets、bytes、high-water mark、drops；
- decrypt invalid、KCP invalid、FEC recovered；
- task/timer 数量、listener close latency；
- send syscalls/datagrams、WouldBlock/writable wakeups；
- 每 shard PPS、CPU、active sessions，检查负载倾斜。

### 10.2 场景矩阵

| 场景 | 建议负载 | 主要指标 |
|---|---|---|
| G1 游戏稳态 | 1k/10k/目标上限 clients，20/60 pps，64–256 B | RTT P50/P99/P999、jitter、CPU、RSS |
| G2 游戏 fan-in | 所有 clients 同一 tick 发包 | drain budget、公平性、尾延迟、drops |
| G3 churn | 每秒大量 connect/disconnect，首包立即带数据 | accept latency、build queue、retrans、RSS |
| G4 攻击/误流量 | 随机/伪造 source、坏密文、超长/短包 | admission drops、RSS plateau、合法流 P999 |
| T1 单隧道 bulk | 1 session，多 SMUX stream，大文件 | Gbps、CPU/Gbps、syscalls/packet |
| T2 多隧道 | 16/64/128 sessions 混合大小流 | aggregate throughput、公平性、RSS |
| T3 混合 | bulk 隧道 + 低延迟小流 | 小流 P99 是否被 bulk 饿死 |
| L1 丢包网络 | 0/1/5/10% loss + reorder/jitter | goodput、retrans、FEC CPU、正确性 |

每个场景至少覆盖：

- Tokio 与 smol；
- Linux（recvmmsg/sendmmsg/SO_REUSEPORT）和一个非 Linux fallback；
- null、AES 硬件路径、一个重型 cipher；
- FEC off/on；compression off/on；
- 不同 `B` 和真实 `D/B` 分布。

现有 bulk 脚本可以复用，但不足以代替游戏 fan-in、连接风暴和恶意首包测试。

### 10.3 证据闭环

每个优化类独立提交并执行：

1. 记录 baseline：固定 commit、CPU、OS、runtime、crypt/FEC/comp、连接数和完整命令；
2. `make profiling-bins`，采集 CPU/heap profile；
3. 同时用 `perf stat`/系统调用追踪验证 syscall 假设；
4. 只改一个优化类；
5. `make gate`；并发路径跑 stress，wire/crypto/FEC 跑 e2e；
6. 交错 A/B 多轮测试，报告中位数和 P95/P99，不采用单次最好结果；
7. 更新 `bench/profiles/HOTSPOTS.md`；
8. 若热点占比已低于约 5% 或 KPI 没有改善，停止继续复杂化。

最低接受门禁：

- wire/e2e/data correctness 零回归；
- 小 burst/低负载延迟不出现统计显著回归；
- RSS 在设定 session/queue 上限下达到平台而非持续增长；
- overload 时 reader 不阻塞、现有会话仍有公平进展；
- batch 优化必须同时降低 syscalls/datagram，且不提高 allocations/datagram；
- 任何宣称的百分比都附原始命令、样本数和 before/after 数据。

---

## 11. 推荐实施顺序

| 阶段 | 改动 | 主要验证 |
|---|---|---|
| P0.1 | 增加 listener/queue/build/pending 指标和 baseline harness | 指标开关关闭时无明显回归 |
| P0.2 | `ListenerLimits`、`Building/Ready`、有界 queue/pending/build、自动清理 | spoof/churn 下 RSS plateau，合法连接继续服务 |
| P0.3 | 修正并验证 executor/shard ownership | thread trace + 每 shard CPU/pps，无跨 shard fd 误触碰声明 |
| P1.1 | allocation-free `try_recv_batch_from_into` + Linux recvmmsg | syscalls/datagram↓，allocations/datagram 不升 |
| P1.2 | 每 burst 一次 sessions lock；按证据增加稳定 grouping/push_batch | G1/G2 与 T1/T2 分别 A/B |
| P1.3 | PeerQueue/CryptoTransport/KcpConn batch receive | 坏包/FEC/顺序测试 + pprof |
| P1.4 | cancelable recv，移除周期 close polling timer | idle 多连接 timer CPU↓、close 及时 |
| P2.1 | 有界/分时 build、lazy per-session buffers、idle/pending reap | G3/G4、heap profile |
| P2.2 | writable 惊群/跨 session send batching（仅 profile 证实后） | send syscalls、P999、吞吐 |
| P3 | shard-owned `KcpEndpoint` 游戏 API | 10k+ 会话、fan-in、timer/task 数、长期 soak |

P0–P2 保持现有 `KcpListener` API 的兼容性应尽量通过 builder 配置完成。P3 是独立的高连接数 API，
需要单独 spec、测试计划和迁移说明。

---

## 12. 最终判断

对网络隧道，修正后的 P0–P2 路线合理：保留当前 `KcpConn`/SMUX stream 结构，重点做有界队列、
零分配 recvmmsg、CryptoTransport 批量解密和公平 input batching。

对高连接数游戏服务器，只优化 `listener.rs` 的 syscall 和 mutex 不完整。真正的上限来自每连接任务、
timer、跨 worker 迁移和不能跨 session 合并发送。应在 listener 安全加固后推进 shard-owned
`KcpEndpoint`，用单 owner state + timer wheel + recvmmsg/sendmmsg 构建游戏数据面。

在这些工作完成并通过 G1–G4/T1–T3 长时间验证前，本项目自身的“非生产软件”免责声明仍然适用；
性能更高不等于已经具备公网生产所需的资源隔离、安全和可运维性。

---

## 13. 实施状态（2026-08-05）

本节的实施遵循上文 v3 优先级，逐里程碑独立提交、每步 `make gate` 全绿
（fmt + workspace test + clippy -D warnings），关键 crates 通过 Linux 交叉编译
（`cargo check --target x86_64-unknown-linux-gnu`），并与 Go kcptun 交叉验证
（`make e2e`：**138 passed / 0 failed**，覆盖全部 crypt × KCP mode × SMUX ×
compression × FEC，tokio 与 smol 双后端）。

### 已完成并验证

| 里程碑 | 改动 | 验证 |
|---|---|---|
| **P0.2** 资源有界 + 生命周期 | `KcpListenerLimits`（**默认不限制**：0=无限 / `Duration::ZERO`=无超时，经 builder `.limits()` 可选配上限）；`PeerState::{Building,Ready}` + generation 防呆；`PeerQueue` drop-tail（配限时生效）；`pending` 满则回收会话；Building/pending 超时清理（配限时生效）；`stats()`/`session_count()`/`pending_count()` 观测 | listener 10 测试 ×2 后端 |
| **P1.1** 零分配批量收包 | kio 新增 allocation-free `try_recv_batch_from_into`（Linux `recvmmsg_from_into` 写调用者 slots + 复用 peers Vec；非 Linux 原地填 slot 不再 to_vec）；listener 排空接入，路由时用 `PeerQueue` spare 原地回填槽位 → 稳态每包零分配 | macOS 测试 + Linux 交叉编译 |
| **P1.2** 每 burst 一次锁 | `route_one` → 持锁 `route_inner`；整个 wakeup 的排空+路由在**一次** `sessions` 临界区内完成（不再每包一锁），按到达顺序、无 unstable sort；`max_drain_packets` 预算防止单 wakeup 无限排空饿死同 runtime 任务 | 同上 |
| **P1.3** 批量接收栈 | `PeerQueue::pop_batch`（一把锁弹整批 + 回收）；`PeerTransport::try_recv_batch`/`supports_recv_batch`；`CryptoTransport::try_recv_batch`（逐槽原位解密 + **坏包稳定压缩**）+ `supports_recv_batch` 透传；input loop `try_recv_batch` 批量排空 + `MAX_INPUT_BATCH` 预算；直接 UDP 客户端也走 recvmmsg_connected | 新增 `pop_batch` 顺序/有界 + 坏包压缩专项测试 |
| **P2.1**（部分） | `KcpConnBuilder::buffer_size(bytes)`（默认 64KiB，惰性增长）；有界分时 build 与 idle/pending 回收已在 P0.2 内 | builder 编译 + 测试 |

### 关键代码位置（实施后）

- `kcp-rs/src/listener.rs`：`KcpListenerLimits`、`PeerState`/`PendingAccept`、`ListenerStats`、
  `ListenerCtx::{route_inner, process_builds, sweep}`、`spawn_listener_reader`（一次 sessions 锁 +
  零分配 recvmmsg 排空 + 预算）。
- `kcp-rs/src/transport.rs`：有界 `PeerQueue`（drop-tail + `pop_batch` + spare 回收）、
  `PeerTransport::try_recv_batch`。
- `kcp-rs/src/conn.rs`：`INPUT_BATCH_GROW`/`MAX_INPUT_BATCH`/`DEFAULT_WRITE_BUFFER`、
  input loop 批量排空分支、`KcpConnBuilder::buffer_size`。
- `kio-rs/src/net/{mmsg,tokio,smol,mod}.rs`：`recvmmsg_from_into` + `try_recv_batch_from_into`。
- `kcptun-common/src/kcp_transport.rs`：`CryptoTransport::try_recv_batch`（坏包压缩）。

### 未实施项与理由（建议下一步，勿在本轮合入）

| 里程碑 | 状态 | 理由 / 建议 |
|---|---|---|
| **P0.3** 执行器归属 | 未实施 | 当前 reader/input/flush 运行在进程级多线程 runtime（跨 worker 迁移），shard 线程只跑 accept/SMUX/TCP——功能正确但代码注释的 "current-thread per shard" 不成立。修正需把 listener 构建 + socket 包装移入 shard runtime（tokio socket 与 reactor 绑定，须用 std fd 工厂在 shard 内包装）。**收益是"设计意图一致"而非明确吞吐提升**（多线程 runtime 反而给了跨 peer 并行），且有回归风险；建议按 v3 §3.4 单独做并用 thread-id trace 验收 |
| **P1.4** 可取消收包 | 未实施 | 需 kio 新增跨运行时 `race(recv, close_notify)`（tokio: select!；smol: async-io race），替换 listener/input loop 的 100ms close 轮询 tick。海量空闲连接时消除 10Hz×N timer churn。kio 现无 select/race 原语，改动面跨双后端；建议单独里程碑 + 空闲连接 timer 专项压测 |
| **P2.2** 发送批量化/惊群 | 未实施 | v3 §P2.2 明确"仅 profile 证实后"。本报告未做 CPU profile，无证据前不合入。建议先 `bash bench/profile_rust_go_pprof.sh server 20` + `go tool pprof` 确认 send/writable 是否可行动热点 |
| P2.1 CLI profile | 未实施 | `buffer_size` 已入 builder；Game/Tunnel 预设 + `--buffer-size` CLI 管道（`kcp_config_from` 显式字段列表需补字段）留作后续 |

---

## 14. 证据门禁的后续尝试（2026-08-05，仅改 kcp-rs）

按用户要求："继续实施后续计划，核心只改 kcp-rs，每阶段必须实测吞吐有明确提升才 commit，
否则回退进下一阶段"。本节的每次尝试都用同一基准命令
`bench_rust_vs_go.py --quick --rust-only --conn 4 --size 65536 --runs 3` 对比。

### 14.1 基线（已提交状态 1a8d6b3f + 0bec9b1e）

```
null/no-comp 25.9 | null/comp 31.4 | aes-128/no-comp 30.0 | aes-128/comp 27.1
aes-128-gcm/no-comp 28.5 | aes-128-gcm/comp 25.5 | salsa20/no-comp 28.2 | salsa20/comp 25.7
blowfish/no-comp 23.9 | blowfish/comp 23.5 | sm4/no-comp 18.6 | sm4/comp 17.0
3des/no-comp 12.1 | 3des/comp 12.4        （单位 MB/s，Rust-tokio）
```

### 14.2 CPU profile（null，server，20s）—— 热点定位

```
send_to   39.0%  （UDP 发送 syscall；macOS 无 sendmmsg → 逐包，kio 层）
recv_from 26.6%  （UDP 接收 syscall；macOS 未启用 recvmmsg → 逐包，kio 层）
io Driver turn 9.5% | Handle::unpark 7.0%   （tokio reactor）
mimalloc 分配 ~10% | Time::now 2.3%         （kcp-rs 可触及的极小部分）
```

结论：**kcp-rs 的 listener 解复用已不在热点中**（P0.2–P1.3 生效）；剩余瓶颈是
kio/socket 层的逐包 syscall + tokio reactor，**kcp-rs 内的改动无法减少这些 syscall**。
allocs profile 显示 kcp-rs 内分配热点：`push_and_reuse` 13.6%、`fec_expand_packets` 9.9%、
`feed_inbound_batch` 17% cum——但绝对分配率低且服务器 syscall-bound，收益在噪声内。

### 14.3 阶段尝试与结果

| 阶段 | 改动 | 实测吞吐 | 结论 |
|---|---|---|---|
| 尝试 1：reader buffer reserve | `push_and_reuse` 改返回 `Option` + reader 预热 32 缓冲 reserve，吸收瞬时队列 spare 短缺 | null/no-comp 25.9→29.8(+15%)、null/comp 31.4→25.8(-18%)、aes-128/no-comp 30.0→24.3(-19%)、aes-128/comp 27.1→22.6(-17%)、3des/no-comp 12.1→13.2(+9%) | **无一致提升**（±15-20% 噪声内），已回退 |

### 14.4 证据结论

1. 该机器上基准噪声约 ±15–20%，"明确提升"需 >20% 的一致增益。
2. CPU profile 显示服务器 syscall-bound（send_to+recv_from = 66%，位于 kio/socket 层，
   不在 kcp-rs），且 macOS 无 sendmmsg、libc 无 recvmmsg 绑定——send/recv 批量在 kcp-rs
   内无法实现。
3. 剩余计划项（P0.3 执行器归属 / P1.4 可取消收包 / P2.2 发送批量化 / P2.1 CLI profile）
   在"只改 kcp-rs"约束下要么需要其他 crate（被排除），要么目标是空闲规模/内存而非吞吐，
   要么实测无明确提升。
4. 依用户规则（必须有明确提升才 commit），**后续阶段全部不满足门槛，不提交**；
   符合 v3 §10.3"热点占比低于约 5% 或 KPI 无改善即停止复杂化"。

---

## 15. Master vs Origin/main 吞吐回归调查（2026-08-05，结论：FEC，非 bug）

背景：外部分析声称当前 master 比 origin/main 慢 16–28%（origin/main 99 MB/s vs master 70 MB/s），
并归因于 commit c32d9eb8（pprof-driven latency optimizations）的 flush-loop 条件 `write_notify`。

### 实测（本机 x86_64，`run_bench.sh` null crypto，tokio→tokio，100MB）

| 版本 | 吞吐 (MB/s) | 延迟 |
|---|---|---|
| origin/main（4e5c2171） | 99.27 | 0.16ms |
| master（b76db61c）FEC 默认 10/3 | 68.69 | 0.20ms |
| master + `--datashard 0 --parityshard 0` | **99.82** | 0.16ms |
| Go→Go（本机对照） | 45.27 | 0.21ms |

### 二分定位

`git bisect`（bad=master, good=origin/main）找到**第一个坏 commit = `11ba065b`
"fix: wire session-layer FEC encode/decode like Go kcp-go"**：该 commit 把会话层 FEC 从
stub 正确接上（默认 10/3，匹配 Go kcp-go）。之前 FEC 未真正编解码。

### 结论

1. **"回归" = FEC 10/3 编解码开销（~30%），是正确行为，不是性能 bug**。master 关 FEC
   = origin/main（99.82 vs 99.27，噪声内）。
2. 外部分析的根因诊断（flush-loop 条件 `write_notify`、ClockOffset 锁竞争）**均被证伪**：
   无条件唤醒实测 67.48 vs 68.69（-1.8%，无提升）；ClockOffset 是 `OnceLock`（初始化后无锁）。
3. 若需恢复 origin/main 级吞吐，配置 `--datashard 0 --parityshard 0` 或调小 FEC 比例即可；
   这是应用层选择，不是 kcp-rs 缺陷。
