# kcp-rs 与 kcp-go 高并发及稳定性测试方案

## 1. 文档目的

本文档用于验证 `kcp-rs` 的 KCP 核心和异步 `KcpConn` 在高并发、长时间运行、丢包/乱序条件下的：

- 数据可靠性：不丢失、不重复、不乱序、不串流、不截断；
- 并发稳定性：连接数、单连接吞吐、单 KCP 多流、读写并发持续增长时不死锁、不活锁；
- 性能稳定性：吞吐、P99/P999 延迟、重传率、CPU、内存和队列长度没有异常漂移；
- 跨实现兼容性：`kcp-rs` 与 `kcp-go/v5` 在相同 KCP 参数下双向互通；
- 双异步后端一致性：tokio 与 smol 的结果、错误语义和资源释放行为一致。

本文针对当前仓库的实现和测试入口编写。当前项目是 wire-compatible / Vibe Coding 实验，不把本方案的通过结果等同于生产环境 SLA。

## 2. 测试范围与边界

### 2.1 本次重点

测试重点是 KCP 层和 `KcpConn`：

```text
UDP → KCP ARQ → 可选 FEC → AsyncRead/AsyncWrite
```

重点覆盖：

1. 多个独立 KCP 连接同时工作；
2. 一个 KCP 连接上高并发读写和大量消息；
3. 发送窗口耗尽、接收窗口耗尽、ACK 批量、快速重传、RTO 重传；
4. FEC 10/3、4/2 和无 FEC；
5. Go `UDPSession` ↔ Rust `KcpConn` 的裸 KCP 交叉测试；
6. Go kcptun ↔ Rust kcptun 的完整链路交叉测试；
7. 连接建立、关闭、重连、半关闭、异常退出和空闲回收。

### 2.2 不应混淆的测试层级

| 层级 | 被测对象 | 目的 | 主要入口 |
|---|---|---|---|
| L0 | `KCP` 同步状态机 | 协议正确性、窗口、ACK、重传、FEC | `cargo test -p kcp-rs` |
| L1 | Rust `KcpConn` | UDP + KCP + 异步读写 | `kcp-rs/tests/*` |
| L2 | Go `UDPSession` ↔ Rust `KcpConn` | 裸 KCP wire compatibility | `latency_p99`、`kcp-go-latency` |
| L3 | kcptun 完整链路 | Crypto/FEC/KCP/Snappy/SMUX/TCP | `bash test_e2e.sh` |
| L4 | 高并发 tunnel | 多本地 TCP 流、多 SMUX 流、多 KCP 连接 | `kcptun-server/tests/stress_test.rs` |
| L5 | 长稳与故障注入 | 资源泄漏、死锁、恢复能力 | 本方案新增或扩展 harness |

L0/L1/L3/L4 是现有仓库已有能力；L2 的基本延迟探针已存在；L5 以及 1k/10k 连接级极限测试需要按本文方案补充自动化 harness。

## 3. 测试前统一约定

### 3.1 固定协议参数

所有 Rust↔Go 裸 KCP 对比必须先使用完全一致的参数：

| 参数 | 基线值 | 说明 |
|---|---:|---|
| MTU | 1350 | 与现有 Rust/Go 延迟探针一致 |
| sndwnd | 512 | 极限测试另测 32/512/2048 |
| rcvwnd | 512 | 极限测试另测 32/512/2048 |
| mode | Fast3 | `nodelay=1, interval=10, resend=2, nc=1` |
| conv | `0x00C0FFEE` | 同一会话两端一致；多连接必须隔离 |
| FEC | 关闭、10/3、4/2 | FEC 需要单独记录恢复和重传指标 |
| payload | 1B、64B、1KB、16KB、64KB、128KB、512KB、1MB | 覆盖小包、分片、窗口压力 |
| 方向 | 单向、反向、双向同时 | 双向时必须分别校验序列 |

完整 kcptun 测试另外固定：`--key`、`--crypt`、`--mode`、`--nocomp`、`--datashard`、`--parityshard`、`--sndwnd`、`--rcvwnd` 必须在两端相同。

### 3.2 数据校验协议

每条逻辑消息使用独立的固定格式，不只依赖长度：

```text
magic(4) | run_id(8) | conn_id(8) | stream_id(8) | seq(8) |
payload_len(8) | seed(8) | payload | checksum(8)
```

建议：

- `payload` 由 `(seed, conn_id, stream_id, seq, offset)` 确定性生成；
- checksum 使用 SHA-256 或 FNV-1a-64；性能测试可使用 FNV，最终验收增加 SHA-256；
- 接收端必须验证 `run_id/conn_id/stream_id/seq`，不能只比较总字节数；
- 每个连接输出 `sent_msgs/sent_bytes/recv_msgs/recv_bytes/bad_checksum/duplicate/out_of_order/truncated`；
- 测试失败时保存第一个差异位置、期望/实际长度、前后各 32B hex dump。

### 3.3 时间和结果格式

每个 case 至少包含：

```text
case_id, implementation, runtime, direction, fec, payload_size,
connections, streams_per_conn, duration_s, sent_msgs, ok_msgs,
failed_msgs, timeout_msgs, bytes, goodput_mbps,
p50_us, p99_us, p999_us, max_us, retrans_rate,
cpu_avg, rss_start_mb, rss_peak_mb, rss_end_mb, result
```

结果必须保存到带时间戳的目录，例如：

```text
artifacts/kcp-stability/2026-08-04T120000Z/<case-id>/
  result.csv
  stdout.log
  stderr.log
  snmp-go.csv
  snmp-rust.csv
  resource.csv
  metadata.txt
```

不要用平均延迟替代尾延迟；延迟报告必须保留 P50/P90/P99/P999/max，且 warm-up 样本与测量样本分开。

## 4. 环境准备

### 4.1 编译和基础门禁

```bash
cd /Users/yangzhiqin/Desktop/kcptun-rs

ulimit -n 65536
cargo fmt --all -- --check
cargo build --release -p kcp-rs -p kcptun-client -p kcptun-server
cargo build --release -p kcp-rs --features async-tokio
cargo build --release -p kcp-rs --features async-smol

# 已有完整门禁
make gate
```

Go 参考实现需要提前构建：

```bash
cd tests/kcptun-go
go build -o server ./server
go build -o client ./client
cd ../kcp-go-latency
GOPROXY=off go build -o kcp-go-latency .
cd ../..
```

如果本机没有 Go module cache，应允许网络下载依赖；`GOPROXY=off` 只适用于依赖已缓存的环境。

### 4.2 系统观测

测试过程中至少采集：

```bash
# 进程资源；macOS 可用 top，Linux 可用 pidstat
top -pid <PID> -stats pid,cpu,mem,rsize,time,threads -l 1

# Linux 丢包/网卡统计
ss -s
ip -s link
nstat -az

# Linux 故障注入前检查
tc qdisc show dev lo
```

Rust 和 Go 两端都打开 KCP SNMP/日志时，记录：`in_pkts/out_pkts`、`in_segs/out_segs`、`retrans_segs`、`fast_retrans`、`early_retrans`、`lost_segs`、`repeat_segs`、`ring_buffer_*`、`fec_recovered`、`fec_errs`、`curr_estab`、`max_conn`。Rust 额外记录 `empty_flush`、`write_inline_sends`、`write_flush_sends`、`input_urgent_sends` 和加解密 offload 计数（完整 kcptun 测试适用）。

## 5. 第一阶段：现有基线必须先通过

### 5.1 kcp-rs 原生测试

```bash
# 同步 KCP、数据正确性和单元测试
cargo test -p kcp-rs

# tokio
cargo test -p kcp-rs --features async-tokio

# smol
cargo test -p kcp-rs --features async-smol

# 仓库提供的独立三段 runner
bash kcp-rs/test.sh
```

必须确认：

- 无测试失败、panic、超时；
- tokio 与 smol 的测试集合都通过；
- 无未回收任务导致测试进程无法退出；
- `kcpconn_integrity` 覆盖无 FEC 和 FEC 10/3；
- `kcpconn_listener` 覆盖多 peer 分流和关闭后新连接。

### 5.2 现有 tunnel 并发测试

```bash
# release 构建后运行已有 stress_test；包含非 ignored 基线
make stress

# 包含 heavy tests；运行时间和资源明显增加
cargo test --release -p kcptun-server --test stress_test \
  -- --nocapture --test-threads=1 --include-ignored
```

现有测试重点包括 10/50/100 个并发连接、单连接多 SMUX 流、64KB/128KB 大数据、分波次 page-refresh 场景以及 Snappy 高压缩比数据。执行时必须保存完整日志，不应只保留终端上的成功行。

### 5.3 现有 Go↔Rust 完整 E2E

```bash
make e2e
# 或在 release 二进制已准备好时：
bash test_e2e.sh
```

该矩阵已经覆盖：

- Go→Go、Go→Rust、Rust→Go、Rust→Rust；
- Rust tokio、Rust smol，以及 tokio↔smol；
- 13 类 cipher/AEAD 组合；
- normal/fast/fast2/fast3；
- SMUX v1/v2；
- 压缩和 `--nocomp`；
- FEC 10/3、4/2；
- Linux root 下可选的 `--tcp` raw transport。

该 E2E 当前主要是 echo smoke test，不能替代下面的长时间、高并发、可注入丢包测试。

## 6. 第二阶段：裸 KCP Rust↔Go 交叉测试

裸 KCP 测试用于把问题定位在 KCP/FEC，而不是 Crypto、Snappy、SMUX 或 TCP。

### 6.1 Go→Go 自身基准

先建立 Go 参考线：

```bash
cd tests/kcp-go-latency
./kcp-go-latency bench \
  --port 0 --warmup 10 --duration 120 --size 1024 --rps 500

./kcp-go-latency closed \
  --port 0 --warmup 10 --duration 120 --size 1024 --concurrency 1
```

这里的 `bench` 是 open-loop 固定 RPS，`closed` 是固定 in-flight 数。两者都必须保留，因为 closed-loop 会测最大可持续吞吐，而 open-loop 能暴露排队和尾延迟增长。

### 6.2 Go server ↔ Rust client

Go 端启动裸 KCP echo server：

```bash
cd tests/kcp-go-latency
./kcp-go-latency server --port 39000
```

Rust 端连接 Go server：

```bash
cargo run --release -p kcp-rs --features async-tokio \
  --example latency_p99 -- \
  --mode peer --addr 127.0.0.1:39000 --size 1024 \
  --rps 500 --warmup 10 --duration 120

cargo run --release -p kcp-rs --features async-smol \
  --example latency_p99 -- \
  --mode peer --addr 127.0.0.1:39000 --size 1024 \
  --rps 500 --warmup 10 --duration 120
```

### 6.3 Rust server ↔ Go client

Rust 端：

```bash
cargo run --release -p kcp-rs --features async-tokio \
  --example latency_p99 -- --mode server --port 39001
```

Go 端：

```bash
cd tests/kcp-go-latency
./kcp-go-latency client --addr 127.0.0.1:39001 \
  --size 1024 --rps 500 --warmup 10 --duration 120
```

smol server 只替换 Rust feature 为 `async-smol`。每个方向分别执行以下矩阵：

| 维度 | 值 |
|---|---|
| runtime | tokio、smol |
| payload | 1B、64B、1KB、16KB、64KB、128KB、512KB |
| 模型 | open-loop 100/500/1000/2000 RPS；closed-loop 1/8/32/128/512 |
| window | 32/512/2048 |
| mode | normal、fast、fast2、fast3 |
| direction | Go→Rust、Rust→Go |

裸 KCP 的基本通过条件：

1. `ok == samples`，没有 timeout 或错误；
2. 每条消息 checksum 正确，不能只看 echo 字节数；
3. 长度大于 MTU 时无截断、重排或粘包解析错误；
4. Rust 与 Go 的相同 case 结果差异可解释，不能出现某一方向特有的系统性失败；
5. `retrans_segs` 在无注入丢包的 loopback 基线中应接近 0；出现增长必须保存对应 SNMP 和系统日志。

## 7. 第三阶段：高并发极限测试

### 7.1 并发维度

必须分开测量以下三种并发，不能用一个“连接数”指标代替：

| 类型 | 含义 | 主要风险 |
|---|---|---|
| C1 独立 KCP | N 个 UDP/KCP session | listener demux、socket、任务数、端口和内存 |
| C2 单 KCP 多流 | 一个 KCP 上 N 个并发上层 stream | 窗口争用、SMUX/写队列、流间饥饿 |
| C3 多 KCP × 多流 | N 个 KCP，每个 M 个流 | 全局调度、锁竞争、任务爆炸、资源上限 |

### 7.2 阶梯矩阵

先逐级增加，首次失败的级别就是候选容量上限。建议级别：

```text
C1: 1, 10, 50, 100, 250, 500, 1,000, 2,000, 5,000, 10,000
C2: 1, 10, 50, 100, 250, 500, 1,000 streams / one KCP
C3: (10×10), (50×10), (100×10), (250×4), (500×4), (1000×2)
```

每一级至少执行三个 profile：

| Profile | 每连接/流负载 | 目的 |
|---|---|---|
| tiny | 1000 条 1B/64B 消息 | 调度、ACK、系统调用、锁竞争 |
| mixed | 70% 64B、20% 1KB、9% 64KB、1% 512KB | 接近真实混合业务 |
| bulk | 每连接 64KB、128KB、512KB、1MB 连续流 | 窗口、分片、背压和吞吐 |

每个 case 的顺序：10 秒 warm-up → 60 秒测量 → 30 秒 drain/close。首次达到 1k 连接以上时，将测量时间提升到 10 分钟。

### 7.3 高并发验收标准

建议将这些作为 CI 外的 release acceptance，而不是在普通单元测试中强制：

- 连接建立成功率 ≥ 99.99%；正式稳定性 run 不允许出现数据错误；
- 已建立连接的消息成功率 100%；任何 checksum mismatch、重复、乱序、截断都判失败；
- 不允许死锁、任务永久挂起、进程无响应或测试无法在 drain 后退出；
- 资源不足必须是显式、可统计的连接失败，不能静默丢数据；
- RSS 在稳定负载阶段不持续单调增长；短连接 churn 后应回落到基线附近；
- `curr_estab`、活动连接数、任务数、文件描述符数在关闭后回到接近 0；
- 单 KCP 多流不能出现一个大流长期占满窗口、导致小流全部超时；
- loopback 无故障注入时重传率应为 0 或极低，任何异常峰值必须能与系统丢包/调度停顿对应。

阈值应以首次建立的 Go→Go 基线为参考。不要直接以 Rust 的绝对 CPU 或吞吐值作为通过条件；跨语言比较时优先看可靠性、尾延迟和随并发增长的退化曲线。

### 7.4 实现建议：专用 stress harness

当前 `kcptun-server/tests/stress_test.rs` 主要通过本地 TCP echo 和 `std::thread` 驱动，适合已有 10/50/100 级别。为覆盖 1k/10k，建议新增独立 harness，而不是把默认单测变成超长测试：

- 使用异步任务或受控 worker pool，避免每个连接无限制创建 OS thread；
- `--connections`、`--streams-per-conn`、`--payload-profile`、`--duration`、`--rate`、`--fec` 可配置；
- 建立 barrier：所有连接 ready 后再开始测量；
- 使用全局 `AtomicU64` 统计，单个 worker 只保存必要的 in-flight metadata；
- 采样而非每包打印日志；错误写入 JSONL，包含 connection/stream/sequence；
- 设置明确的 connect、write、read、drain、close timeout；
- 在每个阶段输出一行机器可解析的 `RESULT`；
- 每 1 秒记录连接数、消息数、bytes、超时、重传、RSS、CPU 和队列长度。

推荐命名：`tools/kcp_concurrency_stress` 或 `kcp-rs/examples/concurrency_stress.rs`。该 harness 同时支持 tokio/smol，参数和输出格式保持一致。

### 7.5 游戏服务器场景：KcpListener 多玩家压力

`kcp-rs` 的 `KcpListener` 只有一个 UDP listener socket，通过 peer 地址分流到独立的 `KcpConn`。如果它直接服务游戏服务器，需要重点验证“许多长期在线玩家 + 周期性 tick + 局部广播 + 短时上线洪峰”的组合，而不能只验证若干个一次性 echo 连接。

#### 7.5.1 适用边界

KCP 提供的是有序、可靠的字节流。它适合：

- 登录、鉴权、角色和房间控制消息；
- 交易、背包、任务、匹配等不能丢失的事件；
- 需要按顺序处理的可靠 RPC；
- 对丢包敏感、但可以接受 Head-of-Line blocking 的游戏协议。

对于实时位置、瞄准、输入帧等“旧状态很快失效”的数据，需要在应用层增加序号、过期策略、快照覆盖或丢弃策略。不能因为底层 KCP 可靠，就默认适合所有实时状态同步；否则丢包时 KCP 重传会阻塞后续新状态。

#### 7.5.2 游戏协议帧

游戏压力 harness 不应直接把随机字节写入 `KcpConn`，建议使用带长度和序号的应用帧：

```text
magic(2) | version(1) | msg_type(1) | frame_len(4) |
player_id(8) | room_id(8) | tick(8) | seq(8) | payload | crc32(4)
```

至少定义以下消息类型：

| 类型 | 可靠性 | 负载示例 | 验证重点 |
|---|---|---|---|
| login/auth | reliable | 1～4KB | 完整到达、顺序、重复登录处理 |
| input/cmd | reliable baseline | 32～256B | tick/seq 连续性、延迟尾部 |
| state snapshot | reliable | 1～64KB | 分片、背压、旧快照处理 |
| room event | reliable | 64B～4KB | 广播 fan-out、玩家隔离 |
| ping/heartbeat | reliable | 16～64B | 空闲连接存活、RTT |
| inventory/rpc | reliable | 1～16KB | 不能丢、不能重复执行 |

每个 `player_id`、`room_id` 和 `tick` 都必须参与校验。服务端不得只统计“收到多少字节”，否则无法发现玩家串线、房间广播错投或 tick 顺序错误。

#### 7.5.3 游戏负载模型

至少实现以下四类模型：

**A. 大厅/登录洪峰**

```text
0 → 1,000/5,000/10,000 clients
每秒 100/500/1,000 个新连接
连接后：login → auth → enter_room → initial_snapshot
```

验证单 listener 在连接建立洪峰中是否仍能持续 accept，已建立玩家是否受到可接受的延迟影响。

**B. 稳态 tick**

```text
tick: 20Hz、30Hz、60Hz
每个玩家每 tick 发送 1 条 input/cmd
每个房间每 tick 广播 1 条 room event 或 snapshot
在线时长：1h、24h
```

服务端应该记录：tick 处理耗时、tick deadline miss、每玩家输入延迟、广播完成时间和积压帧数。

**C. 房间广播 fan-out**

```text
房间人数：2、10、50、100、200
房间数量：1、10、100、1,000
广播类型：单播、房间内广播、跨房间无广播
```

广播测试必须验证：

- 发送给房间 A 的消息不会到达房间 B；
- 每个玩家收到的 `room_id/tick/seq` 正确；
- 一个慢客户端不会无限制阻塞整个房间；
- 广播扇出不会导致 listener 的接收分流停止；
- 正常退出后，房间成员和连接计数归零。

**D. 战斗突发与慢客户端**

混合注入：

- 5% 玩家发送 10～50 倍正常速率的输入；
- 1% 玩家读取速度降低到正常速率的 1/10 或暂时停止读取；
- 10% 玩家同时触发大 snapshot；
- 其余玩家保持正常 tick。

这个模型用于验证单个慢连接不会占满共享任务、共享队列或房间广播资源。游戏服务器必须为连接、玩家、房间和全局分别设置可观测的队列上限。

#### 7.5.4 Listener 专项测试矩阵

| Case | 玩家数 | 房间 | tick | 行为 | FEC | 目的 |
|---|---:|---:|---:|---|---|---|
| G-L1 | 1 | 1 | 20Hz | 登录、心跳、退出 | off | 单玩家生命周期 |
| G-L2 | 100 | 10×10 | 20Hz | input + room broadcast | off | 基本分流和广播 |
| G-L3 | 1,000 | 100×10 | 30Hz | 稳态输入/快照 | off | listener 高并发 |
| G-L4 | 5,000 | 500×10 | 20Hz | 分批上线 + 稳态 | off | 连接洪峰 |
| G-L5 | 10,000 | 1,000×10 | 20Hz | 长连接、心跳 | off | 资源容量上限 |
| G-L6 | 1,000 | 100×10 | 60Hz | 小消息双向 tick | off | 调度和尾延迟 |
| G-L7 | 1,000 | 100×10 | 20Hz | 10% 大 snapshot | off / 10/3 | 分片、窗口、FEC |
| G-L8 | 1,000 | 100×10 | 20Hz | 1% 慢客户端 | off | 背压和隔离 |
| G-L9 | 1,000 | churn | - | 连接随机退出/重连 | off | peer 生命周期和回收 |
| G-L10 | 1,000 | 100×10 | 20Hz | netem 丢包/乱序 | 10/3 | 恢复和可靠性 |

每个 case 至少运行 10 分钟；G-L5、G-L8、G-L9 需要额外运行 1 小时。发布候选版本再对 G-L3 或实际目标容量的 80% 运行 24 小时。

#### 7.5.5 游戏服务器通过标准

- 玩家连接建立成功率和重连成功率单独统计；
- 已建立连接的可靠消息 100% 校验通过；
- `player_id/room_id/tick/seq` 不得串线、重复或倒退；
- 20Hz/30Hz/60Hz tick 的 deadline miss 必须统计，不能只看平均处理时间；
- 心跳超时、客户端主动关闭、服务端踢出和网络故障必须有明确状态；
- 单个慢客户端不能使其他玩家的 input 或 room broadcast 无限等待；
- 连接关闭后，`KcpListener` 能继续接受新连接；
- listener 的 peer demux 队列不会无限增长；
- 房间广播在扇出增加时，发送队列、RSS 和 P99 延迟必须有上限或明确降级策略；
- 重连必须创建新 KCP session，不能错误复用旧 session 的 SN/接收状态；
- 游戏进程优雅退出时，所有 listener、KCP connection 和后台任务都能在 drain timeout 内结束。

建议将“连接接受成功率”“首个 login 响应时间”“tick deadline miss率”“输入 P99/P999”“广播完成 P99/P999”“慢客户端队列峰值”“房间隔离错误数”作为游戏场景的一级指标。

#### 7.5.6 与现有测试的对应关系

现有 `kcpconn_listener` 已验证：单 listener 接受连接、客户端地址返回、多 peer 分流以及旧连接关闭后接受新客户端。这些是 G-L1 的基础，但还不足以覆盖游戏服务器场景。应在同一测试文件或独立的 `kcp_listener_game_test.rs` 中补充：

1. 100/1,000 个 client 同时向一个 listener 首次发包；
2. 每个 client 带唯一 `player_id`，server 回传包含该 ID 的 frame；
3. 多房间同时广播并验证房间隔离；
4. 正常 close、异常 drop、随机重连后继续服务；
5. 一部分连接停止读取，其他连接仍需完成 tick；
6. 关闭 listener 后，已有连接与新连接的行为分别符合 API 约定；
7. tokio 和 smol 使用完全相同的 workload、seed、超时和结果格式。

游戏场景测试不应默认放入每次 `cargo test`。建议分为：

```bash
# 快速 listener 回归
cargo test -p kcp-rs --features async-tokio --test kcpconn_listener

# 游戏 listener 压力（建议 release、单独报告）
cargo test --release -p kcp-rs --features async-tokio \
  --test kcp_listener_game_test -- --nocapture --test-threads=1

# smol 使用同一 workload 重新执行
cargo test --release -p kcp-rs --features async-smol \
  --test kcp_listener_game_test -- --nocapture --test-threads=1
```

若测试 harness 尚未实现，应将上述命令标记为待新增，而不是直接复制为 CI 命令。

## 8. 第四阶段：长时间稳定性测试

### 8.1 24 小时稳态

最小版本为 1 小时，发布候选版本为 24 小时：

```text
warm-up: 5 min
steady: 23 h 50 min
drain: 5 min
```

并发档位建议：

- C1：100、1000、目标容量的 80%；
- C2：每个 KCP 100 个并发流；
- C3：100 个 KCP × 每个 10 个流。

每 30 秒轮换一次 workload：

1. 10 秒小消息 burst；
2. 10 秒 bulk；
3. 5 秒双向同时写；
4. 5 秒新连接 churn；
5. 空闲等待，验证 keepalive/expiry；

### 8.2 Churn / reconnect

持续执行以下循环：

```text
建立 N 条连接 → 每条发送随机数量消息 → 一半正常 close
→ 一半立即 drop/进程级关闭 → 等待 1~5 秒 → 创建下一批
```

覆盖：每秒 1、10、100、500 次新连接；运行至少 30 分钟。重点观察：

- UDP listener 是否仍能 accept 新 peer；
- 同一 source address 的旧 session 是否被正确移除；
- KCP `conv`/SN 从新 session 开始时没有旧状态污染；
- 关闭后后台 input/flush task 是否退出；
- RSS、FD、线程/任务数是否随 churn 线性增长。

### 8.3 读写竞争与关闭竞态

对每个连接同时启动：

- reader：持续读取并验证消息；
- writer：持续写入并 flush；
- closer：随机在发送完成、发送中、读取中触发 close/drop；
- reconnect worker：重新建立新会话。

必须区分“预期的 I/O closed/aborted”与真正的数据错误，不能把所有关闭错误简单忽略。

## 9. 第五阶段：故障注入和恢复

### 9.1 Linux `tc netem`

故障注入只在专用测试机或 loopback namespace 中执行。开始前记录 qdisc，结束后恢复：

```bash
sudo tc qdisc add dev lo root netem loss 1% delay 20ms 5ms reorder 1% 25%
# 测试完成后
sudo tc qdisc del dev lo root
```

最小矩阵：

| 场景 | 注入 | FEC |
|---|---|---|
| N0 | 无丢包、无延迟 | off |
| N1 | loss 0.1% | off / 10/3 |
| N2 | loss 1% | off / 10/3 |
| N3 | loss 3% | 10/3 / 4/2 |
| N4 | delay 20ms ±5ms | off |
| N5 | reorder 1% | off / 10/3 |
| N6 | loss 1% + delay + reorder | 10/3 |
| N7 | 短时 100% loss 1~3 秒 | off / 10/3 |

验收重点不是“零重传”，而是：恢复后数据全部正确、连接不无故死亡、延迟和重传能回落，FEC 恢复计数与实际丢包方向一致。

### 9.2 进程和网络事件

至少测试：

- Go server 运行中 Rust client 重启；
- Rust server 运行中 Go client 重启；
- listener 关闭后不再 accept，但已有连接行为符合约定；
- server 短暂不可达后恢复；
- UDP socket 接收队列短时打满；
- CPU 被人为限速或单核运行；
- 达到 FD 上限前后的显式错误行为。

每个故障 case 都要记录：故障发生时间、预计影响窗口、恢复时间、丢失消息数、重复/乱序数、最终连接状态。

## 10. Go↔Rust 交叉测试矩阵

### 10.1 裸 KCP

| Server | Client | runtime | FEC | 目的 |
|---|---|---|---|---|
| Go | Go | Go | off | Go 基线 |
| Rust | Rust | tokio | off | Rust tokio 基线 |
| Rust | Rust | smol | off | Rust smol 基线 |
| Go | Rust | tokio | off | Rust client wire compatibility |
| Go | Rust | smol | off | smol client compatibility |
| Rust | Go | tokio | off | Rust server compatibility |
| Rust | Go | smol | off | smol server compatibility |
| Go | Rust | tokio/smol | 10/3、4/2 | FEC cross-check |
| Rust | Go | tokio/smol | 10/3、4/2 | FEC reverse cross-check |

每格至少覆盖 payload 1KB/64KB、open 500 RPS、closed concurrency 32 和 512。

### 10.2 完整 kcptun

已有 `test_e2e.sh` 提供完整基础矩阵。高并发扩展时选取代表组合，避免把全部 cipher×mode×SMUX 组合都跑 24 小时：

| 组合 | crypt | comp | mode | FEC | runtime |
|---|---|---|---|---|---|
| G0 | null | off | fast3 | off | Go↔Rust |
| G1 | aes | on | fast3 | 10/3 | Go↔Rust |
| G2 | aes-128-gcm | on | fast3 | 10/3 | Go↔Rust |
| G3 | sm4 | off | fast | 4/2 | Go↔Rust |
| G4 | none | off | normal | off | Go↔Rust |
| G5 | aes | on | fast2 | off | Rust tokio↔smol |

其中 G0 用于极限连接数，G1/G2 用于真实默认配置，G3/G4 用于边界兼容，G5 用于双 runtime 回归。

每个方向都要跑：Go client→Rust server、Rust client→Go server；每个方向都要验证 echo 内容，而不是仅检查进程存活。

## 11. 性能和资源指标

### 11.1 吞吐

```text
goodput = successfully_verified_payload_bytes / measurement_seconds
wire_rate = UDP_bytes_sent_and_received / measurement_seconds
retrans_rate = retrans_segs / out_segs
```

同时报告 payload 大小和消息率，避免只报 Mbps 导致小包场景被掩盖。

### 11.2 延迟

- open-loop：固定发送节奏，测量真实排队后的尾延迟；
- closed-loop：固定 in-flight，观察最大可持续吞吐；
- warm-up 不进入统计；
- P999 必须有足够样本，样本少于 1000 时标记为低置信度；
- 超时样本单独计数，不能从分位数中静默删除。

### 11.3 CPU、内存和调度

记录：

- 进程 CPU 平均/峰值、每核分布；
- RSS 起始/峰值/结束；
- 线程数、tokio/smol task 数（若 harness 能提供）；
- FD 数、UDP receive/send buffer 溢出；
- 每连接内存估算；
- SNMP send/receive queue 峰值；
- 长稳期间的 GC（Go）或 allocator 行为（Rust）。

关注曲线形态：并发增加后如果吞吐平台化但 RSS、队列、尾延迟持续增长，说明系统已进入排队/资源耗尽状态，即使短期没有数据错误也应标记为容量风险。

## 12. 结果判定等级

| 等级 | 条件 |
|---|---|
| PASS | 所有数据正确、无超时/死锁，资源在阈值内，指标可解释 |
| PASS-WITH-LIMIT | 数据正确，但达到明确容量上限；失败为显式资源错误，且已记录容量 |
| FAIL-CORRECTNESS | checksum、长度、顺序、重复、串流或 FEC 恢复错误 |
| FAIL-LIVENESS | 死锁、活锁、永久阻塞、close 后无法退出、listener 不再服务 |
| FAIL-STABILITY | RSS/FD/task 持续泄漏，或长稳指标无界增长 |
| FAIL-INTEROP | Go↔Rust 任一方向在对齐参数下不兼容 |
| INVALID | 环境不足、端口冲突、FD/权限/CPU 外部限制，未能得到有效结论 |

任何 `FAIL-CORRECTNESS`、`FAIL-LIVENESS` 或 `FAIL-INTEROP` 都不能以“重跑通过”直接关闭；必须保留第一次失败的日志并解释原因。

## 13. 推荐执行顺序

```text
S0 编译/环境检查
  ↓
S1 L0/L1 原生单测 + tokio/smol
  ↓
S2 现有 kcptun stress_test（含 ignored）
  ↓
S3 Go→Go、Rust→Rust 裸 KCP基线
  ↓
S4 Go↔Rust 裸 KCP 双向矩阵
  ↓
S5 C1/C2/C3 高并发阶梯
  ↓
S6 tc netem 丢包/延迟/乱序 + FEC
  ↓
S7 1h/24h 稳定性、churn、重启恢复
  ↓
S8 完整 kcptun 代表配置 + Go↔Rust 回归
```

若 S3 基线失败，不应继续解释 S5 的吞吐结果；若 S5 出现数据错误，应先降级到最小复现 case，再进行故障注入。

## 14. 测试报告模板

```markdown
# kcp-rs / kcp-go Test Report

- Commit:
- Date / host / OS / CPU / RAM:
- Rust version:
- Go version and kcp-go commit:
- Runtime: tokio | smol | Go
- Profile: C1 | C2 | C3
- FEC / mode / MTU / sndwnd / rcvwnd:
- Payload profile:
- Warm-up / duration / drain:

## Summary

| Case | Direction | Concurrency | Sent | OK | Timeout | Retrans rate | P99 | RSS peak | Result |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|

## Correctness

- checksum errors:
- duplicates:
- out-of-order:
- truncated streams:
- first failure:

## Resource trend

- CPU:
- RSS:
- FD/task count:
- queue peaks:

## SNMP / recovery

- in/out packets and segments:
- retrans / fast retrans / lost:
- FEC recovered / FEC errors:
- connection open/close counts:

## Failures and interpretation

## Artifacts
```

## 15. 当前覆盖缺口和后续落地建议

当前仓库已有较好的协议正确性和中等规模并发基础，但以下项目仍应补齐后再宣称“高并发极限测试完成”：

1. 可配置的 1k/10k C1/C2/C3 harness；
2. 统一的 Rust/Go JSONL 或 CSV 结果采集；
3. Go 裸 `UDPSession` 与 Rust `KcpConn` 的双向高并发驱动；
4. `tc netem` 自动化和清理保护；
5. 1 小时/24 小时长稳 runner，包含 RSS/FD/task 趋势；
6. 统一的失败样本、SNMP 快照和进程 core/log 收集；
7. 将“默认快速回归”和“release stress/nightly”分离，避免普通 CI 被长测试阻塞。

建议第一步只新增一个共享 concurrency harness，同时支持 Rust server/client 和 Go server/client；第二步再把它接入 `make stress-high` 与 nightly。这样同一套 workload、校验协议和结果格式才能真正比较 Rust 与 Go，而不是比较两套不同测试程序的输出。
