# Plan: kcp-rs 全量测试与 kcp-go 性能对比

> **Canonical path (git):** `docs/superpowers/plans/2026-08-04-KCP_RS_TEST_STRATEGY.md`

| Field | Value |
|-------|-------|
| Status | draft |
| Created | 2026-08-04 |
| Scope | 裸 KCP 层的 Rust↔Rust 性能、P99/P999、吞吐、高并发、可靠性、边界、长稳，以及与 kcp-go v5 的同口径对比 |
| Out of scope | kcptun 完整隧道栈中的加密、Snappy、SMUX、QPP 性能；公网跨地域结果；把单台开发机的绝对性能直接作为所有平台的发布门槛 |
| Related | `kcp-rs/AGENTS.md`, `kcp-rs/README.md`, `kcp-rs/tests/`, `kcp-rs/examples/latency_p99.rs`, `tests/kcp-go-latency/main.go`, `bench/run_p99.sh`, `bench/LATENCY_P99_REPORT.md` |

## 1. 目标

建立一套可重复、可比较、可自动判定的裸 KCP 测试体系，回答五类问题：

1. kcp-rs 在 tokio、smol 以及不同参数下的吞吐、P99、P999 和资源成本是多少？
2. 相同 KCP 参数和负载下，kcp-rs 与 kcp-go v5 的性能差异是多少？
3. 连接数、窗口、丢包、时延和突发流量增加时，系统在哪个点开始排队、超时或失去公平性？
4. 丢包、乱序、重复、抖动、FEC 恢复和长时间运行时，数据是否仍然可靠、顺序且逐字节一致？
5. 畸形包、极限 MTU、最大分片、窗口边界、序号回绕和关闭竞态是否会 panic、死锁、泄漏或破坏 wire compatibility？

本方案只测 `UDP → [可选 FEC] → KCP → KcpConn`。完整 kcptun 隧道继续由
`bench/tunnel_p99.sh`、`bench/run_bench.sh`、`make stress` 和 `test_e2e.sh` 负责，避免把
SMUX、压缩或加密开销误归因到 KCP。

## 2. 当前基线与缺口

| 能力 | 当前状态 | 本方案动作 |
|------|----------|------------|
| Rust 同步可靠性 | 已有 `data_correctness.rs`：clean、20% loss、loss+dup+reorder+delay、FEC 恢复 | 参数化损伤模型，补 burst loss、非对称链路和长稳 |
| Rust 异步完整性 | 已有 tokio/smol `kcpconn_integrity.rs` | 扩展多连接、并发读写、取消与关闭竞态 |
| Listener | 已有 accept、双 peer demux、重连、timeout | 扩展 100/1k/5k peer 与资源回收 |
| Rust P99/P999 | 已有 `latency_p99.rs`，支持开放模型和闭环模型 | 扩展负载矩阵、原始样本/直方图和机器可读结果 |
| Go 对比 | 已有 `tests/kcp-go-latency/main.go` 与 7 个互操作组合 | 统一节拍、消息格式、参数、统计和失败输出 |
| 最大吞吐 | 已有固定 payload、固定并发的闭环 req/s | 增加 goodput、包速率、双向吞吐、连接数和 payload sweep |
| 边界/畸形输入 | 有部分单元测试 | 建立系统矩阵并加入 property/fuzz 测试 |
| 长稳/资源 | 没有裸 KCP 专用 soak 门禁 | 新增 RSS、FD、任务数、队列和 SNMP 趋势检查 |

已有 `bench/run_p99.sh` 可继续作为快速基线，但正式对比前必须处理两个公平性问题：

- Rust 与 Go 当前的开放模型节拍实现不同；Go 端落后时会把 `nextSend` 重置到当前时间，Rust
  端会保留原始发送计划。两端必须统一为同一种“追赶”或“记录 missed offer”语义。
- 两端需要相同的 sender/receiver 分离方式、请求序号和完整性校验；不能只比较各自探针的
  `ok` 数量而不确认返回内容和请求 ID。

## 3. 测试原则与统一口径

### 3.1 构建与环境

- Rust 使用 `cargo build --release`；tokio 与 smol 分别构建、分别保存二进制。
- Go 使用锁定的 `tests/kcp-go-latency/go.mod`/`go.sum`，报告中记录 kcp-go 版本。
- 每次结果记录 commit、dirty 状态、OS、内核、CPU、核数、内存、Rust/Go 版本和命令行。
- 正式比较在空闲机器运行；记录 load average、CPU 频率/温度和后台负载。
- Linux 网络损伤使用独立 network namespace + `tc netem`；macOS 快速测试使用进程内确定性
  损伤 transport。网络损伤必须在报告中注明作用方向。
- 同一组 Rust/Go 对比采用交错顺序，例如 `Rust → Go → Go → Rust`，至少 5 轮，降低温度、
  缓存和系统漂移造成的偏差。

### 3.2 固定 KCP 公平配置

默认对比配置：

| 参数 | 值 |
|------|----|
| conv | `0x00C0_FFEE` |
| mode | Fast3：`nodelay=1, interval=10, resend=2, nc=1` |
| MTU | 1350 |
| sndwnd / rcvwnd | 512 / 512 |
| stream | true；message mode 单独成组 |
| acknodelay | true |
| FEC | 默认关闭；10/3 单独成组 |
| 加密/压缩/SMUX | 全部不进入裸 KCP 测试 |

任一参数不能在 Rust 和 Go 两边表达时，该用例不得进入“性能胜负”表，只能列为功能或实现特有测试。

### 3.3 延迟

- P99/P999 使用开放模型固定 offered RPS，发送节拍不等待响应，避免 coordinated omission。
- 每个请求携带 `sequence + send_timestamp + payload_checksum`，回声端逐字节原样返回。
- 预热样本不进入统计；正式测试默认预热 30 秒、测量 10 分钟。
- P99 至少 10,000 个成功样本；P999 至少 100,000 个成功样本，正式基线建议 1,000,000 个。
- 输出 P50/P90/P95/P99/P999/max、offered/completed/timeout、排队深度和 missed offers。
- 百分位必须由一轮全部原始样本或同精度 HDR Histogram 合并后计算，禁止对多个 P99 再求平均。
- P99/P999 仅由成功响应计算时，必须同时显示失败率；有超时时不能用“成功样本 P999”掩盖失败。

### 3.4 吞吐

- 最大吞吐使用闭环固定并发，分别测 `req/s`、payload goodput MiB/s、UDP pps 和 wire bytes。
- 延迟与最大吞吐分开跑；“低 RPS 下的延迟”不能作为吞吐结论。
- 测量单向 bulk、双向 full-duplex、短请求 echo 三种负载。
- 每档并发先做阶梯升压，吞吐不再增长且 P99/错误率明显上升时记录饱和点。

### 3.5 正确性与资源

- 所有 payload 使用确定性随机数据；校验总长度、顺序、逐字节内容和独立 checksum。
- 保存测试 seed；失败必须能用同一个 seed 单用例复现。
- 每轮采集 KCP SNMP delta：输入/输出段、重传、fast retransmit、重复段、FEC recovered/error。
- 高并发和长稳记录 CPU、RSS、FD、线程/任务数、listener peer 数、send/receive queue 高水位。
- 不允许 panic、死锁、无界队列增长或测试超时后残留进程。

## 4. 分层测试矩阵

### 4.1 L0：单元、协议和属性测试

| ID | 主题 | 用例 |
|----|------|------|
| U-01 | Header | 24B LE encode/decode；0..23B 短包；声明长度大于实际；尾部多 segment |
| U-02 | Command | PUSH/ACK/WASK/WINS；0、80、85、255 未知命令 |
| U-03 | conv | `0`、`u32::MAX`、mismatch；错误优先级与 Go 一致 |
| U-04 | MTU/MSS | MTU 49/50/1350/1400；运行中调整；任何输出 datagram 不超过 MTU |
| U-05 | Fragment | 1B、MSS-1、MSS、MSS+1、255×MSS、255×MSS+1；最后一个必须返回 `TooManyFragments` |
| U-06 | Window | snd/rcv wnd 0、1、32、512、32768；远端窗口 0 后 WASK/WINS 恢复 |
| U-07 | Timer | interval 0/9/10/5000/5001；RTO min/max；32-bit timestamp 回绕 |
| U-08 | Sequence | SN/UNA 接近 `u32::MAX` 的比较、ACK、重复包、窗口外包 |
| U-09 | Stream mode | 小写合并、跨 MSS、切换模式、空写 `NoData`、recv buffer 太小 |
| U-10 | FEC | shard 组合 1/1、3/2、10/3；短 header、非法 SIZE、重复 shard、超出可恢复丢失数 |
| U-11 | Pool | acquire/release/reset、容量上限、并发回收后字段不泄漏 |
| U-12 | Fuzz/property | 任意字节输入不得 panic；encode→decode round-trip；损伤链路最终有序且内容一致 |

建议新增：

- `kcp-rs/tests/kcp_boundaries.rs`：公开 API 边界和错误语义。
- `kcp-rs/tests/kcp_properties.rs`：固定 seed 的 property test；失败 seed 写入输出。
- `kcp-rs/fuzz/fuzz_targets/kcp_input.rs`、`fec_decode.rs`：畸形包 fuzz，语料包含 Go 生成帧。

### 4.2 L1：Rust↔Rust 性能

组合：

| 维度 | 值 |
|------|----|
| Runtime | tokio↔tokio、smol↔smol；tokio↔smol 只做互操作/功能验证 |
| Payload | 1B、64B、512B、1KiB、1350B、4KiB、26KiB、64KiB、256KiB、1MiB bulk |
| 模式 | Normal、Fast、Fast2、Fast3 |
| FEC | 0/0、10/3 |
| Stream | on、off |
| 网络 | loopback clean；10ms/50ms RTT；1%/5% loss |

核心用例：

| ID | 负载 | 输出 |
|----|------|------|
| RR-P01 | 开放模型 latency sweep：RPS 100/500/1k/5k/10k，自动停止在过载后两档 | P99/P999、成功率、missed offers |
| RR-P02 | 闭环并发 1/8/32/128 的短请求 echo | max req/s、goodput、饱和点 |
| RR-P03 | 单连接 1GiB 单向 bulk | MiB/s、CPU、wire amplification |
| RR-P04 | 单连接双向各 1GiB | 双向 MiB/s、公平性、P99 |
| RR-P05 | FEC 0/0 vs 10/3，clean/5% loss | goodput、恢复数、CPU 开销 |
| RR-P06 | 1/10/100/1000 个连接均分总 RPS | aggregate/per-conn P99、Jain fairness index |

### 4.3 L2：kcp-go 对应测试

每个正式性能 cell 至少包含：

1. kcp-rs(tokio) ↔ kcp-rs(tokio)
2. kcp-rs(smol) ↔ kcp-rs(smol)
3. kcp-go ↔ kcp-go
4. kcp-rs(tokio) client → kcp-go server
5. kcp-go client → kcp-rs(tokio) server
6. kcp-rs(smol) client → kcp-go server
7. kcp-go client → kcp-rs(smol) server

Go 与 Rust harness 必须使用同一请求帧、相同 payload 数据、相同发送日程算法和相同结束条件。
双向交叉结果用于区分 client send path 与 server receive/echo path，不能只用 Go↔Go 和 Rust↔Rust
两个总数推断原因。

| ID | 对比 | 目的 |
|----|------|------|
| GO-P01 | Fast3 clean，payload/RPS sweep | 基础 P99/P999 和饱和点 |
| GO-P02 | 闭环 concurrency 1/8/32/128 | 最大 req/s 和 load-at-capacity 延迟 |
| GO-P03 | 1GiB bulk，单向/双向 | 流式 goodput 与 CPU/byte |
| GO-P04 | loss 1/5/10%，RTT 10/50/100ms | 重传与尾延迟差异 |
| GO-P05 | FEC 10/3 | wire compatibility、恢复能力和成本 |
| GO-P06 | mode Normal/Fast/Fast2/Fast3 | 模式曲线是否一致 |
| GO-P07 | 1/100/1000 connections | listener demux、调度和内存差异 |

### 4.4 L3：高并发与公平性

连接阶梯：`1 → 10 → 100 → 500 → 1000 → 5000`；10,000 作为有足够 FD、端口和内存时的扩展目标。

| ID | 场景 | 通过条件 |
|----|------|----------|
| HC-01 | 连接风暴：每秒 100/500/1000 个新 peer | 无 accept 丢失、死锁、残留 peer；建立延迟有界 |
| HC-02 | 1k 长连接，每连接 1 RPS | 100% 内容正确；每连接均有进展；P99 与聚合吞吐可解释 |
| HC-03 | 1k 长连接，10% 热连接承担 90% 流量 | 冷连接不饿死；报告 per-conn P99 与公平指数 |
| HC-04 | 100/1k 连接同时 256KiB burst | 无无界队列、OOM、超过 deadline 的永久挂起 |
| HC-05 | 连接反复创建/关闭 100k 次 | FD、任务、peer map、RSS 回到稳态区间 |
| HC-06 | 并发 read/write/flush/close/cancel | 无 panic、use-after-close、双活 flush、任务泄漏 |

每档至少维持 5 分钟。判定上限前先检查压测端 CPU、UDP socket buffer、端口/FD 和网卡是否成为瓶颈。

### 4.5 L4：可靠性和网络损伤

损伤维度：

| 维度 | 值 |
|------|----|
| 随机丢包 | 0%、1%、5%、10%、20%、30% |
| burst loss | 每 1/5/30 秒连续丢 3/10/50 个包；另加 Gilbert-Elliott 模型 |
| RTT | 0、10、50、100、300、500ms |
| jitter | 0、5、20、100ms |
| reorder | 0%、1%、5%、20%，gap 1/3/10 |
| duplicate | 0%、1%、5%、20% |
| 带宽 | 1、10、100、1000Mbit/s，配合有限 queue |
| 方向 | 对称、仅 data 方向、仅 ACK 方向 |

| ID | 场景 | 断言 |
|----|------|------|
| REL-01 | 单项损伤 sweep | 数据逐字节一致、顺序一致、无重复交付 |
| REL-02 | loss+delay+jitter+reorder+duplicate 组合 | 固定 seed 可重复；deadline 内完成或明确 dead-link |
| REL-03 | ACK-only loss 5/20/50% | 无静默数据损坏；重传和完成时间合理增长 |
| REL-04 | 10/3 FEC 丢 1/2/3/4 shards | 可恢复范围内成功；超范围不伪造数据，交给 KCP 重传 |
| REL-05 | 网络中断 1/10/60 秒后恢复 | 允许窗口/RTO 收敛后继续，无永久停滞 |
| REL-06 | peer 进程 kill/restart | 老 session 有界失败；新 session SN 从 0 并可正常传输 |
| REL-07 | 双向各传 10GiB | 长流 checksum 一致、计数器无不合理溢出 |

### 4.6 L5：长稳与资源泄漏

- 1 小时 smoke soak：100 connections、clean + 1% loss，持续双向随机 payload。
- 8 小时 nightly soak：1000 connections，周期性 5% loss、100ms RTT、10 秒断网。
- 24 小时 release qualification：Rust↔Rust 与 Rust↔Go 各跑一次代表场景。
- 每分钟采集 RSS、FD、线程/任务数、peer/session 数、queue 水位和 SNMP delta。
- 稳态窗口线性回归斜率必须接近 0；RSS/peer/FD 不能随已关闭连接数持续线性增长。

## 5. 实现方案

### 5.1 统一 harness

在现有代码上演进，不另起一套重复的延迟探针：

1. 扩展 `kcp-rs/examples/latency_p99.rs`：支持全部 KCP 参数、payload seed、连接数、单向 bulk、
   双向 bulk、JSONL/HDR 输出和 SNMP delta。
2. 对齐 `tests/kcp-go-latency/main.go`：相同请求帧、sender/receiver 拆分、发送节拍、timeout、
   参数、JSON schema 和 checksum；读缓冲不能把 64KiB 作为隐式 payload 上限。
3. 新增 `bench/run_kcp_matrix.py`：构建二进制、分配端口、启动/清理进程、应用 netem、运行
   7 组合矩阵、保存原始结果、计算多轮汇总和回归判定。
4. 新增 `bench/kcp_test_matrix.toml`：把 smoke/nightly/soak 配置与代码分离，禁止在脚本里复制参数。
5. 结果写入 `target/kcp-bench-results/<timestamp>/`，包含 `environment.json`、`cases.jsonl`、
   `summary.md`、原始 histogram、stderr 和进程退出状态；生成物不提交 Git。

统一结果至少包含：

```json
{
  "schema": 1,
  "case_id": "GO-P01",
  "client": "rust-tokio",
  "server": "go",
  "payload_bytes": 1024,
  "offered_rps": 5000,
  "offered": 3000000,
  "completed": 3000000,
  "timeouts": 0,
  "p99_us": 0,
  "p999_us": 0,
  "goodput_mib_s": 0,
  "cpu_seconds": 0,
  "max_rss_bytes": 0,
  "seed": 1
}
```

### 5.2 正确性与 fuzz

1. 抽取可复用的 deterministic impairment transport，供同步 KCP 和异步 KcpConn 共用。
2. 新增边界测试、损伤属性测试、listener 高并发测试和关闭竞态测试。
3. 加入 `cargo-fuzz` targets；固定最小语料包含合法 PUSH/ACK/WASK/WINS、短包、坏 length、
   conv mismatch、重复 segment、FEC data/parity。
4. fuzz 发现的每个 crash/hang 都转成普通 deterministic regression test 后再修复。

### 5.3 自动化层级

| 层级 | 触发 | 内容 | 预算 |
|------|------|------|------|
| PR | 每次改动 | 现有 `make gate`、边界测试、短 Rust↔Rust/Go 互操作 smoke | ≤10 分钟 |
| Nightly | 每晚 | 全参数 correctness、100/1k 连接、10 分钟 perf matrix、1 小时 soak | ≤2 小时 |
| Weekly | 每周/发布前 | Linux netem 全矩阵、5k/10k 连接、8/24 小时 soak、30 分钟 fuzz | 独立性能机 |

性能测试不与普通共享 CI runner 的随机邻居噪声混为硬门禁。PR 只校验 harness 和明显数量级回归；
正式性能 gate 在固定机器执行。

## 6. 验收标准

### 6.1 硬正确性门禁

- 所有可完成用例 `completed == offered`，`timeouts == 0`，checksum/length/order 100% 正确。
- 明确设计为 dead-link/timeout 的用例必须返回预期错误，不得 panic 或永久挂起。
- Rust↔Go 双向互操作通过，segment、FEC、mode、MTU/window 语义一致。
- fuzz/property 测试不得出现 panic、越界、死锁或不一致；失败 seed/语料可重放。
- 高并发关闭后 FD、peer/session 和任务回到基线容差内。

### 6.2 性能回归门禁

先在固定性能机建立 5 轮基线，再冻结门槛。建议初始规则：

- 同实现相对基线：吞吐中位数下降超过 5% 为失败。
- 同实现相对基线：P99 或 P999 中位数恶化超过 10%，且 5 轮中至少 3 轮复现，为失败。
- 1k 连接代表用例：max RSS 或 CPU/byte 增长超过 10% 为失败。
- Rust 对 Go 的第一阶段目标：tokio raw KCP goodput 不低于 Go 的 90%，P99 不高于 Go 的
  1.25 倍，P999 不高于 1.5 倍。该目标在首轮完整矩阵后按可重复证据调整，不直接写成跨机器常量。
- 任何性能结论必须同时给出绝对值、Rust/Go 比值、5 轮离散度和失败率。

## 7. 实施顺序

1. **P0：冻结口径**——统一 frame、JSON schema、发送节拍、统计与失败语义；验证现有 7 组合。
2. **P1：Rust↔Rust 性能**——payload/RPS/concurrency sweep、bulk、双向、tokio/smol、结果归档。
3. **P2：Go 对比**——实现完全对应的 Go 参数和工作负载，完成 Go↔Go 与双向交叉矩阵。
4. **P3：高并发**——listener 100/1k/5k peer、连接风暴、公平性、关闭回收和资源指标。
5. **P4：可靠性**——确定性 impairment + Linux netem，覆盖随机/突发/非对称损伤和断网恢复。
6. **P5：边界与 fuzz**——协议边界、序号/时间回绕、畸形包、FEC 边界、属性与 fuzz。
7. **P6：自动化**——PR smoke、nightly matrix、weekly soak，冻结固定性能机 baseline。

每一阶段先让 Rust↔Rust harness 自证正确，再实现相同的 kcp-go case，最后才比较性能；若
Rust↔Go checksum 或协议语义不一致，性能结果一律无效。

## 8. 当前可直接运行的基线命令

```bash
# kcp-rs 全部同步 + tokio + smol 测试
bash kcp-rs/test.sh

# 裸 KCP：Rust↔Rust、Go↔Go、双向 Rust↔Go 的 P99/P999 + 闭环吞吐
RPS=500 WARMUP=5 DURATION=60 SIZE=26624 CONCURRENCY=32 \
  bash bench/run_p99.sh

# segment/KCP/FEC 改动后的完整产品互操作保护
make e2e

# flush/lock/session 相关高并发保护
make stress

# 仓库必跑门禁
make gate
```

上述命令是当前基线，不等于本方案的全部测试已经实现。P0–P6 完成后，正式入口应收敛为：

```bash
python3 bench/run_kcp_matrix.py --profile smoke
python3 bench/run_kcp_matrix.py --profile nightly
python3 bench/run_kcp_matrix.py --profile soak
```

