# 调查文档：smol xtea/cast5 offload 阈值 × Snappy 按 cipher 动态阈值

| 字段 | 内容 |
|------|------|
| 日期 | 2026-07-29 |
| 状态 | **待验证（VERIFY）— 禁止在验证门未通过前改代码** |
| 流程 | 提出问题 → 分析问题 → **验证问题（pprof / SNMP / bench）** → 修改问题 → 回归 |
| 范围 | 仅两项**未证实**假设；不覆盖已落地的 ACK headerless / 通用 encrypt offload 等改动 |
| 硬约束 | Go wire 兼容；e2e/stress 绿；一次只改一类策略且可回滚 |

> **纪律（本仓库 CLAUDE / flamegraph-perf skill）**  
> 性能改动必须 **证据门控**：先用 pprof + 可观测计数器证明假设，再动刀。  
> 本文档的「修改方案」仅在对应 **G 门** 全部 PASS 后才可实施；任一 FAIL 则假设作废或改写，**不得**按直觉调阈值。

---

## 0. 文档怎么用

| 读者动作 | 要求 |
|----------|------|
| 审核 | 只审假设、观测指标、通过/否决标准；不要先争论「16KiB 好不好」 |
| 执行验证 | 严格按 §3 命令矩阵采集；把结果填进 §3.6 记录表 |
| 决定改码 | 仅当 §3.6 对应行「结论」为 **支持修改** |
| 改码后 | 按 §5 回归；吞吐/ profile 无改善则 **revert** |

**禁止**：未跑 pprof/SNMP 就合入阈值改动；把「理论 μs 估算」当成已验证事实。

---

## 1. 提出问题（Problem）

### 1.1 现象（来自 bench / Q1·Q2，需本地复测确认）

| ID | 场景 | 报告中的相对表现 | 运行时 | 是否已有落地修复 |
|----|------|------------------|--------|------------------|
| P-A | salsa20/comp、xor/comp 等 fast-cipher + Snappy | 历史报告严重落后 Go；comp 路径尤其差 | 主要 tokio | 部分：ACK headerless、encrypt 大 batch offload 等（**须重新 bench**） |
| P-B | xtea / cast5（尤其 smol） | 报告 smol 明显弱于同场景 Go / 弱于 tokio | smol | **否** — 本文假设 H1 |
| P-C | Snappy 固定 64KiB 才 `cpu_block` | 怀疑与 fast cipher 内联加密叠加阻塞 worker | tokio 为主 | **否** — 本文假设 H2 |

> 数字以本机 `bench_results.json` / 新一轮 `bench_rust_vs_go.py` 为准；Q1/Q2 表格只作 **问题线索**，不作验收基线。

### 1.2 要回答的问题（必须可证伪）

1. **H1**：在 **smol** 上，对 **xtea/cast5** 使用与通用 CFB 相同的 encrypt offload 阈值（≥4 pkt 或 ≥4KiB），是否导致 **调度开销 > 加密收益**，从而压低吞吐？
2. **H2**：在 **fast cipher / AEAD + comp** 下，Snappy 仅在 ≥64KiB 才 offload，是否仍导致 **compress 与 encrypt 双内联** 长时间占用 async worker，从而饿死 UDP/ACK 路径？

### 1.3 非目标（本轮不解决）

- 重写 xtea/cast5 算法、换库、NEON 汇编
- macOS `writev` / 批量 UDP 内核路径
- 再改 salsa/xor wire 格式（若已修 ACK headerless，本轮只 **复测** 是否仍落后）
- 在无 profile 证据时「顺便」调所有 CFB 阈值

### 1.4 成功定义（验证阶段 vs 修改阶段）

| 阶段 | 成功标准 |
|------|----------|
| 验证 | 每个假设有 **支持 / 否定 / 证据不足** 三选一结论 + 原始 pprof/SNMP 附件路径 |
| 修改（仅支持时） | 目标场景吞吐相对 baseline **可复现提升**（建议 ≥5% 或关闭相对 Go 的明显 gap），且无 e2e/stress 回归；非目标场景无 >3% 回退 |
| 否定假设 | 文档结案为「不改」；避免负优化 |

---

## 2. 分析问题（Analysis — 假设，非结论）

### 2.1 相关代码位置（只读分析用）

| 组件 | 路径 | 与假设关系 |
|------|------|------------|
| encrypt offload 决策 | `kcp-rs/src/crypto_buf.rs` → `should_cpu_block_encrypt` | H1：CFB 统一 ≥4 / 4KiB；xtea/cast5 无 smol 特例 |
| compress offload 决策 | 同文件 → `should_cpu_block_compress` | H2：固定 `>= 65536` |
| 调用方 flush | `kcptun-client/src/main.rs`、`kcptun-server/src/main.rs` Phase compress / encrypt | 实际 offload 发生处 |
| `cpu_block` 实现 | `kio-rs`（tokio / smol 后端） | H1 调度成本来源 |
| 可观测 | `SNMP.encrypt_inline` / `encrypt_offload` / `empty_flush`；`--snmplog`；SIGUSR1 | 验证 offload 比例 |
| pprof | `kpprof-rs`；`bench/profile_rust_go_pprof.sh`；`make profiling-bins` | CPU 栈证据 |

### 2.2 假设 H1（smol × xtea/cast5 offload 过密）

**叙述（可证伪）**

在 smol 上，xtea/cast5 单次/小 batch 加密 CPU 时间 **往往小于** 一次 `cpu_block` 跨线程投递+唤醒成本；当前「≥4 包或 ≥4KiB 就 offload」会使 **大量 flush 付调度税**，表现为：

- pprof 中 `cpu_block` / channel / worker 相关帧占比异常高，而 cipher 本体占比相对不高；或
- `EncryptOffload / (EncryptInline+EncryptOffload)` 很高，但吞吐低于「更少 offload」的对照。

**机制草图**

```
flush → should_cpu_block_encrypt(true for xtea@≥4pkts)
     → kio::cpu_block(encrypt_batch)
     → async_channel + smol blocking pool wake
     → 若 encrypt_batch 工作量 < 调度，净吞吐下降
```

**替代解释（必须同时考虑）**

| 编号 | 替代因 | 若成立则 H1 不成立或次要 |
|------|--------|---------------------------|
| A1 | xtea/cast5 **算法本身**慢，主导 cum% | 应优化实现或接受 gap，而非少 offload |
| A2 | 锁 / KCP flush / SMUX 主导 | 改 offload 无效 |
| A3 | comp 路径 Snappy 主导 | 更接近 H2 |
| A4 | tokio 同场景也慢 | 非 smol 调度特有 → 否定「smol 专用」叙事 |
| A5 | bench 噪声 / 未 release / 未对等 Go 配置 | 先修实验再谈假设 |

**理论量级（仅指导实验设计，不能当证据）**

- 粗算：若单次 offload 调度 ~数十 μs，而小 batch CFB-8 加密也在相近量级，则 **小 batch offload 可能为负**；大 batch 仍可能为正。
- 因此 H1 的「修改形态」若被验证，更可能是 **提高** xtea/cast5 在 smol 上的阈值，而不是「永远 inline」。

### 2.3 假设 H2（Snappy 64KiB 阈值对 fast cipher 过高）

**叙述（可证伪）**

在 xor/salsa/（及部分 AEAD）+ **comp** 下，许多 flush 的明文 **< 64KiB**，Snappy **内联**；同时若 encrypt 也内联（小 batch），同一 async worker 连续执行 compress+encrypt，延迟 UDP recv/ACK → 重传/窗口空转 → 吞吐崩溃。

**机制草图**

```
SMUX drain → Snappy inline (<64KiB) → KCP flush → encrypt inline (小 batch)
         → 同一 worker 长时间不 yield
         → UDP reader/ACK 饿死 → retransmit → thr 下降
```

**与已做工作的关系**

- 若 encrypt 侧已对 salsa/xor/AEAD **大 batch** offload，H2 的「双内联」窗口会缩小，但 **中等大小** 明文（例如 16–64KiB）仍可能：compress 内联 + encrypt 视 batch 而定。
- H2 **不能**用「理论双内联很可怕」直接改码；必须看：
  1. 该场景是否仍显著慢于 Go / 慢于 no-comp；
  2. pprof 是否仍见 `FrameEncoder` / snappy / compress 路径与 encrypt 同栈长时间占用；
  3. 临时 **仅** 降低 compress 阈值的 **A/B 实验**（可用环境变量或本地 patch，验证完可丢）是否提升 thr。

**替代解释**

| 编号 | 替代因 | 含义 |
|------|--------|------|
| B1 | 仍是 ACK/wire 错误或重传 | 先看 SNMP retrans / InCsumErrors，不是阈值 |
| B2 | encrypt 仍几乎全 inline | 应先看 `encrypt_offload` 比例，而非只降 Snappy |
| B3 | Snappy 实现/CRC 成本本身 | 降阈值只是把同样工作换线程，可能无净收益甚至负收益 |
| B4 | 64KiB 已接近最优 | pprof 显示 snappy 占比低 → **否定 H2** |

### 2.4 假设依赖关系

```
先复测 thr 与 SNMP
    │
    ├─ P-B 不存在（smol xtea/cast5 已不落后）→ 关闭 H1，不改
    ├─ P-A 不存在（fast+comp 已不落后）→ 关闭 H2，不改
    │
    └─ 落后仍在
           ├─ pprof + offload 比 支持 H1 → 才考虑 smol xtea/cast5 阈值
           ├─ pprof + 临时阈值 A/B 支持 H2 → 才考虑 compress 按 cipher 下调
           └─ 指向 A1/B1… → 另开问题单，不在本方案改阈值
```

---

## 3. 验证问题（Verification — 本阶段核心，先于任何合入）

### 3.1 实验原则

1. **同一机器、同一 `BENCH_DATA_MB`、同一 key/mode/wnd、release 或 profiling 对照说明清楚**。
2. 每个场景至少 **3 次** thr 取中位数；记录 commit / 是否含未合入 patch。
3. pprof 与 thr 尽量同配置；注明 runtime（tokio default vs smol feature）。
4. 过滤 I/O park：`go tool pprof -top -ignore='Inner::park|park_thread|epoll|kqueue'`（按实际栈名微调）。
5. **验证用临时手段**（env、本地一行阈值、或 debug 计数）优先于直接 PR 策略代码。

### 3.2 基线与对照矩阵

| 实验 ID | Runtime | Crypt | Comp | 目的 |
|---------|---------|-------|------|------|
| E0 | tokio | null | no-comp | 控制：数据面基线 |
| E1 | tokio | xtea | no-comp | H1 对照：无 Snappy |
| E2 | tokio | cast5 | no-comp | 同上 |
| E3 | **smol** | xtea | no-comp | **H1 主场景** |
| E4 | **smol** | cast5 | no-comp | **H1 主场景** |
| E5 | smol | xtea | **comp** | H1+H2 交叉 |
| E6 | smol | cast5 | **comp** | 交叉 |
| E7 | tokio | salsa20 | comp | **H2 主场景** |
| E8 | tokio | xor | comp | H2 |
| E9 | tokio | aes-128-gcm | comp | H2 扩展 |
| E10 | tokio | salsa20 | no-comp | H2 对照：无压缩 |
| E-Go-* | Go | 与上对称 | 对称 | 相对 gap |

**构建**

```bash
# thr 用 release（与日常 bench 一致）
make release
make release-smol   # 或项目惯用 smol release 目标

# pprof 用 profiling + pprof
make profiling-bins
# smol profiling：按 Makefile 实际 target（需在记录中写明命令）
```

**吞吐（示例）**

```bash
# 按仓库现有脚本；以下为逻辑占位，执行时对齐 bench_rust_vs_go.py / make bench 参数
python3 bench_rust_vs_go.py --runs 5 --size 1048576  # 或项目等价入口
# 务必覆盖：xtea/cast5 × smol × comp/no-comp；salsa20/xor/gcm × tokio × comp
```

### 3.3 观测指标

#### 3.3.1 吞吐与正确性

| 指标 | 来源 | 用途 |
|------|------|------|
| MB/s 中位数 | bench | 是否存在 P-A/P-B |
| vs Go 比值 | 同机 Go | gap 是否仍在 |
| e2e / 数据 md5 | 现有脚本 | 排除错误当慢 |

#### 3.3.2 SNMP（offload 行为）

启用 snmp 收集（`--snmplog` + 正 `snmpperiod`，或 SIGUSR1 dump，以当前 CLI 为准），记录：

| 计数 | 含义 |
|------|------|
| `EncryptInline` | 内联 encrypt batch 次数 |
| `EncryptOffload` | `cpu_block` encrypt 次数 |
| `EmptyFlush` | 空 flush（辅助） |
| 重传/校验相关 Go 兼容计数 | 排除 B1 |

定义：

\[
r_{\mathrm{off}} = \frac{\mathrm{EncryptOffload}}{\mathrm{EncryptInline}+\mathrm{EncryptOffload}+\varepsilon}
\]

#### 3.3.3 pprof（CPU 结构）

```bash
# 例：cast5 / xtea / salsa20+comp — 按 SIDE 与 CRYPT 调整
CRYPT=cast5 BENCH_DATA_MB=50 bash bench/profile_rust_go_pprof.sh server 20
CRYPT=xtea  BENCH_DATA_MB=50 bash bench/profile_rust_go_pprof.sh server 20
CRYPT=salsa20 # 若脚本仅 --nocomp，需扩展或手动 --pprof 打 comp 负载（§3.4）

go tool pprof -top -ignore='Inner::park' bench/profiles/rust-server-*.pb
go tool pprof -list=should_cpu_block_encrypt bench/profiles/...
go tool pprof -list=encrypt_batch bench/profiles/...
go tool pprof -list=cpu_block bench/profiles/...   # 符号名以实际为准
go tool pprof -list=FrameEncoder bench/profiles/... # Snappy
```

**关注帧族**

| 帧模式 | 解读 |
|--------|------|
| `Xtea` / `Cast5` / `encrypt_cfb` / `CryptEngine` | 算法本体（A1） |
| `cpu_block` / `BlockingPool` / `async_channel` / `spawn` | 调度（H1） |
| `snap` / `FrameEncoder` / compress 闭包 | Snappy（H2） |
| `KCP::flush` / `input` / `Mutex` | 锁与协议（A2） |
| `send_batch` / UDP | I/O（通常过滤 park 后仍可能靠前） |

### 3.4 针对 H1 的验证步骤（顺序固定）

| 步 | 动作 | 记录 |
|----|------|------|
| H1-V0 | E3/E4 thr ×3 + Go 对照 | gap 是否存在；不存在 → **H1 关闭** |
| H1-V1 | 同配置 SNMP：\(r_{\mathrm{off}}\) | offload 是否频繁 |
| H1-V2 | pprof top（ignore park）：cipher vs cpu_block 族 cum% | 谁主导 |
| H1-V3 | **临时对照**（仅本地）：把 xtea/cast5 阈值改为「仅 ≥16 pkt 或 ≥16KiB」或「强制 inline」跑 E3/E4 | thr 升/降/不变 |
| H1-V4 | 同临时对照跑 E1/E2（tokio） | 若仅 smol 改善 → 支持「smol 特有」；两边都改善 → 可能是通用阈值问题 |
| H1-V5 | E5/E6（comp）重复 V1–V3 | 排除「其实是 Snappy」 |

**H1 判定表**

| 结果模式 | 结论 |
|----------|------|
| gap 不在 | **否定（无问题）** — 不改 |
| cipher cum% 主导，提高阈值 thr **下降或不变** | **否定 H1** — 瓶颈在算法/别处 |
| \(r_{\mathrm{off}}\) 高 + cpu_block 族显著 + **提高阈值 thr 稳定上升** | **支持 H1** — 允许进入 §4 修改 |
| 临时强制 inline 升、大阈值也升但不稳 | **证据不足** — 加跑次数或换负载，禁止合入 |
| 仅 comp 改善、no-comp 无差 | **倾向 H2/交叉** — 勿只改 H1 |

### 3.5 针对 H2 的验证步骤（顺序固定）

| 步 | 动作 | 记录 |
|----|------|------|
| H2-V0 | E7/E8/E10 thr：comp vs no-comp；vs Go | comp 是否仍异常差 |
| H2-V1 | SNMP retrans / csum；确认非协议错误 | 排除 B1 |
| H2-V2 | \(r_{\mathrm{off}}\) encrypt | 加密是否已大量 offload |
| H2-V3 | pprof：snappy/FrameEncoder 与 encrypt 是否同任务长栈 | 双内联是否可见 |
| H2-V4 | **临时** 将 `should_cpu_block_compress` 对 salsa/xor/gcm 改为 16KiB（或 32KiB 一组）**仅** E7/E8 | thr 与 pprof 变化 |
| H2-V5 | 同临时改跑 E0/null 与 aes CFB comp | 非目标是否回退（调度变多） |
| H2-V6 | 对比「只降 compress」vs「只动 encrypt 阈值」 | 归因 |

**H2 判定表**

| 结果模式 | 结论 |
|----------|------|
| comp 已不落后 Go / 与 no-comp 合理 | **否定** — 不改 |
| snappy 帧占比极低，降阈值 thr 不变 | **否定** — 64KiB 可保留 |
| 降阈值 thr↑，pprof 上 worker 阻塞缓解，非目标无显著回退 | **支持 H2** — 允许 §4 |
| thr↑ 但 `cpu_block` 饱和、其他 cipher 回退 | **负优化风险** — 否定合入或收窄匹配 cipher |
| 仅在 encrypt 仍全 inline 时降 compress 才有效 | 优先保证 encrypt 策略，H2 为次要 |

### 3.6 验证记录表（执行者填写）

| 实验 | Runtime | Crypt | Comp | thr 中位 | vs Go | \(r_{\mathrm{off}}\) | pprof 主导帧（ignore park） | 临时阈值 thr | 结论 |
|------|---------|-------|------|----------|-------|----------------------|------------------------------|--------------|------|
| E3 | smol | xtea | no | | | | | | |
| E4 | smol | cast5 | no | | | | | | |
| E7 | tokio | salsa20 | yes | | | | | | |
| E8 | tokio | xor | yes | | | | | | |
| … | | | | | | | | | |

**附件路径约定**

```
bench/profiles/invest-h1-smol-xtea-<date>.pb
bench/profiles/invest-h2-tokio-salsa20-comp-<date>.pb
bench/profiles/invest-notes-<date>.md   # 粘贴 top 输出与 SNMP 快照
```

### 3.7 验证门（Gating）— 未满足则禁止改生产策略

| 门 | 条件 |
|----|------|
| G0 | 基线 thr 可复现（3 次，变异可接受） |
| G1 | 目标场景相对 Go（或相对 tokio 对照）落后仍存在，或明确「绝对 thr 目标」 |
| G2 | pprof 已采集且已 ignore park 分析 |
| G3 | SNMP offload 比例已记录 |
| G4 | **至少一次** 临时策略 A/B 与 baseline 对比 |
| G5 | 假设判定为「支持修改」而非「否定/不足」 |

**仅 G0–G5 全过，才打开 §4。**

---

## 4. 修改问题（Change — 仅验证通过后）

> 以下为 **候选实现**，不是已批准补丁。参数以验证 A/B 最优值为准，下文数字仅为占位。

### 4.1 若 H1 支持：smol 上 xtea/cast5 提高 encrypt offload 门槛

**意图**：减少「小 batch 付调度税」。

**候选逻辑（示意）**

```text
if runtime == smol && crypt in {xtea, cast5}:
    offload iff packet_count >= T_pkt || total_bytes >= T_bytes
else:
    保持现有 should_cpu_block_encrypt
```

- `T_pkt` / `T_bytes`：**采用 H1-V3 中 thr 最佳且稳定的一组**（验证前不要写死 16/16384 为教条）。
- 实现时优先：**单一函数内分支 + 单测**，避免复制整份 smol 专用 API。
- 注意：`kcptun-client/server` 与 `kcp-rs` 的 feature 边界；`kcp-rs` 可能 **无** smol feature — 需在分析阶段定「runtime 标志如何传入」（参数 / `cfg` / 调用方包装）。**验证阶段可用调用方硬编码临时阈值，避免过早纠结 API。**

**修改检查单**

- [ ] 仅 xtea/cast5（或验证支持的集合）
- [ ] 单测覆盖：小 batch false、大 batch true
- [ ] 文档：AGENTS 一行说明「阈值经 invest-xxx 验证」
- [ ] 可回滚：独立 commit

### 4.2 若 H2 支持：Snappy 阈值按 cipher 下调

**意图**：在 fast/AEAD+comp 上更早把 compress 移出 async worker。

**候选逻辑（示意）**

```text
should_cpu_block_compress(bytes, crypt_name):
  if crypt_name in VERIFIED_SET:   # 仅验证证明有效的集合
      return bytes >= T_fast       # T_fast 来自 H2-V4，可能是 16Ki 或 32Ki
  return bytes >= 65536
```

- **API 变更**：现签名若只有 `uncompressed_bytes`，需增加 `crypt_name` 或 `CryptEngine` 引用；**所有调用点**（client/server）一并改；禁止半边旧阈值。
- VERIFIED_SET 不得默认抄全 `xor|salsa|gcm`：以 E7/E8/E9 谁受益为准。

**修改检查单**

- [ ] 调用点全更新
- [ ] 单测：旧 64Ki 行为对未列入 cipher 保持
- [ ] 非目标 cipher comp 回归 bench
- [ ] 独立 commit

### 4.3 明确不做的改法（即使理论好听）

| 改法 | 原因 |
|------|------|
| 无 pprof 把所有 CFB 阈值改成 16 | 可能伤害 aes 等已平衡路径 |
| smol 全局提高所有 cipher 阈值 | 未验证；可能让重加密阻塞 executor |
| 为「对称」同时合入 H1+H2 | 必须 **分开 commit、分开验证**，否则无法归因 |
| 用 debug 日志当长期方案 | 验证期临时可以，合入前删除 |

### 4.4 修改后必跑

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
# 或 make gate

# 场景复测：至少 E3/E4 或 E7/E8（视改动）
# 同配置再采一版 pprof，对比 hot 是否按预期移动
# 协议相关：make e2e；会话/flush：make stress（若动 flush 策略）
```

**接受 / 回滚**

| 结果 | 动作 |
|------|------|
| 目标 thr 达 §1.4 且非目标可接受 | 保留；写 CHANGELOG + 更新 HOTSPOTS 一行 |
| thr 无提升或回退 | **revert**；在本文 §6 记「已否定的改法」 |
| e2e/stress 红 | **revert**；先修正确性 |

---

## 5. 端到端流程（执行清单）

```text
[1 提出] 确认 P-A/P-B 仍存在（新 bench）──── 否 ──► 结案：无需本方案
                │是
[2 分析] 选 H1 和/或 H2，列出替代解释 A*/B*
                │
[3 验证] G0–G5：pprof + SNMP + 临时 A/B ──── 否定/不足 ──► 结案不改码
                │支持
[4 修改] 单假设单 commit，参数取自 A/B
                │
[5 回归] gate + 场景 bench + 可选 e2e/stress
                │
[6 固化] CHANGELOG / HOTSPOTS / 本文状态 → 已完成或已回滚
```

**时间盒建议**

| 阶段 | 建议 |
|------|------|
| 复测 thr | 0.5–1h |
| H1 全套验证 | 0.5–1d（含 smol 构建与 profile） |
| H2 全套验证 | 0.5–1d |
| 单假设改码+回归 | 数小时 |

---

## 6. 结案模板（验证或修改结束后填写）

```text
日期 / commit：
假设：H1 / H2
验证结论：支持 | 否定 | 证据不足
关键证据路径：
 thr：
 SNMP r_off：
 pprof top 摘要：
 临时 A/B 结果：
是否改码：是 / 否
若改码：参数最终值、commit、回归结果
若否定：保留的替代解释（A*/B*）与下一步问题单
```

---

## 7. 与错误流程的对比（审核用）

| 错误做法（已否决） | 本文做法 |
|--------------------|----------|
| 理论算 μs → 直接改 16KiB 阈值 | 理论只服务实验设计 |
| 先写 `should_cpu_block_*` 再「跑跑 bench」 | G0–G5 通过前禁止合入策略 |
| 一次改 H1+H2+微优化 | 假设分离、commit 分离 |
| 用 Q1 数字当验收 | 本机复测 + pprof 附件 |

---

## 8. 参考

- 仓库：`bench/PROFILE_RUNBOOK.md`、`.claude/skills/flamegraph-perf/SKILL.md`
- 脚本：`bench/profile_rust_go_pprof.sh`、`make profiling-bins` / `make profile`
- 计数：`kcp-rs` `SNMP.encrypt_inline` / `encrypt_offload`
- 决策函数：`kcp-rs/src/crypto_buf.rs` — `should_cpu_block_encrypt` / `should_cpu_block_compress`
- 历史热点笔记：`bench/profiles/HOTSPOTS.md`（方法论参考，数字需新采）
- 线索来源：`Q1.md`、`Q2.md`（非证据）

---

## 9. 审核关注点（给评审）

1. **流程是否强制「先 pprof/SNMP/A-B 后改码」？** — 是（§3.7 / §4 门控）。
2. **假设是否可证伪？** — 是（§3.4 / §3.5 判定表）。
3. **是否把 16KiB 写成已定结论？** — 否，仅为历史讨论占位，以 A/B 为准。
4. **H1/H2 是否允许捆绑合入？** — 否。
5. **验证失败是否允许「优化心情改一版」？** — 否，应结案或重开分析。

---

**文档状态**：`VERIFY_PENDING`  
**下一步（人工）**：按 §3.2 跑 E3/E4/E7/E8 基线 → 填 §3.6 → 再决定是否授权 §4。  
**代码状态**：本调查项 **零策略合入**，直至 G 门通过。
