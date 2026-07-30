<div align="center">

# kcptun-rs ⚡

**Rust 移植 kcptun — 性能最高达到 Go 版本的 5.38 倍，完全线上兼容**

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![Tests](https://img.shields.io/badge/tests-237%20passed-brightgreen)](#)
[![E2E](https://img.shields.io/badge/e2e-68%20passed-brightgreen)](#)
[![License](https://img.shields.io/badge/license-MIT-blue)](#)
[![Rust](https://img.shields.io/badge/rust-1.92+-orange)](#)
[![Go Compatible](https://img.shields.io/badge/Go%20compat-v5-success)](#)

[English](README.md) | 中文

</div>

---

> <details>
> <summary><b>免责声明</b> — 本项目是 Vibe Coding 移植测试，仅供学习交流使用。</summary>
>
> 本项目是一次 **Vibe Coding**（利用 AI 辅助编程的实践方式）的移植测试——通过尝试移植现有代码库来实践 AI 辅助编程。核心是探索和验证 Vibe Coding 这套工作流本身，而非专门做一个生产级软件移植。本项目**不是**生产级软件，不保证功能正确性、稳定性和安全性。
>
> **严禁用于任何违法违规用途**，包括但不限于翻墙、非法数据传输、网络攻击等。使用者的任何违法行为均与本项目及作者无关，由行为人自行承担全部法律责任。
>
> 完整免责声明请参阅 [DISCLAIMER.zh.md](DISCLAIMER.zh.md)。
> </details>

---

## 🔥 性能概览

kcptun-rs 在**几乎所有加密算法和压缩组合下都超越 Go kcptun**，同时保持**完全的线上兼容**——这意味着你可以将 Go kcptun 隧道的一端替换为 Rust 二进制文件，立即获得加速。

| 加密算法 | vs Go (Tokio) | vs Go (Smol) |
|----------|:------------:|:------------:|
| **SM4** (无压缩) | **4.76倍** 🏆 | **4.58倍** |
| **SM4** (压缩) | **5.32倍** 🏆 | **5.38倍** 🏆 |
| **XOR** (无压缩) | **2.54倍** | **2.34倍** |
| **CAST5** (无压缩) | **1.98倍** | **1.78倍** |
| **Twofish** (压缩) | **1.69倍** | **1.22倍** |
| **AES-128** (无压缩) | **1.59倍** | **1.41倍** |
| **AES-128-CFB** 大吞吐 | **1.67倍** | **2.11倍** 🏆 |

*测试环境：Apple M1，10 并发连接 × 每连接 1 MB。完整矩阵见下文。*

---

## 📖 什么是 kcptun-rs？

**kcptun** 是一个稳定、安全的 TCP-over-UDP 隧道工具，利用 [KCP](https://github.com/skywind3000/kcp)（快速 ARQ 协议）在高延迟或丢包网络环境下加速 TCP 流。它具备 SMUX 多路复用、Reed-Solomon 前向纠错（FEC）、Snappy 压缩以及可选的加密功能，全部整合在一个二进制文件中。

**kcptun-rs** 是用 Rust 完整重写的实现：

- ✅ **线上兼容** Go kcptun（kcp-go v5）— Rust ↔ Go、Go → Rust、Rust → Rust 全部可互通
- ⚡ **性能超越 Go** — 大多数加密/模式组合下都更快，最高达 **5.38 倍**
- 🧩 **13 种加密后端** + AES-128-GCM：AES、SM4、Salsa20、Blowfish、Twofish、CAST5、3DES、TEA、XTEA、XOR 等
- 🔧 **双异步运行时**：tokio（高并发）和 smol（轻量级，ARM 优化）
- 🎯 **生产级功能**：FEC、SMUX v1/v2、QPP 混淆、SNMP 统计、速率限制、pprof 性能分析
- 🔄 **跨平台**：macOS、Linux、ARMv7（树莓派）、ARM64（AWS Graviton）

---

## ✨ 功能特性

| 类别 | 详情 |
|------|------|
| **兼容性** | 与 Go kcptun（kcp-go v5）完全线上兼容 — 所有加密算法、模式、SMUX 版本、FEC、Snappy |
| **加密** | 14 种后端：`null`、`none`、`xor`、`aes-128`、`aes-192`、`aes`(256)、`aes-128-gcm`、`sm4`、`tea`、`xtea`、`salsa20`、`blowfish`、`twofish`、`cast5`、`3des` |
| **KCP 模式** | `normal`、`fast`、`fast2`、`fast3` |
| **SMUX** | v1 & v2 多路复用 — 单条 KCP 连接承载多个 TCP 流 |
| **FEC** | Reed-Solomon 前向纠错（默认 10/3，与 Go 兼容） |
| **压缩** | 会话级 Snappy 压缩，与 Go 字节一致，默认开启 |
| **QPP** | 量子置换垫 — 可选的后量子流混淆层 |
| **运行时** | tokio（默认，多线程）**或** smol（轻量，ARM 优化） |
| **Go pprof** | `--pprof` 输出 Go 兼容的 protobuf 格式 → 可直接用 `go tool pprof` 分析 |
| **速率限制** | 每连接令牌桶限速（`--ratelimit`） |
| **SNMP 统计** | 与 Go 兼容的 SNMP 字段，零开销按需采集 |
| **交叉编译** | ARMv7（树莓派）、ARM64（Graviton）、Linux musl — 全部可从 macOS 编译 |
| **日志** | 结构化日志级别（RUST_LOG），支持文件日志 |

---

## 🚀 快速开始

```bash
# 构建（优化发布版，含 LTO）
cargo build --release
# 二进制文件：target/release/kcptun-server、target/release/kcptun-client

# 启动服务端（监听 UDP :29900，转发到本地 HTTP :8080）
./target/release/kcptun-server -t "127.0.0.1:8080" -l ":29900" --key "my-secret"

# 启动客户端（监听 :12948，隧道到远程服务端）
./target/release/kcptun-client -r "server-ip:29900" -l ":12948" --key "my-secret"
```

现在将你的应用指向 `127.0.0.1:12948` — 所有 TCP 数据都将被加密、压缩并通过 KCP 加速传输到远程服务端。

### 使用配置文件

```bash
kcptun-server -c config.json
kcptun-client -c config.json
```

```json
{
    "localaddr": ":12948",
    "remoteaddr": "vps:29900",
    "key": "my-secret",
    "crypt": "aes-128",
    "mode": "fast2",
    "conn": 2,
    "sndwnd": 1024,
    "rcvwnd": 1024,
    "datashard": 10,
    "parityshard": 3,
    "nocomp": false,
    "smuxver": 2,
    "keepalive": 10
}
```

> ⚠️ **`--key`、`--crypt`、`--mode` 和 `--nocomp` 必须客户端与服务端一致。** 压缩默认开启。

---

## 📊 性能深度分析

### 大吞吐测试（200 MB，AES-128-CFB，无压缩）

路径标签为 **Client → Server**（大流量由客户端发往服务端；见 `bench/run_bench.sh`）。

| 路径 (Client → Server) | 吞吐量 | 延迟 | vs Go→Go |
|------|:-----:|:----:|:--------:|
| **Go → Go** | 51.15 MB/s | 0.31 ms | 1.00× |
| **Rust-Tokio → Rust-Tokio** | **85.60 MB/s** 🥈 | **0.12 ms** | **1.67×** |
| **Rust-Smol → Rust-Smol** | **108.06 MB/s** 🏆 | **0.13 ms** | **2.11×** |
| Rust-Tokio → Go | 76.48 MB/s | 0.11 ms | 1.50× |
| Go → Rust-Tokio | 30.28 MB/s | 0.15 ms | 0.59× |

> 同侧 Rust 路径在 M1 主机上明显快于 Go→Go。Smol 运行时的轻量架构使其在单流大吞吐传输中更具优势。

### 完整加密 × 压缩矩阵

测试：10 并发连接，每连接 1 MB，所有 30+ 轮次全部通过（0 失败）。

**无压缩**（`--nocomp`）：

| 加密算法 | Tokio | Smol | Go | T/Go | S/Go |
|---------|:----:|:----:|:--:|:----:|:----:|
| null | 38.4 | 38.8 | 35.5 | 1.08× | 1.09× |
| none | 29.4 | 33.3 | 39.2 | 0.75× | 0.85× |
| xor | 41.6 | 38.3 | 16.4 | **2.54×** | **2.34×** |
| aes-128 | 43.4 | 38.3 | 27.2 | **1.59×** | **1.41×** |
| aes-128-gcm | 36.6 | 35.8 | 41.5 | 0.88× | 0.86× |
| salsa20 | 35.8 | 35.4 | 32.3 | **1.11×** | **1.10×** |
| blowfish | 31.5 | 31.3 | 28.6 | **1.10×** | **1.09×** |
| twofish | 35.1 | 37.1 | 23.2 | **1.51×** | **1.60×** |
| cast5 | 33.3 | 30.1 | 16.9 | **1.98×** | **1.78×** |
| 3des | 14.5 | 12.6 | 11.8 | **1.23×** | **1.07×** |
| tea | 38.2 | 35.2 | 31.7 | **1.20×** | **1.11×** |
| xtea | 24.7 | 22.2 | 18.6 | **1.33×** | **1.20×** |
| **sm4** | **16.7** | **16.1** | **3.5** | **4.76×** 🏆 | **4.58×** |

**带压缩**（Snappy）：

| 加密算法 | Tokio | Smol | Go | T/Go | S/Go |
|---------|:----:|:----:|:--:|:----:|:----:|
| aes-128-gcm | 36.4 | 36.0 | 27.4 | **1.33×** | **1.31×** |
| salsa20 | 29.0 | 30.6 | 20.1 | **1.44×** | **1.52×** |
| **sm4** | **18.7** | **18.8** | **3.5** | **5.32×** 🏆 | **5.38×** 🏆 |
| twofish | 34.4 | 24.9 | 20.4 | **1.69×** | **1.22×** |
| cast5 | 36.5 | 34.3 | 26.4 | **1.38×** | **1.30×** |
| aes-128 | 31.3 | 35.7 | 26.5 | **1.18×** | **1.35×** |

> **SM4 是最大亮点**：Rust 比 Go 快 4.6–5.4 倍，因为 Go 实现使用纯软件 S-box，而 Rust 受益于编译器的自动向量化和预计算查找表。

### 压力测试（数据完整性）

全部 8 项压力测试通过 — 在并发负载下验证**逐字节精确性**：

| 测试 | 连接数 | 负载大小 | 结果 |
|------|:-----:|:--------:|:----:|
| 单连接混合大小 | 1 | 1B…64KB | ✅ |
| 多线程 10 连接 | 10 | 各 256B | ✅ |
| 多线程 50 连接 | 50 | 各 255B | ✅ |
| 多线程 100 连接 | 100 | 1B + 4KB | ✅ |
| 大数据（100 连接） | 100 | 各 64KB + 128KB | ✅ |
| 页面刷新模拟 | 80（3 波） | 512B…128KB | ✅ |
| 可压缩数据 | 1 | 压缩模式 | ✅ |

---

## 🔗 Go 兼容性

kcptun-rs 与 Go kcptun（kcp-go v5）**完全线上兼容**。全部 68 项端到端互通测试全部通过：

| 功能 | 状态 | 说明 |
|------|:----:|------|
| KCP 段格式 | ✅ | 24 字节小端序头部，与 kcp-go v5 一致 |
| Crypto 头部（CFB） | ✅ | `[nonce 16B][CRC32 4B][payload]` |
| AES-GCM | ✅ | `[nonce 12B][ciphertext+tag 16B]` |
| Snappy（会话级） | ✅ | 与 Go 的 `github.com/golang/snappy` 字节一致 |
| SMUX v1 & v2 | ✅ | 完整帧格式兼容 |
| FEC（10/3、4/2） | ✅ | Reed-Solomon，相同头部格式 |
| 密钥派生 | ✅ | PBKDF2-HMAC-SHA1，盐值 `b"kcp-go"` |
| QPP 混淆 | ✅ | 流级，相同置换算法 |
| 全部 15 种加密算法 | ✅ | 双向（Go→Rust、Rust→Go）|
| 全部 4 种 KCP 模式 | ✅ | normal、fast、fast2、fast3 |
| SM4（国密标准） | ✅ | tjfoc/gmsm S-box + CK 修正 |
| CAST5（RFC 2144） | ✅ | 完整实现，从 Go 移植 |

### 端到端测试结果

```
加密算法:    15/15 种通过（Go→Rust + Rust→Go）
KCP 模式:    4/4 通过
SMUX:        2/2 版本通过
压缩:        8/8 种加密×压缩组合通过
FEC:         2/2 配置通过
总计:        68 通过，0 失败，0 跳过 🎉
```

---

## 🏗️ 架构

### 协议栈

```
┌──────────────────────────────────┐
│         TCP / UNIX Socket        │
├──────────────────────────────────┤
│        SMUX Stream (多路复用)     │
├──────────────────────────────────┤
│       SMUX Session (多路复用)     │
├──────────────────────────────────┤
│  Snappy 压缩 (会话级)             │  ← 与 Go 字节一致
├──────────────────────────────────┤
│  BlockCrypt / FEC / KCP (ARQ)    │
├──────────────────────────────────┤
│           UDP / TCPraw           │
└──────────────────────────────────┘
```

### 工作空间（9 个 crate）

```
kcptun-rs/
├── kcp-rs/          — KCP ARQ 协议状态机
├── kcrypt-rs/       — 13 种分组密码 + AES-128-GCM
├── smux-rs/         — SMUX 流多路复用器 (v1/v2)
├── qpp-rs/          — 量子置换垫混淆
├── kio-rs/          — 异步运行时抽象 (tokio / smol)
├── kpprof-rs/       — Go 兼容 pprof HTTP 服务
├── kcptun-common/   — 客户端/服务端共享辅助
├── kcptun-client/   — 客户端二进制
└── kcptun-server/   — 服务端二进制 + 压力测试
```

### 双运行时设计

- **tokio**（默认）— 多线程、高并发、适合生产规模
- **smol**（`--no-default-features --features smol`）— 轻量、极小二进制、ARM 优化
- 互斥特性 — 每次构建选择其一
- 业务代码仅使用 `kio::*` 抽象 — 绝不直接使用 tokio/smol API

### 刷新循环优化

刷新循环分为 **4 个阶段**，以最小化 KCP 互斥锁持有时间：

| 阶段 | 工作内容 | KCP 锁 |
|:----:|---------|:------:|
| 1 | 排空 SMUX 发送缓冲区，收集 FIN 待处理的流 | ❌ 未持有 |
| 2 | 编码 SMUX 帧 | ❌ 未持有 |
| 3 | Snappy 压缩（如启用） | ❌ 未持有 |
| 4 | `kcp.send()` + `kcp.update()` + `kcp.flush()` | ✅ 短暂持有 |

这使得 UDP 接收循环可以在刷新循环准备下一批帧的同时将数据输入 KCP — 消除了高并发下的锁争用问题。

---

## 🔧 构建与运行

```bash
make build          # 调试构建（tokio）
make release        # 发布构建（LTO、strip、panic=abort）
make test           # 全部单元测试
make stress         # 数据完整性压力测试（需先构建 release）
make e2e            # Go↔Rust 互通测试（需 Go kcptun 二进制）
make clippy         # 代码检查（警告 = 错误）
make fmt            # 格式化所有 Rust 代码
make profile        # 火焰图性能分析（samply → Speedscope）
```

### 交叉编译

```bash
make release-armv7     # 树莓派 2/3、OpenWrt（二进制约 1.3M）
make release-arm64     # 树莓派 4/5、AWS Graviton
make linux             # x86_64 Linux musl（从 macOS 交叉编译）
make linux-aarch64     # ARM64 Linux musl（从 macOS 交叉编译）
```

ARM 交叉构建使用 **smol** 运行时，禁用 `pprof` 以保持二进制最小。

---

## 🔬 优化历程

本项目通过火焰图驱动的性能分析，从最初的 **5.4 MB/s** 进化到超过 **108 MB/s**：

| 里程碑 | 吞吐量 | vs Go |
|:------|:------:|:-----:|
| 初始移植 | 5.4 MB/s | 0.71× |
| + 事件驱动刷新调度 | 7.1 MB/s | 0.87× |
| + 零拷贝 KCP 输出管道 | 68.8 MB/s | 1.43× |
| + ARMv8 AES 硬件加速 | ~85 MB/s | 1.67× |
| + Tokio 持久阻塞线程池 | +108% | 2.1× |
| + SMUX v2 写窗口控制 | 性能瓶颈消除 | — |
| + Snappy 卸载与阈值调优 | — | — |
| + sendmmsg/recvmmsg 批量 I/O | — | — |
| + 加密算法枚举静态分发 | vtable 消除 | — |
| → **最终（smol 大吞吐）** | **108 MB/s** | **2.11×** 🏆 |

### 沿途发现的关键 Bug 修复

| Bug | 影响 | 修复 |
|:----|:----|:-----|
| Blowfish 每块密钥调度 | 0.0 MB/s（100 倍提升） | 缓存加密器实例 |
| Twofish 每块密钥调度 | 0.4 → 4.5 MB/s（11 倍） | 自定义预计算表 |
| Snappy 中 CRC32C vs CRC32/IEEE | 数据被 Go 静默丢弃 | 改用 `snap::FrameEncoder` |
| KCP ACK 从未填充 | 无限重传 → 死锁 | 对每个收到的 Push 段排队 ACK |
| `snd_buf` 从不清理 | 窗口卡在 32 个包 | flush() 中前缓冲清理 |
| Twofish 256 位密钥 S-box | 与 Go 密文不符 | 增加第 5 层 sbox |

---

## 🧪 测试严谨性

| 测试类型 | 数量 | 验证内容 |
|:--------|:---:|---------|
| 单元测试 | 237 | 各 crate 的正确性 |
| E2E 互通 | 68 | Go↔Rust 双向兼容性 |
| 压力测试 | 8 | 大规模下逐字节数据完整性 |
| Clippy | `-D warnings` | 零警告强制 |
| Fmt | `cargo fmt --check` | 一致的代码格式 |

---

## 💡 为什么选择 Rust？

- **内存安全** — 无野指针、无缓冲区溢出、无释放后使用
- **零成本抽象** — 枚举分发消除了热路径上的虚函数表开销
- **真正并行** — `std::thread::scope` 用于批量并行加密，无 GIL 限制
- **编译期保证** — 借用检查器在数据竞争发生前就捕获它们
- **ARM 生态** — Rust 在 aarch64 上是一等公民，支持硬件 AES（`aes_armv8`）
- **小体积二进制** — 剥离后的发布版二进制约 2 MB，远小于 Go 的静态链接文件
- **交叉编译** — 一条 `make` 命令即可在 macOS 上编译 ARM Linux 二进制

---

## 📝 许可证

MIT — 详见 [LICENSE](LICENSE)。

本项目是 [kcptun](https://github.com/xtaci/kcptun)（作者 [xtaci](https://github.com/xtaci)）的 Rust 移植版本。  
源码： [github.com/xsean2020/kcptun-rs](https://github.com/xsean2020/kcptun-rs)

---

<div align="center">

**如果觉得这个项目有用或令人印象深刻，请在 GitHub 上 ⭐ 星标！**

*用 Rust 构建，由好奇心驱动，用基准测试验证。*

</div>
