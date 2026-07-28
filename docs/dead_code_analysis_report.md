# kcptun-rs Dead Code 分析报告

> **审计日期**: 2026-07-24
> **审计范围**: `kcp-rs`, `kcrypt-rs`, `smux-rs`, `qpp-rs`, `kio-rs`, `kpprof-rs`, `kcptun-client`, `kcptun-server`
> **审计方法**: `cargo clippy` (dead_code/unreachable_pub lints) + 跨 crate 交叉引用搜索
> **排除范围**: `vendor/` 目录（第三方依赖）

---

## 一、总结

| 类别 | 数量 | 严重程度 |
|:-----|:----:|:--------:|
| 完全未使用（零引用） | 4 | 中 |
| 仅测试中使用 | 2 | 低 |
| 跨 crate 未使用的 pub 项 | 13 | 低 |
| 可见性过宽（pub 可降为 pub(crate)） | 3 | 低 |
| **合计** | **22** | — |

**总体评价**: 代码库非常干净。`cargo clippy` 的 `dead_code` lint 全 workspace 零告警。发现的死代码主要集中在两类：
1. **QPP crate 的 Go 兼容性包装函数** — 为了对应 Go API 签名但 Rust 侧无需调用；
2. **KCP/FEC 的常量和工具函数** — 对应 Go 源码但在 Rust 实现中不再需要。

---

## 二、完全未使用（零引用）

以下 `pub` 项在**整个 workspace（含测试）中从未被引用**，属于完全死代码。

### 2.1 `qpp-rs` — 3 项

| # | 符号 | 文件:行 | 说明 |
|---|------|---------|------|
| 1 | `pub const QPP_POWER: u16 = 8` | `qpp-rs/src/lib.rs:31` | 定义后从未被任何代码引用（内部使用 `QUBITS` 常量代替） |
| 2 | `pub const QPP_PAD_SIZE: usize = 256` | `qpp-rs/src/lib.rs:33` | 定义后从未被任何代码引用（内部使用 `1 << QUBITS` 代替） |
| 3 | `pub const FEC_TYPE_OOB: u16 = 0x00f3` | `kcp-rs/src/fec.rs:23` | 定义后从未被任何代码引用；Go 侧也未使用此类型 |

### 2.2 `kcp-rs` — 1 项

| # | 符号 | 文件:行 | 说明 |
|---|------|---------|------|
| 4 | `pub const IKCP_PROBE_INIT: u32 = 500` | `kcp-rs/src/kcp.rs:38` | 定义后从未被引用；Go 侧使用字面量 `500` 内联，Rust 也采用了同样方式 |

> **注**: `IKCP_PROBE_LIMIT`（`kcp.rs:40`）在 kcp.rs 内部有使用（通过 `probe_limit` 字段），不属于死代码。

---

## 三、仅测试中使用（生产代码中未调用）

以下 `pub` 函数在生产路径中**从未被调用**，仅在 `#[cfg(test)]` 测试中被引用。

### 3.1 `kcp-rs/src/fec.rs` — 2 项

| # | 符号 | 文件:行 | 测试引用 | 说明 |
|---|------|---------|----------|------|
| 5 | `pub fn parse_fec_header(data: &[u8]) -> Option<(u32, u16)>` | `fec.rs:577` | `fec.rs:646,651` | 仅在 `fec_header_parse` 测试中使用；生产代码中 FEC header 解析内联在 `FecDecoder::decode` 中 |
| 6 | `pub fn is_data_packet(data: &[u8]) -> bool` | `fec.rs:587` | `fec.rs:658,660` | 仅在 `is_data_packet` 测试中使用；生产代码中未调用此辅助函数 |

**建议**: 可将这两个函数降为 `pub(crate)` 或 `#[cfg(test)]`，或直接内联到测试中。

---

## 四、跨 crate 未使用的 pub 项

以下项在**自身 crate 内部有使用**，但被标记为 `pub` 且在整个 workspace 中**无外部 crate 引用**。这类项的 `pub` 可见性过宽，可以安全降为 `pub(crate)`。

### 4.1 `qpp-rs` — 4 项

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 7 | `pub const QPP_MIN_SEED_LENGTH: usize = 32` | `lib.rs:29` | `lib.rs:412`（`qpp_minimum_seed_length` 返回值） | 无 | → `pub(crate)` 或删除（Go 兼容性常量） |
| 8 | `pub const QPP_MINIMUM_PADS: u16 = 3` | `lib.rs:35` | `lib.rs:415`（`qpp_minimum_pads` 返回值） | 无 | → `pub(crate)` 或删除 |
| 9 | `pub fn qpp_minimum_seed_length(_power: u8) -> usize` | `lib.rs:411` | 仅返回 `QPP_MIN_SEED_LENGTH` | 无 | → `pub(crate)` 或删除（参数 `_power` 未使用） |
| 10 | `pub fn qpp_minimum_pads(_power: u8) -> u16` | `lib.rs:414` | 仅返回 `QPP_MINIMUM_PADS` | 无 | → `pub(crate)` 或删除（参数 `_power` 未使用） |
| 11 | `pub fn create_qpp_prng(seed: &[u8]) -> Rand` | `lib.rs:419` | 仅包装 `create_prng` | 无 | → `pub(crate)` 或删除（冗余包装） |

> **注**: `create_prng` 和 `Rand` 在 kcptun-client / kcptun-server 中有使用（QPP 加密流），属于活代码。`encrypt_with_pads` / `decrypt_with_pads` 也有外部使用。

### 4.2 `kcp-rs` — 4 项

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 12 | `pub const KCP_MIN_WND: u32 = 32` | `segment.rs:34` | 仅定义 | 无 | → `pub(crate)` 或删除（与 `KCP_DEFAULT_WND` 重复值） |
| 13 | `pub fn snmp::is_enabled()` | `snmp.rs:25` | snmp.rs 内部多处使用 | 无（`snmp_enabled` 重导出无外部调用） | lib.rs 中的 `is_enabled as snmp_enabled` 重导出可删除 |
| 14 | `pub struct SnmpSnapshot` | `snmp.rs:328` | snmp.rs 内部（`snapshot()` → `to_slice()`） | 无 | → `pub(crate)`；kcptun-server 通过 `to_slice()` 间接使用，不直接引用 `SnmpSnapshot` |
| 15 | `pub const FEC_HEADER_SIZE_PLUS_2: usize = 8` | `fec.rs:17` | 仅在 lib.rs 重导出 | 无 | → `pub(crate)` 或从 lib.rs 移除重导出 |

### 4.3 `kio-rs` — 2 项

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 16 | `pub async fn copy_bidirectional` | `lib.rs:68` | `copy_bidirectional_idle` 在 `idle_secs==0` 时调用 | 无（外部均使用 `copy_bidirectional_idle`） | → `pub(crate)` |
| 17 | `pub type Sleep` | `time/mod.rs:7,10` | 仅类型别名定义 | 无 | → `pub(crate)` 或删除 |

### 4.4 `kpprof-rs` — 2 项（clippy 已标记）

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 18 | `pub const DEFAULT_SAMPLE_RATE: usize = 524_288` | `heap.rs:27` | 仅定义 | 无 | → `pub(crate)` 或删除 |
| 19 | `pub fn set_sample_rate(rate: usize)` | `heap.rs:50` | 仅定义 | 无 | → `pub(crate)` 或删除 |

### 4.5 `smux-rs` — 1 项

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 20 | `pub const SMUX_VER: u8 = 2` | `frame.rs:11` | 仅定义 | 无（代码中直接使用字面量 `2`） | → `pub(crate)` 或删除 |

### 4.6 `kcrypt-rs` — 1 项

| # | 符号 | 文件:行 | 内部使用 | 外部引用 | 建议 |
|---|------|---------|----------|----------|------|
| 21 | `pub type SelectBlockCrypt` | `crypt.rs:402` | 仅定义 | 无 | → `pub(crate)` 或删除（未被作为函数指针类型使用） |

---

## 五、pub 重导出（lib.rs）中无外部引用的项

以下 `pub use` 重导出在 `lib.rs` 中暴露，但在 kcptun-client / kcptun-server 中**从未被引用**。

### 5.1 `kcp-rs/src/lib.rs`

| # | 重导出 | 行 | 说明 |
|---|--------|----|------|
| — | `is_enabled as snmp_enabled` | `lib.rs:76` | kcptun-client/server 从不调用 `snmp_enabled()` |

### 5.2 `kcrypt-rs/src/lib.rs` 和 `kcrypt-rs/src/crypt.rs`

| # | 重导出 | 说明 |
|---|--------|------|
| 22 | `pub use crypt::{... SelectBlockCrypt}` | 类型别名重导出，外部从未引用 |

> **注**: 12 个 cipher struct（`AesCfbCrypt`, `Aes128GcmCrypt`, `BlowfishCrypt` 等）虽然在 `crypt.rs:39-50` 有 `pub use` 重导出，但外部 crate 从不通过具体类型名引用它们 — 只通过 `select_block_crypt` 返回的 `Box<dyn BlockCrypt>` trait 对象使用。这些重导出可以降为 `pub(crate)`，但作为库 API 设计保留也无害。

---

## 六、按 Crate 汇总

| Crate | 完全未使用 | 仅测试 | pub 过宽 | 重导出 | 小计 |
|-------|:--------:|:------:|:--------:|:------:|:----:|
| `qpp-rs` | 2 | — | 5 | — | 7 |
| `kcp-rs` | 2 | 2 | 4 | 1 | 9 |
| `kio-rs` | — | — | 2 | — | 2 |
| `kpprof-rs` | — | — | 2 | — | 2 |
| `smux-rs` | — | — | 1 | — | 1 |
| `kcrypt-rs` | — | — | 1 | — | 1 |
| **合计** | **4** | **2** | **15** | **1** | **22** |

---

## 七、清理建议

### 7.1 可安全删除（零风险）

| 优先级 | 项 | 操作 |
|--------|----|------|
| P1 | `QPP_POWER`, `QPP_PAD_SIZE` | 删除常量（Go 兼容性不需要，Go 侧也用字面量） |
| P1 | `FEC_TYPE_OOB` | 删除常量（Go 侧也无 OOB 类型使用） |
| P1 | `IKCP_PROBE_INIT` | 删除常量（字面量已内联） |
| P1 | `SMUX_VER` | 删除常量（代码中直接使用字面量 `2`） |

### 7.2 可降为 `pub(crate)`（低风险）

| 优先级 | 项 | 操作 |
|--------|----|------|
| P2 | `qpp_minimum_seed_length`, `qpp_minimum_pads` | → `pub(crate)` 或删除（参数未使用，仅返回常量） |
| P2 | `create_qpp_prng` | → `pub(crate)` 或删除（冗余包装 `create_prng`） |
| P2 | `QPP_MIN_SEED_LENGTH`, `QPP_MINIMUM_PADS` | → `pub(crate)` |
| P2 | `KCP_MIN_WND` | → `pub(crate)` 或删除（与 `KCP_DEFAULT_WND` 重复） |
| P2 | `SnmpSnapshot` | → `pub(crate)` |
| P2 | `copy_bidirectional` | → `pub(crate)` |
| P2 | `Sleep` type alias | → `pub(crate)` 或删除 |
| P2 | `DEFAULT_SAMPLE_RATE`, `set_sample_rate` | → `pub(crate)` 或删除 |
| P2 | `SelectBlockCrypt` | → `pub(crate)` 或删除 |
| P2 | `FEC_HEADER_SIZE_PLUS_2` | 从 `lib.rs` 重导出中移除 |

### 7.3 仅测试使用的函数

| 优先级 | 项 | 操作 |
|--------|----|------|
| P3 | `parse_fec_header` | → `#[cfg(test)]` 或 `pub(crate)` |
| P3 | `is_data_packet` | → `#[cfg(test)]` 或 `pub(crate)` |

### 7.4 重导出清理

| 优先级 | 项 | 操作 |
|--------|----|------|
| P3 | `snmp_enabled` (lib.rs 重导出) | 从 `kcp-rs/src/lib.rs:76` 移除 `is_enabled as snmp_enabled` |

---

## 八、方法论说明

### 检测工具

1. **`cargo clippy --workspace`** with `-W dead_code -W unreachable_pub -W unused_imports` — 检测 crate 内部死代码和过宽可见性
2. **跨 crate grep 搜索** — 对每个 `pub` 项搜索其在 workspace 其他 crate 中的引用，排除 `vendor/` 目录
3. **双 backend 验证** — 同时在 `tokio`（默认）和 `smol` feature 下检查，确保两个 runtime 后端结果一致

### 已知限制

- `cargo udeps` 不可用（未安装 nightly `cargo-udeps`），未检测未使用的 crate 依赖
- 跨 crate 搜索基于文本 grep，可能遗漏通过 trait 动态分发调用的项（但本项目 cipher struct 均通过 `select_block_crypt` 返回 `Box<dyn BlockCrypt>`，不直接引用类型名）
- `pub use` 重导出的 cipher struct（`AesCfbCrypt` 等）作为库 API 设计保留，不计为死代码
