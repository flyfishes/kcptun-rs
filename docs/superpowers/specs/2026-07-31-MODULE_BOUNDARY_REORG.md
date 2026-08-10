# B2: crypto_buf → kcrypt-rs::wire (no kcp-rs dependency)

**日期**: 2026-07-31  
**状态**: ✅ 已完成  
**目的**: 按文档 B2-A，`crypto_buf` 迁到 `kcrypt-rs::wire`，kcp-rs **完全去掉** kcrypt-rs 依赖（不依赖，无 re-export）。

---

## 1. 死循环纠正（本次发现）

原计划 Section 6 保留 `kcrypt-rs = { path = "../kcrypt-rs", optional = true }` + 临时 `wire` feature，
同时 Section 5 又要求 "re-export 全部删掉"。两者冲突：只要 client/server 还消费 `kcp_rs::CryptoBuf`
等符号，kcp-rs 就无法去掉 kcrypt-rs 依赖。

**决议**：先迁移所有 in-tree 消费者（client/server）到 `kcrypt_rs::` 直接引用，然后 kcp-rs
**彻底删除** kcrypt-rs 依赖（连 optional 都不留），`wire` 临时 feature 也不需要。

---

## 2. 最终 crate 结构（已实现）

```
kcrypt-rs/          (已存在)
├── src/
│   ├── lib.rs      (re-export crypt + wire)
│   ├── crypt.rs    (算法引擎: AeadCrypt / BlockCrypt / CryptEngine)
│   ├── wire.rs     (NEW)  ← 线格式 / batch / offload / CryptoBuf
│   └── crypt/ …    (13 种 cipher 实现)
```

kcp-rs/：**完全删除** `crypto_buf.rs`；**无 kcrypt-rs 依赖**；**无任何 crypto re-export**。

---

## 3. 迁移步骤（已全部完成）

1. **kcrypt-rs** 新增 `src/wire.rs`  
   - `kcrypt-rs/src/crypto_buf.rs` → `kcrypt-rs/src/wire.rs`（重命名）  
   - 含 CryptoBuf, encrypt_batch/encrypt_batch_into, offload 启发式, constants 等。  
   - Cargo.toml 补 `crc32fast` / `parking_lot`；feature `wire`（default on）。

2. **kcp-rs**：  
   - 删除 `crypto_buf.rs`（已删）  
   - **删除** kcrypt-rs 依赖（`kcp-rs/Cargo.toml` 已移除）  
   - `lib.rs` 删除所有 re-export（`cast5` / `crypt` / `AeadCrypt` / `BlockCrypt` / `CryptEngine` / wire helpers）  
   - 恢复被误删的 crate 级 `#![allow(...)]` clippy 块（Go kcp-go 对应关系，勿动）  
   - `lib.rs` 只保留 `KCP` / `FecEncoder` / `PacketTransport` 等 kcp-rs 自有 API

3. **kcptun-common**（已依赖 kcp-rs）：  
   - `CryptoTransport` 改用 `kcrypt_rs::wire::*`  
   - `CryptEngine` 改用 `kcrypt_rs::crypt::CryptEngine`  
   - `kcp_session` / `kcp_config_from` 保持 `KcpConfig`（在 kcp-rs）

4. **client/server**：  
   - 所有 `kcp_rs::CryptoBuf` / `decrypt_cfb_in_place` / `inbound_null` / `should_cpu_block_*` /
     `encrypt_batch(_into)` / `set_offload_profile` / `OffloadProfile` → `kcrypt_rs::` 同义路径  
   - 不再依赖 `kcp_rs::` 的 crypto/wire 符号  
   - `kcp_rs::{KCP, FecDecoder, FecEncoder, fec_*, snmp_*, DEFAULT_SNMP}` 保持不动

5. **测试**：  
   - kcrypt-rs 含 wire 单元测试（roundtrip encrypt/decrypt，随 `wire.rs`）  
   - 修复 `crypt/salsa20.rs` x86_64 `sse2_matches_scalar` 测试：`*b"…"` 为 `[u8;29]`，
     与 aarch64 测试一致地补零到 `[u8;32]`

6. **Cargo**：  
   - kcp-rs：**无** kcrypt-rs 依赖（连 optional 都不留）  
   - kcrypt-rs：feature `wire`（default = ["wire"]）  
   - kcptun-common：新增 `kcrypt-rs = { path = "../kcrypt-rs" }`

7. **文档/AGENTS**：  
   - kcp-rs/AGENTS.md：crypto_buf 已迁到 kcrypt-rs::wire，kcp-rs 无 crypto 依赖  
   - kcptun-common/AGENTS.md：CryptoTransport 依赖 kcrypt_rs::wire

---

## 4. 与 B1 的关系

B1（KcpConfig）先做（已规划）；B2-A 可与 B1 同 sprint 或紧随（B1 完成后 crypto_buf 不再在 kcp-rs）。
B2 完成后 B3（kcp-rs 去 kcrypt 默认依赖）**已不需要**——kcp-rs 已彻底无依赖。

---

## 5. 验收清单（全部通过）

- [x] `kcrypt-rs` 有 `wire.rs`（重命名自 crypto_buf）  
- [x] `kcp-rs` **删除** `crypto_buf.rs`  
- [x] `kcp-rs` **无** kcrypt-rs 依赖（默认 & 所有 feature）  
- [x] `kcp-rs` re-export 全部删掉  
- [x] `kcptun-common` 依赖 `kcrypt_rs::wire::*`  
- [x] client/server 改用 `kcrypt_rs::`（无 `kcp_rs::` crypto 符号）  
- [x] `cargo check -p kcp-rs` 通过  
- [x] `cargo check --workspace` 通过（含 client/server）  
- [x] `cargo test --workspace --no-run` 通过  
- [x] kcrypt-rs 单元测试（wire roundtrip）通过  
- [x] 修复 salsa20 x86_64 测试 `[u8;29]` → `[u8;32]`  
- [x] `make gate` 三闸：fmt ✓ / clippy `-D warnings` ✓ / 单元测试 122 项全过 ✓  
  ⚠️ 注：`kcptun-server` stress_test 在 `cargo test --workspace`（debug/并行）下会失败，
  且 `make stress`（release + `--test-threads=1`）也有 2 个已知 flaky 测试
  （`test_multithread_large_data` / `test_page_refresh_simulation`，截断 `recv 120000/131072`）。
  已用基线 worktree（HEAD 1f1b5bc8）验证：**与 B2 改动无关，改动前同样失败**。

---

## 6. 后续

B2 完成后 B3（kcp-rs 去 kcrypt 默认依赖）**已不需要**。剩余可选项：
- 迁移 AGENTS.md 文档指向（已完成）  
- 若外部代码仍用 `kcp_rs::CryptoBuf` 等，需改用 `kcrypt_rs::wire::*`（本仓库已全部迁移）

---

## 7. 后续修复（2026-07-31 下午）：config 对齐 Go + make stress

### 7.1 config `-1` 解析错误（Go signed int 对齐）

**症状**：`Error: invalid value: integer -1, expected u64 at line 22 column 21`
（Go kcptun config 全用 signed `int`，可含负值；Rust 原用 unsigned `u32/u64` 拒绝）。

**修复**：
- 时间/时长类字段（`autoexpire` `scavengettl` `keepalive` `closewait` `snmpperiod`）→ `i64`
  （Go 允许负值，如 `-1`）。
- 计数/窗口/大小类字段保持 unsigned（`u32`/`u8`/`usize`/`u16`）——不可能为负。
- 负值在应用到 KCP/SMUX config 时 `.max(0)` clamp 到 0。
- 移除临时 `kcptun_common::go_config` deserializer 模块（signed 类型原生解析 `-1`）。

### 7.2 CLI 默认值与 merge 优先级

- **保留** CLI `default_value`（`key`/`crypt`/`mode`/`listen`/`target`），`--help` 显示默认值。
- merge 优先级改为 **config 优先**（`cfg.x.or(cli.x)`），与 Go 一致
  （Go 先赋 flag 默认值，再 `parseJSONConfig` 覆盖 → config 生效）。
- 原 `cli.x.or(cfg.x)` 让 `default_value` 遮蔽 config（如 `"crypt":"null"` 被忽略）→ 已修复。
- `allow_negative_numbers` 使 `--keepalive -1` 等 CLI 负值可解析。

### 7.3 空字符串 log/snmplog

- Go 默认 `"log": ""` = stderr；Rust 原先 `open("")` 报 ENOENT。已修复：空串视为无日志文件。

### 7.4 make stress

- Makefile `stress` 目标只 build server，未 build client → 干净环境 `cli: NotFound`。
  **已修复**：先 `cargo build --release -p kcptun-client -p kcptun-server` 再跑 stress。
- 已知 flaky 测试（`test_multithread_large_data` / `test_page_refresh_simulation`）
  是**既有问题**（基线 HEAD 同样失败），单通道 100 流下 `recv 120000/131072` 截断，
  与本次改动无关，见 `bugs/BUGREPORT.md`（多次修复历史）。
