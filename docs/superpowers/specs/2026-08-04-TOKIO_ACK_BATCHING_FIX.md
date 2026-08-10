# Spec: tokio server ACK batching fix — implementation record

> **Canonical path (git):** `docs/superpowers/specs/2026-08-04-TOKIO_ACK_BATCHING_FIX.md`

| Field | Value |
|-------|-------|
| Implemented | 2026-08-04 |
| All commits | single session, ahead of `origin/master` |
| Bug report | n/a (perf gap, not correctness bug) |

## 改动的文件列表（Files changed）

### `kcp-rs/src/conn.rs`

| 改动 | 原因 |
|------|------|
| `PeerQueue::push_and_reuse` 移除内部 `notify_one()` | notify 改由 reader 批量控制；逐包 notify 是 tokio 下 ACK 膨胀的根因 |
| `spawn_listener_reader` 改为批量 drain + 每 affected queue notify 一次 | tokio 多线程下 input loop 被逐包跨线程唤醒 → 1 包 burst → 每数据段一个 ACK |
| 新增 spare 缓冲池（`spares: Vec<Vec<u8>>`） | drain 循环复用 2KB 缓冲，避免逐 datagram 分配（`WouldBlock` 时回收 scratch） |
| `buf.resize(MAX_DATAGRAM, 0)` 安全网 | build-error 路径可能回收截断缓冲，防止下次 `recv_from` 拿到空缓冲 |

### `bench/profiles/HOTSPOTS.md`

记录 2026-08-04 发现（pprof + SNMP 证据）与修复。

### `CHANGELOG.md`

新增 Unreleased → Performance 条目，含根因、修复、验证数字。

### `bench_results.json`

三方（tokio/smol/Go）完整复测结果（修复后）。

## 修复的故障路径（Fixed failure paths）

1. **tokio 服务端 ACK 膨胀（14×）**：reader 逐包 notify → input loop 小 burst →
   `flush_input_batch` 每数据段发一个 ACK datagram → `send_to` syscall 占 51.6%（1.73 核）。
2. **每字节 send syscall 数 3.4× vs smol**：ACK 批量差 + 逐包处理开销叠加。
3. **4 并发下 tokio 每连接 5.4× 退化**：共享 UDP reader 被 4 流 datagram 压垮 + ACK 洪泛回灌。

## 测试结果（Test results）

- `make gate`（fmt + workspace tests + clippy -D warnings）：全过
- `cargo test -p kcp-rs`（sync 4 + async-tokio listener 2/integrity 3 + async-smol 同）：全过
- `make stress`（kcptun-server stress_test）：8 passed, 0 failed
- `bash test_e2e.sh`（Go↔Rust 全矩阵 interop）：138 passed, 0 failed
- SNMP 复测（50 轮 × 4 并发，release，null/nocomp）：
  - tokio：25.3 → **41.2 MB/s**（+63%），ACK datagram 70,473 → **959**（73×）
  - smol：38.4 → 43.4 MB/s（无回归）
- `bench_rust_vs_go.py` 完整复测：运行中

## 修订记录（Revision history）

| 日期 | 变更 |
|------|------|
| 2026-08-04 | 初稿；实现 reader 批量 notify + 验证 |
