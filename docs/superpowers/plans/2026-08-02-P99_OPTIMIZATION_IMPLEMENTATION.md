# P99延迟优化实施计划（利用单发送者修复）

> **给代理工作者的要求：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 按任务逐个执行此计划。步骤使用复选框（- [ ]）语法进行跟踪。

**目标：** 在 256KiB@500RPS 饱和回声负载下，将 Rust raw-KCP P99 延迟提升 20-40% 超过 Go kcp-go v5，使用 P99_SINGLE_OWNER_FIX.md 中的单发送者模型。将 Rust raw-KCP 设为生产隧道的新基线。

**架构：** 单发送者 flush 循环保持基础（不更改 raw_packets 或 ACK 生产）。添加：
- 探针间隔降低（IKCP_PROBE_INIT_NODELAY=20）。
- 回声服务器背压（WAIT_FALLBACK_MS）。
- 非阻塞 flush 唤醒（mpsc + notify_one）。
- pprof + 结构化日志用于证据。
所有更改仅限于 kcp-rs/src/kcp.rs + bench/ 脚本。

**技术栈：** Rust（tokio/smol）、Cargo、pprof、perf（macOS）、Git。

## 全局约束
- 每次更改后必须通过 `make gate`（fmt + test + clippy -D warnings）。
- 必须与 Go kcp-go v5.6.64 保持 100% 线协议和格式兼容。
- 无新依赖。
- 所有更改必须在 release 构建（opt-level=3 + LTO）下。
- 使用 `pprof -http=127.0.0.1:6060` + `go tool pprof` 获取 CPU/堆栈。
- 回归测试：fast retrans < 3000/2s（与单发送者修复相同）。

---

## 任务 1：探针间隔降低（20 µs）

**文件：**
- 修改：`kcp-rs/src/kcp.rs:40-45`
- 测试：`bench/run_p99_regression.sh`（复用）

**接口：**
- 消耗：无（纯配置更改）
- 产生：降低 nodelay 模式的探针时间

- [ ] **步骤 1：编写失败的测试**
```bash
# 在 bench/run_p99_regression.sh 中
assert_p99_lt_go() {
    local p99=$(grep -A5 "kcp-rs(tokio)↔kcp-rs(tokio)" LATENCY_P99_REPORT.md | grep P99 | awk '{print $2}')
    assert_true "P99 $p99 > Go P99" [ "$p99" -lt "$go_p99" ]
}
```

- [ ] **步骤 2：运行测试验证失败**
```bash
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 3：编写最小实现**
```diff
- pub(crate) const IKCP_PROBE_INIT_NODELAY: u32 = 50;
+ pub(crate) const IKCP_PROBE_INIT_NODELAY: u32 = 20;
```

- [ ] **步骤 4：运行测试验证通过**
```bash
cargo test --release --package kcp-rs --test raw_kcp -- --nocapture
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 5：提交**
```bash
git add kcp-rs/src/kcp.rs bench/run_p99_regression.sh
git commit -m "chore(kcp): reduce probe interval to 20µs for nodelay mode"
```

## 任务 2：非阻塞 flush 唤醒（mpsc + notify_one）

**文件：**
- 修改：`kcp-rs/src/kcp.rs:300-350`（flush_data_only 和 notify 路径）
- 测试：`bench/run_p99_regression.sh`

**接口：**
- 消耗：无
- 产生：保持单发送者，但无忙等待

- [ ] **步骤 1：编写失败的测试**
```bash
# 添加到回归脚本
assert_no_stall() {
    pprof -http=127.0.0.1:6060 -sample_period=1000000000 2>/dev/null > /tmp/flush.pprof
    go tool pprof -sample_trace=10 /tmp/flush.pprof | grep -q "yield_now\|sleep"
    assert_false "yield_now or sleep found in flush stack"
}
```

- [ ] **步骤 2：运行测试验证失败**
```bash
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 3：编写最小实现**
```diff
- std::thread::sleep(0);
+ std::sync::mpsc::channel::<()>();
+ notify_one.send(()).unwrap();
```

- [ ] **步骤 4：运行测试验证通过**
```bash
cargo test --release --package kcp-rs --test raw_kcp -- --nocapture
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 5：提交**
```bash
git add kcp-rs/src/kcp.rs bench/run_p99_regression.sh
git commit -m "chore(kcp): replace busy-wait with mpsc notify_one in flush_data_only"
```

## 任务 3：回声服务器背压

**文件：**
- 修改：`bench/echo_server.py`
- 测试：`bench/run_p99_regression.sh`

**接口：**
- 消耗：无
- 产生：移除客户端写阻塞

- [ ] **步骤 1：编写失败的测试**
```python
# 在 bench/echo_server.py 中
def test_backpressure():
    # 模拟客户端写阻塞
    assert_server_p99_lt("with fallback", 50)  # 目标 <50ms
```

- [ ] **步骤 2：运行测试验证失败**
```bash
python3 bench/echo_server.py --help
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 3：编写最小实现**
```diff
- parser.add_argument("--wait-fallback-ms", type=int, default=0)
+ parser.add_argument("--wait-fallback-ms", type=int, default=10)
```

- [ ] **步骤 4：运行测试验证通过**
```bash
cargo test --release --package kcp-rs --test raw_kcp -- --nocapture
python3 bench/echo_server.py --wait-fallback-ms 10
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 5：提交**
```bash
git add bench/echo_server.py bench/run_p99_regression.sh
git commit -m "feat(bench): add --wait-fallback-ms=10 to break self-reinforcing p99 stalls"
```

## 任务 4：证据收集（pprof + 日志）

**文件：**
- 修改：`bench/run_p99.sh` + `bench/profile_rust_go_pprof.sh`

**接口：**
- 消耗：无
- 产生：pprof 端点 + 结构化日志

- [ ] **步骤 1：编写失败的测试**
```bash
# 添加到回归脚本
assert_pprof_stacks() {
    pprof -http=127.0.0.1:6060 2>/dev/null | grep -q "kcp.send"
    assert_true "pprof endpoint responds"
}
```

- [ ] **步骤 2：运行测试验证失败**
```bash
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 3：编写最小实现**
```diff
- # 添加 --features pprof 到服务器
+ cargo build --release --features pprof
```

- [ ] **步骤 4：运行测试验证通过**
```bash
cargo test --release --package kcp-rs --test raw_kcp -- --nocapture
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 5：提交**
```bash
git add bench/run_p99.sh bench/profile_rust_go_pprof.sh
git commit -m "chore(bench): enable pprof in kcptun-server for CPU/heap evidence"
```

## 任务 5：回归保护（fast retrans）

**文件：**
- 修改：`bench/run_p99_regression.sh`

**接口：**
- 消耗：所有先前更改
- 产生：未来更改的硬保护

- [ ] **步骤 1：编写失败的测试**
```bash
assert_fast_retrans_lt() {
    curl -s http://127.0.0.1:6060/debug/pprof/heap | head -1
    # 或从日志解析
}
```

- [ ] **步骤 2：运行测试验证失败**
```bash
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 3：编写最小实现**
```diff
- # 添加断言
+ assert_fast_retrans_lt() { [ "$fast_retrans" -lt 3000 ] }
```

- [ ] **步骤 4：运行测试验证通过**
```bash
cargo test --release --package kcp-rs --test raw_kcp -- --nocapture
bash bench/run_p99_regression.sh --rps 500 --size 256KiB --report-only
```

- [ ] **步骤 5：提交**
```bash
git add bench/run_p99_regression.sh
git commit -m "chore(bench): add fast retrans <3000/2s guard from single-owner fix"
```

## 全局后计划步骤
- `make gate`
- `cargo test --release --package kcp-rs --test raw_kcp`
- 运行完整 P99 矩阵并比较表格
- 使用原子 PR 描述提交所有更改
- 在 LATENCY_P99_REPORT.md 中标记新基线

计划已保存到 `docs/superpowers/plans/2026-08-02-P99_OPTIMIZATION_IMPLEMENTATION.md`。有两种执行选项：

**1. 子代理驱动（推荐）** - 我为每个任务分派新的子代理，并在任务间进行审查，快速迭代

**2. 直接执行** - 在此会话中执行任务，使用执行计划进行批量执行并设置检查点

**你选择哪种方法？**