# kcptun-rs 项目布局优化迁移计划

> **目标**: 根目录精简至 8~10 个入口文件，脚本/文档/Docker 按类型归类到一级目录，确保所有 Makefile 和脚本引用同步更新，迁移后 `make bench` / `make e2e` / `make check-all` 等全部命令可正常运行。
>
> **约束**: `CLAUDE.md`、`AGENTS.md` 是 AI agent 指令文件，必须保留在原位，不可移动。

---

## 一、目标结构

```
根目录 (精简后)
├── Cargo.toml
├── Cargo.lock
├── Makefile
├── .gitignore
├── .dockerignore
├── README.md
├── README.zh.md
├── CHANGELOG.md
├── DISCLAIMER.md
├── DISCLAIMER.zh.md
├── CLAUDE.md                     ← AI 指令文件，不可移动
├── AGENTS.md                     ← AI 指令文件，不可移动
│
├── docs/                         ← 📚 所有文档统一入口
│   ├── project/                  ← 项目元信息 (CONTEXT/PAGE)
│   ├── architecture/             ← 架构审计/迁移文档
│   ├── performance/              ← 性能分析报告 (从 bench/ 迁入)
│   ├── bugs/                     ← Bug 报告 (从 bugs/ 迁入，可选保留原位置)
│   ├── superpowers/              ← 保持不变 (plans/ + specs/)
│   └── *.md                      ← 根级单文件文档
│
├── scripts/                      ← 📜 所有脚本统一入口
│   ├── test/                     ← 测试脚本 (从根目录迁入)
│   │   ├── run_all_tests.sh
│   │   ├── test_e2e.sh
│   │   ├── smoke_test_rust_rust.sh
│   │   ├── fix_e2e.sh       # 已在 .gitignore，仅移动不修改引用
│   │   └── verify.sh
│   ├── bench/                   ← 基准测试脚本 (从 bench/ 迁入)
│   │   ├── run_bench.sh
│   │   ├── bench_rust_vs_go.py
│   │   ├── throughput.py
│   │   ├── latency_cmp.py
│   │   ├── echo_server.py
│   │   ├── probe_tunnel.py
│   │   ├── quick_tunnel_test.py
│   │   ├── tunnel_echo_cmp.py
│   │   └── thr_random.py
│   ├── profile/                  ← 性能分析脚本 (从 bench/ 迁入)
│   │   ├── profile_rust_go_pprof.sh
│   │   ├── profile_go_pprof.sh
│   │   ├── profile_under_load.sh
│   │   ├── run_tunnel_impl.sh
│   │   ├── run_p99.sh
│   │   ├── run_p99_regression.sh
│   │   ├── tunnel_p99.sh
│   │   ├── sweep_h1_h2_thresholds.sh
│   │   └── verify_h2_xor_comp.sh
│   └── docker/                   ← Docker 辅助脚本
│       ├── docker-bench.sh
│       └── docker-verify.sh
│
├── docker/                       ← 🐳 Dockerfile 集中
│   ├── Dockerfile.bench
│   └── Dockerfile.linux-test
│
├── bench/                        ← 🧪 精简为纯数据+工具
│   ├── kcptun_prof_wl            ← 二进制工具
│   ├── profiles/                 ← profiling 输出数据
│   │   ├── README.md
│   │   ├── HOTSPOTS.md
│   │   └── .gitkeep
│   └── results/                  ← 📊 新建: 结果数据
│       ├── bench_results.json    ← 从根迁入
│       └── bench_docker_results.json ← 从根迁入
│
├── tests/                        ← 保持不变 + 接收 kcptun-go-linux
│   ├── kcptun-go/
│   ├── kcptun-go-linux/          ← 从根迁入
│   └── kcp-go-latency/
│
└── [9 个 crate 目录保持不变]
```

---

## 二、迁移步骤（按依赖顺序执行）

### Step 1: 创建目标目录结构

```bash
mkdir -p scripts/test
mkdir -p scripts/bench
mkdir -p scripts/profile
mkdir -p scripts/docker
mkdir -p docker
mkdir -p bench/results
mkdir -p docs/project
mkdir -p docs/architecture
mkdir -p docs/performance
mkdir -p tests/kcptun-go-linux
```

### Step 2: 移动文件

#### 2.1 根目录脚本 → `scripts/test/`

```bash
git mv run_all_tests.sh   scripts/test/
git mv test_e2e.sh        scripts/test/
git mv smoke_test_rust_rust.sh scripts/test/
git mv verify.sh          scripts/test/
git mv fix_e2e.sh         scripts/test/    # 已在 .gitignore
```

#### 2.2 根目录 Python → `scripts/bench/`

```bash
git mv bench_rust_vs_go.py scripts/bench/
```

#### 2.3 根目录 Docker 辅助脚本 → `scripts/docker/`

```bash
git mv docker-bench.sh    scripts/docker/
git mv docker-verify.sh   scripts/docker/
```

#### 2.4 根目录 Dockerfile → `docker/`

```bash
git mv Dockerfile.bench     docker/
git mv Dockerfile.linux-test docker/
```

#### 2.5 bench/ 脚本按类型拆分

```bash
# 基准测试脚本 (Python + shell)
git mv bench/run_bench.sh       scripts/bench/
git mv bench/throughput.py      scripts/bench/
git mv bench/latency_cmp.py     scripts/bench/
git mv bench/echo_server.py     scripts/bench/
git mv bench/probe_tunnel.py    scripts/bench/
git mv bench/quick_tunnel_test.py   scripts/bench/
git mv bench/tunnel_echo_cmp.py     scripts/bench/
git mv bench/thr_random.py      scripts/bench/

# 性能分析脚本
git mv bench/profile_rust_go_pprof.sh  scripts/profile/
git mv bench/profile_go_pprof.sh       scripts/profile/
git mv bench/profile_under_load.sh     scripts/profile/
git mv bench/run_tunnel_impl.sh        scripts/profile/
git mv bench/run_p99.sh                scripts/profile/
git mv bench/run_p99_regression.sh     scripts/profile/
git mv bench/tunnel_p99.sh             scripts/profile/
git mv bench/sweep_h1_h2_thresholds.sh scripts/profile/
git mv bench/verify_h2_xor_comp.sh     scripts/profile/
```

#### 2.6 bench/ 文档 → `docs/performance/`

```bash
git mv bench/LATENCY_OPTIMIZATION_ANALYSIS.md docs/performance/
git mv bench/LATENCY_P99_REPORT.md            docs/performance/
git mv bench/LIB_SATURATION_PLAN.md           docs/performance/
git mv bench/P99_SINGLE_OWNER_FIX.md          docs/performance/
git mv bench/PERF_REGRESSION_REPORT_00e5e3df.md docs/performance/
git mv bench/PROFILE_RUNBOOK.md               docs/performance/
git mv bench/TUNNEL_LATENCY_ANALYSIS.md       docs/performance/
git mv bench/TUNNEL_TEST_MATRIX.md            docs/performance/
git mv bench/REPORT_TEMPLATE.md               docs/performance/
```

#### 2.7 根目录文档 → `docs/project/` 或 `docs/architecture/`

```bash
# 仅移动非 AI 指令文件（CLAUDE.md 和 AGENTS.md 不可动）
git mv CONTEXT.md  docs/project/
git mv PAGE.md     docs/project/     # 已在 .gitignore，也移过去整理

# 架构相关 (从 docs/ 根级整理)
git mv docs/CODE_AUDIT_AND_STANDARDS.md     docs/architecture/
git mv docs/KCPTUN_RS_CODEBASE_AUDIT_REPORT.md docs/architecture/
git mv docs/RUST_IDIOMS_MIGRATION.md           docs/architecture/
git mv docs/RUST_IDIOMS_MIGRATION.proposed.2026-07-23.md docs/architecture/
git mv docs/DEAD_CODE_ANALYSIS_REPORT.md       docs/architecture/
git mv docs/R4_STREAM_INNER_AND_COMMON_EXTRACT.md docs/architecture/
git mv docs/SMOKE_TEST_COVERAGE_ANALYSIS.md    docs/architecture/

# 性能相关 (docs/ 中已有的)
git mv docs/PERF_OPTIMIZATION_PLAN.md            docs/performance/
git mv docs/GOAL_RUNTIME_PERF_EVIDENCE.md        docs/performance/
git mv docs/OPT_CPU_BLOCK_SNAPPY_INVESTIGATION.md docs/performance/
```

#### 2.8 根目录数据文件 → `bench/results/`

```bash
git mv bench_results.json        bench/results/
git mv bench_docker_results.json bench/results/
```

#### 2.9 kcptun-go-linux → tests/

```bash
git mv kcptun-go-linux tests/
```

#### 2.10 bugs/ → docs/bugs/ (可选)

```bash
# 如果希望统一文档管理
mkdir -p docs/bugs
git mv bugs/*.md docs/bugs/
# 保留空 bugs/ 目录或删除
```

---

### Step 3: 更新 Makefile 引用

> **关键**: 所有 Makefile 中的脚本路径必须同步更新。

| 行号 | 当前内容 | 修改为 |
|------|----------|--------|
| 520 | `@bash test_e2e.sh` | `@bash scripts/test/test_e2e.sh` |
| 556 | `@bash run_all_tests.sh $(CHECK_ALL_ARGS)` | `@bash scripts/test/run_all_tests.sh $(CHECK_ALL_ARGS)` |
| 580 | `@bash bench/run_bench.sh` | `@bash scripts/bench/run_bench.sh` |
| 585 | `@bash bench/profile_rust_go_pprof.sh` | `@bash scripts/profile/profile_rust_go_pprof.sh` |
| 588 | `@bash bench/profile_rust_go_pprof.sh mem` | `@bash scripts/profile/profile_rust_go_pprof.sh mem` |
| 599 | `@bash bench/profile_go_pprof.sh` | `@bash scripts/profile/profile_go_pprof.sh` |
| 603 | `@bash bench/profile_rust_go_pprof.sh` | `@bash scripts/profile/profile_rust_go_pprof.sh` |

Makefile 中无其他需要修改的引用（bench 路径通过 Makefile 变量传入脚本，不硬编码）。

---

### Step 4: 更新脚本内部引用

#### 4.1 `scripts/test/run_all_tests.sh`

| 行号 | 当前 | 修改为 |
|------|------|--------|
| 131 | `python3 bench_rust_vs_go.py` | `python3 scripts/bench/bench_rust_vs_go.py` |
| 139 | `bash test_e2e.sh` | `bash scripts/test/test_e2e.sh` |

脚本内的 `ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` 会自动适配新路径，`cd "$ROOT"` 后所有相对路径（`cargo`、`target/`、`tests/` 等）无需修改。

#### 4.2 `scripts/test/test_e2e.sh`

```bash
# 第 6 行: cd "$(dirname "$0")" 会自动适配，因为路径变成了 scripts/test/
# 但二进制引用使用 ./target/... (脚本 cd 到脚本目录后相对引用)
# 需要在 cd 后回到项目根目录

# 第 6 行改为:
cd "$(dirname "$0")/../.."
```

| 行号 | 变量 | 当前值 | 修改为 |
|------|------|--------|--------|
| 9 | `GO_SERVER` | `./tests/kcptun-go/server` | 不变 (cd 已在根目录) |
| 10 | `GO_CLIENT` | `./tests/kcptun-go/client` | 不变 |
| 11 | `RUST_SERVER` | `./target/release/kcptun-server` | 不变 |
| 12 | `RUST_CLIENT` | `./target/release/kcptun-client` | 不变 |
| 13 | `RUST_SMOL_SERVER` | `./target/smol-release/release/kcptun-server` | 不变 |
| 14 | `RUST_SMOL_CLIENT` | `./target/smol-release/release/kcptun-client` | 不变 |

#### 4.3 `scripts/test/smoke_test_rust_rust.sh`

```bash
# 第 19 行改为:
cd "$(dirname "$0")/../.."
```

其他路径不变（`./target/release/...`、`./target/smol-release/...` 都已从根目录相对引用）。

第 1651 行提到 `bugs/BUGREPORT_PROXY_MEMORY_GROWTH.md` 仅在注释中，不需要修改。

#### 4.4 `scripts/test/verify.sh`

```bash
# 第 6 行改为:
cd "$(dirname "$0")/../.."
```

其余 `cargo` 命令不需要路径前缀（`cargo` 在项目根运行即可）。

#### 4.5 `scripts/bench/run_bench.sh`

```bash
# 第 13 行: cd "$(dirname "$0")/.." → cd "$(dirname "$0")/../.."

# 第 173 行: python3 bench/throughput.py → python3 scripts/bench/throughput.py
```

`cd "$(dirname "$0")/../.."` 确保 `cd` 到项目根，后续 `./target/...`、`./tests/...` 路径无需修改。

`GO_SERVER` / `GO_CLIENT` 等变量使用 `./` 前缀从根目录相对引用，无需改值。

`BENCH_FILTER` 中的 label 比较不变。

#### 4.6 `scripts/profile/profile_rust_go_pprof.sh`

```bash
# 第 22 行: cd "$(dirname "$0")/.." → cd "$(dirname "$0")/../.."

# 第 43 行: OUT_DIR="${OUT_DIR:-bench/profiles}" → 不变 (bench/ 仍然是数据目录)

# 第 156 行: python3 bench/throughput.py → python3 scripts/bench/throughput.py
```

#### 4.7 `scripts/profile/profile_go_pprof.sh`

```bash
# 第 24 行: cd "$(dirname "$0")/.." → cd "$(dirname "$0")/../.."

# 第 35 行: OUT_DIR="${OUT_DIR:-bench/profiles}" → 不变

# 第 147 行: python3 bench/throughput.py → python3 scripts/bench/throughput.py
```

#### 4.8 `scripts/profile/run_p99.sh`

```bash
# 第 29 行: REPORT="$REPO/bench/LATENCY_P99_REPORT.md" → REPORT="$REPO/docs/performance/LATENCY_P99_REPORT.md"
```

检查 `cd` 行是否需要调整（同模式：`cd "$(dirname "$0")/../.."`）。

#### 4.9 `scripts/profile/tunnel_p99.sh`

检查 `cd` 行，如果脚本内使用了 `$ROOT/bench/` 形式的路径，需要改为 `$ROOT/scripts/bench/`。

#### 4.10 `scripts/profile/sweep_h1_h2_thresholds.sh`

```bash
# 第 9 行: OUT="...bench/profiles/..." → 不变 (数据目录)

# 第 93 行: python3 "$ROOT/bench/thr_random.py" → python3 "$ROOT/scripts/bench/thr_random.py"
```

#### 4.11 `scripts/profile/verify_h2_xor_comp.sh`

```bash
# 第 15 行: OUT_DIR="...bench/profiles/..." → 不变

# 第 170 行: python3 "$ROOT/bench/throughput.py" → python3 "$ROOT/scripts/bench/throughput.py"
```

#### 4.12 `scripts/bench/bench_rust_vs_go.py`

```python
# 第 37 行: REPO = os.path.dirname(os.path.abspath(__file__))
# 该代码自动适配脚本所在目录，但因为脚本从根移到了 scripts/bench/，
# REPO 会指向 scripts/bench/ 而非项目根

# 修改为:
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
```

检查脚本内部对 `target/release/kcptun-server` 等二进制路径的引用 —— 如果使用了 `os.path.join(REPO, ...)`，需要确保 REPO 指向项目根。

#### 4.13 `scripts/docker/docker-bench.sh`

```bash
# 第 19 行: cd "$(dirname "$0")" → cd "$(dirname "$0")/../.."

# 第 22 行: GO_OUT_DIR="kcptun-go-linux" → GO_OUT_DIR="tests/kcptun-go-linux"

# 第 76 行: docker build -f Dockerfile.bench -t "$IMAGE_NAME" .
# → docker build -f docker/Dockerfile.bench -t "$IMAGE_NAME" .
```

注意：第 55 行引用了硬编码路径 `/Users/yangzhiqin/Desktop/kcptun-rs/$GO_OUT_DIR/`，需要跟随更新。

#### 4.14 `scripts/docker/docker-verify.sh`

该脚本在 Docker 容器内运行，使用 `/app/` 前缀的绝对路径，不需要修改。

### Step 5: 更新 Dockerfile 引用

#### 5.1 `docker/Dockerfile.bench`

| 行号 | 当前 | 修改为 |
|------|------|--------|
| 6 | `kcptun-go-linux/` (注释) | `tests/kcptun-go-linux/` |
| 54 | `COPY kcptun-go-linux/server /app/...` | `COPY tests/kcptun-go-linux/server /app/...` |
| 55 | `COPY kcptun-go-linux/client /app/...` | `COPY tests/kcptun-go-linux/client /app/...` |
| 59 | `COPY bench_rust_vs_go.py /app/...` | `COPY scripts/bench/bench_rust_vs_go.py /app/...` |

注意：Docker build context 是 `.`（项目根目录），`COPY` 的源路径始终从项目根开始计算。

#### 5.2 `docker/Dockerfile.linux-test`

| 行号 | 当前 | 修改为 |
|------|------|--------|
| 56 | `COPY docker-verify.sh /app/verify.sh` | `COPY scripts/docker/docker-verify.sh /app/verify.sh` |

---

### Step 6: 更新 `.gitignore`

当前 `.gitignore` 中：
- `PAGE.md` — 现在在 `docs/project/`，改为 `docs/project/PAGE.md` 或删除（如果不再需要 gitignore）
- `fix_e2e.sh` — 现在在 `scripts/test/`，改为 `scripts/test/fix_e2e.sh` 或删除
- `/bench/kcptun_prof_wl` — 保持不变
- `/bench/profiles/**` — 保持不变

添加：
```gitignore
# 基准测试结果文件 (运行时产物)
bench_results.json
bench_docker_results.json
__pycache__/
```

或者改为：
```gitignore
/bench/results/bench_results.json
/bench/results/bench_docker_results.json
```

---

### Step 7: 更新 `.dockerignore`

| 行号 | 当前 | 修改为 |
|------|------|--------|
| 19 | `!kcptun-go-linux/` | `!tests/kcptun-go-linux/` |
| 19 后 | — | 添加 `!scripts/bench/bench_rust_vs_go.py` (确保 Docker build 可以 COPY) |

检查：`.dockerignore` 中有 `*.md` 排除规则，如果 Docker build 需要 COPY 任何 .md 文件，需要 `!` 放行。

---

### Step 8: 更新文档中的路径引用（非阻塞，可后续处理）

这些是纯文档引用，不影响功能，但建议逐步更新。

> **注意**: `CLAUDE.md`、`AGENTS.md`（根目录及各 crate 内）不移动、不修改路径引用结构，仅在内容中更新对其他文件的描述性路径。

| 文件 | 旧引用 | 新引用 |
|------|--------|--------|
| `AGENTS.md` 根 | `bench_rust_vs_go.py` | `scripts/bench/bench_rust_vs_go.py` |
| `AGENTS.md` 根 | `bench/run_bench.sh` | `scripts/bench/run_bench.sh` |
| `AGENTS.md` 根 | `test_e2e.sh` | `scripts/test/test_e2e.sh` |
| `AGENTS.md` 根 | `bench/PROFILE_RUNBOOK.md` | `docs/performance/PROFILE_RUNBOOK.md` |
| `CHANGELOG.md` | `bench/profile_rust_go_pprof.sh` | `scripts/profile/profile_rust_go_pprof.sh` |
| `CHANGELOG.md` | `bench/run_bench.sh` | `scripts/bench/run_bench.sh` |
| `CHANGELOG.md` | `bench/PROFILE_RUNBOOK.md` | `docs/performance/PROFILE_RUNBOOK.md` |
| `README.zh.md` | `bench/run_bench.sh` | `scripts/bench/run_bench.sh` |
| `docs/PERF_OPTIMIZATION_PLAN.md` | `test_e2e.sh` | `scripts/test/test_e2e.sh` |
| `docs/SMOKE_TEST_COVERAGE_ANALYSIS.md` | `smoke_test_rust_rust.sh` / `test_e2e.sh` | `scripts/test/...` |
| `docs/R4_STREAM_INNER_AND_COMMON_EXTRACT.md` | `test_e2e.sh` / `smoke_test_rust_rust.sh` | `scripts/test/...` |
| `docs/architecture/KCPTUN_RS_CODEBASE_AUDIT_REPORT.md` | `test_e2e.sh` | `scripts/test/test_e2e.sh` |
| `bench/AGENTS.md` | 全文 | 更新为 `scripts/bench/` + `docs/performance/` 新路径 |
| `bench/PROFILE_RUNBOOK.md` | `bash bench/...` | `bash scripts/profile/...` |
| `bench/PERF_REGRESSION_REPORT_00e5e3df.md` | `bash bench/run_bench.sh` | `bash scripts/bench/run_bench.sh` |
| `bugs/BUGREPORT*.md` | `bash test_e2e.sh` | `bash scripts/test/test_e2e.sh` |
| `.claude/skills/flamegraph-perf/SKILL.md` | `bash test_e2e.sh` | `bash scripts/test/test_e2e.sh` |
| `smux-rs/AGENTS.md` | `bash test_e2e.sh` | `bash scripts/test/test_e2e.sh` |

---

### Step 9: 验证清单

迁移完成后，按顺序验证：

```bash
# 1. 构建检查
make build
make release

# 2. 单元测试
make test

# 3. Clippy
make clippy

# 4. E2E 测试 (需要 Go 二进制)
make e2e
# 或: bash scripts/test/test_e2e.sh

# 5. 冒烟测试 (需要 release 构建)
make release release-smol
bash scripts/test/smoke_test_rust_rust.sh

# 6. Benchmark (需要 release 构建)
make bench
# 或: bash scripts/bench/run_bench.sh

# 7. 全量检查
make check-all
# 或: bash scripts/test/run_all_tests.sh

# 8. Profiling
make profiling-bins
make profile

# 9. Docker (如果有 Docker 环境)
bash scripts/docker/docker-bench.sh --quick --rust-only

# 10. 检查无残留引用
grep -rn "bench/run_bench\|bench/profile_rust\|bench/profile_go" Makefile scripts/ 2>/dev/null
# 预期: 只有 scripts/ 内部的自引用
```

---

## 三、风险与注意事项

| 风险 | 缓解措施 |
|------|----------|
| **AI 指令文件误移动** | `CLAUDE.md`、`AGENTS.md` 在任何位置都不可移动，计划中已排除 |
| **Docker build context 变化** | `.dockerignore` 需要确保 `scripts/` 和 `tests/kcptun-go-linux/` 不被排除 |
| **脚本 `cd` 层级错误** | 所有 `cd "$(dirname "$0")"` 改为 `cd "$(dirname "$0")/../.."`，用相对路径两次上溯到项目根 |
| **Python `REPO` 计算错误** | `bench_rust_vs_go.py` 和 bench/ 下其他 Python 脚本的 `REPO` 需要 `dirname(dirname(__file__))` |
| **AGENTS.md 路径过时** | 建议在 AGENTS.md 中只写逻辑描述不写路径，或批量 sed 替换 |
| **CI/CD 脚本 (如果有)** | 检查是否有外部 CI 配置直接引用旧路径 |
| **Git 历史** | 全部使用 `git mv` 保留文件历史 |

---

## 四、执行建议

### 建议一次性执行整个计划（约 15 分钟）

1. 切新分支: `git checkout -b refactor/layout-reorg`
2. 按 Step 1-8 顺序执行
3. 每完成一个 Step 做一次 `git commit`，便于回溯
4. 最后执行 Step 9 验证清单
5. 全部通过后合并

### 最小可行方案（30 分钟内可完成）

如果时间有限，优先执行：
- Step 1-3 (创建目录、移动文件、更新 Makefile)
- Step 4.1-4.13 (更新核心脚本 `cd` 和路径)
- Step 5 (Dockerfile)
- Step 6-7 (.gitignore / .dockerignore)
- Step 9 (验证 `make bench` / `make e2e` / `make check-all`)

文档中的路径引用 (Step 8) 可以后续用批量 `sed` 一次性处理。
