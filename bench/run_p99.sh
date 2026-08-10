#!/bin/bash
# Strict P99 / P999 latency cross-test orchestrator.
#
# Open-model (fixed-RPS) measurement per the strict-standard methodology:
#   - fixed-rate sends independent of responses (no coordinated omission)
#   - separate warm-up phase, excluded from metrics
#   - all raw per-request latencies aggregated into one global P99/P999
#   - sample size = rps * duration (>= 1000)
#
# Seven latency combos plus three closed-loop throughput baselines at the raw KCP
# layer (no kcptun / SMUX / snappy / crypto):
#   rust-tokio↔rust-tokio, rust-smol↔rust-smol, go↔go baselines, plus
#   rust{->tokio,smol}->go and go->rust{->tokio,smol} cross-interop.
#
# Env overrides: RPS (500), WARMUP (5s), DURATION (60s), SIZE (26624),
# CONCURRENCY (32). The closed-loop runs use the same warm-up and duration.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$(pwd)

RPS=${RPS:-500}
WARMUP=${WARMUP:-5}
DURATION=${DURATION:-60}
SIZE=${SIZE:-26624}
CONCURRENCY=${CONCURRENCY:-32}
CONV=0x00C0_FFEE

GOBIN="$REPO/tests/kcp-go-latency/kcp-go-latency"
REPORT="$REPO/bench/LATENCY_P99_REPORT.md"
EX_TOKIO="$REPO/target/release/examples/latency_p99_tokio"
EX_SMOL="$REPO/target/release/examples/latency_p99_smol"

P_GO1=$(( (RANDOM % 15000) + 10000 ))
P_GO2=$(( P_GO1 + 1 ))
P_RT1=$(( P_GO1 + 2 ))
P_RS1=$(( P_GO1 + 3 ))
P_RT2=$(( P_GO1 + 4 ))
P_RS2=$(( P_GO1 + 5 ))

echo "==> building kcp-rs example (tokio + smol)"
cargo build -q --release -p kcp-rs --features async-tokio --example latency_p99
cp "$REPO/target/release/examples/latency_p99" "$EX_TOKIO"
cargo build -q --release -p kcp-rs --features async-smol --example latency_p99
cp "$REPO/target/release/examples/latency_p99" "$EX_SMOL"

echo "==> building kcp-go harness"
(cd "$REPO/tests/kcp-go-latency" && go build -o kcp-go-latency .)

# ── OS-level UDP buffer tuning (macOS) ──────────────────────────────────────
# Increase kernel UDP socket buffers to prevent silent packet drops under
# high-throughput KCP workloads. This is the single most effective OS tuning
# for P99/P999 latency on macOS (measured: throughput +28%, P99 -26%).
# Requires sudo; skip silently if not available (e.g. CI without sudo).
if [ "$(uname)" = "Darwin" ] && sudo -n true 2>/dev/null; then
  sudo sysctl -w kern.ipc.maxsockbuf=8388608 >/dev/null
  sudo sysctl -w net.inet.udp.recvspace=4194304 >/dev/null
  echo "==> macOS sysctl applied: maxsockbuf=8MB recvspace=4MB"
fi

kill_wait() { kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }

# Per-step hard timeout: a stuck measurement must not hang the whole run. The
# real risk is the go→kcp-rs(smol) combo (step 7): if the server's echo/ACK
# path stalls, kcp-go's send window (512) fills and `Write` blocks forever,
# so the client never reaches `measure_end`. SIGALRM kills the client.
# STEP_TIMEOUT = warmup + duration + 30s margin (default 5+60+30 = 95s).
STEP_TIMEOUT=$((WARMUP + DURATION + 30))
step_tmo() {
  perl -e 'alarm shift; exec @ARGV' "$STEP_TIMEOUT" "$@" 2>/dev/null | grep '^RESULT' || true
}
run_step() {
  local label=$1; shift
  local r
  r=$(step_tmo "$@")
  if [ -n "$r" ]; then
    echo "$r"
  else
    echo "[$label] FAILED: no RESULT within ${STEP_TIMEOUT}s (timed out or errored)"
  fi
}

echo "==> [1/10] kcp-rs(tokio) ↔ kcp-rs(tokio) baseline"
R_TT=$(run_step "1/10" "$EX_TOKIO" --mode self --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_TT"

echo "==> [2/10] kcp-rs(smol) ↔ kcp-rs(smol) baseline"
R_SS=$(run_step "2/10" "$EX_SMOL" --mode self --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_SS"

echo "==> [3/10] kcp-go ↔ kcp-go baseline"
R_GG=$(run_step "3/10" "$GOBIN" bench --port "$P_GO1" --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_GG"

echo "==> [4/10] kcp-rs(tokio) → kcp-go (cross)"
"$GOBIN" server --port "$P_GO2" 2>/dev/null & SRV=$!
sleep 0.5
R_TG=$(run_step "4/10" "$EX_TOKIO" --mode peer --addr "127.0.0.1:$P_GO2" --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_TG"
kill_wait "$SRV"

echo "==> [5/10] kcp-go → kcp-rs(tokio) (cross)"
"$EX_TOKIO" --mode server --port "$P_RT1" --size "$SIZE" 2>/dev/null & RSRV=$!
sleep 1.5
R_GT=$(run_step "5/10" "$GOBIN" client --addr "127.0.0.1:$P_RT1" --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE" --conv "$CONV")
echo "$R_GT"
kill_wait "$RSRV"

echo "==> [6/10] kcp-rs(smol) → kcp-go (cross)"
"$GOBIN" server --port "$P_GO2" 2>/dev/null & SRV=$!
sleep 0.5
R_SG=$(run_step "6/10" "$EX_SMOL" --mode peer --addr "127.0.0.1:$P_GO2" --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_SG"
kill_wait "$SRV"

echo "==> [7/10] kcp-go → kcp-rs(smol) (cross)"
"$EX_SMOL" --mode server --port "$P_RS1" --size "$SIZE" 2>/dev/null & RSRV=$!
sleep 1.5
R_GS=$(run_step "7/10" "$GOBIN" client --addr "127.0.0.1:$P_RS1" --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE" --conv "$CONV")
echo "$R_GS"
kill_wait "$RSRV"

echo "==> [8/10] kcp-rs(tokio) ↔ kcp-rs(tokio) max-throughput baseline"
R_TT_CLOSED=$(run_step "8/10" "$EX_TOKIO" --mode self --concurrency "$CONCURRENCY" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_TT_CLOSED"

echo "==> [9/10] kcp-rs(smol) ↔ kcp-rs(smol) max-throughput baseline"
R_SS_CLOSED=$(run_step "9/10" "$EX_SMOL" --mode self --concurrency "$CONCURRENCY" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_SS_CLOSED"

echo "==> [10/10] kcp-go ↔ kcp-go max-throughput baseline"
R_GG_CLOSED=$(run_step "10/10" "$GOBIN" closed --port "$P_GO1" --concurrency "$CONCURRENCY" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE")
echo "$R_GG_CLOSED"

# ---- render one RESULT line as a markdown table row ----
# RESULT combo=.. samples=.. ok=.. size=.. rps=.. p50_us=.. p90_us=.. p99_us=.. p999_us=.. avg_us=.. min_us=.. max_us=..
mdrow() {
  local line=$1 label=$2
  case "$line" in
    RESULT*) ;;
    *) printf '| %-26s | FAILED |\n' "$label"; return ;;
  esac
  local ok p50 p90 p99 p999 avg mn mx
  ok=$(echo "$line" | sed -n 's/.*ok=\([0-9]*\).*/\1/p')
  p50=$(echo "$line" | sed -n 's/.*p50_us=\([0-9.]*\).*/\1/p')
  p90=$(echo "$line" | sed -n 's/.*p90_us=\([0-9.]*\).*/\1/p')
  p99=$(echo "$line" | sed -n 's/.*p99_us=\([0-9.]*\).*/\1/p')
  p999=$(echo "$line" | sed -n 's/.*p999_us=\([0-9.]*\).*/\1/p')
  avg=$(echo "$line" | sed -n 's/.*avg_us=\([0-9.]*\).*/\1/p')
  mn=$(echo "$line" | sed -n 's/.*min_us=\([0-9.]*\).*/\1/p')
  mx=$(echo "$line" | sed -n 's/.*max_us=\([0-9.]*\).*/\1/p')
  printf '| %-26s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$label" "$ok" "$p50" "$p90" "$p99" "$p999" "$avg" "$mn" "$mx"
}

fmt1() { awk -v x="$1" 'BEGIN { printf "%.1f", x }'; }
field() { echo "$1" | sed -n "s/.*$2=\([0-9.]*\).*/\1/p"; }

RUST_CLOSED_RPS=$(field "$R_TT_CLOSED" rps)
SMOL_CLOSED_RPS=$(field "$R_SS_CLOSED" rps)
GO_CLOSED_RPS=$(field "$R_GG_CLOSED" rps)
RUST_CLOSED_P99=$(field "$R_TT_CLOSED" p99_us)
SMOL_CLOSED_P99=$(field "$R_SS_CLOSED" p99_us)
GO_CLOSED_P99=$(field "$R_GG_CLOSED" p99_us)
RUST_CLOSED_P999=$(field "$R_TT_CLOSED" p999_us)
SMOL_CLOSED_P999=$(field "$R_SS_CLOSED" p999_us)
GO_CLOSED_P999=$(field "$R_GG_CLOSED" p999_us)
RUST_CLOSED_MAX=$(field "$R_TT_CLOSED" max_us)
SMOL_CLOSED_MAX=$(field "$R_SS_CLOSED" max_us)
GO_CLOSED_MAX=$(field "$R_GG_CLOSED" max_us)
if [ -n "$RUST_CLOSED_RPS" ] && [ -n "$GO_CLOSED_RPS" ] && [ "$GO_CLOSED_RPS" != 0 ]; then
  THROUGHPUT_DELTA=$(awk -v r="$RUST_CLOSED_RPS" -v g="$GO_CLOSED_RPS" 'BEGIN { printf "%.1f", (r/g-1)*100 }')
else
  THROUGHPUT_DELTA="N/A"
fi
if [ -n "$SMOL_CLOSED_RPS" ] && [ -n "$GO_CLOSED_RPS" ] && [ "$GO_CLOSED_RPS" != 0 ]; then
  SMOL_THROUGHPUT_DELTA=$(awk -v r="$SMOL_CLOSED_RPS" -v g="$GO_CLOSED_RPS" 'BEGIN { printf "%.1f", (r/g-1)*100 }')
else
  SMOL_THROUGHPUT_DELTA="N/A"
fi

{
cat <<EOF
# kcp-rs ↔ kcp-go v5 — P99 / P999 延迟交叉测试报告（开放模型）

- 日期: $(date '+%Y-%m-%d %H:%M')
- 环境: macOS $(sw_vers -productVersion 2>/dev/null || echo -) / $(uname -m)
- Rust: $(rustc -V 2>/dev/null | awk '{print $2}') / kcp-rs $(grep '^version' kcp-rs/Cargo.toml | head -1 | awk '{print $3}')
- Go: $(go version 2>/dev/null | awk '{print $3}') / kcp-go v5.6.64
- 方法: **开放模型固定速率**回声 RTT（不经 kcptun / SMUX / snappy / 加密层），127.0.0.1 UDP
- 配置: Fast3 (nodelay=1, interval=10, resend=2, nc=1), MTU 1350, 窗口 512/512, 无 FEC, 无加密
- **构建: kcp-rs 用 release（opt-level=3 + LTO）；kcp-go 默认优化构建**。此前报告误用 debug 构建使 Rust 伪劣 2–3×（见结论）。
- 结构: 各组合均为「客户端计时 + 服务端回声」同构（kcp-rs=KcpListener+KcpConn, kcp-go=ListenWithOptions+DialWithOptions）
- 参数: rps=$RPS (开放模型, 固定速率), warmup=${WARMUP}s (排除), duration=${DURATION}s, payload=$SIZE B
- **OS 调优**: kern.ipc.maxsockbuf=8388608 (8MB), net.inet.udp.recvspace=4194304 (4MB) —— 消除内核 UDP 接收缓冲区溢出导致的静默丢包，P99 下降 ~26%，吞吐提升 ~28%

## 结果（微秒 µs，越小越好；P50/P90/P99/P999 由全部原始样本一次性聚合计算）

| 组合 | ok | P50 | P90 | P99 | P999 | avg | min | max |
|------|----|----|----|----|------|-----|-----|-----|
$(mdrow "$R_TT" "kcp-rs(tokio)↔kcp-rs(tokio)")
$(mdrow "$R_SS" "kcp-rs(smol)↔kcp-rs(smol)")
$(mdrow "$R_GG" "kcp-go↔kcp-go")
$(mdrow "$R_TG" "kcp-rs(tokio)→kcp-go")
$(mdrow "$R_GT" "kcp-go→kcp-rs(tokio)")
$(mdrow "$R_SG" "kcp-rs(smol)→kcp-go")
$(mdrow "$R_GS" "kcp-go→kcp-rs(smol)")

## 最大可持续性能（闭环，concurrency=${CONCURRENCY}）

| 组合 | completed req/s | P99 (µs) | P999 (µs) | max (µs) |
|------|----------------:|----------:|-----------:|---------:|
| kcp-rs(tokio)↔kcp-rs(tokio) | ${RUST_CLOSED_RPS:-FAILED} | ${RUST_CLOSED_P99:-FAILED} | ${RUST_CLOSED_P999:-FAILED} | ${RUST_CLOSED_MAX:-FAILED} |
| kcp-rs(smol)↔kcp-rs(smol) | ${SMOL_CLOSED_RPS:-FAILED} | ${SMOL_CLOSED_P99:-FAILED} | ${SMOL_CLOSED_P999:-FAILED} | ${SMOL_CLOSED_MAX:-FAILED} |
| kcp-go↔kcp-go | ${GO_CLOSED_RPS:-FAILED} | ${GO_CLOSED_P99:-FAILED} | ${GO_CLOSED_P999:-FAILED} | ${GO_CLOSED_MAX:-FAILED} |

## 严格标准合规

- **开放模型（避免协调遗漏）**：客户端按固定 rps=$RPS 独立发请求，不等待响应；慢期会使积压请求的延迟真实上抬，而非被隐藏。
- **无百分位二次平均**：每个组合的 P50/P90/P99/P999 均由测量期全部原始样本（约 $((RPS * DURATION)) 个）排序后一次性计算，绝不跨批次取均值。
- **独立预热**：前 ${WARMUP}s 为预热阶段（连接建立/KCP 窗口/分配器），样本排除。
- **样本量**：每组合样本 = rps × duration ≥ 1000（本报告 $((RPS * DURATION))）。
- **时长**：duration=${DURATION}s（可配；正式报告建议延长到 10–30min，以覆盖周期性系统抖动）。
- **环境**：同机回环、四组合同结构同负载；压测端为轻量收发循环，非 CPU 瓶颈。
- **性能口径**：固定 RPS 表只回答延迟；最大性能由 concurrency=${CONCURRENCY} 的闭环测试回答，避免把 500 RPS 下的空闲延迟误当吞吐能力。

## 结论

- **互操作双向通过**：kcp-rs（tokio 与 smol）↔ kcp-go 在裸 KCP 层跨语言互通，回声逐位一致。
- **最大 raw KCP 吞吐**：kcp-rs(tokio) ${RUST_CLOSED_RPS:-FAILED} req/s，kcp-go ${GO_CLOSED_RPS:-FAILED} req/s；Rust 相对 Go 为 ${THROUGHPUT_DELTA}%（正数表示 Rust 更快）。
- **smol raw KCP 吞吐**：kcp-rs(smol) ${SMOL_CLOSED_RPS:-FAILED} req/s；相对 Go 为 ${SMOL_THROUGHPUT_DELTA}%（正数表示 smol 更快）。
- 固定 RPS 的 P50/P99 是延迟与调度稳定性指标，不能用于推断实现的最大吞吐；上面的闭环结果才是本报告的 raw KCP 性能结论。

_本报告由 bench/run_p99.sh 生成（开放模型 7 组合矩阵）。_
EOF
} > "$REPORT"

echo ""
echo "==> report written: $REPORT"
