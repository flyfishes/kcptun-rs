#!/bin/bash
# Capture CPU / heap profiles for the raw KCP latency_p99 example (rust↔rust).
#
# Profiles the first two test groups from bench/run_p99.sh:
#   1. kcp-rs(tokio) ↔ kcp-rs(tokio)
#   2. kcp-rs(smol)  ↔ kcp-rs(smol)
#
# Uses the Go-compatible pprof HTTP endpoint (kpprof-rs) baked into the example
# when built with --features pprof.  Profiles are saved as Go pprof protobuf
# (.pb) and can be analyzed with `go tool pprof`.
#
# Usage:
#   bash bench/profile_p99_latency.sh                  # both tokio + smol, 30s
#   bash bench/profile_p99_latency.sh tokio 20         # tokio only, 20s
#   bash bench/profile_p99_latency.sh smol 20          # smol only, 20s
#   RPS=1000 SIZE=65536 bash bench/profile_p99_latency.sh tokio 30
#
# Env: RPS (500), WARMUP (5), DURATION (60), SIZE (26624), PPROF_PORT (17060)
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$(pwd)

RT="${1:-both}"        # tokio | smol | both
PROFILE_SECS="${2:-30}"

RPS=${RPS:-500}
WARMUP=${WARMUP:-5}
DURATION=${DURATION:-60}
SIZE=${SIZE:-26624}
PPROF_PORT=${PPROF_PORT:-17060}
OUT_DIR="${OUT_DIR:-bench/profiles}"
mkdir -p "$OUT_DIR"

if ! command -v go >/dev/null 2>&1; then
    echo "error: go not found — install Go to analyze profiles"
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl required"
    exit 1
fi

# ── Build profiling binaries (force-frame-pointers + pprof + symbols) ──
build_profiling_bin() {
    local rt="$1"   # tokio | smol
    local feature="async-${rt}"
    local outdir="$REPO/target/profiling/examples"
    mkdir -p "$outdir"
    local bin="$outdir/latency_p99_${rt}"

    echo "==> building kcp-rs example (${rt}, pprof, profiling profile)"
    RUSTFLAGS="-C force-frame-pointers=yes" \
        cargo build -q --profile profiling -p kcp-rs \
        --features "${feature} pprof" --example latency_p99
    cp "$REPO/target/profiling/examples/latency_p99" "$bin"
    echo "    binary: $bin"
    echo "$bin"
}

# ── Run one profiled measurement ──
run_profile() {
    local rt="$1"      # tokio | smol
    local bin="$2"
    local ts; ts=$(date +%Y%m%d-%H%M%S)
    local pprof_addr="127.0.0.1:${PPROF_PORT}"
    local cpu_pb="$OUT_DIR/p99-${rt}-cpu-${ts}.pb"
    local heap_pb="$OUT_DIR/p99-${rt}-heap-${ts}.pb"
    local allocs_pb="$OUT_DIR/p99-${rt}-allocs-${ts}.pb"

    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  P99 latency profile: kcp-rs(${rt}) ↔ kcp-rs(${rt})"
    echo "║  rps=$RPS  size=${SIZE}B  warmup=${WARMUP}s  duration=${DURATION}s"
    echo "║  pprof: http://${pprof_addr}/debug/pprof/"
    echo "║  CPU capture: ${PROFILE_SECS}s"
    echo "╚══════════════════════════════════════════════════════════════╝"

    # The example runs in --mode self (listener + client in one process).
    # The pprof server runs concurrently inside the same process, so we
    # capture CPU samples while the KCP echo test is running.
    #
    # We start the test in the background, wait for pprof to be ready,
    # capture the profile, then wait for the test to finish.
    local test_log="/tmp/p99-${rt}-$$.log"
    "$bin" --mode self \
        --rps "$RPS" --warmup "$WARMUP" --duration "$DURATION" --size "$SIZE" \
        --pprof "$pprof_addr" \
        >"$test_log" 2>&1 &
    local test_pid=$!

    # Wait for pprof to be ready (max 5s)
    local tries=50
    while [ $tries -gt 0 ]; do
        if curl -sf "http://${pprof_addr}/debug/pprof/" >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
        tries=$((tries - 1))
    done
    if [ $tries -le 0 ]; then
        echo "  ERROR: pprof server not ready"
        kill "$test_pid" 2>/dev/null || true
        wait "$test_pid" 2>/dev/null || true
        return 1
    fi

    echo "  pprof ready, capturing CPU profile (${PROFILE_SECS}s)..."
    if ! curl -fsS -o "$cpu_pb" "http://${pprof_addr}/debug/pprof/profile?seconds=${PROFILE_SECS}"; then
        echo "  ERROR: CPU profile capture failed"
        kill "$test_pid" 2>/dev/null || true
        wait "$test_pid" 2>/dev/null || true
        return 1
    fi

    # Heap + allocs snapshots
    curl -fsS -o "$heap_pb" "http://${pprof_addr}/debug/pprof/heap" \
        || echo "  (heap capture failed)"
    curl -fsS -o "$allocs_pb" "http://${pprof_addr}/debug/pprof/allocs" \
        || echo "  (allocs capture failed)"

    # Wait for the test to finish and show its RESULT line
    wait "$test_pid" 2>/dev/null || true
    echo ""
    echo "  test output:"
    grep -E '^RESULT|samples=|p50=|p99=|p999=' "$test_log" | sed 's/^/    /'
    rm -f "$test_log"

    echo ""
    echo "  artifacts:"
    echo "    CPU:    $cpu_pb ($(wc -c < "$cpu_pb" | tr -d ' ') bytes)"
    [ -s "$heap_pb" ] && echo "    heap:   $heap_pb ($(wc -c < "$heap_pb" | tr -d ' ') bytes)"
    [ -s "$allocs_pb" ] && echo "    allocs: $allocs_pb ($(wc -c < "$allocs_pb" | tr -d ' ') bytes)"

    # Quick top-10 hotspot view (filter out I/O wait)
    echo ""
    echo "  === go tool pprof -top (CPU, filtering I/O wait) ==="
    go tool pprof -top -ignore="Inner::park" "$cpu_pb" 2>&1 | head -30 || true
    echo ""
    echo "  Analyze interactively:"
    echo "    go tool pprof -http=127.0.0.1:0 $cpu_pb"
    echo "    go tool pprof -http=127.0.0.1:0 $heap_pb"
}

# ── Main ──
case "$RT" in
    tokio|smol)
        bin=$(build_profiling_bin "$RT")
        run_profile "$RT" "$bin"
        ;;
    both)
        bin_tokio=$(build_profiling_bin tokio)
        run_profile tokio "$bin_tokio"
        # Increment port for smol to avoid bind conflict if tokio process lingers
        PPROF_PORT=$((PPROF_PORT + 1))
        bin_smol=$(build_profiling_bin smol)
        run_profile smol "$bin_smol"
        ;;
    *)
        echo "Usage: bash bench/profile_p99_latency.sh [tokio|smol|both] [profile_seconds]"
        exit 1
        ;;
esac

echo ""
echo "==> done. Profiles in $OUT_DIR/p99-{tokio,smol}-*.pb"
echo "    Compare hotspots: go tool pprof -http=127.0.0.1:0 <file.pb>"
