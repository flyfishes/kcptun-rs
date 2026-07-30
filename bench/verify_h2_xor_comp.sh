#!/usr/bin/env bash
# H2 verification: xor ±comp thr + SNMP rustobs (EncryptInline/Offload).
# Does NOT invent numbers — all outputs come from this run.
#
# Usage:
#   bash bench/verify_h2_xor_comp.sh              # thr+SNMP only (release bins)
#   WITH_PPROF=1 bash bench/verify_h2_xor_comp.sh # also CPU pprof (profiling bins)
#
# Env:
#   RUNS=3 BENCH_DATA_MB=30 KEY=bench-key MODE=fast2
#   SKIP_REBUILD=1   reuse existing bins (only if you just rebuilt)
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
OUT_DIR="${OUT_DIR:-bench/profiles/h2-xor-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"
NOTES="$OUT_DIR/notes.md"
RUNS="${RUNS:-3}"
DATA_MB="${BENCH_DATA_MB:-30}"
KEY="${KEY:-bench-key}"
MODE="${MODE:-fast2}"
SNDWND="${SNDWND:-1024}"
RCVWND="${RCVWND:-1024}"
SMUXVER="${SMUXVER:-2}"
WITH_PPROF="${WITH_PPROF:-0}"
SKIP_REBUILD="${SKIP_REBUILD:-0}"

log() { printf '%s\n' "$*" | tee -a "$NOTES"; }

TOKIO_S="$ROOT/target/release/kcptun-server"
TOKIO_C="$ROOT/target/release/kcptun-client"
SMOL_S="$ROOT/target/smol-release/release/kcptun-server"
SMOL_C="$ROOT/target/smol-release/release/kcptun-client"
PROF_S="$ROOT/target/profiling/kcptun-server"
PROF_C="$ROOT/target/profiling/kcptun-client"
# smol profiling (optional)
SMOL_PROF_S="$ROOT/target/smol-profiling/profiling/kcptun-server"
SMOL_PROF_C="$ROOT/target/smol-profiling/profiling/kcptun-client"

log "# H2 xor/comp verify"
log "- date: $(date -Iseconds 2>/dev/null || date)"
log "- git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
log "- dirty: $(git status --porcelain | tr '\n' '; ' || true)"
log "- RUNS=$RUNS DATA_MB=$DATA_MB MODE=$MODE WITH_PPROF=$WITH_PPROF"
log ""

if [ "$SKIP_REBUILD" != "1" ]; then
  log "## rebuild release (tokio + smol) with current tree"
  make release 2>&1 | tee -a "$OUT_DIR/build-release.log" | tail -5
  make release-smol 2>&1 | tee -a "$OUT_DIR/build-smol.log" | tail -5
  if [ "$WITH_PPROF" = "1" ]; then
    log "## rebuild profiling (tokio pprof)"
    make profiling-bins 2>&1 | tee -a "$OUT_DIR/build-prof.log" | tail -5
    log "## rebuild smol profiling + pprof"
    extra="-C force-frame-pointers=yes"
    case "$(uname -m)" in arm64|aarch64) extra="--cfg aes_armv8 $extra" ;; esac
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }$extra" \
      cargo build --profile profiling --no-default-features --features "smol,pprof" \
      -p kcptun-server -p kcptun-client -j "$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)" \
      --target-dir target/smol-profiling \
      2>&1 | tee -a "$OUT_DIR/build-smol-prof.log" | tail -8
  fi
else
  log "## SKIP_REBUILD=1 — using existing bins"
fi

for p in "$TOKIO_S" "$TOKIO_C" "$SMOL_S" "$SMOL_C"; do
  if [ ! -x "$p" ]; then
    log "ERROR missing binary: $p"
    exit 1
  fi
  log "- bin $(basename "$(dirname "$p")")/$(basename "$p"): $(ls -la "$p" | awk '{print $5,$6,$7,$8,$9}')"
done

# free leftover processes on our ports if any (best-effort)
cleanup_ports() {
  local base=$1
  for p in "$base" $((base+1)) $((base+2)) $((base+3)); do
    lsof -tiTCP:"$p" -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
  done
}

# One thr run: start echo + server + client, measure, collect SNMP, stop.
# Args: label server_bin client_bin crypt nocomp(0|1) snmp_tag [pprof_port]
run_one() {
  local label="$1" SBIN="$2" CBIN="$3" CRYPT="$4" NOCOMP="$5" TAG="$6"
  local PPROF_PORT="${7:-0}"
  local PORT_BASE=$((34000 + (RANDOM % 500) * 4))
  local ECHO_PORT=$PORT_BASE
  local SERVER_PORT=$((PORT_BASE + 1))
  local CLIENT_PORT=$((PORT_BASE + 2))
  local SNMP_CSV="$OUT_DIR/snmp-${TAG}.csv"
  local SLOG="$OUT_DIR/log-${TAG}-s.txt"
  local CLOG="$OUT_DIR/log-${TAG}-c.txt"
  rm -f "$SNMP_CSV" "${SNMP_CSV}.rustobs"
  cleanup_ports "$PORT_BASE"

  local COMP_FLAGS=()
  if [ "$NOCOMP" = "1" ]; then
    COMP_FLAGS+=(--nocomp)
  fi
  local COMMON=(--key "$KEY" --crypt "$CRYPT" --mode "$MODE"
    --sndwnd "$SNDWND" --rcvwnd "$RCVWND" --smuxver "$SMUXVER"
    --snmplog "$SNMP_CSV" --snmpperiod 1)
  COMMON+=("${COMP_FLAGS[@]+"${COMP_FLAGS[@]}"}")

  python3 -u -c "
import socket, threading
def echo(s,a):
    try:
        while True:
            d=s.recv(65536)
            if not d: break
            s.sendall(d)
    except: pass
    s.close()
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('0.0.0.0',$ECHO_PORT)); s.listen(32)
while True:
    c,a=s.accept()
    threading.Thread(target=echo,args=(c,a),daemon=True).start()
" >/dev/null 2>&1 &
  local ECHO_PID=$!

  local S_EXTRA=()
  if [ "$PPROF_PORT" != "0" ]; then
    S_EXTRA+=(--pprof "127.0.0.1:${PPROF_PORT}")
  fi

  "$SBIN" -l "0.0.0.0:$SERVER_PORT" -t "127.0.0.1:$ECHO_PORT" \
    "${COMMON[@]}" "${S_EXTRA[@]+"${S_EXTRA[@]}"}" >"$SLOG" 2>&1 &
  local SPID=$!
  sleep 0.6
  "$CBIN" -l "127.0.0.1:$CLIENT_PORT" -r "127.0.0.1:$SERVER_PORT" \
    "${COMMON[@]}" >"$CLOG" 2>&1 &
  local CPID=$!

  # wait for client listen
  local tries=80
  while [ $tries -gt 0 ]; do
    if python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.2)
try:
 s.connect(('127.0.0.1',$CLIENT_PORT)); s.close()
except: sys.exit(1)" 2>/dev/null; then
      break
    fi
    sleep 0.1
    tries=$((tries - 1))
  done
  if [ $tries -le 0 ]; then
    log "FAIL tunnel not ready label=$label"
    tail -20 "$SLOG" "$CLOG" | tee -a "$NOTES" || true
    kill $CPID $SPID $ECHO_PID 2>/dev/null || true
    wait 2>/dev/null || true
    echo "0"
    return 1
  fi

  local PB=""
  if [ "$PPROF_PORT" != "0" ]; then
    PB="$OUT_DIR/pprof-${TAG}.pb"
    (
      sleep 0.5
      curl -fsS -o "$PB" "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=12" \
        || echo "pprof curl failed" >>"$NOTES"
    ) &
    local PPROF_CURL_PID=$!
  fi

  python3 "$ROOT/bench/throughput.py" "$CLIENT_PORT" \
    --data-mb "$DATA_MB" --chunk-kb 128 --latency-iterations 0 \
    >"$OUT_DIR/thr-${TAG}.out" 2>"$OUT_DIR/thr-${TAG}.err" || true
  local THR
  THR=$(python3 - <<PY
import re
p=open("$OUT_DIR/thr-${TAG}.out").read()+"\n"+open("$OUT_DIR/thr-${TAG}.err").read()
m=re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*MB/s", p)
print(m.group(1) if m else "0")
PY
)

  if [ "$PPROF_PORT" != "0" ]; then
    wait "$PPROF_CURL_PID" 2>/dev/null || true
  fi

  # snmp_logger: initial sleep(period) then loop sleep(period); wait for ≥1 post-load sample
  sleep 2.5
  kill $CPID $SPID $ECHO_PID 2>/dev/null || true
  wait $CPID $SPID $ECHO_PID 2>/dev/null || true
  cleanup_ports "$PORT_BASE"

  # parse last non-header rustobs line
  local RUSTOBS="${SNMP_CSV}.rustobs"
  local INLINE=0 OFFLOAD=0 EMPTY=0 RETRANS=""
  if [ -f "$RUSTOBS" ]; then
    local LAST
    LAST=$(grep -v '^timestamp' "$RUSTOBS" | tail -1 || true)
    if [ -n "$LAST" ]; then
      # timestamp,EmptyFlush,EncryptInline,EncryptOffload,DecryptOffloadSkipped
      EMPTY=$(echo "$LAST" | cut -d, -f2)
      INLINE=$(echo "$LAST" | cut -d, -f3)
      OFFLOAD=$(echo "$LAST" | cut -d, -f4)
    fi
  fi
  if [ -f "$SNMP_CSV" ]; then
    RETRANS=$(python3 - <<PY
import csv
p="$SNMP_CSV"
try:
  with open(p) as f:
    rows=list(csv.DictReader(f))
  if not rows:
    print("na"); raise SystemExit
  print(rows[-1].get("RetransSegs", "na"))
except Exception:
  print("na")
PY
)
  fi

  python3 - <<PY
inline=int(float("${INLINE}" or 0))
off=int(float("${OFFLOAD}" or 0))
den=inline+off
r = (off/den) if den else 0.0
print(
  f"RESULT\t{'$label'}\tthr_MBps={'$THR'}\tEncryptInline={inline}\t"
  f"EncryptOffload={off}\tr_off={r:.4f}\tEmptyFlush={'$EMPTY'}\t"
  f"RetransSegs={'$RETRANS'}\trustobs={'$RUSTOBS'}",
  flush=True,
)
PY
  echo "$label,$THR,$INLINE,$OFFLOAD,$EMPTY,$RETRANS" >>"$OUT_DIR/summary.csv"
  # only thr on last stdout line for callers using tail -1 after filtering RESULT
  echo "${THR:-0}"
}

echo "label,thr_MBps,EncryptInline,EncryptOffload,EmptyFlush,RetransSegs" >"$OUT_DIR/summary.csv"

log "## thr matrix (median of $RUNS runs)"
run_series() {
  local name="$1" SBIN="$2" CBIN="$3" CRYPT="$4" NOCOMP="$5"
  local vals=()
  local i
  for i in $(seq 1 "$RUNS"); do
    local tag="${name}-r${i}"
    log "### run $tag"
    local thr
    thr=$(run_one "$tag" "$SBIN" "$CBIN" "$CRYPT" "$NOCOMP" "$tag" 0 | tail -1)
    vals+=("$thr")
    log "- thr raw: $thr MB/s (see RESULT line above in notes from run_one stdout mixed — check summary.csv)"
  done
  python3 - <<PY
vals=sorted(float(x) for x in """${vals[*]}""".split() if x)
print("MEDIAN", "${name}", vals[len(vals)//2] if vals else 0, "all", vals)
PY
}

# Capture run_one RESULT lines into notes via tee of summary after

log "### smol xor comp"
for i in $(seq 1 "$RUNS"); do
  tag="smol-xor-comp-r${i}"
  log "run $tag"
  out=$(run_one "$tag" "$SMOL_S" "$SMOL_C" xor 0 "$tag" 0 | tee -a "$NOTES" | tail -1)
  log "thr=$out"
done

log "### smol xor no-comp"
for i in $(seq 1 "$RUNS"); do
  tag="smol-xor-nocomp-r${i}"
  log "run $tag"
  out=$(run_one "$tag" "$SMOL_S" "$SMOL_C" xor 1 "$tag" 0 | tee -a "$NOTES" | tail -1)
  log "thr=$out"
done

log "### tokio xor comp"
for i in $(seq 1 "$RUNS"); do
  tag="tokio-xor-comp-r${i}"
  log "run $tag"
  out=$(run_one "$tag" "$TOKIO_S" "$TOKIO_C" xor 0 "$tag" 0 | tee -a "$NOTES" | tail -1)
  log "thr=$out"
done

log "### tokio xor no-comp"
for i in $(seq 1 "$RUNS"); do
  tag="tokio-xor-nocomp-r${i}"
  log "run $tag"
  out=$(run_one "$tag" "$TOKIO_S" "$TOKIO_C" xor 1 "$tag" 0 | tee -a "$NOTES" | tail -1)
  log "thr=$out"
done

if [ "$WITH_PPROF" = "1" ]; then
  if [ -x "$SMOL_PROF_S" ] && [ -x "$SMOL_PROF_C" ]; then
    log "### pprof smol xor comp (12s sample during load)"
    run_one "smol-xor-comp-pprof" "$SMOL_PROF_S" "$SMOL_PROF_C" xor 0 "smol-xor-comp-pprof" 16071 \
      | tee -a "$NOTES" || true
    if command -v go >/dev/null 2>&1 && [ -f "$OUT_DIR/pprof-smol-xor-comp-pprof.pb" ]; then
      go tool pprof -top -ignore='Inner::park|park_thread|kevent|kqueue|epoll' \
        "$OUT_DIR/pprof-smol-xor-comp-pprof.pb" 2>&1 | head -40 | tee -a "$NOTES" || true
    fi
  else
    log "smol profiling bins missing — skip pprof"
  fi
fi

log ""
log "## summary.csv"
cat "$OUT_DIR/summary.csv" | tee -a "$NOTES"
log ""
log "## median / r_off by series"
python3 - <<PY | tee -a "$NOTES"
import csv, statistics, collections, os
path="$OUT_DIR/summary.csv"
rows=list(csv.DictReader(open(path)))
# group by prefix without -rN
series=collections.defaultdict(list)
for r in rows:
    lab=r["label"]
    base=lab.rsplit("-r",1)[0] if "-r" in lab else lab
    try:
        thr=float(r["thr_MBps"] or 0)
    except: thr=0
    try:
        inline=int(float(r["EncryptInline"] or 0))
        off=int(float(r["EncryptOffload"] or 0))
    except:
        inline=off=0
    series[base].append((thr, inline, off, r.get("RetransSegs","?")))
print(f"{'series':32} {'median_MB/s':>12} {'last_r_off':>10} {'last_inline':>12} {'last_off':>10} retrans_last")
for k,vals in sorted(series.items()):
    thrs=[v[0] for v in vals]
    med=statistics.median(thrs) if thrs else 0
    thr,inline,off,ret=vals[-1]
    den=inline+off
    roff=off/den if den else 0
    print(f"{k:32} {med:12.2f} {roff:10.4f} {inline:12d} {off:10d} {ret}")
print("OUT_DIR=$OUT_DIR")
PY

log "DONE out=$OUT_DIR"
echo "$OUT_DIR"
