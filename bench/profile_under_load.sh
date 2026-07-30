#!/usr/bin/env bash
# Continuous multi-conn load + CPU pprof (avoids park-only samples).
# Usage:
#   CRYPT=aes-128-gcm bash bench/profile_under_load.sh
#   CRYPT=xor NOCOMP=1 SECONDS=20 CONN=10 bash bench/profile_under_load.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
CRYPT=${CRYPT:-aes-128-gcm}
NOCOMP=${NOCOMP:-1}
MODE=${MODE:-fast}
SECONDS_N=${SECONDS:-20}
CONN=${CONN:-10}
SIZE_MB=${SIZE_MB:-2}
KEY=${KEY:-bench-key}
OUT_DIR=${OUT_DIR:-bench/profiles/goal-load-$(date +%Y%m%d-%H%M%S)}
PPROF_PORT=${PPROF_PORT:-16360}
SERVER=${RUST_SERVER:-$ROOT/target/profiling/kcptun-server}
CLIENT=${RUST_CLIENT:-$ROOT/target/profiling/kcptun-client}
[ -x "$SERVER" ] || SERVER=$ROOT/target/release/kcptun-server
[ -x "$CLIENT" ] || CLIENT=$ROOT/target/release/kcptun-client
mkdir -p "$OUT_DIR"
PORT_BASE=$((34000 + RANDOM % 2000))
ECHO=$PORT_BASE; SP=$((PORT_BASE+1)); LP=$((PORT_BASE+2))
COMMON=(--key "$KEY" --crypt "$CRYPT" --mode "$MODE" --sndwnd 2048 --rcvwnd 2048 --sockbuf $((4*1024*1024)) --datashard 0 --parityshard 0)
[ "$NOCOMP" = 1 ] && COMMON+=(--nocomp)

python3 -u -c "
import socket,threading
def echo(c,a):
  try:
    while True:
      d=c.recv(65536)
      if not d: break
      c.sendall(d)
  except: pass
  c.close()
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('0.0.0.0',$ECHO)); s.listen(128)
while True:
  c,a=s.accept(); threading.Thread(target=echo,args=(c,a),daemon=True).start()
" >/dev/null 2>&1 &
EPID=$!
"$SERVER" -l "0.0.0.0:$SP" -t "127.0.0.1:$ECHO" "${COMMON[@]}" --pprof "127.0.0.1:$PPROF_PORT" \
  >"$OUT_DIR/server.log" 2>&1 &
SPID=$!
sleep 0.8
"$CLIENT" -l "127.0.0.1:$LP" -r "127.0.0.1:$SP" "${COMMON[@]}" --conn "$CONN" \
  >"$OUT_DIR/client.log" 2>&1 &
CPID=$!
# wait listen
for i in $(seq 1 80); do
  python3 -c "import socket;s=socket.socket();s.settimeout(0.15);s.connect(('127.0.0.1',$LP));s.close()" 2>/dev/null && break
  sleep 0.1
done

# continuous load in background for SECONDS_N+5
python3 -u - <<PY >"$OUT_DIR/load.log" 2>&1 &
import socket,threading,time,os,hashlib
port=$LP; conn=$CONN; size=$SIZE_MB*1024*1024; deadline=time.time()+$SECONDS_N+8
payload=os.urandom(min(size, 256*1024))
def worker(wid):
  while time.time()<deadline:
    try:
      s=socket.socket(); s.settimeout(15); s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
      s.connect(('127.0.0.1',port))
      received=0; total=size
      def rx():
        nonlocal received
        try:
          while received<total:
            d=s.recv(65536)
            if not d: break
            received+=len(d)
        except: pass
      t=threading.Thread(target=rx,daemon=True); t.start()
      sent=0
      while sent<total and time.time()<deadline:
        chunk=payload if sent+len(payload)<=total else payload[:total-sent]
        n=s.send(chunk); 
        if n<=0: break
        sent+=n
      t.join(timeout=10); s.close()
    except Exception as e:
      time.sleep(0.05)
threads=[threading.Thread(target=worker,args=(i,),daemon=True) for i in range(conn)]
for t in threads: t.start()
for t in threads: t.join()
print('load done')
PY
LOADPID=$!
sleep 1.5
TS=$(date +%Y%m%d-%H%M%S)
OUT_PB="$OUT_DIR/rust-server-${CRYPT}-${TS}.pb"
echo "capturing $SECONDS_N s -> $OUT_PB"
curl -fsS -o "$OUT_PB" "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=${SECONDS_N}" || {
  echo "curl pprof failed"; tail -30 "$OUT_DIR/server.log"; exit 1
}
wait $LOADPID 2>/dev/null || true
kill $CPID $SPID $EPID 2>/dev/null || true
wait 2>/dev/null || true
echo "artifact=$OUT_PB ($(wc -c <"$OUT_PB") bytes)"
if command -v go >/dev/null; then
  echo "=== top (ignore park) ==="
  go tool pprof -top -ignore='Inner::park|park_thread|kevent|kqueue|epoll|__psynch|pthread_cond|mach_msg|thread_yield' "$OUT_PB" 2>&1 | head -45
  echo "=== top cum (ignore park) ==="
  go tool pprof -top -cum -ignore='Inner::park|park_thread|kevent|kqueue|epoll|__psynch|pthread_cond|mach_msg|thread_yield' "$OUT_PB" 2>&1 | head -40
fi
echo "OUT_DIR=$OUT_DIR"
echo "$OUT_DIR"
