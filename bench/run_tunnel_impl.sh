#!/bin/bash
# Run tunnel P99 at a given RPS for one implementation.
# Usage: run_tunnel_impl.sh <lib|legacy|go> <rps> <size> <warmup> <duration> [concurrency]
#
# The kcptun binaries + asyncio echo server are started/stopped per invocation
# and a TIME_WAIT drain is applied so back-to-back runs don't exhaust macOS
# ephemeral ports (EADDRNOTAVAIL) from the probe's connect-per-request pattern.
set -u
cd "$(dirname "$0")/.."
IMPL=$1 RPS=$2 SIZE=$3 WARMUP=$4 DUR=$5 CONC=${6:-32}

pkill -9 -f "kcptun" 2>/dev/null; pkill -9 -f "python3" 2>/dev/null; sleep 1

# asyncio echo server (single event loop — no thread-per-connection CPU cost)
python3 "$(dirname "$0")/echo_server.py" 29900 >/dev/null 2>&1 &

if [ "$IMPL" = "go" ]; then
  tests/kcptun-go/server -l 0.0.0.0:29900 -t 127.0.0.1:29900 --key test --crypt aes --mode fast3 --smuxver 2 --sndwnd 1024 --rcvwnd 1024 --nocomp >/dev/null 2>&1 &
else
  target/release/kcptun-server -l 0.0.0.0:29900 -t 127.0.0.1:29900 --key test --crypt aes --mode fast3 --smuxver 2 --sndwnd 1024 --rcvwnd 1024 --nocomp >/dev/null 2>&1 &
fi
sleep 0.5

if [ "$IMPL" = "go" ]; then
  tests/kcptun-go/client -l 127.0.0.1:12948 -r 127.0.0.1:29900 --key test --crypt aes --mode fast3 --smuxver 2 --conn 1 --sndwnd 1024 --rcvwnd 1024 --nocomp >/dev/null 2>&1 &
elif [ "$IMPL" = "lib" ]; then
  KCPTUN_USE_LIB_KCP=1 target/release/kcptun-client -l 127.0.0.1:12948 -r 127.0.0.1:29900 --key test --crypt aes --mode fast3 --smuxver 2 --conn 1 --sndwnd 1024 --rcvwnd 1024 --nocomp >/dev/null 2>&1 &
else
  target/release/kcptun-client -l 127.0.0.1:12948 -r 127.0.0.1:29900 --key test --crypt aes --mode fast3 --smuxver 2 --conn 1 --sndwnd 1024 --rcvwnd 1024 --nocomp >/dev/null 2>&1 &
fi

up=0
for i in $(seq 1 40); do
  if python3 -c "import socket;s=socket.socket();s.settimeout(0.2);s.connect(('127.0.0.1',12948));s.close()" 2>/dev/null; then up=1; break; fi
  sleep 0.25
done
[ "$up" = "1" ] || { echo "RESULT client-not-ready"; pkill -9 -f "kcptun"; pkill -9 -f "python3 -u"; exit 1; }

python3 "$(dirname "$0")/probe_tunnel.py" 12948 "$RPS" "$SIZE" "$WARMUP" "$DUR" "$CONC"

pkill -9 -f "kcptun" 2>/dev/null; pkill -9 -f "python3" 2>/dev/null
# drain TIME_WAIT before next run
sleep 12
true
