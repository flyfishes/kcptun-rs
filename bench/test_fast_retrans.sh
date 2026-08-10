#!/bin/bash
set -e

# Start server in background
echo "Starting server..."
./target/release/examples/latency_p99 --mode server --port 39100 > server.log 2>&1 &
SERVER_PID=$!

# Give server time to start
sleep 2

# Run client test
echo "Running client test..."
timeout 15 ./target/release/examples/latency_p99 --mode peer --addr 127.0.0.1:39100 --size 65536 --rps 500 --duration 10 --warmup 2 --snmp > client.log 2>&1

# Kill server
kill $SERVER_PID || true
wait $SERVER_PID || true

echo "=== CLIENT RESULTS ==="
grep -E "(RESULT|SNMP)" client.log

echo "=== SERVER RESULTS ==="
grep -E "(RESULT|SNMP)" server.log