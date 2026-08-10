#!/bin/bash

echo "=== Tokio Runtime 优化对比测试 ==="
echo ""

echo "1. 默认多线程模式 (baseline)"
./target/release/examples/latency_p99 --snmp --mode self --size 65536 --rps 500 --duration 8 --warmup 2 > tokio_baseline.txt 2>&1
P99_BASELINE=$(grep "p99=" tokio_baseline.txt | cut -d'=' -f2 | cut -d' ' -f1)
P999_BASELINE=$(grep "p999=" tokio_baseline.txt | cut -d'=' -f2 | cut -d' ' -f1)
echo "   P99: ${P99_BASELINE}ms, P999: ${P999_BASELINE}ms"

echo ""
echo "2. 单线程模式 (--rt-single)"
./target/release/examples/latency_p99 --rt-single --snmp --mode self --size 65536 --rps 500 --duration 8 --warmup 2 > tokio_single.txt 2>&1
P99_SINGLE=$(grep "p99=" tokio_single.txt | cut -d'=' -f2 | cut -d' ' -f1)
P999_SINGLE=$(grep "p999=" tokio_single.txt | cut -d'=' -f2 | cut -d' ' -f1)
echo "   P99: ${P99_SINGLE}ms, P999: ${P999_SINGLE}ms"

echo ""
echo "3. Smol runtime"
./target/release/examples/latency_p99 --snmp --mode self --size 65536 --rps 500 --duration 8 --warmup 2 > smol.txt 2>&1
P99_SMOL=$(grep "p99=" smol.txt | cut -d'=' -f2 | cut -d' ' -f1)
P999_SMOL=$(grep "p999=" smol.txt | cut -d'=' -f2 | cut -d' ' -f1)
echo "   P99: ${P99_SMOL}ms, P999: ${P999_SMOL}ms"

echo ""
echo "=== 结果汇总 ==="
echo "| 模式 | P99 | P999 | 相对基准 |"
echo "|------|-----|------|----------|"
echo "| Tokio多线程 | ${P99_BASELINE}ms | ${P999_BASELINE}ms | 基准 |"
echo "| Tokio单线程 | ${P99_SINGLE}ms | ${P999_SINGLE}ms | x$(echo "${P99_SINGLE}/${P99_BASELINE}" | bc -l | cut -c1-4) |"
echo "| Smol | ${P99_SMOL}ms | ${P999_SMOL}ms | x$(echo "${P99_SMOL}/${P99_BASELINE}" | bc -l | cut -c1-4) |"

# 清理
tail -n 1 tokio_baseline.txt
echo ""
tail -n 1 tokio_single.txt
echo ""
tail -n 1 smol.txt