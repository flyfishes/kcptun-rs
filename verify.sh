#!/bin/bash
# kcptun-rs one-command verification.
# Usage: bash verify.sh
# Output: last line is PASS or FAIL
set -eo pipefail
cd "$(dirname "$0")"

FAILED=0

echo "=== Format ==="
if cargo fmt --all -- --check 2>&1 | tail -3; then
    echo "fmt: OK"
else
    echo "fmt: FAIL"; FAILED=1
fi

echo ""
echo "=== Build ==="
if cargo build --workspace 2>&1 | tail -5; then
    echo "build: OK"
else
    echo "build: FAIL"; FAILED=1
fi

echo ""
echo "=== Clippy ==="
if cargo clippy --workspace -- -D warnings 2>&1 | tail -5; then
    echo "clippy: OK"
else
    echo "clippy: FAIL"; FAILED=1
fi

echo ""
echo "=== Tests ==="
if cargo test --workspace 2>&1 | tail -10; then
    echo "tests: OK"
else
    echo "tests: FAIL"; FAILED=1
fi

echo ""
echo "═══════════════════════════════════════"
if [ $FAILED -eq 0 ]; then
    echo "PASS"
else
    echo "FAIL"
fi
echo "═══════════════════════════════════════"
exit $FAILED
