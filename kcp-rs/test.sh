#!/usr/bin/env bash
#
# kcp-rs standalone test runner.
#
# Usage:  bash kcp-rs/test.sh        (from anywhere in the workspace)
#
# Runs every kcp-rs test on its own — no client/server binaries, no Go e2e
# harness, no network beyond 127.0.0.1 loopback:
#
#   1. Sync KCP data-correctness tests   (default features, no async deps)
#   2. Full suite + async KcpConn/KcpListener tests (tokio backend)
#   3. Full suite + async KcpConn/KcpListener tests (smol backend)
#
# `--all-features` is intentionally NOT used: kio-rs enforces that tokio and
# smol are mutually exclusive (see kio-rs/build.rs).

set -euo pipefail

cd "$(dirname "$0")"

echo "==== kcp-rs standalone tests ===="
echo

echo "==> [1/3] Sync KCP data-correctness (default features)"
cargo test -p kcp-rs

echo
echo "==> [2/3] Async KcpConn integrity (tokio)"
cargo test -p kcp-rs --features async-tokio

echo
echo "==> [3/3] Async KcpConn integrity (smol)"
cargo test -p kcp-rs --features async-smol

echo
echo "All kcp-rs standalone tests passed."
