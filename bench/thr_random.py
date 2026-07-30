#!/usr/bin/env python3
"""Single-connection random-payload thr (matches bench_rust_vs_go.py data plane).

Prints one line: THR_MBps=<float>
"""
from __future__ import annotations

import argparse
import hashlib
import os
import socket
import sys
import threading
import time


def thr(port: int, size: int, timeout: float) -> float:
    payload = os.urandom(size)
    expected = hashlib.md5(payload).hexdigest()
    received = bytearray()
    err: list[BaseException | None] = [None]

    s = socket.socket()
    s.settimeout(timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect(("127.0.0.1", port))

    def rx() -> None:
        try:
            while len(received) < size:
                d = s.recv(65536)
                if not d:
                    break
                received.extend(d)
        except BaseException as e:  # noqa: BLE001
            err[0] = e

    t = threading.Thread(target=rx, daemon=True)
    t.start()
    t0 = time.perf_counter()
    sent = 0
    while sent < size:
        n = s.send(payload[sent : sent + 65536])
        if n <= 0:
            break
        sent += n
    t.join(timeout=timeout)
    elapsed = time.perf_counter() - t0
    s.close()
    if err[0] is not None:
        raise RuntimeError(f"rx error: {err[0]}")
    if len(received) != size:
        raise RuntimeError(f"short {len(received)}/{size}")
    if hashlib.md5(bytes(received)).hexdigest() != expected:
        raise RuntimeError("md5 mismatch")
    if elapsed <= 0:
        return 0.0
    return (size / (1024 * 1024)) / elapsed


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("port", type=int)
    p.add_argument("--size", type=int, default=2 * 1024 * 1024)
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--warmup-kb", type=int, default=256)
    args = p.parse_args()
    # warmup compressible path + window
    try:
        thr(args.port, min(args.warmup_kb * 1024, args.size), args.timeout)
    except Exception as e:  # noqa: BLE001
        print(f"WARN warmup: {e}", file=sys.stderr)
    try:
        mbps = thr(args.port, args.size, args.timeout)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR {e}", file=sys.stderr)
        print("THR_MBps=0")
        sys.exit(1)
    print(f"THR_MBps={mbps:.4f}")


if __name__ == "__main__":
    main()
