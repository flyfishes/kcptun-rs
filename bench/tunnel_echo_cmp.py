#!/usr/bin/env python3
"""Back-to-back Go vs Rust kcptun tunnel latency: sequential echo (closed model)."""
import socket, time, subprocess, sys, os, threading

def start_tcp_echo(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', port))
    srv.listen(8)
    def loop():
        while True:
            try: c, _ = srv.accept()
            except: break
            def echo(c):
                while True:
                    d = c.recv(65536)
                    if not d: break
                    c.sendall(d)
            threading.Thread(target=echo, args=(c,), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()
    return srv

def run_tunnel_echo(tunnel_port, size, count):
    payload = bytes(range(256)) * (size // 256)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(('127.0.0.1', tunnel_port))
    lats = []
    for _ in range(count):
        t0 = time.monotonic()
        s.sendall(payload)
        got = 0
        while got < size:
            d = s.recv(65536)
            if not d: break
            got += len(d)
        lats.append((time.monotonic() - t0) * 1e6)
    s.close()
    lats.sort()
    n = len(lats)
    return {
        'p50': lats[n // 2],
        'p99': lats[min(int(n * 0.99), n - 1)],
        'n': n,
    }

def main():
    size = 262144
    count = 200
    tcp_echo = start_tcp_echo(18080)
    procs = []
    try:
        # Go
        print(f"=== Go kcptun tunnel (size={size}B, n={count}) ===", flush=True)
        srv = subprocess.Popen(
            ['tests/kcptun-go/server', '-l', ':29901', '-t', '127.0.0.1:18080',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(srv)
        time.sleep(0.5)
        cli = subprocess.Popen(
            ['tests/kcptun-go/client', '-l', ':29902', '-r', '127.0.0.1:29901',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(cli)
        time.sleep(1.5)
        r = run_tunnel_echo(29902, size, count)
        print(f"  Go:  p50={r['p50']/1000:.2f}ms  p99={r['p99']/1000:.2f}ms  n={r['n']}", flush=True)
        for p in procs: p.terminate()
        for p in procs: p.wait()
        procs.clear()
        time.sleep(1)

        # Rust
        print(f"=== Rust kcptun tunnel (size={size}B, n={count}) ===", flush=True)
        srv = subprocess.Popen(
            ['target/release/kcptun-server', '-l', ':29901', '-t', '127.0.0.1:18080',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(srv)
        time.sleep(0.5)
        cli = subprocess.Popen(
            ['target/release/kcptun-client', '-l', ':29902', '-r', '127.0.0.1:29901',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(cli)
        time.sleep(2)
        # Retry connect for up to 5s
        for _ in range(10):
            try:
                r = run_tunnel_echo(29902, size, count)
                break
            except ConnectionRefusedError:
                time.sleep(0.5)
        else:
            print("  Rust: FAILED to connect", flush=True)
            r = None
        if r:
            print(f"  Rust: p50={r['p50']/1000:.2f}ms  p99={r['p99']/1000:.2f}ms  n={r['n']}", flush=True)
    finally:
        for p in procs: p.terminate()
        for p in procs: p.wait()

if __name__ == '__main__':
    main()
