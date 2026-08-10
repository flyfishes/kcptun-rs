#!/usr/bin/env python3
"""Fair latency comparison: Go kcptun tunnel vs Rust kcptun tunnel.

Both are TCP→KCP→TCP echo tunnels with null cipher, no compression, smuxver 2.
Sends N requests at fixed RPS through the tunnel, measures per-request RTT.
"""
import socket, time, subprocess, sys, os, signal, json, statistics

def start_tcp_echo(port):
    """Start a simple TCP echo server."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', port))
    srv.listen(8)
    import threading
    def echo_loop():
        while True:
            try:
                c, _ = srv.accept()
            except:
                break
            def echo(c):
                while True:
                    d = c.recv(65536)
                    if not d: break
                    c.sendall(d)
            threading.Thread(target=echo, args=(c,), daemon=True).start()
    threading.Thread(target=echo_loop, daemon=True).start()
    return srv

def run_latency_test(port, rps, size, duration, warmup):
    """Send fixed-rate requests through tunnel, measure RTT."""
    interval = 1.0 / rps
    payload = bytes(range(256)) * (size // 256)
    rx = bytearray()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(('127.0.0.1', port))
    
    latencies = []
    in_flight = []
    next_send = time.monotonic()
    warmup_end = next_send + warmup
    measure_end = warmup_end + duration
    measuring = False
    
    while time.monotonic() < measure_end:
        now = time.monotonic()
        if now >= next_send:
            s.sendall(payload)
            in_flight.append(time.monotonic())
            if measuring:
                pass  # count sends
            next_send += interval
            if next_send < now:
                next_send = now + interval
        
        # Read response
        s.settimeout(max(0, min(0.001, next_send - time.monotonic())))
        try:
            data = s.recv(65536)
            if data:
                rx.extend(data)
                while len(rx) >= size and in_flight:
                    rx = rx[size:]
                    t0 = in_flight.pop(0)
                    if time.monotonic() >= warmup_end:
                        latencies.append((time.monotonic() - t0) * 1e6)
        except socket.timeout:
            pass
        
        if not measuring and time.monotonic() >= warmup_end:
            measuring = True
    
    s.close()
    return latencies

def stats(lat):
    if not lat:
        return None
    lat.sort()
    n = len(lat)
    def pct(q):
        return lat[min(int(n * q), n - 1)]
    return {
        'p50': pct(0.50),
        'p90': pct(0.90),
        'p99': pct(0.99),
        'avg': sum(lat) / n,
        'min': lat[0],
        'max': lat[-1],
        'n': n,
    }

def main():
    rps = int(sys.argv[1]) if len(sys.argv) > 1 else 450
    size = int(sys.argv[2]) if len(sys.argv) > 2 else 262144
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    warmup = 3
    
    procs = []
    try:
        # TCP echo server
        tcp_echo = start_tcp_echo(18080)
        
        results = {}
        
        # --- Go kcptun tunnel ---
        print(f"=== Go kcptun tunnel (RPS={rps}, size={size}) ===", flush=True)
        go_srv = subprocess.Popen(
            ['tests/kcptun-go/server', '-l', ':29901', '-t', '127.0.0.1:18080',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(go_srv)
        time.sleep(0.5)
        go_cli = subprocess.Popen(
            ['tests/kcptun-go/client', '-l', ':29902', '-r', '127.0.0.1:29901',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(go_cli)
        time.sleep(1.5)
        
        lat = run_latency_test(29902, rps, size, duration, warmup)
        s = stats(lat)
        if s:
            print(f"  Go: p50={s['p50']/1000:.2f}ms p90={s['p90']/1000:.2f}ms p99={s['p99']/1000:.2f}ms n={s['n']}", flush=True)
            results['go'] = s
        else:
            print("  Go: no data", flush=True)
        
        for p in procs:
            p.terminate()
        procs.clear()
        time.sleep(1)
        
        # --- Rust kcptun tunnel ---
        print(f"=== Rust kcptun tunnel (RPS={rps}, size={size}) ===", flush=True)
        rs_srv = subprocess.Popen(
            ['target/release/kcptun-server', '-l', ':29901', '-t', '127.0.0.1:18080',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(rs_srv)
        time.sleep(0.5)
        rs_cli = subprocess.Popen(
            ['target/release/kcptun-client', '-l', ':29902', '-r', '127.0.0.1:29901',
             '--key', 'test', '--crypt', 'null', '--mode', 'fast3', '--nocomp',
             '--smuxver', '2', '--sndwnd', '512', '--rcvwnd', '512'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(rs_cli)
        time.sleep(1.5)
        
        lat = run_latency_test(29902, rps, size, duration, warmup)
        s = stats(lat)
        if s:
            print(f"  Rust: p50={s['p50']/1000:.2f}ms p90={s['p90']/1000:.2f}ms p99={s['p99']/1000:.2f}ms n={s['n']}", flush=True)
            results['rust'] = s
        else:
            print("  Rust: no data", flush=True)
        
        # Summary
        if 'go' in results and 'rust' in results:
            g, r = results['go'], results['rust']
            print(f"\n=== Summary (RPS={rps}, {size}B) ===")
            print(f"  p50: Go={g['p50']/1000:.2f}ms  Rust={r['p50']/1000:.2f}ms  ratio={r['p50']/g['p50']:.2f}x")
            print(f"  p99: Go={g['p99']/1000:.2f}ms  Rust={r['p99']/1000:.2f}ms  ratio={r['p99']/g['p99']:.2f}x")
    
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()

if __name__ == '__main__':
    main()
