#!/usr/bin/env python3
"""Quick tunnel verification: send 256KB through Rust kcptun tunnel, expect echo."""
import socket, time, subprocess, sys, os

def main():
    # Start TCP echo server
    echo_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    echo_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    echo_srv.bind(('127.0.0.1', 18080))
    echo_srv.listen(8)
    def echo_loop():
        while True:
            c, _ = echo_srv.accept()
            def echo(c):
                while True:
                    d = c.recv(65536)
                    if not d: break
                    c.sendall(d)
            import threading
            threading.Thread(target=echo, args=(c,), daemon=True).start()
    import threading
    threading.Thread(target=echo_loop, daemon=True).start()

    # Start kcptun server
    srv = subprocess.Popen(
        ['target/release/kcptun-server', '-l', ':29901', '-t', '127.0.0.1:18080',
         '--key', 'test', '--crypt', 'aes', '--mode', 'fast3', '--nocomp', '--smuxver', '2'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)

    # Start kcptun client
    cli = subprocess.Popen(
        ['target/release/kcptun-client', '-l', ':29902', '-r', '127.0.0.1:29901',
         '--key', 'test', '--crypt', 'aes', '--mode', 'fast3', '--nocomp', '--smuxver', '2'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)

    try:
        # Test 256KB echo
        data = bytes(range(256)) * 1024  # 256KB
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect(('127.0.0.1', 29902))
        t0 = time.time()
        s.sendall(data)
        got = b''
        while len(got) < len(data):
            d = s.recv(65536)
            if not d: break
            got += d
        rtt = (time.time() - t0) * 1000
        ok = got == data
        print(f'256KB tunnel echo: {rtt:.1f}ms, data_ok={ok}, len={len(got)}')
        s.close()

        # Test 1KB echo
        data1k = b'A' * 1024
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.settimeout(10)
        s2.connect(('127.0.0.1', 29902))
        t0 = time.time()
        s2.sendall(data1k)
        got2 = b''
        while len(got2) < len(data1k):
            d = s2.recv(65536)
            if not d: break
            got2 += d
        rtt2 = (time.time() - t0) * 1000
        ok2 = got2 == data1k
        print(f'1KB tunnel echo: {rtt2:.1f}ms, data_ok={ok2}, len={len(got2)}')
        s2.close()

        if ok and ok2:
            print('TUNNEL: PASS')
        else:
            print('TUNNEL: FAIL')
            sys.exit(1)
    finally:
        cli.terminate()
        srv.terminate()
        os.system(f'kill {os.getpid()} 2>/dev/null')

if __name__ == '__main__':
    main()
