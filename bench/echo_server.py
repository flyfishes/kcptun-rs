#!/usr/bin/env python3
"""asyncio TCP echo server — single event loop, no thread-per-connection.

Keeps the Python echo path off the measurement critical path under high
concurrency (thread-per-connection + GIL would saturate CPU and inflate the
tunnel RTT numbers).
"""
import asyncio
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 29900


async def handle(r, w):
    try:
        while True:
            d = await r.read(262144)
            if not d:
                break
            w.write(d)
            await w.drain()
    except Exception:
        pass
    w.close()


async def main():
    srv = await asyncio.start_server(handle, '0.0.0.0', PORT)
    await srv.serve_forever()


asyncio.run(main())
