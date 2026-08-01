# TCP Transport (tcpraw) Implementation Spec

**Date**: 2026-07-29
**Status**: draft
**Go Reference**: `/Users/yangzhiqin/Documents/Project/kcptun/vendor/github.com/xtaci/tcpraw/`

## Overview

Implement TCP raw socket transport in kcptun-rs, wire-compatible with Go kcptun's `tcpraw` package. The `--tcp` flag currently emits a warning and falls back to UDP; this spec replaces the stub with a working implementation.

## Motivation

- The `--tcp` CLI flag is already defined and parsed in both client and server but is a no-op
- Users need TCP transport to bypass firewalls/DPI that block UDP while allowing TCP
- Must be wire-compatible with Go kcptun so Rust↔Go interop works over TCP

## Architecture

### New: `tcpraw` module in `kio-rs`

```
kio-rs/src/net/
├── mod.rs           ← + DatagramSocket enum
├── tcpraw.rs        ← NEW (Linux raw socket TCP emulation)
├── tcpraw_stub.rs   ← NEW (non-Linux compile stub)
├── tokio.rs
├── smol.rs
└── mmsg.rs
```

### New: `DatagramSocket` enum in `kio-rs`

A unified type over UDP and TCP raw sockets, enabling client/server code to operate on either transport via match dispatch without trait/generic overhead.

```rust
pub enum DatagramSocket {
    Udp(UdpSocket),
    #[cfg(target_os = "linux")]
    TcpRaw(TcpRawConn),
}
```

Methods: `recv_from`, `send_to`, `send_batch_to`, `try_recv_from`, `try_recv_batch_from`, `local_addr`.

## Core Mechanism: TCP_REPAIR

Instead of Go's iptables+TTL=1 hack, we use Linux `TCP_REPAIR` socket option to silence the kernel TCP stack after the three-way handshake:

### Dial (client)

1. `TcpStream::connect(addr)` — kernel completes TCP 3-way handshake
2. `getsockopt(TCP_INFO)` — capture post-handshake seq/ack/mss/timestamp state
3. `setsockopt(TCP_REPAIR, TCP_REPAIR_ON)` — kernel goes silent, stops processing this connection
4. Create raw IP socket: `socket(AF_INET, SOCK_RAW, IPPROTO_TCP)` + `IP_HDRINCL`
5. Spawn capture task: read raw socket → parse IP+TCP headers → verify seq/ack → strip headers → push payload to mpsc channel
6. Continue numbering from captured seq/ack state

### Listen (server)

1. `TcpListener::bind(addr)` + `.accept()` — kernel completes 3-way handshake per connection
2. For each accepted connection: `getsockopt(TCP_INFO)` → `TCP_REPAIR_ON`
3. Create shared raw IP socket for capture (one per listener)
4. Route incoming packets by source address to per-connection channels

### Close

1. Remove `TCP_REPAIR` mode (or just close sockets)
2. Close raw socket
3. Close real TCP socket (kernel cleans up)

## Wire Format (Go-Compatible)

Each KCP datagram is wrapped in a complete TCP/IP packet sent via raw socket:

```
[IP Header 20B][TCP Header 20B + 12B Timestamp Option][KCP Datagram N bytes]
```

### IP Header Fields

| Field | Value | Note |
|-------|-------|------|
| Version/IHL | 4/5 (0x45) | IPv4, 20-byte header |
| TOS/DSCP | 0 | Match Go default |
| Total Length | 52 + payload.len() | Computed |
| Identification | 0 | Kernel fills for raw sockets |
| Flags/Fragment | 0x40 (Don't Fragment) | |
| TTL | 64 | Match Go fingerprint |
| Protocol | 6 (TCP) | |
| Header Checksum | Computed | RFC 791 |
| Src/Dst IP | From connection | |

### TCP Header Fields

| Field | Value | Note |
|-------|-------|------|
| Src/Dst Port | From connection | |
| Seq Number | Increment per-packet | Initial value from TCP_INFO |
| Ack Number | Tracked from received packets | Initial value from TCP_INFO |
| Data Offset | 0x80 (8 × 4 = 32B = 20B base + 12B options) | |
| Flags | PSH + ACK (0x18) | Match Go |
| Window | 65535 | Match Go fingerprint |
| Checksum | Computed | RFC 793 pseudo-header + TCP segment |
| Urgent Pointer | 0 | |

### TCP Options (12 bytes)

| Byte | Value | Meaning |
|------|-------|---------|
| 0 | 1 | NOP |
| 1 | 1 | NOP |
| 2 | 8 | Timestamp option kind |
| 3 | 10 | Timestamp option length |
| 4-7 | TSval | Timestamp value |
| 8-11 | TSecr | Timestamp echo reply |

## Types

### TcpRawConn (client-side + server-side accepted connection)

```rust
struct TcpRawConn {
    real: TcpStream,              // real TCP, in TCP_REPAIR mode
    raw: socket2::Socket,         // raw IP socket
    flow: TcpFlowState,           // seq, ack, ts state
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    rx: mpsc::Receiver<Vec<u8>>,  // captured payloads
    close_notify: Notify,         // signal capture task to stop
}

struct TcpFlowState {
    seq: AtomicU32,
    ack: AtomicU32,
    ts_val: AtomicU32,            // my timestamp
    ts_ecr: AtomicU32,            // peer's last timestamp
}
```

### TcpRawListener (server-side)

```rust
struct TcpRawListener {
    real: TcpListener,
    raw: socket2::Socket,         // shared raw IP capture socket
    connections: DashMap<SocketAddr, Sender<Vec<u8>>>,
    close_notify: Notify,
}
```

### DatagramSocket

```rust
pub enum DatagramSocket {
    Udp(UdpSocket),
    #[cfg(target_os = "linux")]
    TcpRaw(TcpRawConn),
}
```

## Client-Side Changes

### KcpConn

```
BEFORE:
  udp: Arc<UdpSocket>

AFTER:
  socket: Arc<DatagramSocket>
```

- `KcpConn::new()` takes a `DatagramSocket` instead of creating `UdpSocket` internally
- I/O loops (recv, flush, batch send) operate on `DatagramSocket` — no logic changes
- Initialization split: `async_main` decides Transport, creates the appropriate socket, passes it in

### async_main

```rust
let transport = if cli.tcp { Transport::Tcp } else { Transport::Udp };

let socket = match transport {
    Transport::Udp => {
        let udp = raw_udp(remote)?;
        DatagramSocket::Udp(UdpSocket::from(udp))
    }
    Transport::Tcp => {
        let conn = tcpraw::dial(&remote)?;
        DatagramSocket::TcpRaw(conn)
    }
};
```

## Server-Side Changes

### KcpServerSession

```
BEFORE:
  udp: Arc<UdpSocket>
  per-socket recv loop with get_or_create_session (addr-based routing)

AFTER (UDP mode):
  same as before

AFTER (TCP mode):
  TcpRawListener::bind → accept loop → each conn is a dedicated session
  No need for get_or_create_session — accept gives us 1:1 conn:session
```

### async_main

Server spawns a TCP listener loop OR a UDP recv loop based on `--tcp`:

```rust
if cli.tcp {
    let listener = tcpraw::listen(&listen_addr)?;
    loop {
        let (conn, peer_addr) = listener.accept().await?;
        let socket = Arc::new(DatagramSocket::TcpRaw(conn));
        // Create KcpServerSession directly — no DashMap routing needed
        spawn_session(socket, peer_addr, &key, &session_cfg);
    }
} else {
    // existing UDP per-socket recv loop
}
```

## Non-Linux Platforms

`--tcp` on non-Linux platforms exits with a clear error:

```rust
Err(io::Error::new(ErrorKind::Unsupported, 
    "tcpraw transport requires Linux (raw sockets + TCP_REPAIR)"))
```

No silent fallback to UDP — the user asked for TCP and should know it's unavailable.

## Dependencies

| Crate | Already in tree? | Purpose |
|-------|------------------|---------|
| `socket2` | Yes (kio-rs) | Raw socket creation, sockopts, TCP_REPAIR |
| `libc` | Yes (transitive) | `SOL_TCP`, `TCP_REPAIR`, `TCP_INFO`, `IP_HDRINCL` |
| `bytes` | Yes (transitive) | Buffer management for packet construction |

No new external dependencies. IP/TCP header construction is hand-written (fixed-size, well-defined formats).

## Implementation Order

1. **`kio-rs`: tcpraw module** — `TcpRawConn`, `TcpRawListener`, TCP state machine, IP/TCP header serialization, capture loop
2. **`kio-rs`: DatagramSocket enum** — unify UDP and TCP raw behind match-based dispatch
3. **`kio-rs`: tcpraw_stub** — non-Linux compile stub returning Unsupported error
4. **`kcptun-client`: wire --tcp** — Transport enum, async_main dispatch, KcpConn socket field change
5. **`kcptun-server`: wire --tcp** — accept loop, per-connection session creation
6. **Gate checks** — `make gate` (fmt, test, clippy all clean)
7. **Go interop test** — Rust client ↔ Go server over TCP, verify wire compatibility

## Risk Analysis

| Risk | Mitigation |
|------|------------|
| IP/TCP checksum computation error | Unit test with known packet captures from Go |
| seq/ack drift causing RST from peer | Validate with Go interop test; compare with Go's flow tracking |
| Raw socket recv buffer overflow | Tune buffer size (2MB default, matching UDP buffers) |
| TCP_REPAIR not available (old kernel) | Feature probe at startup; clear error message |
| tokio/smol dual backend | Raw socket I/O uses `socket2` + direct syscalls, runtime-agnostic via `cpu_block` or async fd wrappers |

## Verification Gate

1. `cargo test --workspace` — all existing tests pass
2. `cargo clippy --workspace -- -D warnings` — zero warnings
3. `cargo fmt --all -- --check` — zero diff
4. Go interop: Rust client ↔ Go server with `--tcp` flag, data transfer test
5. Go interop: Go client ↔ Rust server with `--tcp` flag, data transfer test
6. Non-Linux: `--tcp` on macOS exits with clear Unsupported error (current dev platform)
