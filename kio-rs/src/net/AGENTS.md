<!-- Parent: ../../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-07-22 -->

# net

## Purpose

TCP/UDP socket wrappers. All sockets created via **socket2** (2 MB buffers, SO_REUSEADDR, non-blocking), then handed to tokio or smol async wrappers.

## Key Files

| File | Description |
|------|-------------|
| `mod.rs` | Shared `raw_udp` / TCP setup; re-exports backend types; `DatagramSocket` enum |
| `tokio.rs` | Tokio `TcpListener` / `TcpStream` / `UdpSocket` |
| `smol.rs` | Smol backend equivalents |
| `mmsg.rs` | Optional multi-message / batch UDP helpers if present |
| `tcpraw.rs` | Linux-only TCP raw (`TCP_REPAIR` + `SOCK_RAW`/`IPPROTO_TCP`, **no** IP_HDRINCL — TCP segment only, Go-compatible); seq/ack via `TCP_QUEUE_SEQ` |
| `tcpraw_stub.rs` | Non-Linux stub returning `Unsupported` |

## Subdirectories

None.

## For AI Agents

### Working In This Directory

- Keep socket options identical across backends (`SOCK_BUF = 2MB`).
- Bidirectional copy lives in **crate root** (`copy_bidirectional*`), not here.
- Client mode may `connect()` UDP when remote is known.
- **tcpraw** is Linux-only. Seed seq/ack with `TCP_REPAIR` + `TCP_QUEUE_SEQ` (not `tcp_info.tcpi_snd_nxt` — those fields are absent from the `libc` crate). Do **not** use `TCP_TIMESTAMP` after repair for peer ts_ecr — it returns local `tsoffset`; start ts_ecr at 0 and fill from captured options. Use `socket2` feature `all` for `Type::RAW`.
- **Wire: TCP segments only** (`IPPROTO_TCP` raw, no `IP_HDRINCL`). Go `DialIP("ip:tcp")` same shape — kernel adds/strips IP. Sending full IP+TCP with HDRINCL breaks connectivity.
- Capture must update flow (seq←peer ACK, ack←peer seq+len, ts_ecr←peer TSval) and filter `dst_port`; only push PSH payloads (Go `captureFlow`).
- **TcpRawListener must stay blocking.** `accept()` runs in `cpu_block` (OS thread). Non-blocking accept returns EAGAIN when idle → server accept loop exits → client `--tcp` dial gets Connection refused.
- **mmsg IPv4 `s_addr`**: use `u32::from_ne_bytes(ip.octets())`, never `from_be_bytes` — LE hosts would send to `1.0.0.127` instead of `127.0.0.1` (silent blackhole / hang). Only affects Linux `sendmmsg_to` (server flush path).

### Testing Requirements

- Covered by `kio-rs` tests and binary e2e

### Common Patterns

- `UdpSocket` / `TcpStream` types re-exported as `kio::{UdpSocket, TcpStream, TcpListener}`

## Dependencies

### Internal

- Parent `kio` facade

### External

- `socket2` (feature `all` — needed for RAW / IP_HDRINCL), `libc`, runtime crates

<!-- MANUAL: -->
