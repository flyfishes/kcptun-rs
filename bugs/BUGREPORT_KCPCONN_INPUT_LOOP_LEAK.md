# KcpConn input loop leaks UDP socket after close/drop (fixed)

## Status

**Fixed** (2026-08-01) — `spawn_input_loop` recv is now bounded by a 100ms `kio::timeout` +
`is_closed()` re-check, so the task exits within ~100ms of `close()`. Verified by the `lsof`
probe on both `async-tokio` and `async-smol`. Suspected contributor to
`BUGREPORT_PROXY_MEMORY_GROWTH.md` (that report's status is unchanged; the leak may explain
part of it).

## Symptom

Dropping (or calling `close()` on) a **dialed** `KcpConn` does not release the connection's UDP
socket. In a long-running process, every closed session whose peer has gone silent leaks one socket
fd + one background task.

## Evidence (reproduced)

Probe: dial `KcpConn` → `kcp-rs` `KcpConn::connect(peer).build()` → `drop(conn)` → `lsof` the
process. Peer = a live-but-silent bound UDP socket (so no ICMP `ECONNREFUSED` wakes the recv).

```
$ lsof -a -p <pid> -i udp
tmp_leak_ ...  4u  IPv4 ... UDP localhost:51798                       <- peer (kept)
tmp_leak_ ...  5u  IPv4 ... UDP localhost:54207->localhost:51798      <- conn#0, DROPPED, still open
tmp_leak_ ...  6u  IPv4 ... UDP localhost:65092->localhost:51798      <- conn#1, DROPPED, still open
tmp_leak_ ...  7u  IPv4 ... UDP localhost:53518->localhost:51798      <- conn#2, DROPPED, still open
```

Same result on `async-tokio` and `async-smol`.

(Note: a naive `/dev/fd | wc -l` delta test is unreliable on macOS and misses this — use `lsof`.)

## Root cause

- `kcp-rs/src/conn.rs` `spawn_input_loop` (line ~697):
  ```rust
  let n = match shared.transport.recv(&mut buf).await { ... };
  ```
  `transport.recv()` blocks until a datagram arrives. There is **no timeout and no `closed`-check**
  around it; the `is_closed()` check is only at the top of the outer loop, which the task cannot
  reach while parked in `recv`.
- `KcpConnShared::close()` (line ~178) only sets `closed` + notifies `flush_notify` /
  `write_notify` / `read_notify` + wakes the read waker. It **never closes the UDP socket**.
- On `KcpConn` drop, `_handles` `JoinHandle`s are detached (kio semantics), so the input-loop task
  keeps running; it holds `Arc<KcpConnShared>` → `Arc<dyn PacketTransport>` → the socket. Nothing
  closes the socket, so the fd stays open until a stray datagram finally wakes `recv`.

The flush loop is fine (`close()` does `flush_notify.notify_one()` and it checks `is_closed()`).

Compare: Go kcp-go's `Close()` closes the underlying packet conn, so its `readLoop` unblocks with
an error and exits.

## Impact

- kcptun client/server: sessions closed while the peer is silent leak an fd + a parked task each.
  Over thousands of sessions this is unbounded growth (see `BUGREPORT_PROXY_MEMORY_GROWTH.md`).
- The leaked task holds a reference to the (potentially large) `KcpConnShared`, so memory too.

## Fix (applied)

`spawn_input_loop` (`kcp-rs/src/conn.rs`) wraps the blocking `transport.recv()` in a
`kio::timeout(Duration::from_millis(100), ...)`. On timeout it loops back to the top and re-checks
`is_closed()`, so a closed connection tears the task down within ~100ms. Same pattern the listener
reader already used; active links complete `recv` well inside the window (no added latency).

```rust
let n = match kio::timeout(Duration::from_millis(100), shared.transport.recv(&mut buf)).await {
    Ok(Ok(n)) if n > 0 => n,
    Ok(Ok(_)) => continue,
    Ok(Err(_)) if shared.is_closed() => break,
    Ok(Err(_)) => { kio::sleep_ms(10).await; continue; }
    Err(_) => continue, // 100ms tick → re-check closed
};
```

## Verify

```bash
# probe described in Evidence: dial a live-but-silent peer, drop, lsof → socket must be gone
```
