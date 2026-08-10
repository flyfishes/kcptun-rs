<!-- Parent: ../../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-05 (single custom Notify in mod.rs; add cancel.rs) -->

# sync

## Purpose

Runtime-agnostic sync primitives used for backpressure, cancellation, and mutual exclusion: a custom permit-storing `Notify`, `Mutex` from `async_lock`, and `CancellationToken` + `race` for cancelable I/O.

## Key Files

| File | Description |
|------|-------------|
| `mod.rs` | Re-exports `async_lock::Mutex`; the single custom permit-storing `Notify` (backend-independent) |
| `cancel.rs` | `CancellationToken` (built on `Notify`) + runtime-agnostic `race(a, b)` combinator for racing a socket recv against close cancellation |

## Subdirectories

None.

## For AI Agents

### Working In This Directory

- Prefer `kio::sync::Notify` over `tokio::sync::Notify` in shared code. `Notify` is a **single-waiter** permit-storing primitive — at most one task may `notified()` at a time.
- `Mutex` is always `async_lock` — works on both backends.
- `CancellationToken`: `cancel()` sets a flag + wakes the waiting `cancelled()` future. Each recv loop holds one and races its socket recv against `cancelled()` so `close()` cancels the recv immediately instead of a 100ms poll tick.
- `race(a, b)` polls both futures in turn (runtime-agnostic); both must be `Unpin` (box-pin a non-Unpin recv at the call site). The loser is dropped — a cancelled socket recv just stops polling, the datagram stays buffered.

### Testing Requirements

- Crate-level kio tests / usage from smux & binaries
- `cancel.rs` unit tests run on both tokio and smol backends

### Common Patterns

- Wake flush/read loops on buffer space or new data
- `kio::race(socket.recv(...), token.cancelled())` → exit the recv loop on close

## Dependencies

### Internal

- Parent `kio`

### External

- `async_lock`; tokio or smol feature crates

<!-- MANUAL: -->
