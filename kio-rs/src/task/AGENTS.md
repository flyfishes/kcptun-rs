<!-- Parent: ../../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-03 -->

# task

## Purpose

Task spawning and CPU offload: `spawn_task`, `cpu_block`, `block_on`, `JoinHandle`. Critical for flush-path crypto/snappy offload.

## Key Files

| File | Description |
|------|-------------|
| `mod.rs` | API surface + JoinHandle semantics notes |
| `tokio.rs` | `tokio::spawn` / `spawn_blocking` style offload |
| `smol.rs` | Global executor + blocking pool; JoinHandle detaches on drop |

## Subdirectories

None.

## For AI Agents

### Working In This Directory

- Dropping `JoinHandle` must **not** cancel work (smol detaches explicitly).
- `cpu_block` is the shared offload path — binaries and CryptoBuf policy call into it; keep behavior stable.
- Avoid nested parallel encrypt inside already-offloaded work without measuring.
- Smol `block_on` intentionally runs the global executor on exactly two
  participants (caller + one worker). Do not restore one worker per logical
  CPU without tail-latency evidence; shared-queue task migration caused
  10–20ms raw-KCP P999/max spikes on SMT machines.

### Testing Requirements

- kio tests; stress/e2e for offload correctness under load

### Common Patterns

```rust
kio::spawn_task(async move { ... });
let out = kio::cpu_block(|| heavy()).await;
```

## Dependencies

### Internal

- Parent `kio`

### External

- tokio or smol/`async-executor`/`num_cpus`/`event-listener`

<!-- MANUAL: -->
