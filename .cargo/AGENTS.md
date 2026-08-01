<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-01 (vendoring removed) -->

# .cargo

## Purpose

Cargo build configuration: target-specific rustflags. Dependencies are fetched from crates.io per build platform (not vendored).

## Key Files

| File | Description |
|------|-------------|
| `config.toml` | aarch64 `rustflags = ["--cfg", "aes_armv8"]` for Apple Darwin + Linux GNU |

## Subdirectories

None.

## For AI Agents

### Working In This Directory

- **Keep** aarch64 `aes_armv8` cfg — without it AES falls back to soft fixslice (major perf cliff).
- Dependencies are pulled from crates.io at build time; each platform resolves its own appropriate libs. Users need network (or a crates.io mirror) for a fresh build.

### Testing Requirements

- `cargo build --workspace` / `make build`

### Common Patterns

- (none — vendoring removed)

## Dependencies

### Internal

- None

### External

- Cargo / rustc

<!-- MANUAL: -->
