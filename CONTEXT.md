# kcptun-rs Project Context

> This file is the hot-start cache for AI sessions. Read this FIRST, not the codebase.
> Update after each session. Keep under 200 lines.

## Current Task

- [ ] Fix Config type compatibility with Go kcptun (u64 → i64 for negative-value fields)
- [ ] Add `keyfile` field to Config struct
- [ ] Remove `deny_unknown_fields` (Go silently ignores unknown fields)
- [ ] Align client defaults (sndwnd 128→1024, rcvwnd 512→1024)

## Key File Anchors

| What | File | Lines |
|------|------|-------|
| Client Config struct | `kcptun-client/src/main.rs` | 54-95 |
| Client CLI struct | `kcptun-client/src/main.rs` | 100-257 |
| Client merge() | `kcptun-client/src/main.rs` | 261-328 |
| Client defaults | `kcptun-client/src/main.rs` | 1500-1530 |
| Server Config struct | `kcptun-server/src/main.rs` | 77-115 |
| Server CLI struct | `kcptun-server/src/main.rs` | 120-265 |
| Server merge() | `kcptun-server/src/main.rs` | 269-360 |
| Server defaults | `kcptun-server/src/main.rs` | 1561-1595 |
| Config file load | both `main.rs` | ~1458 / ~1527 |
| KCP core | `kcp-rs/src/kcp.rs` | full (1116 lines) |
| Crypto dispatch | `kcrypt-rs/src/crypt.rs` | 460 lines |
| SMUX session | `smux-rs/src/session.rs` | 647 lines |
| Wire formats | `AGENTS.md` root | see "Wire formats" table |

## Verified Constraints (DO NOT BREAK)

1. **Wire compatibility with Go kcptun/kcp-go v5 is the hard constraint**
2. KCP segment: 24B LE header `conv|cmd|frg|wnd|ts|sn|una|len`
3. SMUX frame: 8B `ver|cmd|length(2LE)|stream_id(4LE)`
4. CFB crypto: `[nonce 16B][CRC32 4B][payload]`, fixed IV `GO_CFB_IV`
5. AES-GCM: `[nonce 12B][ciphertext+tag 16B]`
6. FEC header: 6B `seqid(4)+type(2)`, types `0x00f1`/`0x00f2`
7. PBKDF2-HMAC-SHA1, salt `b"kcp-go"`, 32-byte key
8. Snappy is session-level (not per-stream)
9. tokio and smol are mutually exclusive
10. `vendor/` must not be edited by hand (`make vendor` regenerates)

## Config Compatibility Analysis (2026-07-31)

### Type mismatches (Go uses `int`, Rust uses unsigned)

**High risk** (Go configs commonly use `-1` as sentinel):

| Field | Rust type | Should be | Go `-1` meaning |
|-------|-----------|-----------|-----------------|
| `keepalive` | `Option<u64>` | `Option<i64>` | disable heartbeat |
| `autoexpire` | `Option<u64>` | `Option<i64>` | disable auto-expire |
| `scavengettl` | `Option<u64>` | `Option<i64>` | disable scavenger |
| `closewait` | `Option<u64>` | `Option<i64>` | disable close wait |
| `snmpperiod` | `Option<u64>` | `Option<i64>` | disable SNMP log |

**Medium risk** (all `Option<u32>`, Go uses `int`):

`conn`, `mtu`, `sndwnd`, `rcvwnd`, `datashard`, `parityshard`, `dscp`,
`nodelay`, `interval`, `resend`, `nc`, `sockbuf`, `ratelimit`

**Low risk** (`Option<u8>` / `Option<usize>`):

`smuxver`, `smuxbuf`, `streambuf`, `framesize`

### Missing field

- `keyfile`: Go supports loading key from file; Rust Config lacks this field

### Default value differences

| Field | Go default | Rust client | Rust server |
|-------|-----------|-------------|-------------|
| `sndwnd` | 1024 | **128** | 1024 |
| `rcvwnd` | 1024 | **512** | 1024 |
| `closewait` | — | 0 | **30** |

### `deny_unknown_fields` incompatibility

Go's `json.Unmarshal` silently ignores unknown fields.
Rust's `#[serde(deny_unknown_fields)]` rejects them.
Any Go config with extra fields will fail to parse in Rust.

## Session Log

- 2026-07-31: Analyzed config compatibility; identified type mismatches, missing keyfile, deny_unknown_fields issue, default differences. No code changes made yet.

## References (local snapshots)

> To create: save Go kcptun source files to `refs/` directory

- `refs/` — not yet created. Next session: fetch Go Config struct and save locally.
