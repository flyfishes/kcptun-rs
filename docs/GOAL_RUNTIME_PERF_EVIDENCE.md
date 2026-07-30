# Goal: runtime-aware perf to beat Go — evidence log

| Field | Value |
|-------|-------|
| Date | 2026-07-30 |
| HEAD | `479b5e8` (parallel encrypt gated by cipher cost) |
| Discipline | **find → confirm → only then change code** |
| Unproven WIP | `git stash`: OffloadProfile (stashed; **not** landed) |

## 0. Success criteria (from objective)

1. Prefer **runtime-specific** optimization (tokio vs smol) where evidence supports it.
2. `bench_rust_vs_go.py` results **as far above Go as practical**.
3. Use **flamegraphs / pprof + code** analysis.
4. **Never** change production code on unconfirmed hypotheses.

## 1. What is already true on clean HEAD (no OffloadProfile)

From `bench_results.json` at investigation start (after parallel gate):

- **Most ciphers already beat Go** on both runtimes (sm4/cast5/twofish/blowfish/3des/xtea/aes often 1.1–5×).
- Residual “lose to Go” cells (either runtime &lt; 0.9× in that JSON):

| Case | tokio/Go | smol/Go | Notes |
|------|----------|---------|-------|
| xor/no-comp | 0.85 | 0.84 | small gap |
| none/no-comp | 0.89 | **1.15** | smol already wins |
| null/no-comp | **1.05** | 0.86 | smol only |
| tea/comp | **1.17** | 0.90 | smol borderline |

User’s earlier paste (`none/comp` 0.53×, `3des/no-comp` 0.28×) was **during broken/over-eager parallel**; **not** the current clean HEAD baseline.

## 2. Controlled re-measure (clean release bins @ `479b5e8`)

### 2.1 Single-conn 2 MiB random (not the official harness)

| | xor nocomp | none nocomp | null nocomp |
|--|------------|-------------|-------------|
| Go med | 49.9 | 59.1 | 55.1 |
| tokio | **70.9** | **70.2** | **69.9** |
| smol | **72.1** | **65.3** | **72.5** |

→ Under this load, Rust **already beats Go** on the “gap” ciphers.

### 2.2 Multi-conn matching `bench_rust_vs_go.py` defaults

`conn=10`, `size=1MiB`, `mode=fast`, `sndwnd/rcvwnd=2048`, 3 runs median:

| Case | Go | tokio (×Go) | smol (×Go) | best |
|------|-----|-------------|------------|------|
| xor/nocomp | 35.4 | **48.1 (1.36×)** | **62.4 (1.77×)** | 1.77× |
| none/nocomp | 24.0 | **52.9 (2.20×)** | **60.2 (2.50×)** | 2.50× |
| null/nocomp | 50.3 | **70.5 (1.40×)** | **70.2 (1.40×)** | 1.40× |
| aes-128-gcm/nocomp | 73.0 | 67.2 (**0.92×**) | 69.5 (**0.95×**) | **0.95×** |
| xtea/nocomp | 21.0 | 19.3 (0.92×) | **27.5 (1.31×)** | 1.31× |

**Confirmed remaining systematic gap under multi-conn:** primarily **`aes-128-gcm/no-comp` (~0.92–0.95× Go)**.  
xor/none/null “losses” in older JSON are **not stable** under re-measure (noise / prior code).

### 2.3 SNMP (GCM multi-conn)

`/tmp/snmp-gcm-evidence.csv.rustobs` (server+client interleaved):

- `EncryptInline` ≫ `EncryptOffload` (e.g. 1370–3068 inline vs ~220–240 offload) → **r_off low (~6–15%)**.
- Go-compatible CSV showed **non-zero RetransSegs** on one side → part of thr variance is **ARQ**, not only crypto µs.

## 3. Flamegraph / pprof status

### 3.1 Tooling limitation (macOS + current kpprof)

Fresh continuous-load captures:

- `bench/profiles/goal-load-20260730-141425/` (GCM)
- `bench/profiles/goal-load-20260730-141452/` (xor)

`go tool pprof -top` shows **~99% `tokio::runtime::park::Inner::park`** on the profiled thread. After `-ignore=park|…`, **almost no cipher frames remain**.

Interpretation:

- SIGPROF / pprof HTTP path is **not attributing worker-thread CPU** well in this setup (main thread parks while work runs elsewhere), **or** load did not keep the sampled thread busy.
- Historical July-28 profiles (null/none) after ignore still show **UDP recv + feed_body/KCP** at low absolute %, not a dominant soft-AES story.

**Gate:** Do **not** treat “need OffloadProfile” or “need GCM rewrite” as **pprof-confirmed**. They remain **code-level hypotheses** until worker-thread CPU profiles work (e.g. `samply` on the process, Linux perf, or fix pprof multi-thread sampling).

### 3.2 Script added for future evidence

`bench/profile_under_load.sh` — continuous multi-conn load + curl pprof (better than one-shot `throughput.py` burst). Still hit park-dominated samples today; keep iterating capture method.

## 4. Code-level hypotheses (ranked; confirmation status)

| ID | Hypothesis | Evidence | Confirmed for code change? |
|----|------------|----------|----------------------------|
| H0 | Over-eager `thread::scope` parallel on cheap ciphers hurt tokio | Prior user matrix + fix `479b5e8` (none/xor never parallel) | **Yes — already fixed** |
| H1 | snappy 64KiB offload too high for smol fast+comp | Prior H2 sweep (random payload) | **Yes — already 16KiB** |
| H2 | xor/salsa headerless wire broke Go interop | e2e ConvMismatch + Go sess.go 20B header | **Yes — already fixed** |
| H3 | **GCM no-comp**: serial per-packet `seal_into`, no batch parallel; decrypt `open` allocates | Code inspection `encrypt_batch` AEAD branch + `aes_gcm.rs` `open` → `to_vec` | **Partial** — explains possible gap; **not** pprof-proven; thr gap only ~5–8% multi-conn |
| H4 | Runtime-specific heavy8 offload (smol raise threshold) | Theory + old H1 doc; **current multi-conn xtea smol already 1.31× Go** | **No** — do not land without A/B showing smol xtea/cast5 still loses |
| H5 | xor/none still “slow” vs Go | Re-measure: Rust **faster** | **Falsified** for current HEAD under controlled multi-conn |
| H6 | Bench noise / retransmit dominates small % gaps | SNMP RetransSegs; high run-to-run variance | **Plausible** — re-run official matrix before any more threshold thrashing |

## 5. Unproven code deliberately **not** merged

Stash message: `wip: unproven OffloadProfile (do not land without pprof evidence)`

Contents (for later):

- `kio::RuntimeKind` / `runtime_kind()`
- `kcp_rs::OffloadProfile` + smol-heavier heavy8 gates
- `set_offload_profile` in client/server `main`

**Why not merge:** H4 not confirmed; risk of silent thr regression on tokio heavy paths; unit tests with global atomic profile are order-sensitive.

## 6. Next actions (still evidence-first)

1. **Refresh official matrix** on clean `479b5e8` bins:  
   `python3 bench_rust_vs_go.py --runs 5`  
   Publish to `bench_results.json`; identify cells still &lt; 1.0× Go with high confidence.
2. **Fix multi-thread CPU sampling** (samply record of release binary under `profile_under_load.sh`, or Linux `perf`). Goal: see `seal_into` / `encrypt_cfb` / `cpu_block` **cum%**.
3. If GCM remains the only stable loser and samply shows seal/open dominant:  
   - A/B **only** AEAD path (e.g. reduce alloc on `open`, or careful batch sealing) with wire-compatible layout.  
   - **Do not** start with OffloadProfile.
4. If multi-conn xtea/cast5 **smol** re-appears &lt; Go with high retrans / high `EncryptOffload` ratio: **then** unstash OffloadProfile and A/B heavy8 gates with `.rustobs`.

## 7. Theoretical “Rust should beat Go”

Still holds for:

- AES-CFB / heavy software CFB (already true in matrix)
- null/none/xor when scheduling is sane (true under re-measure)

Does **not** automatically hold for:

- GCM if Go’s assembly/GCM pipeline is tighter and Rust pays extra copies per segment
- Loopback multi-conn where **noise &gt; crypto**

## 8. macOS `sample(1)` under GCM multi-conn load (profiling binary)

Artifact: `bench/profiles/goal-sample-gcm-prof/sample.txt`  
Binary: `target/profiling/kcptun-server`, crypt=`aes-128-gcm`, `--nocomp`, 10 conn continuous load.

### 8.1 What dominates CPU (symbolized)

| Observation | Detail |
|-------------|--------|
| Most samples | `pthread_cond_wait` / `kevent` / tokio park (idle workers + main) |
| Largest **non-wait** application path | `KcpServerSession::start_flush_loop` → **`UdpSocket::try_send_to` → `__sendto` (~2048 leaf samples)** |
| KCP | `KCP::flush_with_current`, `KCP::send` appear (tens of samples) |
| **GCM crypto** | **`Aes128GcmCrypt::seal_into` / `encrypt_in_place_detached` / `compute_tag` appear only ~1–2 samples** in the extract |

### 8.2 Confirmed implication for GCM gap

**Hypothesis H3 (“GCM seal is the main bottleneck”) is NOT supported by this sample.**

Under this load shape, beating Go further is more likely about:

1. **UDP send path / batching** (already largest non-wait leaf),
2. **KCP flush/send**,
3. **Idle/scheduling density** (many workers parked — maybe over-subscription or wait-for-ACK),

not about rewriting GCM first.

Code still shows AEAD is serial `seal_into` and `open` allocates — that can matter on other machines/loads, but **this flame/sample does not justify a GCM-first patch**.

## 9. Bottom line (this turn)

| Question | Answer |
|----------|--------|
| Is runtime-specific tuning a good idea in theory? | **Yes** |
| Is it the proven next lever on current HEAD? | **No** — multi-conn remeasure already beats Go on xor/none/null; smol xtea already 1.31× |
| Stable residual gap? | **`aes-128-gcm/no-comp` ~0.92–0.95×** under multi-conn (small) |
| Did sample prove GCM seal is hot? | **No** — `sendto` / flush dominate non-wait CPU |
| Did we change production code for the goal? | **No** (OffloadProfile stashed only) |
| Flamegraph quality | pprof HTTP ≈ park-only; **`sample` on profiling bin is usable** |

## 10. Official `bench_rust_vs_go.py --runs 3` on clean `479b5e8`

Saved: `bench_results.json` (2026-07-30 full matrix).

### 10.1 Headline

| Metric | Value |
|--------|-------|
| Cells where **best(Rust)** ≥ Go | **24 / 26** |
| Clear loss (best &lt; 1.0×) | **`salsa20/no-comp` only** (tokio 0.93×, smol 0.81×, best **0.93×**) |
| Tie | `aes-128/no-comp` best **1.00×** |
| GCM (earlier worry) | **both beat Go** (1.10× / 1.09× no-comp; 1.19× / 1.29× comp) |

### 10.2 Runtime split (official)

| Pattern | Examples | Implication |
|---------|----------|-------------|
| smol ≪ tokio on heavy CFB | xtea/sm4/cast5 no-comp (T/S up to 1.9×) | **smol-specific** heavy path still a real issue for *margin*, even when best&gt;Go |
| smol wins cast5/comp | T 0.68× Go, S 1.25× Go | Don’t assume “smol always worse on heavy” |
| Cheap paths mostly fine | none/xor/null | Prior “lose” cells were noise or pre-fix code |

### 10.3 salsa20/no-comp — sample (Rust profiling bin)

Artifact: `bench/profiles/goal-sample-salsa20-nocomp/sample.txt`

Keyword-weighted hits (call-graph line counts):

| Rank | Symbol family | Relative weight |
|------|---------------|-----------------|
| 1 | `start_flush_loop` → **`try_send_to` / `__sendto`** | ~20k |
| 2 | **`Salsa20Crypt::encrypt`** | ~5k |
| 3 | UDP recv batch | ~3k |
| 4 | `encrypt_batch` / `encrypt_batch_into` | ~3k |
| 5 | `crc32fast::hash` | ~400 |

**Confirmed:** On salsa20 nocomp under multi-conn load, **UDP send dominates**; **Salsa20 encrypt is real #2** (unlike GCM where seal was almost invisible).  
**Not yet confirmed:** Whether Rust Salsa20 is *slower than Go’s* for the same bytes, or thr gap is mostly **retransmit / bench variance** (controlled multi-conn sometimes showed Rust ≫ Go with huge `RetransSegs`).

### 10.4 salsa20 confirmed + fixed (after evidence)

| Step | Result |
|------|--------|
| Official sole loss | salsa20/no-comp best 0.93× |
| Rust `sample` | `__sendto` #1, **`Salsa20Crypt::encrypt` #2** (~5k vs ~20k send) |
| Go `sample` | `__sendto` + **`salsa.core`** both heavy |
| Microbench (1320B, 200k iters, arm64) | Rust scalar **374 MB/s** vs Go **444 MB/s** (**confirmed slower**) |
| Fix | aarch64 **NEON 4-block** `saltwenty_x4` (same layout as x86 SSE2) |
| Microbench after | Rust **~606 MB/s** (~**1.36× Go**) |
| Multi-conn thr after | go med 36 / tokio **53** / smol **46** |
| e2e | G2R + R2G salsa20 **OK** |

### 10.5 Still open (not blocking “beat Go” matrix)

1. **smol heavy CFB margin** (xtea/sm4 no-comp T≫S): optional OffloadProfile A/B with `.rustobs`.
2. **UDP `sendto` batching**: still top non-wait cost on multi-conn; helps all ciphers’ absolute thr, not just vs Go.
3. **OffloadProfile stash**: still optional; 24/26 already win without it; salsa20 fixed without runtime split.


## 11. OffloadProfile landed (after A/B confirmation)

| Evidence | Result |
|----------|--------|
| smol xtea env A/B | baseline r_off=1.0 med~17; raised 4/2KiB r_off=0.14 **+19.6%** |
| tokio same raise | thr **regressed** |
| Code | `OffloadProfile::{Tokio,Smol}` + `kio::runtime_kind()` at main |
| Defaults | tokio heavy8 1/512; smol **4/2048**; decrypt CFB 512 vs 1024 |
| Post-default verify | smol xtea med 22.6 vs go 19.0 (1.19×), r_off≈0.15; sm4 smol 5.2× Go |

**Not** a substitute for Salsa20 NEON (already fixed). Complements runtime-specific scheduling.
