//! kcptun-client -- KCP-based TCP stream accelerator.
//!
//! A Rust port of the Go kcptun client.
//! Listens locally, forwards connections over KCP/UDP multiplexed via SMUX.

#![allow(
    clippy::collapsible_match,
    clippy::question_mark,
    clippy::explicit_auto_deref
)]

#[cfg(not(feature = "pprof"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "pprof"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "pprof")]
#[global_allocator]
static GLOBAL: kpprof::ProfilingAllocator = kpprof::ProfilingAllocator::new();

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyContext, Result};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use log::{debug, error, info, trace, warn};
use parking_lot::Mutex;
use serde::Deserialize;

use kcp_rs::{fec_kcp_from_recovered, FecDecoder, FecEncoder, KCP};
#[cfg(feature = "qpp")]
use kcptun_common::QPPPort;
use kcptun_common::{apply_mode, derive_key, pipe, snmp_logger, SnappyStreamDecoder};

// ─── Constants ──────────────────────────────────────────────────────────────────

/// Default KCP conversation ID.
const DEFAULT_CONV: u32 = 0xDEADBEEF;

/// Maximum UDP datagram size.
const MAX_DATAGRAM: usize = 2048;

/// How often the KCP update loop fires (milliseconds).
const KCP_UPDATE_INTERVAL_MS: u64 = 2;

// ─── Config (JSON config file support) ─────────────────────────────────────────-

/// Configuration struct matching the kcptun JSON config format.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub localaddr: Option<String>,
    pub remoteaddr: Option<String>,
    pub key: Option<String>,
    pub crypt: Option<String>,
    pub mode: Option<String>,
    pub conn: Option<u32>,
    pub autoexpire: Option<u64>,
    pub scavengettl: Option<u64>,
    pub mtu: Option<u32>,
    pub ratelimit: Option<u32>,
    pub sndwnd: Option<u32>,
    pub rcvwnd: Option<u32>,
    pub datashard: Option<u32>,
    pub parityshard: Option<u32>,
    pub dscp: Option<u32>,
    pub nocomp: Option<bool>,
    pub acknodelay: Option<bool>,
    pub nodelay: Option<u32>,
    pub interval: Option<u32>,
    pub resend: Option<u32>,
    pub nc: Option<u32>,
    pub sockbuf: Option<u32>,
    pub smuxver: Option<u8>,
    pub smuxbuf: Option<usize>,
    pub streambuf: Option<usize>,
    pub framesize: Option<usize>,
    pub keepalive: Option<u64>,
    pub closewait: Option<u64>,
    pub snmplog: Option<String>,
    pub snmpperiod: Option<u64>,
    pub log: Option<String>,
    pub quiet: Option<bool>,
    pub tcp: Option<bool>,
    pub pprof: Option<String>,
    #[cfg(feature = "qpp")]
    pub qpp: Option<bool>,
    #[cfg(feature = "qpp")]
    pub qppcount: Option<u16>,
}

// ─── CLI Args ───────────────────────────────────────────────────────────────────

/// kcptun client -- accelerate TCP over KCP.
#[derive(Debug, Parser)]
#[command(name = "kcptun-client", about, version, disable_version_flag = true)]
pub struct Cli {
    /// Local listening address.
    #[arg(short = 'l', long)]
    pub localaddr: Option<String>,

    /// Remote server address.
    #[arg(short = 'r', long)]
    pub remoteaddr: Option<String>,

    /// Pre-shared secret between client and server.
    #[arg(short, long, default_value = "it's a secrect", env = "KCPTUN_KEY")]
    pub key: Option<String>,

    /// Encryption method: null, none, xor, aes, aes-128, aes-192, aes-256,
    /// sm4, tea, xtea, salsa20, blowfish, twofish, cast5, 3des.
    #[arg(long, default_value = "aes")]
    pub crypt: Option<String>,

    /// Protocol mode: normal, fast, fast2, fast3.
    #[arg(short, long, default_value = "fast")]
    pub mode: Option<String>,

    /// Number of UDP connections to use.
    #[arg(long)]
    pub conn: Option<u32>,

    /// Auto-expire connections after N seconds of inactivity.
    #[arg(long)]
    pub autoexpire: Option<u64>,

    /// Scavenge TTL in seconds for expired connections.
    #[arg(long)]
    pub scavengettl: Option<u64>,

    /// MTU value.
    #[arg(long)]
    pub mtu: Option<u32>,

    /// Rate limit in bytes per second per connection (0 = disabled).
    #[arg(long, default_value_t = 0)]
    pub ratelimit: u32,

    /// Send window size.
    #[arg(long)]
    pub sndwnd: Option<u32>,

    /// Receive window size.
    #[arg(long)]
    pub rcvwnd: Option<u32>,

    /// FEC data shards.
    #[arg(long)]
    pub datashard: Option<u32>,

    /// FEC parity shards.
    #[arg(long)]
    pub parityshard: Option<u32>,

    /// DSCP value for IP packets.
    #[arg(long)]
    pub dscp: Option<u32>,

    /// Disable compression.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub nocomp: bool,

    /// Enable ACK nodelay.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub acknodelay: bool,

    /// Enable KCP nodelay.
    #[arg(long)]
    pub nodelay: Option<u32>,

    /// KCP update interval in ms.
    #[arg(long)]
    pub interval: Option<u32>,

    /// KCP fast resend threshold.
    #[arg(long)]
    pub resend: Option<u32>,

    /// KCP no congestion control flag.
    #[arg(long)]
    pub nc: Option<u32>,

    /// Socket buffer size in bytes.
    #[arg(long)]
    pub sockbuf: Option<u32>,

    /// SMUX protocol version (1 or 2).
    #[arg(long)]
    pub smuxver: Option<u8>,

    /// SMUX receive buffer size.
    #[arg(long)]
    pub smuxbuf: Option<usize>,

    /// SMUX stream buffer size.
    #[arg(long)]
    pub streambuf: Option<usize>,

    /// SMUX max frame size.
    #[arg(long)]
    pub framesize: Option<usize>,

    /// SMUX keepalive interval in seconds.
    #[arg(long)]
    pub keepalive: Option<u64>,

    /// Close wait timeout in seconds.
    #[arg(long)]
    pub closewait: Option<u64>,

    /// SNMP log file path.
    #[arg(long)]
    pub snmplog: Option<String>,

    /// SNMP logging period in seconds.
    #[arg(long)]
    pub snmpperiod: Option<u64>,

    /// Log file path.
    #[arg(long)]
    pub log: Option<String>,

    /// Suppress log output.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub quiet: bool,

    /// Use TCP instead of UDP for the underlying transport.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub tcp: bool,

    /// Enable pprof HTTP server on the given address.
    #[arg(long)]
    pub pprof: Option<String>,

    /// Enable QPP encryption.
    #[cfg(feature = "qpp")]
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub qpp: bool,

    /// QPP pad count (should be prime).
    #[cfg(feature = "qpp")]
    #[arg(long)]
    pub qppcount: Option<u16>,

    /// Path to JSON config file.
    #[arg(short = 'c', long)]
    pub c: Option<String>,

    /// Print version and exit (Go-compatible: `-v` / `--version`).
    #[arg(short = 'v', long = "version", action = clap::ArgAction::SetTrue, default_value_t = false)]
    pub version_flag: bool,
}

impl Cli {
    /// Merge CLI args with config file, CLI taking precedence.
    fn merge(cli: Self, cfg: Config) -> Self {
        Cli {
            localaddr: cli.localaddr.or(cfg.localaddr),
            remoteaddr: cli.remoteaddr.or(cfg.remoteaddr),
            key: cli.key.or(cfg.key),
            crypt: cli.crypt.or(cfg.crypt),
            mode: cli.mode.or(cfg.mode),
            conn: cli.conn.or(cfg.conn),
            autoexpire: cli.autoexpire.or(cfg.autoexpire),
            scavengettl: cli.scavengettl.or(cfg.scavengettl),
            mtu: cli.mtu.or(cfg.mtu),
            ratelimit: if cli.ratelimit != 0 {
                cli.ratelimit
            } else {
                cfg.ratelimit.unwrap_or(0)
            },
            sndwnd: cli.sndwnd.or(cfg.sndwnd),
            rcvwnd: cli.rcvwnd.or(cfg.rcvwnd),
            datashard: cli.datashard.or(cfg.datashard),
            parityshard: cli.parityshard.or(cfg.parityshard),
            dscp: cli.dscp.or(cfg.dscp),
            nocomp: if cli.nocomp {
                true
            } else {
                cfg.nocomp.unwrap_or(false)
            },
            acknodelay: if cli.acknodelay {
                true
            } else {
                cfg.acknodelay.unwrap_or(false)
            },
            nodelay: cli.nodelay.or(cfg.nodelay),
            interval: cli.interval.or(cfg.interval),
            resend: cli.resend.or(cfg.resend),
            nc: cli.nc.or(cfg.nc),
            sockbuf: cli.sockbuf.or(cfg.sockbuf),
            smuxver: cli.smuxver.or(cfg.smuxver),
            smuxbuf: cli.smuxbuf.or(cfg.smuxbuf),
            streambuf: cli.streambuf.or(cfg.streambuf),
            framesize: cli.framesize.or(cfg.framesize),
            keepalive: cli.keepalive.or(cfg.keepalive),
            closewait: cli.closewait.or(cfg.closewait),
            snmplog: cli.snmplog.or(cfg.snmplog),
            snmpperiod: cli.snmpperiod.or(cfg.snmpperiod),
            log: cli.log.or(cfg.log),
            quiet: if cli.quiet {
                true
            } else {
                cfg.quiet.unwrap_or(false)
            },
            tcp: if cli.tcp {
                true
            } else {
                cfg.tcp.unwrap_or(false)
            },
            pprof: cli.pprof.or(cfg.pprof),
            #[cfg(feature = "qpp")]
            qpp: if cli.qpp {
                true
            } else {
                cfg.qpp.unwrap_or(false)
            },
            #[cfg(feature = "qpp")]
            qppcount: cli.qppcount.or(cfg.qppcount),
            c: cli.c,            // CLI --c/-c flag only, not in Config struct
            version_flag: false, // never from config file
        }
    }
}

// ─── Log Rotation ──────────────────────────────────────────────────────────────

/// Rotate log file if it exceeds max_size bytes. Keeps up to 5 rotated copies.
fn rotate_log(log_path: &str, max_size: u64) {
    if let Ok(meta) = std::fs::metadata(log_path) {
        if meta.len() > max_size {
            for i in (1..5).rev() {
                let old = format!("{}.{}", log_path, i);
                let new = format!("{}.{}", log_path, i + 1);
                let _ = std::fs::rename(&old, &new);
            }
            let _ = std::fs::rename(log_path, format!("{}.1", log_path));
        }
    }
}

// ─── Key Derivation ─────────────────────────────────────────────────────────────
// ─── Mode Profiles ──────────────────────────────────────────────────────────────

// (parse_multi_port is now shared via kcptun_common::parse_multi_port)

// ─── KCP Connection ─────────────────────────────────────────────────────────────

/// KCP + SMUX session parameters shared by dial/reconnect paths.
#[derive(Clone, Debug)]
struct SessionConfig {
    crypt: String,
    mode: String,
    mtu: u32,
    sndwnd: u32,
    rcvwnd: u32,
    datashard: u32,
    parityshard: u32,
    acknodelay: bool,
    nodelay: u32,
    interval: u32,
    resend: u32,
    nc: u32,
    smuxver: u8,
    smuxbuf: usize,
    streambuf: usize,
    framesize: usize,
    keepalive: u64,
    nocomp: bool,
    ratelimit: u32,
}

/// A single KCP connection that carries an SMUX session.
struct KcpConn {
    /// Datagram socket for network I/O (UDP or TCP raw transport).
    socket: Arc<kio::DatagramSocket>,
    /// KCP state machine (shared between tasks).
    kcp: Arc<Mutex<KCP>>,
    /// SMUX session multiplexing streams over KCP.
    smux: Arc<smux_rs::Session>,
    /// Task handles for background loops.
    _handles: Vec<kio::JoinHandle<()>>,
    /// Session cipher — concrete [`kcrypt_rs::CryptEngine`] (enum match, no
    /// `dyn` vtable on encrypt/decrypt). Shared without Mutex: encrypt/decrypt
    /// take `&self` and are stateless after construction. AEAD (GCM) is the
    /// `Aes128Gcm` variant; use `crypt.as_aead()` / `crypt.is_aead()`.
    crypt: Arc<kcrypt_rs::CryptEngine>,
    /// Whether CFB-style crypto headers (nonce+CRC32) or AEAD framing apply.
    /// False only for `"null"` (raw payload, no header). True for `"none"`
    /// (header + identity cipher), all CFB methods, and AEAD (AEAD path
    /// branches via `crypt.is_aead()` before CFB packing).
    has_encryption: bool,
    /// Raw KCP segments collected by the output callback during flush().
    /// Drained and encrypted+sent AFTER the KCP lock is released,
    /// to avoid starving the UDP reader task.
    ///
    /// Each entry is a `Bytes` (reference-counted slice of the KCP output
    /// buffer) handed directly by the output callback — no per-packet
    /// `Vec` alloc + `extend_from_slice` copy (R2: output Bytes pipeline).
    raw_packets: Arc<parking_lot::Mutex<Vec<bytes::Bytes>>>,
    /// Shared atomic counter of KCP wait_send, updated by the flush loop.
    /// Read by SmuxIo::poll_write for backpressure.
    wait_send: Arc<AtomicUsize>,
    /// KCP send window size, used for backpressure threshold.
    snd_wnd: usize,
    /// Notify for waking up writers blocked by backpressure.
    /// The flush loop calls notify_waiters() after each flush cycle,
    /// so blocked writers resume immediately when the window drains,
    /// instead of waiting for a 10ms sleep timeout.
    write_notify: Arc<kio::Notify>,
    /// Disable snappy compression at the SMUX session level.
    /// Must match Go kcptun's --nocomp flag for interop.
    nocomp: bool,
    /// Whether ACK nodelay is enabled (passed to kcp.input()).
    acknodelay: bool,
    /// Last activity time for auto-expire (monotonic ms).
    last_activity: Arc<AtomicU64>,
    /// Persistent Snappy framing encoder shared between send_frame and Task 2.
    /// snap::write::FrameEncoder uses CRC32C (Castagnoli) matching Go's golang/snappy.
    /// The stream identifier is written once by the first write; subsequent writes
    /// continue the same snappy stream without re-emitting the header.
    compressor: Arc<parking_lot::Mutex<snap::write::FrameEncoder<Vec<u8>>>>,
    /// Reusable encryption buffer with counter-based nonce (flush / data path).
    /// Eliminates per-packet vec![] allocation and rand::thread_rng() calls.
    /// All KCP segments produced by `KCP::flush()` are encrypted through this buffer.
    crypto_buf: Arc<parking_lot::Mutex<kcp_rs::CryptoBuf>>,
    /// Separate CryptoBuf for UDP-reader ACK encrypt — avoids lock contention
    /// with the flush-loop batch encrypt on the shared crypto_buf.
    ///
    /// Rationale:
    /// - Flush loop (Task 2) may hold `crypto_buf` while preparing/encrypting dozens
    ///   of segments under compression. Concurrent ACK encrypt on the same buffer
    ///   would serialize behind that work and delay ACKs, causing retransmits.
    /// - Using a distinct buffer also gives ACKs their own nonce counter range,
    ///   so even if both paths interleave, nonces never collide.
    ack_crypto_buf: Arc<parking_lot::Mutex<kcp_rs::CryptoBuf>>,
    /// Notify for waking up the flush loop immediately when SMUX streams
    /// have new data to send. Eliminates the 0~10ms wait of the fixed
    /// sleep interval.
    flush_notify: Arc<kio::Notify>,
    /// Session-layer FEC encoder (Go fecEncoder). None when ds/ps == 0.
    fec_encoder: Option<Arc<parking_lot::Mutex<FecEncoder>>>,
    /// Session-layer FEC decoder (Go fecDecoder).
    fec_decoder: Option<Arc<parking_lot::Mutex<FecDecoder>>>,
    /// Set when KCP dead_link or SMUX keepalive timeout is detected.
    /// Background loops exit once this is true; accept path reconnects.
    dead: Arc<AtomicBool>,
    /// Per-connection rate limiter (token bucket). 0 = unlimited.
    rate_limiter: Arc<kcptun_common::RateLimiter>,
}

impl KcpConn {
    /// Create a new KCP connection to the given remote address.
    async fn new(
        _remote_addr: SocketAddr,
        key: &[u8; 32],
        cfg: &SessionConfig,
        socket: Arc<kio::DatagramSocket>,
    ) -> Result<Self> {
        let crypt = cfg.crypt.as_str();
        let mode = cfg.mode.as_str();
        let mtu = cfg.mtu;
        let sndwnd = cfg.sndwnd;
        let rcvwnd = cfg.rcvwnd;
        let datashard = cfg.datashard;
        let parityshard = cfg.parityshard;
        let acknodelay = cfg.acknodelay;
        let nodelay = cfg.nodelay;
        let interval = cfg.interval;
        let resend = cfg.resend;
        let nc = cfg.nc;
        let smuxver = cfg.smuxver;
        let smuxbuf = cfg.smuxbuf;
        let streambuf = cfg.streambuf;
        let framesize = cfg.framesize;
        let keepalive = cfg.keepalive;
        let nocomp = cfg.nocomp;

        // Single CryptEngine for CFB + AEAD (no separate Arc<dyn AeadCrypt>).
        let (engine, _) = kcrypt_rs::CryptEngine::select(crypt, &key[..]);
        let crypt_state = Arc::new(engine);

        // Create KCP instance with output callback that collects raw KCP segments.
        //
        // CRITICAL: The output callback runs INSIDE the KCP lock (during flush()).
        // If it does encryption + UDP send + tokio::spawn per packet, it can take
        // 10+ ms with 240+ segments, starving the UDP reader that processes ACKs.
        //
        // Fix: The callback just pushes raw KCP data (as `Bytes` — the KCP
        // output buffer's reference-counted slice) to a shared Vec (nearly
        // instant, zero-copy). After flush() returns and the KCP lock is
        // released, the caller drains the Vec and does encryption + UDP send
        // outside the lock.
        let raw_packets = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<bytes::Bytes>::new()));
        let raw_packets_cb = raw_packets.clone();
        let has_encryption = crypt != "null";

        let mut kcp = KCP::new(
            DEFAULT_CONV,
            0,
            Box::new(move |data: bytes::Bytes| {
                raw_packets_cb.lock().push(data);
            }),
        );

        // Configure KCP
        kcp.set_mtu(mtu);
        kcp.set_snd_wnd(sndwnd);
        kcp.set_rcv_wnd(rcvwnd);
        // Session-layer FEC (Go newUDPSession). header_offset=0: crypto wraps whole FEC frame.
        let fec_encoder = if datashard > 0 && parityshard > 0 {
            FecEncoder::new(datashard as usize, parityshard as usize, 0)
                .map(|e| Arc::new(parking_lot::Mutex::new(e)))
        } else {
            None
        };
        let fec_decoder = if datashard > 0 && parityshard > 0 {
            FecDecoder::new(datashard as usize, parityshard as usize)
                .map(|d| Arc::new(parking_lot::Mutex::new(d)))
        } else {
            None
        };
        if acknodelay {
            // kcp.set_ack_nodelay(true); // removed: pass ack_nodelay to input() instead
        }
        kcp.set_stream_mode(true);

        // Apply mode profile or explicit parameters
        // Go semantics: known modes (normal/fast/fast2/fast3) override hidden flags.
        // "manual" or unknown modes use the explicit nodelay/interval/resend/nc values.
        match mode {
            "normal" | "fast" | "fast2" | "fast3" => {
                apply_mode(&mut kcp, mode);
            }
            _ => {
                // manual mode or unknown: use explicit values (from CLI or config)
                let n = nodelay;
                let i = if interval >= 10 { interval } else { 40 };
                kcp.set_nodelay(n, i, resend, nc);
            }
        }

        let kcp = Arc::new(Mutex::new(kcp));

        // Create SMUX config
        let smux_cfg = smux_rs::Config {
            version: smuxver,
            max_receive_buffer: smuxbuf,
            max_stream_buffer: streambuf,
            max_frame_size: framesize,
            keepalive_interval: keepalive,
            keepalive_timeout: if keepalive == 0 {
                0
            } else {
                keepalive.saturating_mul(3).max(1)
            },
        };
        let smux = Arc::new(smux_rs::Session::new_client(&smux_cfg)?);

        let mut conn = KcpConn {
            socket: socket.clone(),
            kcp,
            smux,
            _handles: Vec::new(),
            crypt: crypt_state,
            has_encryption,
            nocomp,
            acknodelay,
            raw_packets,
            wait_send: Arc::new(AtomicUsize::new(0)),
            snd_wnd: sndwnd as usize,
            write_notify: Arc::new(kio::Notify::new()),
            last_activity: Arc::new(AtomicU64::new(kio::mono_ms())),
            compressor: Arc::new(parking_lot::Mutex::new(snap::write::FrameEncoder::new(
                Vec::new(),
            ))),
            crypto_buf: Arc::new(parking_lot::Mutex::new(kcp_rs::CryptoBuf::new(
                DEFAULT_CONV as u64,
            ))),
            // Distinct session_id so ACK nonces never collide with data-path nonces.
            // XOR with a recognizable constant makes it easy to spot in traces.
            ack_crypto_buf: Arc::new(parking_lot::Mutex::new(kcp_rs::CryptoBuf::new(
                (DEFAULT_CONV as u64) ^ 0xA11C_B0FF_u64,
            ))),
            flush_notify: Arc::new(kio::Notify::new()),
            fec_encoder,
            fec_decoder,
            dead: Arc::new(AtomicBool::new(false)),
            rate_limiter: Arc::new(kcptun_common::RateLimiter::new(cfg.ratelimit)),
        };

        conn.start_background_loops();
        Ok(conn)
    }

    /// Start the background processing loops for this connection.
    ///
    /// Two tasks run in the background:
    /// 1. UDP reader + KCP input/output — reads datagrams, feeds them through
    ///    KCP, extracts user data (KCP recv) and dispatches it to the SMUX
    ///    session.
    /// 2. SMUX flush + KCP update — drains SMUX stream send buffers, wraps
    ///    them as SMUX Data frames, sends them through KCP, then advances the
    ///    KCP timer for retransmission.
    fn start_background_loops(&mut self) {
        let kcp = self.kcp.clone();
        let socket = self.socket.clone();
        let smux = self.smux.clone();
        let crypt = self.crypt.clone();
        let raw_packets = self.raw_packets.clone();
        let has_encryption = self.has_encryption;
        let last_activity = self.last_activity.clone();

        let nocomp = self.nocomp;
        let acknodelay1 = self.acknodelay;

        // ── Task 1: UDP reader + decrypt + KCP input → decompress → SMUX recv ──
        let kcp1 = kcp.clone();
        let smux1 = smux.clone();
        let crypt1 = crypt.clone();
        let raw_packets1 = raw_packets.clone();
        let socket1 = socket.clone();
        let has_encryption1 = has_encryption;
        let has_aead1 = crypt1.is_aead();
        let crypto_buf1 = self.ack_crypto_buf.clone();
        let write_notify1 = self.write_notify.clone();
        let wait_send1 = self.wait_send.clone();
        let flush_notify1 = self.flush_notify.clone();
        let fec_decoder1 = self.fec_decoder.clone();
        let fec_encoder1 = self.fec_encoder.clone();
        let dead1 = self.dead.clone();
        let h1 = kio::spawn_task(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            // Persistent Snappy framing decoder.
            // Handles Go kcptun's snappy.NewBufferedWriter framing format.
            // Use Arc<Mutex> so that if we offload inbound heavy work (decrypt + feed)
            // the decompress state can be accessed from the cpu_block task.
            let snappy_dec: Option<Arc<parking_lot::Mutex<SnappyStreamDecoder>>> = if !nocomp {
                Some(Arc::new(
                    parking_lot::Mutex::new(SnappyStreamDecoder::new()),
                ))
            } else {
                None
            };
            loop {
                if dead1.load(Ordering::Acquire) || smux1.is_closed() {
                    break;
                }
                let mut n = match socket1.recv(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    Ok(_) => continue,
                    Err(e) => {
                        error!("UDP recv error: {}", e);
                        kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_errs, 1);
                        kio::sleep_ms(100).await;
                        continue;
                    }
                };
                // Process this datagram and any further ready ones (P1.3).
                loop {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_pkts, 1);
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_bytes, n as u64);
                    // ── Decrypt and strip header (in-place on recv buf for CFB/null) ──
                    const FEC_HDR: usize = 8;

                    // AEAD still owns a Vec; CFB/null use slices of `buf` until
                    // KCP::input finishes (same task, before next recv/try_recv).
                    let aead_plain: Option<Vec<u8>> = if has_aead1 {
                        match crypt1.as_aead().unwrap().open(&buf[..n]) {
                            Ok(plain) => Some(plain),
                            Err(_) => {
                                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                                None
                            }
                        }
                    } else {
                        None
                    };
                    if has_aead1 && aead_plain.is_none() {
                        match socket1.try_recv(&mut buf) {
                            Ok(m) if m > 0 => {
                                n = m;
                                continue;
                            }
                            _ => break,
                        }
                    }

                    // Body offset into buf after successful in-place decrypt.
                    let mut cfb_body_off: Option<usize> = None;
                    if has_encryption1 && !has_aead1 {
                    // All BlockCrypt ciphers (including xor/salsa20) use the standard
                    // 20-byte CFB header: [nonce 16B][CRC32 4B][KCP segment]
                    match kcp_rs::decrypt_cfb_in_place(
                    &mut buf[..n],
                    crypt1.as_ref(),
                    false,
                ) {
                    Ok(body) => {
                        cfb_body_off = Some(n - body.len());
                    }
                    Err(_) => {
                        kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                        match socket1.try_recv(&mut buf) {
                            Ok(m) if m > 0 => {
                                n = m;
                                continue;
                            }
                            _ => break,
                        }
                    }
                }
            }

                    // Body view: AEAD owned, CFB payload slice, or null full buf.
                    let input: &[u8] = if let Some(ref plain) = aead_plain {
                        plain.as_slice()
                    } else if let Some(off) = cfb_body_off {
                        &buf[off..n]
                    } else {
                        kcp_rs::inbound_null(&buf[..n])
                    };

                    // Inbound CPU offload is permanently disabled: the decrypt + KCP input + SMUX
                    // processing must stay on the main receive task to maintain strict ordering.
                    // Offloading causes out-of-order KCP input (broken cumulative ACK/UNA),
                    // and SMUX reassembly corruption (md5 mismatches, short reads).

                    // ── FEC handling & KCP input (matching Go's kcpInput) ── (inline path)
                    // Feed KCP with slices only — no intermediate Vec of Bytes.
                    let mut had_input = false;
                    {
                        let mut kcp_guard = kcp1.lock();
                        if let Some(ref dec) = fec_decoder1 {
                            if input.len() >= 6 {
                                let fec_flag = u16::from_le_bytes(input[4..6].try_into().unwrap());
                                let recovered = {
                                    let mut d = dec.lock();
                                    d.decode(input)
                                };
                                match fec_flag {
                                    0x00f1 => {
                                        if input.len() > FEC_HDR {
                                            if kcp_guard
                                                .input(&input[FEC_HDR..], acknodelay1)
                                                .is_err()
                                            {
                                                kcp_rs::snmp_add(
                                                    &kcp_rs::DEFAULT_SNMP.kcp_in_errors,
                                                    1,
                                                );
                                            }
                                            had_input = true;
                                        }
                                        // recovered = [SIZE 2][KCP…][RS pad]; Go: Input(r[2:sz])
                                        for r in &recovered {
                                            if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                                                if kcp_guard.input(kcp_slice, acknodelay1).is_err()
                                                {
                                                    kcp_rs::snmp_add(
                                                        &kcp_rs::DEFAULT_SNMP.kcp_in_errors,
                                                        1,
                                                    );
                                                }
                                                had_input = true;
                                            }
                                        }
                                    }
                                    0x00f2 => {
                                        for r in &recovered {
                                            if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                                                if kcp_guard.input(kcp_slice, acknodelay1).is_err()
                                                {
                                                    kcp_rs::snmp_add(
                                                        &kcp_rs::DEFAULT_SNMP.kcp_in_errors,
                                                        1,
                                                    );
                                                }
                                                had_input = true;
                                            }
                                        }
                                    }
                                    0x00f3 => {
                                        log::trace!("OOB packet received: {} bytes", input.len());
                                    }
                                    _ => {
                                        if kcp_guard.input(input, acknodelay1).is_err() {
                                            kcp_rs::snmp_add(
                                                &kcp_rs::DEFAULT_SNMP.kcp_in_errors,
                                                1,
                                            );
                                        }
                                        had_input = true;
                                    }
                                }
                            } else if input.len() >= 24 {
                                if kcp_guard.input(input, acknodelay1).is_err() {
                                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.kcp_in_errors, 1);
                                }
                                had_input = true;
                            }
                        } else if input.len() >= 24 {
                            if kcp_guard.input(input, acknodelay1).is_err() {
                                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.kcp_in_errors, 1);
                            }
                            had_input = true;
                        }

                        if had_input {
                            last_activity.store(kio::mono_ms(), Ordering::Relaxed);
                        }

                        // Extract KCP recv data (decompressed on the KCP stream level)
                        while let Ok(d) = kcp_guard.recv_bytes() {
                            if !nocomp {
                                if let Some(ref sd) = snappy_dec {
                                    match sd.lock().feed(&d) {
                                        Ok(decompressed) => {
                                            if !decompressed.is_empty() {
                                                if let Err(e) = smux1.process_data(&decompressed) {
                                                    warn!("SMUX process_data error: {:?}", e);
                                                }
                                                flush_notify1.notify_one();
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Snappy decompress error: {:?}", e);
                                        }
                                    }
                                }
                            } else if let Err(e) = smux1.process_data(&d) {
                                warn!("SMUX process_data error: {:?}", e);
                                flush_notify1.notify_one();
                            } else {
                                flush_notify1.notify_one();
                            }
                        }
                    }

                    // ── Notify writers after ACK processing ──
                    // Go's kcpInput() calls notifyWriteEvent() when waitsnd < snd_wnd
                    // after processing incoming ACKs. This wakes up blocked writers
                    // immediately, rather than waiting for the next flush cycle.
                    {
                        let kcp_guard = kcp1.lock();
                        let ws = kcp_guard.wait_send() as usize;
                        let snd_wnd = kcp_guard.snd_wnd() as usize;
                        // Update the shared wait_send counter immediately so
                        // poll_write sees the current (post-ACK) value instead
                        // of the stale value from the last flush cycle. This
                        // prevents false backpressure from blocking writers.
                        wait_send1.store(ws, Ordering::Relaxed);
                        if ws < snd_wnd {
                            write_notify1.notify_waiters();
                        }
                    }

                    // ── Drain and send ACKs collected during input() ──
                    // FEC-encode then encrypt inline, then batch-send to reduce
                    // per-packet syscall overhead (sendmmsg on Linux).
                    let acks: Vec<bytes::Bytes> = std::mem::take(&mut *raw_packets1.lock());
                    let acks: Vec<bytes::Bytes> = if let Some(ref enc) = fec_encoder1 {
                        let mut e = enc.lock();
                        kcp_rs::fec_expand_packets(&mut e, &acks, 500)
                    } else {
                        acks
                    };
                    // Must use the same wire format as data-path encrypt_batch:
                    // salsa/xor → headerless; AEAD → seal; other CFB → 20B header.
                    // Using encrypt_cfb on salsa/xor corrupts ACKs → retransmit storm
                    // (salsa20/comp collapse).
                    let encrypted: Vec<bytes::Bytes> = if has_aead1 {
                        let aead = crypt1.as_aead().unwrap();
                        let mut cb = crypto_buf1.lock();
                        acks.iter().map(|data| cb.seal_aead(aead, data)).collect()
                    } else if has_encryption1 {
                        let mut cb = crypto_buf1.lock();
                        acks.iter()
                            .map(|data| cb.encrypt_packet(data, crypt1.as_ref()))
                            .collect()
                    } else {
                        // null: Bytes already owns the slice — pass through.
                        acks
                    };
                    // Batch send all encrypted ACKs in one call.
                    // Send directly (not via spawn_task) — on smol backend,
                    // spawned tasks may not be scheduled promptly, causing
                    // ACKs to be delayed and KCP retransmissions to fire.
                    if !encrypted.is_empty() {
                        match socket1.send_batch(&encrypted).await {
                            Ok(()) => {
                                let nbytes: u64 = encrypted.iter().map(|b| b.len() as u64).sum();
                                kcp_rs::snmp_add(
                                    &kcp_rs::DEFAULT_SNMP.out_pkts,
                                    encrypted.len() as u64,
                                );
                                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.out_bytes, nbytes);
                            }
                            Err(e) => error!("UDP send_batch error (ack): {}", e),
                        }
                    }
                    // Non-blocking drain of further ready UDP datagrams.
                    match socket1.try_recv(&mut buf) {
                        Ok(m) if m > 0 => {
                            n = m;
                            continue;
                        }
                        _ => break,
                    }
                } // end ready-packet drain loop
            }
        });
        self._handles.push(h1);

        // ── Task 2b: Decompression is now handled in h1 (before kcp.input()).
        // This task is no longer needed — keep an idle loop for compatibility.
        let h1b = kio::spawn_task(async move {
            loop {
                kio::sleep_ms(3_600_000).await;
            }
        });
        self._handles.push(h1b);

        // ── Task 2: SMUX stream drain + compress → KCP update/flush ──
        let kcp2 = kcp.clone();
        let smux2 = smux.clone();
        let raw_packets2 = raw_packets.clone();
        let nocomp2 = self.nocomp;
        let socket2 = socket.clone();
        let crypt2 = crypt.clone();
        let has_encryption2 = has_encryption;
        let has_aead2 = crypt2.is_aead();
        let wait_send2 = self.wait_send.clone();
        let write_notify2 = self.write_notify.clone();
        let compressor2 = self.compressor.clone();
        let crypto_buf2 = self.crypto_buf.clone();
        let smuxver = self.smux.version();
        let flush_notify2 = self.flush_notify.clone();
        let fec_encoder2 = self.fec_encoder.clone();
        let dead2 = self.dead.clone();
        let rate_limiter2 = self.rate_limiter.clone();
        let h2 = kio::spawn_task(async move {
            let mut next_update: u64 = KCP_UPDATE_INTERVAL_MS;
            // Reused across iterations: single buffer for SMUX frame assembly (P0.3).
            let mut out_buf = bytes::BytesMut::with_capacity(64 * 1024);
            // Throttle Phase 0 health checks — bulk flush every 1-2ms; dead_link
            // / keepalive only need ~100ms resolution (matches Go pingLoop scale).
            let mut health_checks_left: u32 = 0;

            loop {
                // Wait for either the dynamic interval (nearest RTO or
                // default) or an immediate notify from SMUX stream writes.
                let _ = kio::timeout(Duration::from_millis(next_update), flush_notify2.notified())
                    .await;

                // Fresh frame buffer each cycle (NOP + data frames assembled below).
                out_buf.clear();

                // ── Phase 0: dead-link + SMUX keepalive (Go smux pingLoop / kcp-go Die) ──
                // Cheap flags every cycle; KCP lock + keepalive timers at ~100ms.
                if dead2.load(Ordering::Acquire) || smux2.is_closed() {
                    break;
                }
                if health_checks_left == 0 {
                    health_checks_left = 50; // ~100ms at 2ms KCP_UPDATE_INTERVAL_MS
                    {
                        let kcp_dead = kcp2.lock().is_dead();
                        if kcp_dead {
                            error!("KCP dead_link detected — closing SMUX session");
                            smux2.close();
                            dead2.store(true, Ordering::Release);
                            break;
                        }
                    }
                    if smux2.is_keepalive_timeout() {
                        error!("SMUX keepalive timeout — closing session");
                        smux2.close();
                        dead2.store(true, Ordering::Release);
                        break;
                    }
                    if smux2.check_keepalive() {
                        let nop = smux2.keepalive_frame();
                        nop.encode(&mut out_buf);
                        smux2.mark_keepalive_sent();
                        debug!("SMUX: queued NOP keepalive");
                    }
                } else {
                    health_checks_left -= 1;
                }

                // ── Phase 1: Prepare outbound SMUX frames (PSH + FINs + UPDs) ──
                // Unified API. Same zero-copy drain_send_max path, single copy into
                // out_buf, peer window respected, FIN headers encoded for eligible
                // streams (mark only after kcp.send success). Replaces previous
                // manual Phase 1/1a/1c. Reaping stays below as policy.
                const MAX_DRAIN_BYTES: usize = 64 * 1024;
                let fin_ids: Vec<u32> =
                    smux2.prepare_outbound_into(&mut out_buf, MAX_DRAIN_BYTES, smuxver);

                // ── Phase 1b: Reap fully closed + local-closed past linger ──
                // Linger bounds map growth when peer FIN is lost (proxy short-connect leak).
                const STREAM_LINGER_SECS: u64 = 30;
                {
                    let streams = smux2.streams();
                    let mut stream_map = streams.lock();
                    let linger = std::time::Duration::from_secs(STREAM_LINGER_SECS);
                    let to_remove: Vec<u32> = stream_map
                        .iter()
                        .filter(|(_, s)| {
                            if s.is_local_closed() && s.is_remote_closed() && s.is_fin_sent() {
                                return true;
                            }
                            if s.is_local_closed() && s.pending_send() == 0 {
                                if let Some(e) = s.local_closed_elapsed() {
                                    return e >= linger;
                                }
                            }
                            false
                        })
                        .map(|(id, _)| *id)
                        .collect();
                    for id in &to_remove {
                        if let Some(s) = stream_map.remove(id) {
                            s.close();
                        }
                    }
                    if !to_remove.is_empty() {
                        debug!("SMUX: reaped {} closed/stale streams", to_remove.len());
                    }
                    drop(stream_map);
                }

                // ── Phase 2: Snappy compress OUTSIDE KCP lock (P0.4) ──
                // Matches server Phase 3/4 split — keeps ACK path unblocked.
                // Large flushes offload to cpu_block so the reactor can process
                // UDP/ACKs concurrently (esp. smol). Small flushes stay inline.
                //
                // `send_data` is `Option<bytes::Bytes>` — zero-copy reference-
                // counted slice. Both the compress path (Vec→Bytes via into())
                // and the nocomp path (BytesMut→Bytes via freeze()) avoid an
                // extra copy compared to the previous `Vec<u8>` pipeline.
                let send_data: Option<bytes::Bytes> = if out_buf.is_empty() {
                    None
                } else if !nocomp2 {
                    use std::io::Write;
                    let plain = out_buf.split().freeze();
                    let plain_len = plain.len();
                    let compress_fn = {
                        let compressor = compressor2.clone();
                        move || -> bytes::Bytes {
                            let mut enc = compressor.lock();
                            enc.write_all(&plain).ok();
                            enc.flush().ok();
                            // Vec<u8> → Bytes is zero-copy (into_bytes).
                            std::mem::take(enc.get_mut()).into()
                        }
                    };
                    // P0: Always offload snappy to cpu_block when the batch is large
                    // enough. Previously, snappy was kept inline when has_encryption
                    // was true to avoid a "double pool hop" (snappy on pool, then
                    // encrypt on pool). However, running snappy inline blocks the
                    // tokio worker thread, preventing it from processing UDP recv /
                    // ACKs. The throughput cost of blocking the worker far exceeds
                    // the latency cost of a second pool hop.
                    let compressed = if kcp_rs::should_cpu_block_compress(plain_len) {
                        kio::cpu_block(compress_fn).await
                    } else {
                        compress_fn()
                    };
                    if compressed.is_empty() {
                        None
                    } else {
                        Some(compressed)
                    }
                } else {
                    Some(out_buf.split().freeze())
                };

                // ── Phase 3: kcp.send + flush only (KCP lock held briefly) ──
                {
                    let mut kcp_guard = kcp2.lock();
                    let had_outbound = send_data.is_some();

                    if let Some(to_send) = send_data {
                        if !to_send.is_empty() {
                            // Split into chunks of at most (KCP_MAX_FRAG - 1) * MSS
                            // to avoid TooManyFragments error. Matches server behavior.
                            let mss = kcp_guard.mss() as usize;
                            let max_chunk = (kcp_rs::segment::KCP_MAX_FRAG as usize)
                                .saturating_sub(1)
                                .saturating_mul(mss)
                                .max(mss);
                            let mut offset = 0;
                            let mut send_ok = true;
                            while offset < to_send.len() {
                                let end = (offset + max_chunk).min(to_send.len());
                                if let Err(e) = kcp_guard.send(&to_send[offset..end]) {
                                    warn!(
                                        "KCP send error at offset {}/{}: {:?}",
                                        offset,
                                        to_send.len(),
                                        e
                                    );
                                    send_ok = false;
                                    break;
                                }
                                offset = end;
                            }
                            // Only mark FIN after the whole batch was accepted by KCP.
                            if send_ok && !fin_ids.is_empty() {
                                let streams = smux2.streams();
                                let stream_map = streams.lock();
                                for id in &fin_ids {
                                    if let Some(s) = stream_map.get(id) {
                                        s.mark_fin_sent();
                                    }
                                }
                            }
                        }
                    } else if !fin_ids.is_empty() {
                        // FIN-only cycle (no PSH/UPD payload after compress empty path handled above).
                        // fin frames live in send_data when out_buf non-empty; if send_data is None
                        // there was nothing to send — leave fin_sent false for retry.
                    }

                    // Call flush() directly (matching Go's UDPSession.update()
                    // which calls s.kcp.flush() directly, NOT the deprecated
                    // Update() that throttles via ts_flush). This avoids
                    // double-flushing (update() internally calls flush() too).
                    next_update = kcp_guard.flush() as u64;
                    let ws = kcp_guard.wait_send() as usize;
                    // P2.2: data just queued or still in-flight → wake ASAP so
                    // ACKs/retrans and remaining SMUX bytes are not delayed by
                    // the full interval clamp.
                    if had_outbound || ws > 0 {
                        next_update = 1;
                    } else {
                        next_update = next_update.clamp(1, KCP_UPDATE_INTERVAL_MS);
                    }
                    // Update shared wait_send counter for poll_write backpressure
                    wait_send2.store(ws, Ordering::Relaxed);
                    // Wake up any writers blocked by backpressure — they'll
                    // re-check wait_send() and resume if the window drained.
                    write_notify2.notify_waiters();
                }

                // ── Encrypt + send raw KCP packets OUTSIDE the KCP lock ──
                // The output callback (called during flush) just collected
                // raw KCP segments into raw_packets. Now we drain and send
                // them. This allows the UDP reader task to acquire the KCP
                // lock concurrently to process incoming ACKs.
                let packets: Vec<bytes::Bytes> = {
                    let mut g = raw_packets2.lock();
                    let n = g.len();
                    let cap = g.capacity();
                    let p = std::mem::take(&mut *g);
                    // Keep capacity on the shared buffer for the next flush (P0.4).
                    if cap < n {
                        g.reserve(n - cap);
                    }
                    p
                };
                if packets.is_empty() {
                    // No KCP wire output this cycle (P2.2 empty_flush metric).
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.empty_flush, 1);
                } else if packets.len() > 1 {
                    debug!("raw_packets drain: {} packets", packets.len());
                }
                if !packets.is_empty() {
                    // FEC encode (Go: KCP → FEC → encrypt → UDP), rto=500ms.
                    let packets: Vec<bytes::Bytes> = if let Some(ref enc) = fec_encoder2 {
                        let mut e = enc.lock();
                        kcp_rs::fec_expand_packets(&mut e, &packets, 500)
                    } else {
                        packets
                    };

                    let total_bytes: usize = packets.iter().map(|p| p.len()).sum();
                    let use_cpu_block = kcp_rs::should_cpu_block_encrypt(
                        has_encryption2,
                        has_aead2,
                        packets.len(),
                        total_bytes,
                        &crypt2,
                    );

                    let crypt_sb = crypt2.clone();
                    let crypto_buf_sb = crypto_buf2.clone();
                    // When offloaded to cpu_block, disable nested thread::scope
                    // parallel encrypt (already on a pool worker). Inline path
                    // may still parallelize large CFB batches (P1.1).
                    let allow_parallel = !use_cpu_block;
                    // Reuse encrypt output Vec capacity across flush cycles (P0.4).
                    let mut enc_out: Vec<bytes::Bytes> = Vec::new();
                    if use_cpu_block {
                        kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.encrypt_offload, 1);
                        let crypt_c = crypt_sb.clone();
                        let cb_c = crypto_buf_sb.clone();
                        enc_out = kio::cpu_block(move || {
                            kcp_rs::encrypt_batch(
                                packets,
                                crypt_c.as_ref(),
                                &cb_c,
                                has_encryption2,
                                allow_parallel,
                            )
                        })
                        .await;
                    } else {
                        kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.encrypt_inline, 1);
                        kcp_rs::encrypt_batch_into(
                            packets,
                            crypt_sb.as_ref(),
                            &crypto_buf_sb,
                            has_encryption2,
                            allow_parallel,
                            &mut enc_out,
                        );
                    }
                    let encrypted = enc_out;
                    // Rate limit the send (token bucket, 0 = unlimited).
                    {
                        let total_bytes: usize = encrypted.iter().map(|b| b.len()).sum();
                        rate_limiter2.acquire(total_bytes);
                    }

                    match socket2.send_batch(&encrypted).await {
                        Ok(()) => {
                            let nbytes: u64 = encrypted.iter().map(|b| b.len() as u64).sum();
                            kcp_rs::snmp_add(
                                &kcp_rs::DEFAULT_SNMP.out_pkts,
                                encrypted.len() as u64,
                            );
                            kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.out_bytes, nbytes);
                        }
                        Err(e) => {
                            error!("UDP send error: {}", e);
                            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                                warn!("ConnectionRefused — socket poisoned, closing");
                                dead2.store(true, Ordering::Release);
                                smux2.close();
                                break;
                            }
                        }
                    }
                }

                // If SMUX still has buffered data *and* peer window allows more
                // send, wake immediately. When peer_send_window==0 we must NOT
                // busy-spin — wait for an UPD (UDP reader notifies flush).
                {
                    let streams = smux2.streams();
                    let stream_map = streams.lock();
                    let still_pending = stream_map
                        .values()
                        .any(|s| s.pending_send() > 0 && s.peer_send_window() > 0);
                    drop(stream_map);
                    if still_pending {
                        next_update = 1;
                        flush_notify2.notify_one();
                    }
                }
            }
        });
        self._handles.push(h2);
    }

    /// Send an SMUX frame through KCP.
    /// When compression is enabled, the frame is passed through the persistent
    /// snap::write::FrameEncoder (CRC32C/Castagnoli, correct snappy framing format)
    /// matching Go kcptun's CompStream behaviour. The stream header is written once
    /// on the first call and subsequent calls continue the same snappy stream.
    fn send_frame(&self, frame: &smux_rs::Frame) -> Result<()> {
        let mut buf = BytesMut::with_capacity(12 + frame.data.len());
        frame.encode(&mut buf);
        trace!("send_frame: {} bytes, nocomp={}", buf.len(), self.nocomp);
        // Write to KCP — flush is handled by the flush loop (every 10ms)
        // and by the immediate flush in poll_write's backpressure path.
        // Calling flush() here would cause excessive lock contention when
        // many streams write concurrently.
        if !self.nocomp {
            use std::io::Write;
            let mut enc = self.compressor.lock();
            enc.write_all(&buf).ok();
            enc.flush().ok();
            let to_send = std::mem::take(enc.get_mut());
            self.kcp.lock().send(&to_send)?;
        } else {
            self.kcp.lock().send(&buf)?;
        }
        Ok(())
    }

    /// Access the SMUX session.
    fn session(&self) -> &smux_rs::Session {
        &self.smux
    }

    /// True if KCP dead_link / SMUX keepalive timeout closed this connection.
    fn is_dead(&self) -> bool {
        if self.dead.load(Ordering::Acquire) || self.smux.is_closed() {
            return true;
        }
        // Flush loop may not have observed dead_link yet — check KCP directly.
        if self.kcp.lock().is_dead() {
            self.smux.close();
            self.dead.store(true, Ordering::Release);
            return true;
        }
        false
    }

    /// True if the connection has been idle longer than the auto-expire window.
    ///
    /// The expiry deadline is `last_activity + (autoexpire + scavengettl) * 1000` ms
    /// (monotonic clock). Matches Go kcptun's scavenger logic.
    fn is_expired(&self, autoexpire_secs: u64, scavengettl_secs: u64) -> bool {
        let now = kio::mono_ms();
        let deadline = self.last_activity.load(Ordering::Relaxed)
            + (autoexpire_secs + scavengettl_secs) * 1000;
        now > deadline
    }
}
/// Handle a single client connection: pipe between local TCP and SMUX stream
/// with optional QPP. Compression is handled at the KCP/SMUX session level
/// (matching Go kcptun architecture).
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "qpp"), allow(unused_variables))]
async fn handle_client(
    local: kio::TcpStream,
    smux_stream: Arc<smux_rs::stream::Stream>,
    wait_send: Arc<AtomicUsize>,
    snd_wnd: usize,
    qpp_enabled: bool,
    qpp_key: Vec<u8>,
    qpp_count: u16,
    flush_notify: Arc<kio::Notify>,
    write_notify: Arc<kio::Notify>,
    closewait: u64,
) -> Result<()> {
    let smux_async = smux_rs::SmuxIo::with_backpressure(
        smux_stream.clone(),
        flush_notify,
        wait_send,
        snd_wnd,
        write_notify,
    );

    // Use closewait (from --closewait) as the post-copy grace period.
    // Matches Go kcptun semantics: after both sides reach EOF, wait closewait
    // seconds before tearing down. closewait=0 means no wait (Go client default).
    let pipe_result = if qpp_enabled {
        #[cfg(feature = "qpp")]
        {
            let qpp_port = QPPPort::new(smux_async, &qpp_key, qpp_count);
            let mut local_pin = local;
            let mut qpp_pin = qpp_port;
            pipe(&mut local_pin, &mut qpp_pin, closewait).await
        }
        #[cfg(not(feature = "qpp"))]
        {
            unreachable!("qpp_enabled should be false when qpp feature disabled")
        }
    } else {
        let mut local_pin = local;
        let mut smux_pin = smux_async;
        pipe(&mut local_pin, &mut smux_pin, closewait).await
    };

    // Local half-close only. Do NOT mark_fin_sent here — that blocked the flush
    // loop from ever encoding a real FIN (BUGREPORT_PROXY_MEMORY_GROWTH).
    // Flush marks fin_sent after FIN is queued; linger reaps if peer never FINs.
    //
    // IMPORTANT: Do NOT call clear_buffers() here. The flush loop may still have
    // data in the stream's send_buf that has been written by the pipe but not yet
    // drained and sent through KCP. Clearing buffers would discard unsent data,
    // causing silent data loss (observed in stress tests as truncated/zero responses).
    // The flush loop is responsible for draining send_buf, sending FIN after drain,
    // and the linger reaper will eventually clear buffers after is_fin_sent + timeout.
    smux_stream.mark_local_closed();
    // Do not clear buffers — let flush drain and linger reap.

    match pipe_result {
        Ok((a, b)) => {
            #[cfg(feature = "qpp")]
            let qpp_suffix = if qpp_enabled { " (QPP)" } else { "" };
            #[cfg(not(feature = "qpp"))]
            let qpp_suffix = "";
            info!("pipe completed: {} sent, {} recv{}", a, b, qpp_suffix);
        }
        Err(e) => {
            warn!("pipe error: {}", e);
        }
    }

    Ok(())
}
// ─── Main ───────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    // Runtime-shaped cpu_block thresholds only (no wire change).
    kcp_rs::set_offload_profile(match kio::runtime_kind() {
        kio::RuntimeKind::Tokio => kcp_rs::OffloadProfile::Tokio,
        kio::RuntimeKind::Smol => kcp_rs::OffloadProfile::Smol,
    });
    kio::block_on(async_main())
}

/// Create a connected UDP socket for a KCP client connection.
fn create_client_udp_socket(
    remote_addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
) -> std::io::Result<kio::UdpSocket> {
    let domain = if remote_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;
    let buf_size = if sockbuf > 0 {
        sockbuf as usize
    } else {
        2 * 1024 * 1024
    };
    let _ = socket.set_recv_buffer_size(buf_size);
    let _ = socket.set_send_buffer_size(buf_size);
    let _ = socket.set_reuse_address(true);
    if dscp > 0 {
        let dscp_shifted = dscp << 2;
        if let Err(e) = socket.set_tos(dscp_shifted) {
            warn!("set_tos (DSCP) failed for client socket: {}", e);
        }
    }
    socket.connect(&remote_addr.into())?;
    socket.set_nonblocking(true)?;
    kio::UdpSocket::from_std(socket.into())
}

async fn async_main() -> Result<()> {
    // Ignore SIGPIPE to prevent crashes when writing to closed sockets.
    kio::ignore_sigpipe();
    // Install SIGUSR1 handler for SNMP stats dump (matching Go kcptun).
    kio::install_sigusr1_handler();

    let cli = Cli::parse();
    if cli.version_flag {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // Load config file if specified
    let cli = if let Some(ref config_path) = cli.c {
        let config_str = kio::read_to_string(config_path.clone()).await?;
        let cfg: Config = serde_json::from_str(&config_str)?;
        Cli::merge(cli, cfg)
    } else {
        cli
    };

    // Logging: controlled by RUST_LOG env var, defaults to "info".
    // Use RUST_LOG=debug for debug output, RUST_LOG=trace for everything.
    // Example: RUST_LOG=kcptun_client=debug,kcp_rs=info cargo run --release
    //
    // Redirect to file when --log is specified (matching Go kcptun).
    if let Some(ref log_path) = cli.log {
        rotate_log(log_path, 10 * 1024 * 1024);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .target(env_logger::Target::Pipe(Box::new(file)))
            .init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .init();
        info!(
            "log level: {} (set RUST_LOG=debug for verbose output)",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        );
    }

    let local_addr = cli.localaddr.as_deref().unwrap_or(":12948");
    let remote_addr_str = cli
        .remoteaddr
        .as_deref()
        .context("remote address (-r) is required")?;

    let key_str = cli.key.as_deref().unwrap();
    let crypt = cli.crypt.as_deref().unwrap();
    let mode = cli.mode.as_deref().unwrap();
    let conn_count = cli.conn.unwrap_or(1).max(1);
    let mtu = cli.mtu.unwrap_or(1350);
    let sndwnd = cli.sndwnd.unwrap_or(128);
    let rcvwnd = cli.rcvwnd.unwrap_or(512);
    let datashard = cli.datashard.unwrap_or(10);
    let parityshard = cli.parityshard.unwrap_or(3);
    let nocomp = cli.nocomp;
    let acknodelay = cli.acknodelay;
    let nodelay = cli.nodelay.unwrap_or(0);
    let interval = cli.interval.unwrap_or(50);
    let resend = cli.resend.unwrap_or(0);
    let nc = cli.nc.unwrap_or(0);
    let smuxver = cli.smuxver.unwrap_or(2);
    let smuxbuf = cli.smuxbuf.unwrap_or(4 * 1024 * 1024);
    let streambuf = cli.streambuf.unwrap_or(2097152);
    let framesize = cli.framesize.unwrap_or(8192);
    let sockbuf = cli.sockbuf.unwrap_or(4 * 1024 * 1024);
    let keepalive = cli.keepalive.unwrap_or(10);
    let autoexpire = cli.autoexpire.unwrap_or(0);
    let scavengettl = cli.scavengettl.unwrap_or(600);
    let closewait = cli.closewait.unwrap_or(0);
    let ratelimit = cli.ratelimit;
    let dscp = cli.dscp.unwrap_or(0);
    #[cfg(feature = "qpp")]
    let qpp_enabled = cli.qpp;
    #[cfg(not(feature = "qpp"))]
    let qpp_enabled = false;
    #[cfg(feature = "qpp")]
    let qpp_count = cli.qppcount.unwrap_or(61);
    #[cfg(not(feature = "qpp"))]
    let qpp_count = 0u16;

    // Validate QPP parameters (matching Go's ValidateQPPParams)
    #[cfg(feature = "qpp")]
    if qpp_enabled {
        match kcptun_common::validate_qpp_params(qpp_count, key_str.as_bytes()) {
            Ok(warnings) => {
                for w in &warnings {
                    warn!("{}", w);
                }
            }
            Err(e) => {
                error!("QPP configuration error: {}", e);
                return Err(anyhow::anyhow!("QPP: {}", e));
            }
        }
    }

    // Derive encryption key
    let key = derive_key(key_str);
    let session_cfg = SessionConfig {
        crypt: crypt.to_string(),
        mode: mode.to_string(),
        mtu,
        sndwnd,
        rcvwnd,
        datashard,
        parityshard,
        acknodelay,
        nodelay,
        interval,
        resend,
        nc,
        smuxver,
        smuxbuf,
        streambuf,
        framesize,
        keepalive,
        nocomp,
        ratelimit,
    };

    info!(
        "key derived: crypt={}, key={:02x}..{:02x}",
        crypt, key[0], key[31]
    );

    // Parse remote addresses (supports multi-port format)
    let remote_addrs = kcptun_common::parse_multi_port(remote_addr_str)?;

    // Create KCP connection pool (shared with scavenger for auto-expire)
    let conns = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(
        conn_count as usize,
    )));
    if cli.tcp {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("--tcp requires Linux (raw sockets + TCP_REPAIR)");

        // TCP mode: single connection (TCP is point-to-point).
        // Server must already be listening with --tcp; dial does a real TCP
        // handshake then switches the socket into TCP_REPAIR + raw IP I/O.
        #[cfg(target_os = "linux")]
        {
            let remote = remote_addrs[0];
            info!("creating TCP raw KCP connection -> {}", remote);
            let conn = kio::tcpraw_dial(&remote).map_err(|e| {
                anyhow::anyhow!(
                    "tcpraw dial to {}: {} (needs Linux + CAP_NET_RAW/ADMIN, server --tcp up)",
                    remote,
                    e
                )
            })?;
            let socket = Arc::new(kio::DatagramSocket::TcpRaw(conn));
            let conn = KcpConn::new(remote, &key, &session_cfg, socket).await?;
            kcp_rs::DEFAULT_SNMP.session_opened(true);
            conns.lock().push(conn);
        }
    } else {
        // UDP mode: create conn_count connections
        for i in 0..conn_count as usize {
            let remote = remote_addrs[i % remote_addrs.len()];
            info!(
                "creating KCP connection {}/{} -> {}",
                i + 1,
                conn_count,
                remote
            );
            let socket = create_client_udp_socket(remote, sockbuf, dscp)?;
            let socket = Arc::new(kio::DatagramSocket::Udp(socket));
            let conn = KcpConn::new(remote, &key, &session_cfg, socket).await?;
            kcp_rs::DEFAULT_SNMP.session_opened(true);
            conns.lock().push(conn);
        }
    }

    info!("established {} KCP connections", conns.lock().len());
    if ratelimit > 0 {
        info!("ratelimit: {} bytes/sec", ratelimit);
    }
    if dscp > 0 {
        info!("dscp: {}", dscp);
    }
    info!("sockbuf: {}", sockbuf);

    // Parse local listen address
    let listen_addr: SocketAddr = kcptun_common::parse_multi_port(local_addr)?
        .into_iter()
        .next()
        .context("invalid local address")?;

    // Start SNMP logger if configured
    let stop_flag = Arc::new(AtomicBool::new(false));
    // SNMP collection is off by default (zero hot-path cost). Enable only when
    // a log path is set and period > 0.
    if let Some(ref snmplog_path) = cli.snmplog {
        let secs = cli.snmpperiod.unwrap_or(60);
        if secs > 0 && !snmplog_path.is_empty() {
            kcp_rs::snmp_enable();
            let period = Duration::from_secs(secs);
            let s = stop_flag.clone();
            let p = snmplog_path.clone();
            kio::spawn_task(async move {
                snmp_logger(p, period, s).await;
            });
        } else {
            log::warn!("snmplog set but snmpperiod=0 or empty path — SNMP collection disabled");
        }
    }

    // Start pprof if configured (requires --features pprof)
    #[cfg(feature = "pprof")]
    if let Some(ref pprof_addr) = cli.pprof {
        info!("starting pprof HTTP server on {}", pprof_addr);
        #[cfg(feature = "pprof-deadlock")]
        kpprof::start_deadlock_detector();
        let pprof_stop = stop_flag.clone();
        let addr = pprof_addr.clone();
        kio::spawn_task(async move {
            if let Err(e) = kpprof::run_pprof(&addr, pprof_stop).await {
                error!("pprof server error: {}", e);
            }
        });
    }
    #[cfg(not(feature = "pprof"))]
    if cli.pprof.is_some() {
        log::warn!("--pprof requested but binary built without `pprof` feature; rebuild with --features pprof");
    }

    // Start auto-expire scavenger if enabled (matching Go client)
    if autoexpire > 0 {
        let s = stop_flag.clone();
        let scavenge_conns = conns.clone();
        let scavenge_autoexpire = autoexpire;
        let scavenge_ttl = scavengettl;
        kio::spawn_task(async move {
            // Matches Go kcptun: scavenger polls every 5 seconds and closes
            // sessions whose expiryDate (autoexpire + scavengettl) has passed.
            info!(
                "scavenger started: autoexpire={}s, scavengettl={}s",
                scavenge_autoexpire, scavenge_ttl
            );
            loop {
                kio::sleep_ms(5000).await;
                if s.load(Ordering::Acquire) {
                    break;
                }
                let guard = scavenge_conns.lock();
                for conn in guard.iter() {
                    if !conn.is_dead() && conn.is_expired(scavenge_autoexpire, scavenge_ttl) {
                        info!(
                            "scavenger: closing expired connection (last activity {}ms ago)",
                            kio::mono_ms()
                                .saturating_sub(conn.last_activity.load(Ordering::Relaxed))
                        );
                        conn.dead.store(true, Ordering::Release);
                        conn.smux.close();
                    }
                }
            }
        });
    }

    // Start the TCP listener
    let listener = kio::TcpListener::bind(listen_addr).await?;
    info!("listening on {}", listen_addr);

    // Spawn Ctrl-C handler (runtime-agnostic)
    {
        let stop = stop_flag.clone();
        kio::spawn_task(async move {
            let _ = kio::ctrl_c().await;
            stop.store(true, Ordering::Relaxed);
        });
    }

    // Accept loop with round-robin across KCP connections
    let round_robin = Arc::new(AtomicUsize::new(0));
    let conn_count_usize = conn_count as usize;

    loop {
        if stop_flag.load(Ordering::Relaxed) {
            info!("shutting down...");
            break;
        }

        match kio::timeout(Duration::from_millis(500), listener.accept()).await {
            Ok(Ok((local, peer))) => {
                if stop_flag.load(Ordering::Relaxed) {
                    info!("shutting down, rejecting new connection from {}", peer);
                    break;
                }

                let idx = round_robin.fetch_add(1, Ordering::Relaxed) % conn_count_usize;

                // Ensure a live KCP/SMUX session (Go muxSession.Open auto-redial).
                let mut opened: Option<Arc<smux_rs::stream::Stream>> = None;
                for attempt in 0..2 {
                    let needs_reconnect = {
                        let guard = conns.lock();
                        guard[idx].is_dead()
                    };
                    if needs_reconnect {
                        let remote = remote_addrs[idx % remote_addrs.len()];
                        info!(
                            "connection {} is dead, reconnecting to {} (attempt {})...",
                            idx,
                            remote,
                            attempt + 1
                        );
                        let reconnect_res = {
                            let socket = create_client_udp_socket(remote, sockbuf, dscp)?;
                            let socket = Arc::new(kio::DatagramSocket::Udp(socket));
                            KcpConn::new(remote, &key, &session_cfg, socket).await
                        };
                        match reconnect_res {
                            Ok(new_conn) => {
                                conns.lock()[idx] = new_conn;
                                kcp_rs::DEFAULT_SNMP.session_opened(true);
                                info!("connection {} reconnected", idx);
                            }
                            Err(e) => {
                                error!("reconnect connection {} failed: {:#}", idx, e);
                                break;
                            }
                        }
                    }

                    let stream_result = {
                        let guard = conns.lock();
                        let c = &guard[idx];
                        match c.session().open_stream() {
                            Ok(s) => {
                                let stream_id = s.id();
                                debug!("sending SYN for stream {}", stream_id);
                                let syn_frame =
                                    smux_rs::Frame::new(smux_rs::Cmd::Syn, stream_id, Bytes::new())
                                        .with_ver(c.session().version());
                                if let Err(e) = c.send_frame(&syn_frame) {
                                    error!("failed to send Syn frame: {}", e);
                                    c.session().remove_stream(stream_id);
                                    c.dead.store(true, Ordering::Release);
                                    c.session().close();
                                    None
                                } else {
                                    c.kcp.lock().flush();
                                    c.flush_notify.notify_one();
                                    Some(s)
                                }
                            }
                            Err(e) => {
                                error!("failed to open SMUX stream: {:?}", e);
                                c.dead.store(true, Ordering::Release);
                                c.session().close();
                                None
                            }
                        }
                    };

                    match stream_result {
                        Some(s) => {
                            opened = Some(s);
                            break;
                        }
                        None => continue,
                    }
                }

                let smux_stream = match opened {
                    Some(s) => s,
                    None => continue,
                };

                let stream_id = smux_stream.id();
                info!("accepted connection from {} (stream {})", peer, stream_id);

                let (ws, sw, flush_notify_ref, write_notify_ref) = {
                    let guard = conns.lock();
                    let c = &guard[idx];
                    (
                        c.wait_send.clone(),
                        c.snd_wnd,
                        c.flush_notify.clone(),
                        c.write_notify.clone(),
                    )
                };

                let qpp_key = key.to_vec();
                kio::spawn_task(async move {
                    if let Err(e) = handle_client(
                        local,
                        smux_stream,
                        ws,
                        sw,
                        qpp_enabled,
                        qpp_key,
                        qpp_count,
                        flush_notify_ref,
                        write_notify_ref,
                        closewait,
                    )
                    .await
                    {
                        error!("client handler error (stream {}): {:?}", stream_id, e);
                    }
                    info!("stream {} closed", stream_id);
                });
            }
            Ok(Err(e)) => {
                error!("accept error: {}", e);
                continue;
            }
            Err(_) => continue, // timeout, loop back to check stop_flag
        }
    }

    // Graceful shutdown
    info!("shutting down...");
    kio::sleep_ms(1000).await;
    info!("bye");

    Ok(())
}

// pprof run_pprof() is now provided by the kpprof-rs crate.
// When --features pprof is enabled, kpprof::run_pprof() serves all
// Go-compatible /debug/pprof/* endpoints.

// ─── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key() {
        let key = derive_key("test-password");
        assert_eq!(key.len(), 32);
        let key2 = derive_key("test-password");
        assert_eq!(key, key2);
    }

    #[test]
    fn test_derive_key_different() {
        let key1 = derive_key("password1");
        let key2 = derive_key("password2");
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_apply_mode_normal() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "normal");
        assert_eq!(kcp.interval(), 40);
    }

    #[test]
    fn test_apply_mode_fast3() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "fast3");
        assert_eq!(kcp.interval(), 10);
    }

    #[test]
    fn test_config_deserialize() {
        let json = r#"{
            "localaddr": "127.0.0.1:12948",
            "remoteaddr": "127.0.0.1:29900",
            "key": "test-key",
            "crypt": "aes-128",
            "mode": "fast2",
            "conn": 2,
            "mtu": 1350,
            "sndwnd": 1024,
            "rcvwnd": 1024,
            "datashard": 10,
            "parityshard": 3,
            "nocomp": false,
            "smuxver": 2,
            "keepalive": 10
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.localaddr.as_deref(), Some("127.0.0.1:12948"));
        assert_eq!(cfg.mode.as_deref(), Some("fast2"));
        assert_eq!(cfg.conn, Some(2));
        assert_eq!(cfg.smuxver, Some(2));
    }

    #[test]
    fn test_cli_merge() {
        let cli = Cli {
            localaddr: Some("0.0.0.0:8080".into()),
            remoteaddr: None,
            key: None,
            crypt: None,
            mode: None,
            conn: None,
            autoexpire: None,
            scavengettl: None,
            mtu: None,
            ratelimit: 0,
            sndwnd: None,
            rcvwnd: None,
            datashard: None,
            parityshard: None,
            dscp: None,
            nocomp: false,
            acknodelay: false,
            nodelay: None,
            interval: None,
            resend: None,
            nc: None,
            sockbuf: None,
            smuxver: None,
            smuxbuf: None,
            streambuf: None,
            framesize: None,
            keepalive: None,
            closewait: None,
            snmplog: None,
            snmpperiod: None,
            log: None,
            quiet: false,
            tcp: false,
            pprof: None,
            qpp: false,
            qppcount: None,
            c: None,
            version_flag: false,
        };
        let cfg = Config {
            remoteaddr: Some("server:1234".into()),
            key: Some("cfg-key".into()),
            ..Default::default()
        };
        let merged = Cli::merge(cli, cfg);
        assert_eq!(merged.localaddr.as_deref(), Some("0.0.0.0:8080"));
        assert_eq!(merged.remoteaddr.as_deref(), Some("server:1234"));
        assert_eq!(merged.key.as_deref(), Some("cfg-key"));
    }

    #[test]
    fn test_empty_config() {
        let cfg: Config = serde_json::from_str("{}").unwrap();
        assert!(cfg.localaddr.is_none());
    }

    #[test]
    fn test_smux_frame_roundtrip() {
        use smux_rs::{Cmd, Frame};
        let frame = Frame::new(Cmd::Psh, 42, Bytes::from("test data"));
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let (decoded, _) = Frame::decode(&buf).unwrap();
        assert_eq!(decoded.cmd, Cmd::Psh);
        assert_eq!(decoded.stream_id, 42);
        assert_eq!(&decoded.data[..], b"test data");
    }

    #[test]
    fn test_smux_syn_frame() {
        use smux_rs::{Cmd, Frame};
        let frame = Frame::new(Cmd::Syn, 1, Bytes::new());
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let (decoded, _) = Frame::decode(&buf).unwrap();
        assert_eq!(decoded.cmd, Cmd::Syn);
        assert_eq!(decoded.stream_id, 1);
        assert_eq!(
            buf.len(),
            8,
            "Go smux frame header is 8 bytes (ver|cmd|sid|len)"
        );
    }

    // ─── is_expired tests (scavenger / auto-expire) ──────────────────────────

    /// Helper: create a minimal KcpConn for is_expired testing.
    /// Uses a temporary UDP socket to get a valid remote address.
    async fn make_test_conn() -> KcpConn {
        let tmp = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind temp UDP");
        let addr = tmp.local_addr().expect("local addr");
        drop(tmp);
        let socket = create_client_udp_socket(addr, 0, 0).expect("create test UDP socket");
        let socket = Arc::new(kio::DatagramSocket::Udp(socket));
        let key = derive_key("test-scavenger-password");
        let cfg = SessionConfig {
            crypt: "null".into(),
            mode: "fast".into(),
            mtu: 1350,
            sndwnd: 128,
            rcvwnd: 512,
            datashard: 0,
            parityshard: 0,
            acknodelay: false,
            nodelay: 0,
            interval: 30,
            resend: 2,
            nc: 1,
            smuxver: 2,
            smuxbuf: 4 * 1024 * 1024,
            streambuf: 2 * 1024 * 1024,
            framesize: 8192,
            keepalive: 10,
            nocomp: true,
            ratelimit: 0,
        };
        KcpConn::new(addr, &key, &cfg, socket)
            .await
            .expect("create test KcpConn")
    }

    #[test]
    fn test_is_expired_when_recently_active() {
        kio::block_on(async {
            let conn = make_test_conn().await;
            // Right after creation, last_activity is set to now.
            // With a large autoexpire window, it should NOT be expired.
            assert!(
                !conn.is_expired(3600, 600),
                "fresh connection must not be expired"
            );
        });
    }

    #[test]
    fn test_is_expired_with_zero_autoexpire() {
        kio::block_on(async {
            let conn = make_test_conn().await;
            // autoexpire=0 + scavengettl=0 means deadline = construction time.
            // Even 1ms later it should be expired.
            std::thread::sleep(std::time::Duration::from_millis(5));
            assert!(
                conn.is_expired(0, 0),
                "zero autoexpire+scavengettl must expire immediately"
            );
        });
    }

    #[test]
    fn test_is_expired_respects_scavengettl() {
        kio::block_on(async {
            let conn = make_test_conn().await;
            // autoexpire=0 but scavengettl=3600 → not expired
            assert!(
                !conn.is_expired(0, 3600),
                "scavengettl grace period must prevent expiry"
            );
            // autoexpire=0 scavengettl=0 → expired
            std::thread::sleep(std::time::Duration::from_millis(5));
            assert!(conn.is_expired(0, 0), "zero scavengettl must expire");
        });
    }
}
