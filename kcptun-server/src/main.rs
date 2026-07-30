//! kcptun-server -- KCP-based TCP stream accelerator (server side).
//!
//! A Rust port of the Go kcptun server.
//! Listens on UDP for KCP connections, accepts SMUX streams, forwards to TCP targets.

#![allow(
    clippy::collapsible_match,
    clippy::question_mark,
    clippy::explicit_auto_deref,
    clippy::redundant_closure,
    clippy::too_many_arguments
)]

#[cfg(not(feature = "pprof"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "pprof"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "pprof")]
#[global_allocator]
static GLOBAL: kpprof::ProfilingAllocator = kpprof::ProfilingAllocator::new();

use dashmap::DashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyContext, Result};
use bytes::BytesMut;
use clap::Parser;
use log::{debug, error, info, warn};
use serde::Deserialize;

use kcp_rs::KCP;
use kcp_rs::{fec_kcp_from_recovered, FecDecoder, FecEncoder};
#[cfg(feature = "qpp")]
use kcptun_common::QPPPort;
use kcptun_common::{apply_mode, derive_key, pipe, snmp_logger, SnappyStreamDecoder};
use kcrypt_rs::BlockCrypt;

// ─── Constants ──────────────────────────────────────────────────────────────────

/// Maximum UDP datagram size.
const MAX_DATAGRAM: usize = 2048;

/// How often the KCP update loop fires (milliseconds).
const KCP_UPDATE_INTERVAL_MS: u64 = 2;

// ─── KCP-level Snappy compression (matching Go) ────────────────────────────

// Note: Compression is handled by the persistent snap::write::FrameEncoder
// in the KcpServerSession.compressor field, matching Go's snappy.NewBufferedWriter.
// Decompression is handled by SnappyStreamDecoder, matching Go's snappy.NewReader.
// ─── Log file rotation ─────────────────────────────────────────────────────────

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

// ─── Config (JSON config file support) ──────────────────────────────────────────

/// Configuration struct matching the kcptun JSON config format.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: Option<String>,
    pub target: Option<String>,
    pub key: Option<String>,
    pub crypt: Option<String>,
    pub mode: Option<String>,
    pub ratelimit: Option<u32>,
    pub mtu: Option<u32>,
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

/// kcptun server -- accept KCP connections and forward to TCP targets.
#[derive(Debug, Parser)]
#[command(name = "kcptun-server", about, version, disable_version_flag = true)]
pub struct Cli {
    /// KCP listen address (UDP).
    #[arg(short = 'l', long, default_value = ":29900")]
    pub listen: Option<String>,

    /// TCP target address to forward connections to.
    #[arg(short = 't', long, default_value = "127.0.0.1:12948")]
    pub target: Option<String>,

    /// Pre-shared secret between client and server.
    #[arg(short, long, default_value = "it's a secrect", env = "KCPTUN_KEY")]
    pub key: Option<String>,

    /// Encryption method: aes, aes-128, aes-128-gcm, aes-192, salsa20, blowfish,
    /// twofish, cast5, 3des, tea, xtea, xor, sm4, none, null.
    #[arg(long, default_value = "aes")]
    pub crypt: Option<String>,

    /// Protocol mode: normal, fast, fast2, fast3.
    #[arg(short, long, default_value = "fast")]
    pub mode: Option<String>,

    /// Rate limit in bytes per second per connection (0 = disabled).
    #[arg(long, default_value_t = 0)]
    pub ratelimit: u32,

    /// MTU value.
    #[arg(long)]
    pub mtu: Option<u32>,

    /// Send window size.
    #[arg(long)]
    pub sndwnd: Option<u32>,

    /// Receive window size.
    #[arg(long)]
    pub rcvwnd: Option<u32>,

    /// FEC data shards.
    #[arg(long, default_value_t = 10)]
    pub datashard: u32,

    /// FEC parity shards.
    #[arg(long, default_value_t = 3)]
    pub parityshard: u32,

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
    #[arg(long, default_value_t = 2097152)]
    pub streambuf: usize,

    /// SMUX max frame size.
    #[arg(long, default_value_t = 8192)]
    pub framesize: usize,

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
            listen: cli.listen.or(cfg.listen),
            target: cli.target.or(cfg.target),
            key: cli.key.or(cfg.key),
            crypt: cli.crypt.or(cfg.crypt),
            mode: cli.mode.or(cfg.mode),
            ratelimit: {
                let v = cli.ratelimit;
                if v != 0 {
                    v
                } else {
                    cfg.ratelimit.unwrap_or(0)
                }
            },
            mtu: cli.mtu.or(cfg.mtu),
            sndwnd: cli.sndwnd.or(cfg.sndwnd),
            rcvwnd: cli.rcvwnd.or(cfg.rcvwnd),
            datashard: {
                let v = cli.datashard;
                if v != 10 {
                    v
                } else {
                    cfg.datashard.unwrap_or(10)
                }
            },
            parityshard: {
                let v = cli.parityshard;
                if v != 3 {
                    v
                } else {
                    cfg.parityshard.unwrap_or(3)
                }
            },
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
            streambuf: {
                let v = cli.streambuf;
                if v != 2097152 {
                    v
                } else {
                    cfg.streambuf.unwrap_or(2097152)
                }
            },
            framesize: {
                let v = cli.framesize;
                if v != 8192 {
                    v
                } else {
                    cfg.framesize.unwrap_or(8192)
                }
            },
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
            c: cli.c,
            version_flag: false, // never from config file
        }
    }
}

// ─── Key Derivation ─────────────────────────────────────────────────────────────
// ─── Mode Profiles ──────────────────────────────────────────────────────────────
// ─── MultiPort Parser ───────────────────────────────────────────────────────────

/// Parse a "host:port" string into a SocketAddr.
#[allow(dead_code)]
fn parse_addr(addr: &str) -> Result<SocketAddr> {
    // Handle ":port" shorthand by defaulting to "0.0.0.0"
    if addr.starts_with(':') {
        let host_addr = format!("0.0.0.0{}", addr);
        return host_addr.parse::<SocketAddr>().context("invalid address");
    }
    addr.parse::<SocketAddr>().context("invalid address")
}

/// Create a UDP socket bound to `addr` with the given buffer sizes and DSCP.
fn create_udp_socket(addr: SocketAddr, sockbuf: u32, dscp: u32) -> Result<kio::UdpSocket> {
    let socket = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::DGRAM,
        None,
    )?;
    if let Err(e) = socket.set_recv_buffer_size(sockbuf as usize) {
        warn!("set_recv_buffer_size failed: {}", e);
    }
    if let Err(e) = socket.set_send_buffer_size(sockbuf as usize) {
        warn!("set_send_buffer_size failed: {}", e);
    }
    if dscp > 0 {
        let dscp_shifted = dscp << 2;
        if let Err(e) = socket.set_tos(dscp_shifted) {
            warn!("set_tos (DSCP) failed: {}", e);
        }
    }
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(kio::UdpSocket::from_std(socket.into())?)
}

/// Handle a single SMUX stream: connect to the TCP target and pipe data
/// bidirectionally with optional QPP. Compression is handled at the
/// KCP/SMUX session level (matching Go kcptun architecture).
#[cfg_attr(not(feature = "qpp"), allow(unused_variables))]
async fn handle_stream(
    target: String,
    smux_stream: Arc<smux_rs::stream::Stream>,
    stream_id: u32,
    qpp_enabled: bool,
    qpp_key: Vec<u8>,
    qpp_count: u16,
    quiet: bool,
    close_wait: u64,
    flush_notify: Arc<kio::Notify>,
) -> Result<()> {
    let tcp = kio::TcpStream::connect(&target)
        .await
        .with_context(|| format!("failed to connect to target {}", target))?;

    if !quiet {
        info!("stream {} connected to target {}", stream_id, target);
    }

    let smux_io = smux_rs::SmuxIo::new(smux_stream.clone(), flush_notify);

    // Default idle timeout to prevent FD leaks when close_wait is 0.
    // Matches Go kcptun semantics: after both sides reach EOF, wait close_wait
    // seconds before tearing down. close_wait=0 means no wait.
    // Go server default is 30; Rust mirrors this.
    let pipe_result = if qpp_enabled {
        #[cfg(feature = "qpp")]
        {
            let qpp_port = QPPPort::new(smux_io, &qpp_key, qpp_count);
            let mut tcp_pin = tcp;
            let mut qpp_pin = qpp_port;
            pipe(&mut tcp_pin, &mut qpp_pin, close_wait).await
        }
        #[cfg(not(feature = "qpp"))]
        {
            unreachable!("qpp_enabled should be false when qpp feature disabled")
        }
    } else {
        let mut tcp_pin = tcp;
        let mut smux_pin = smux_io;
        debug!("server pipe started for stream {}", stream_id);
        pipe(&mut tcp_pin, &mut smux_pin, close_wait).await
    };

    // Local half-close only. Do NOT mark_fin_sent here — that made flush skip
    // encoding FIN (same class of leak as client; see BUGREPORT_PROXY_MEMORY_GROWTH).
    // Flush encodes FIN then marks fin_sent after kcp.send success; linger reaps zombies.
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
            if !quiet {
                info!(
                    "stream {} pipe completed: {} sent, {} recv{}",
                    stream_id,
                    a,
                    b,
                    if qpp_enabled { " (QPP)" } else { "" }
                );
            }
        }
        Err(e) => {
            warn!("stream {} pipe error: {}", stream_id, e);
        }
    }

    Ok(())
}

// ─── KcpServerSession ───────────────────────────────────────────────────────────

/// A server-side KCP session representing one connection from a remote peer.
///
/// Each session owns a KCP state machine, an SMUX server session, and a
/// background flush loop. Incoming encrypted datagrams are fed via `feed_data`,
/// which decrypts, strips the Go kcp-go v5 outer header, and drives the KCP
/// state machine to extract reassembled user data (SMUX frames).
/// KCP + SMUX session parameters (shared create path).
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

struct KcpServerSession {
    /// KCP state machine (shared between the recv and flush tasks).
    kcp: Arc<parking_lot::Mutex<KCP>>,
    /// Session cipher — concrete [`kcrypt_rs::CryptEngine`] (enum match, no
    /// `dyn` vtable). Shared without Mutex; AEAD is the `Aes128Gcm` variant
    /// (`crypt.as_aead()` / `crypt.is_aead()`).
    crypt: Arc<kcrypt_rs::CryptEngine>,
    /// SMUX server session multiplexing streams over KCP.
    smux: Arc<smux_rs::Session>,
    /// Set of SMUX stream IDs that have already been accepted and dispatched.
    handled_streams: Arc<parking_lot::Mutex<HashSet<u32>>>,
    /// Peer address for sending responses.
    peer: SocketAddr,
    /// Background task handles.
    _handles: Vec<kio::JoinHandle<()>>,
    /// Disable Snappy compression (matches Go --nocomp).
    nocomp: bool,
    /// Raw KCP segments collected by the output callback during flush/input.
    ///
    /// Each entry is a `Bytes` (reference-counted slice of the KCP output
    /// buffer) handed directly by the output callback — no per-packet
    /// `Vec` alloc + `extend_from_slice` copy (R2: output Bytes pipeline).
    raw_packets: Arc<parking_lot::Mutex<Vec<bytes::Bytes>>>,
    /// Persistent Snappy framing decoder (Go interop fallback).
    snappy_fallback: Option<parking_lot::Mutex<SnappyStreamDecoder>>,
    /// Data socket for sending (shared with the recv loop).
    socket: Arc<kio::DatagramSocket>,
    /// Whether encryption is enabled.
    has_encryption: bool,
    /// Whether ACK nodelay is enabled.
    ack_nodelay: bool,
    /// Optional FEC decoder for Reed-Solomon error correction recovery.
    fec_decoder: Option<parking_lot::Mutex<FecDecoder>>,
    /// Optional FEC encoder (Go fecEncoder).
    fec_encoder: Option<Arc<parking_lot::Mutex<FecEncoder>>>,
    /// Persistent Snappy framing compressor. Uses snap's FrameEncoder
    /// (CRC32C/Castagnoli) matching Go's golang/snappy for interop.
    compressor: Option<Arc<parking_lot::Mutex<snap::write::FrameEncoder<Vec<u8>>>>>,
    /// Reusable encryption buffer with counter-based nonce.
    /// Eliminates per-packet vec![] allocation and rand::thread_rng() calls.
    crypto_buf: Arc<parking_lot::Mutex<kcp_rs::CryptoBuf>>,
    /// Notify for waking up the flush loop immediately when SMUX streams
    /// have new data to send. Eliminates the 0~10ms wait of the fixed
    /// sleep interval.
    flush_notify: Arc<kio::Notify>,

    /// Set when KCP dead_link or SMUX keepalive timeout is detected on this
    /// session. Allows the UDP dispatcher (get_or_create_session) to evict a
    /// dead session for a given peer address and create a fresh one when the
    /// client reconnects (e.g. after server restart or prolonged outage).
    ///
    /// This is the lowest-risk way to "notice historical connections":
    /// we never send any new control frames or change wire format.
    /// The client will still discover the old side is dead via its own
    /// KCP dead_link (20 retransmits) or SMUX keepalive timeout (typically
    /// a few seconds). 4-10s end-to-end is acceptable per requirements.
    dead: Arc<AtomicBool>,
    /// Per-connection rate limiter (token bucket). 0 = unlimited.
    ratelimiter: Arc<kcptun_common::RateLimiter>,
}

impl KcpServerSession {
    /// Create a new server-side KCP session for the given peer.
    fn new(
        conv: u32,
        peer: SocketAddr,
        socket: &Arc<kio::DatagramSocket>,
        key: &[u8; 32],
        cfg: &SessionConfig,
    ) -> Self {
        let crypt_method = cfg.crypt.as_str();
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
        let (engine, _) = kcrypt_rs::CryptEngine::select(crypt_method, &key[..]);
        let crypt_state = Arc::new(engine);

        // Create KCP instance with output callback that collects raw packets.
        // Same optimization as client: the callback just collects raw KCP
        // data (fast), encryption + UDP send happens after KCP lock release.
        // R2: output callback receives owned `Bytes` (reference-counted slice
        // of the KCP output buffer) — no per-packet `Vec` alloc +
        // `extend_from_slice` copy.
        let raw_packets = Arc::new(parking_lot::Mutex::new(Vec::<bytes::Bytes>::new()));
        let raw_packets_cb = raw_packets.clone();
        let has_encryption = crypt_method != "null";
        let mut kcp = KCP::new(
            conv,
            0,
            Box::new(move |data: bytes::Bytes| {
                raw_packets_cb.lock().push(data);
            }),
        );

        // Configure KCP
        kcp.set_mtu(mtu);
        kcp.set_snd_wnd(sndwnd);
        kcp.set_rcv_wnd(rcvwnd);
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

        let kcp = Arc::new(parking_lot::Mutex::new(kcp));

        // Create SMUX server config
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
        let smux = match smux_rs::Session::new_server(&smux_cfg) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                error!("failed to create SMUX server session: {:?}", e);
                // Return a placeholder that will fail gracefully
                let default_cfg = smux_rs::DEFAULT_CONFIG.clone();
                Arc::new(smux_rs::Session::new_server(&default_cfg).unwrap())
            }
        };

        let handled_streams = Arc::new(parking_lot::Mutex::new(HashSet::new()));

        // Session-layer FEC (matching Go newUDPSession).
        let (fec_decoder, fec_encoder) = if datashard > 0 && parityshard > 0 {
            (
                FecDecoder::new(datashard as usize, parityshard as usize)
                    .map(parking_lot::Mutex::new),
                FecEncoder::new(datashard as usize, parityshard as usize, 0)
                    .map(|e| Arc::new(parking_lot::Mutex::new(e))),
            )
        } else {
            (None, None)
        };

        let mut session = KcpServerSession {
            kcp,
            crypt: crypt_state,
            smux,
            handled_streams,
            peer,
            _handles: Vec::new(),
            nocomp,
            raw_packets,
            snappy_fallback: if nocomp {
                None
            } else {
                Some(parking_lot::Mutex::new(SnappyStreamDecoder::new()))
            },
            socket: socket.clone(),
            has_encryption,
            ack_nodelay: acknodelay,
            fec_decoder,
            fec_encoder,
            compressor: if nocomp {
                None
            } else {
                Some(Arc::new(parking_lot::Mutex::new(
                    snap::write::FrameEncoder::new(Vec::new()),
                )))
            },
            crypto_buf: Arc::new(parking_lot::Mutex::new(kcp_rs::CryptoBuf::new(conv as u64))),
            flush_notify: Arc::new(kio::Notify::new()),
            dead: Arc::new(AtomicBool::new(false)),
            ratelimiter: Arc::new(kcptun_common::RateLimiter::new(cfg.ratelimit)),
        };

        session.start_flush_loop();
        session
    }

    /// Returns true if this session has been marked dead (KCP dead_link,
    /// SMUX keepalive timeout, or explicit close). The UDP dispatcher uses
    /// this to evict a stale session for a peer address so that a reconnecting
    /// client (after server restart, prolonged outage, etc.) gets a fresh
    /// session instead of being handed a dead one.
    ///
    /// This is the lowest-risk "historical connection" detection:
    /// we do not introduce any new wire frames or protocol changes.
    /// The client still discovers deadness via its own KCP dead_link (20
    /// retransmits) or SMUX keepalive timeout. End-to-end 4-10s is accepted.
    fn is_dead(&self) -> bool {
        if self.dead.load(Ordering::Acquire) {
            return true;
        }
        if self.smux.is_closed() {
            return true;
        }
        // Check KCP directly for freshness (mirrors client behavior).
        if self.kcp.lock().is_dead() {
            self.dead.store(true, Ordering::Release);
            return true;
        }
        false
    }

    /// Start the background KCP update/flush loop for this session.
    ///
    /// Event-driven flush loop (notify + next_update, max KCP_UPDATE_INTERVAL_MS) and:
    /// 1. Drains all SMUX streams' send buffers into SMUX Data frames
    /// 2. Sends the frames through KCP
    /// 3. Advances the KCP timer (update + flush)
    fn start_flush_loop(&mut self) {
        let kcp = self.kcp.clone();
        let smux = self.smux.clone();
        let _nocomp = self.nocomp;
        let raw_packets = self.raw_packets.clone();
        let compressor = self.compressor.clone();
        let smuxver = self.smux.version();
        let socket = self.socket.clone();
        let crypt = self.crypt.clone();
        let peer = self.peer;
        let has_encryption = self.has_encryption;
        let has_aead = crypt.is_aead();
        let handled_streams = self.handled_streams.clone();
        let crypto_buf = self.crypto_buf.clone();
        let flush_notify = self.flush_notify.clone();
        let fec_encoder = self.fec_encoder.clone();
        let dead = self.dead.clone();
        let ratelimiter = self.ratelimiter.clone();

        let h = kio::spawn_task(async move {
            let mut next_update: u64 = KCP_UPDATE_INTERVAL_MS;
            // Reused across iterations: single buffer for SMUX frame assembly (P0.3).
            let mut out_buf = BytesMut::with_capacity(64 * 1024);
            // Throttle Phase 0 health checks (~100ms); flush still runs at full rate.
            let mut health_checks_left: u32 = 0;

            loop {
                // Wait for either the dynamic interval (nearest RTO or
                // default) or an immediate notify from SMUX stream writes.
                // notify_one() stores a permit, so there's no lost-wakeup.
                let _ =
                    kio::timeout(Duration::from_millis(next_update), flush_notify.notified()).await;

                // Fresh frame buffer each cycle.
                out_buf.clear();

                // ── Phase 0: dead-link + SMUX keepalive ──
                if smux.is_closed() {
                    dead.store(true, Ordering::Release);
                    break;
                }
                if health_checks_left == 0 {
                    health_checks_left = 50; // ~100ms at 2ms update interval
                    if kcp.lock().is_dead() {
                        error!("KCP dead_link detected for peer {} — closing session", peer);
                        dead.store(true, Ordering::Release);
                        smux.close();
                        break;
                    }
                    if smux.is_keepalive_timeout() {
                        error!("SMUX keepalive timeout for peer {} — closing session", peer);
                        dead.store(true, Ordering::Release);
                        smux.close();
                        break;
                    }
                    if smux.check_keepalive() {
                        let nop = smux.keepalive_frame();
                        nop.encode(&mut out_buf);
                        smux.mark_keepalive_sent();
                        debug!("SMUX: queued NOP keepalive for {}", peer);
                    }
                } else {
                    health_checks_left -= 1;
                }

                // ── Phase 1: Prepare outbound SMUX frames (PSH + FINs + UPDs) ──
                // Unified `prepare_outbound_into` does drain + FIN header encoding + UPDs.
                // Same zero-copy path. Returns stream IDs whose FINs were encoded;
                // mark_fin_sent only after kcp.send succeeds.
                const MAX_DRAIN_BYTES: usize = 64 * 1024;
                let fin_streams: Vec<u32> =
                    smux.prepare_outbound_into(&mut out_buf, MAX_DRAIN_BYTES, smuxver);

                // ── Phase 1a: Clean up closed / stale streams ──
                // Stale = local_closed past linger without peer FIN (proxy short-connect).
                // Also clean up handled_streams to prevent unbounded growth.
                //
                // NOTE: FIN headers and UPDs are already encoded by prepare_outbound_into above.
                // We do NOT re-encode them here. Mark fin_sent only after kcp.send succeeds (below).
                const STREAM_LINGER_SECS: u64 = 30;
                {
                    let streams = smux.streams();
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
                        let mut handled = handled_streams.lock();
                        handled.remove(id);
                    }
                    if !to_remove.is_empty() {
                        debug!("SMUX: reaped {} closed/stale streams", to_remove.len());
                    }
                    drop(stream_map);
                }

                // ── Phase 3: Snappy compress (NO KCP lock held) ──
                // Large flushes → cpu_block; small stay inline (same threshold as client).
                //
                // `send_data` is `Option<bytes::Bytes>` — zero-copy reference-
                // counted slice, avoiding `.to_vec()` copies on both the compress
                // and nocomp paths.
                let send_data = if out_buf.is_empty() {
                    None
                } else if !_nocomp {
                    use std::io::Write;
                    let plain = out_buf.split().freeze();
                    let plain_len = plain.len();
                    let compress_fn = {
                        let compressor = compressor.clone();
                        move || -> bytes::Bytes {
                            let mut enc = compressor.as_ref().map(|c| c.lock()).unwrap();
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
                    let to_send = if kcp_rs::should_cpu_block_compress(plain_len) {
                        kio::cpu_block(compress_fn).await
                    } else {
                        compress_fn()
                    };
                    if to_send.is_empty() {
                        None
                    } else {
                        Some(to_send)
                    }
                } else {
                    let to_send = out_buf.split().freeze();
                    if to_send.is_empty() {
                        None
                    } else {
                        Some(to_send)
                    }
                };

                // ── Phase 4: Send via KCP + update + flush (KCP lock held briefly) ──
                // Wrapped in a block so the MutexGuard is dropped before any
                // .await point (spawn_blocking below) — MutexGuard is !Send.
                {
                    let mut kcp_guard = kcp.lock();
                    let had_outbound = send_data.is_some();
                    if let Some(data) = send_data {
                        let mss = kcp_guard.mss() as usize;
                        let max_chunk = (kcp_rs::segment::KCP_MAX_FRAG as usize)
                            .saturating_sub(1)
                            .saturating_mul(mss)
                            .max(mss);
                        let mut offset = 0;
                        let mut total_sent = 0usize;
                        let mut send_ok = true;
                        while offset < data.len() {
                            let end = (offset + max_chunk).min(data.len());
                            if let Err(e) = kcp_guard.send(&data[offset..end]) {
                                warn!(
                                    "[flush] KCP send error at offset {}/{}: {:?}",
                                    offset,
                                    data.len(),
                                    e
                                );
                                send_ok = false;
                                break;
                            }
                            total_sent += end - offset;
                            offset = end;
                        }
                        // Mark FIN sent only after the entire batch was queued.
                        if send_ok && !fin_streams.is_empty() {
                            let streams = smux.streams();
                            let stream_map = streams.lock();
                            for id in &fin_streams {
                                if let Some(s) = stream_map.get(id) {
                                    s.mark_fin_sent();
                                }
                            }
                        }
                        // Only log when there's backpressure
                        let ws = kcp_guard.wait_send();
                        if ws > 0 {
                            debug!(
                                "[flush] sent {} bytes, wait_send={}, snd_buf={}, snd_queue={}",
                                total_sent,
                                ws,
                                kcp_guard.snd_buf_len(),
                                kcp_guard.snd_queue_len()
                            );
                        }
                    }

                    // Call flush() directly (matching Go's UDPSession.update()
                    // which calls s.kcp.flush() directly, NOT the deprecated
                    // Update() that throttles via ts_flush). This avoids
                    // double-flushing (update() internally calls flush() too).
                    // The return value gives ms until the next meaningful event.
                    next_update = kcp_guard.flush() as u64;
                    // P2.2: pending send or in-flight → 1ms; idle → clamp to max.
                    if had_outbound || kcp_guard.wait_send() > 0 {
                        next_update = 1;
                    } else {
                        next_update = next_update.clamp(1, KCP_UPDATE_INTERVAL_MS);
                    }
                }

                // Batch-encrypt raw KCP packets. Offload to cpu_block only when the
                // batch is large enough that thread-pool scheduling tax is
                // amortized (P0.2). Small batches encrypt inline on this task.
                let packets: Vec<bytes::Bytes> = {
                    let mut g = raw_packets.lock();
                    let n = g.len();
                    let cap = g.capacity();
                    let p = std::mem::take(&mut *g);
                    if cap < n {
                        g.reserve(n - cap);
                    }
                    p
                };
                if packets.is_empty() {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.empty_flush, 1);
                }
                if !packets.is_empty() {
                    // FEC encode (Go postProcess: KCP → FEC → encrypt → UDP).
                    let packets: Vec<bytes::Bytes> = if let Some(ref enc) = fec_encoder {
                        let mut e = enc.lock();
                        kcp_rs::fec_expand_packets(&mut e, &packets, 500)
                    } else {
                        packets
                    };

                    let total_bytes: usize = packets.iter().map(|p| p.len()).sum();
                    let use_cpu_block = kcp_rs::should_cpu_block_encrypt(
                        has_encryption,
                        has_aead,
                        packets.len(),
                        total_bytes,
                        &crypt,
                    );

                    let crypt_sb = crypt.clone();
                    let crypto_buf_sb = crypto_buf.clone();
                    let allow_parallel = !use_cpu_block;
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
                                has_encryption,
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
                            has_encryption,
                            allow_parallel,
                            &mut enc_out,
                        );
                    }
                    let encrypted = enc_out;
                    // Rate limit the send (token bucket, 0 = unlimited).
                    {
                        let total_bytes: usize = encrypted.iter().map(|b| b.len()).sum();
                        ratelimiter.acquire(total_bytes);
                    }

                    match socket.send_batch_to(&encrypted, peer).await {
                        Ok(()) => {
                            let nbytes: u64 = encrypted.iter().map(|b| b.len() as u64).sum();
                            kcp_rs::snmp_add(
                                &kcp_rs::DEFAULT_SNMP.out_pkts,
                                encrypted.len() as u64,
                            );
                            kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.out_bytes, nbytes);
                        }
                        Err(e) => {
                            warn!("UDP send_to error ({}): {}", peer, e);
                            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                                error!("ConnectionRefused for {} — broken socket, closing", peer);
                                smux.close();
                                break;
                            }
                        }
                    }
                }

                // If SMUX still has buffered data *and* peer window allows more
                // send, wake immediately. When peer_send_window==0 we must NOT
                // busy-spin — wait for an UPD (UDP path notifies flush).
                {
                    let streams = smux.streams();
                    let stream_map = streams.lock();
                    let still_pending = stream_map
                        .values()
                        .any(|s| s.pending_send() > 0 && s.peer_send_window() > 0);
                    drop(stream_map);
                    if still_pending {
                        next_update = 1;
                        flush_notify.notify_one();
                    }
                }
            }
        });
        self._handles.push(h);
    }

    /// Owned-data variant of feed_data_mut for cpu_block offload.
    ///
    /// Use this when the caller has decided (via should_cpu_block_decrypt)
    /// that the decrypt + KCP input + SMUX processing for this datagram
    /// should run on the blocking thread pool to avoid stalling the main
    /// UDP receiver task.
    fn feed_data_mut(&self, data: &mut [u8]) {
        // AEAD open still allocates; CFB/null use `data` in place.
        // Branch then call feed_body so lifetimes stay simple.
        if let Some(aead) = self.crypt.as_aead() {
            match aead.open(data) {
                Ok(plain) => self.feed_body(&plain),
                Err(_) => {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                }
            }
            return;
        }
        if self.has_encryption {
            // All ciphers with CFB headers (including xor/salsa20) use the
            // standard 20-byte header. probe_header=false because the header
            // is always present for encrypted traffic (unlike legacy "none").
            match kcp_rs::decrypt_cfb_in_place(data, self.crypt.as_ref(), false) {
                Ok(body) => self.feed_body(body),
                Err(_) => {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                }
            }
            return;
        }
        // null cipher: no crypto header
        self.feed_body(kcp_rs::inbound_null(data));
    }

    /// FEC + KCP input + SMUX for an already-decrypted body slice.
    fn feed_body(&self, body: &[u8]) {
        // FEC decode may allocate recovered shards; data-path KCP input uses
        // slices only (no intermediate to_vec for feed_slice[FEC_HDR..]).
        if let Some(ref fec) = self.fec_decoder {
            const FEC_HDR: usize = 8; // fecHeaderSizePlus2
            let recovered = {
                let mut fec_dec = fec.lock();
                if body.len() >= FEC_HDR {
                    fec_dec.decode(body)
                } else {
                    Vec::new()
                }
            };

            if body.len() >= FEC_HDR + 24 {
                let fec_type = u16::from_le_bytes(body[4..6].try_into().unwrap());
                match fec_type {
                    0x00f1 => {
                        self.kcp_input_and_smux(&body[FEC_HDR..]);
                        // recovered = [SIZE 2][KCP…][RS pad]; Go: Input(r[2:sz])
                        for r in &recovered {
                            if let Some(kcp) = fec_kcp_from_recovered(r) {
                                self.kcp_input_and_smux(kcp);
                            }
                        }
                    }
                    0x00f2 => {
                        for r in &recovered {
                            if let Some(kcp) = fec_kcp_from_recovered(r) {
                                self.kcp_input_and_smux(kcp);
                            }
                        }
                    }
                    0x00f3 => {
                        log::trace!("OOB packet received: {} bytes", body.len());
                        for r in &recovered {
                            if let Some(kcp) = fec_kcp_from_recovered(r) {
                                self.kcp_input_and_smux(kcp);
                            }
                        }
                    }
                    _ => {
                        self.kcp_input_and_smux(body);
                    }
                }
            } else {
                self.kcp_input_and_smux(body);
            }
        } else {
            self.kcp_input_and_smux(body);
        }

        // ── Wake flush loop to send ACKs immediately ──
        // kcp.input() (with ack_nodelay) generates ACK packets into
        // raw_packets via the KCP output callback. Rather than spawning a
        // fire-and-forget task per packet (spawn_task + cpu_block), which
        // creates thousands of micro-tasks/sec under load and overwhelms the
        // tokio runtime, we notify the flush loop. It wakes immediately,
        // drains raw_packets, encrypts in a single batch, and sends via UDP.
        if !self.raw_packets.lock().is_empty() {
            self.flush_notify.notify_one();
        }
    }

    /// Feed one KCP wire slice, then drain `recv_bytes` into SMUX (with optional
    /// Snappy). No intermediate `Vec` of messages — matches client inbound path.
    fn kcp_input_and_smux(&self, slice: &[u8]) {
        let mut kcp = self.kcp.lock();
        let conv = kcp.conv();
        let input_result = kcp.input(slice, self.ack_nodelay);
        if input_result.is_err() {
            kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.kcp_in_errors, 1);
        }
        debug!(
            "feed_data: KCP input result = {:?}, conv=0x{:08x}",
            input_result, conv
        );
        while let Ok(data) = kcp.recv_bytes() {
            debug!("feed_data: KCP recv {} bytes", data.len());
            // Drop KCP lock before snappy/SMUX to reduce contention.
            drop(kcp);
            if !self.nocomp {
                if let Some(ref fb) = self.snappy_fallback {
                    if let Ok(decompressed) = fb.lock().feed(&data) {
                        if !decompressed.is_empty() {
                            if let Err(e) = self.smux.process_data(&decompressed) {
                                debug!("SMUX process_data error: {:?}", e);
                            }
                        }
                    }
                }
            } else if let Err(e) = self.smux.process_data(&data) {
                debug!("SMUX process_data error: {:?}", e);
            }
            kcp = self.kcp.lock();
        }
    }

    /// Check for newly accepted SMUX streams that need TCP handler tasks.
    ///
    /// Returns a list of (stream_id, Arc<Stream>) pairs for streams that
    /// were accepted by the SMUX session but have not yet been dispatched.
    fn drain_new_streams(&self) -> Vec<(u32, Arc<smux_rs::stream::Stream>)> {
        let handled = self.handled_streams.lock().clone();
        let streams = self.smux.streams();
        let stream_map = streams.lock();
        let new_streams: Vec<(u32, Arc<smux_rs::stream::Stream>)> = stream_map
            .iter()
            .filter(|(&id, s)| {
                if handled.contains(&id) {
                    return false;
                }
                // Accept streams that are ready (SYN received) OR have data buffered.
                // A FIN might arrive before the server reads the data, so we must
                // also accept streams with pending data even if state is FinReceived.
                s.is_ready() || s.available() > 0
            })
            .map(|(&id, s)| (id, s.clone()))
            .collect();
        drop(stream_map);

        // Mark as handled
        {
            let mut h = self.handled_streams.lock();
            for (id, _) in &new_streams {
                h.insert(*id);
            }
        }

        new_streams
    }
}
// ─── Main ───────────────────────────────────────────────────────────────────────

/// Get or create a KCP server session for a peer connection.
/// Extracted to avoid block-in-let parsing issues.
fn get_or_create_session(
    sessions: &Arc<DashMap<SocketAddr, Arc<KcpServerSession>>>,
    peer: &SocketAddr,
    buf: &[u8],
    key_arr: &[u8; 32],
    socket: &Arc<kio::DatagramSocket>,
    session_cfg: &SessionConfig,
) -> Arc<KcpServerSession> {
    let datashard = session_cfg.datashard;
    let parityshard = session_cfg.parityshard;
    let crypt_method = session_cfg.crypt.as_str();
    let nocomp = session_cfg.nocomp;
    // Step 1: Try to get an existing session WITHOUT locking (DashMap shard read).
    // If we have a session for this peer but it is dead (KCP dead_link or
    // SMUX keepalive timeout), evict it so a reconnecting client (after server
    // restart or prolonged outage) will get a fresh session. This is the
    // lowest-risk "notice historical connections" behavior: we do not send any
    // new control frames or change the wire format. The client discovers
    // deadness via its own mechanisms (dead_link ~20 retransmits or SMUX
    // keepalive timeout). 4-10s end-to-end detection is acceptable.
    //
    // IMPORTANT: Do not hold the get() guard while calling remove(), as that
    // can deadlock on DashMap shard lock (read guard vs write for remove).
    // Snapshot the liveness first, then remove if needed.
    let need_evict = sessions.get(peer).is_some_and(|s| s.is_dead());
    if need_evict {
        sessions.remove(peer);
    }

    if let Some(s) = sessions.get(peer) {
        return s.clone();
    }

    // Step 2: Extract conv OUTSIDE any lock — this involves decryption
    // which is expensive and must not block other sessions.
    let fec_offset = if datashard > 0 || parityshard > 0 {
        8
    } else {
        0
    };
    let conv = if buf.len() >= 12 && crypt_method == "aes-128-gcm" {
        // AEAD: open the first packet to extract conv via CryptEngine.
        let (engine, _) = kcrypt_rs::CryptEngine::select(crypt_method, &key_arr[..]);
        if let Some(aead) = engine.as_aead() {
            match aead.open(buf) {
                Ok(plain) if plain.len() >= 4 => {
                    // Check for FEC header (matching Go's kcpInput logic)
                    if plain.len() >= 6 {
                        let fec_flag = u16::from_le_bytes([plain[4], plain[5]]);
                        if fec_flag == 0x00f1 || fec_flag == 0x00f2 || fec_flag == 0x00f3 {
                            // FEC header present: conv is after 8-byte FEC header
                            if plain.len() >= 12 {
                                u32::from_le_bytes([plain[8], plain[9], plain[10], plain[11]])
                            } else {
                                0xDEADBEEF
                            }
                        } else {
                            u32::from_le_bytes([plain[0], plain[1], plain[2], plain[3]])
                        }
                    } else {
                        u32::from_le_bytes([plain[0], plain[1], plain[2], plain[3]])
                    }
                }
                _ => 0xDEADBEEF,
            }
        } else {
            0xDEADBEEF
        }
    } else if buf.len() >= 32 + fec_offset && crypt_method != "null" {
        // Go approach: decrypt first, then check FEC flag at data[4..6]
        // to determine conv offset (matching kcp-go's packetInput)
        let mut hdr = buf[..(32 + fec_offset).min(buf.len())].to_vec();
        let (block_crypt, _) = kcrypt_rs::CryptEngine::select(crypt_method, &key_arr[..]);
        block_crypt.decrypt(&mut hdr);
        // After decrypt, strip the standard 20-byte crypto header:
        // [nonce 16B][CRC32 4B] — same for ALL BlockCrypt ciphers including xor/salsa20.
        let crypt_header = 20;
        let payload = &hdr[crypt_header..];
        // Extract conv directly from the KCP segment header.
        // The KCP segment header (including conv) is NOT compressed — only
        // the KCP segment's DATA payload is compressed (and that's decompressed
        // later by SnappyStreamDecoder after kcp.recv()).
        // This matches Go's kcp-go Listener.packetInput which reads conv
        // directly from the KCP segment without any decompression.
        let flag = u16::from_le_bytes([payload[4], payload[5]]);
        let off = if flag == 0x00f1 || flag == 0x00f2 || flag == 0x00f3 {
            8
        } else {
            0
        };
        let conv_val = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap());
        debug!(
            "get_or_create_session: extracted conv=0x{:08x}, nocomp={}",
            conv_val, nocomp
        );
        conv_val
    } else if buf.len() >= 4 {
        // null cipher: no crypto header. Check for FEC header to find conv.
        if buf.len() >= 6 {
            let fec_flag = u16::from_le_bytes([buf[4], buf[5]]);
            if fec_flag == 0x00f1 || fec_flag == 0x00f2 || fec_flag == 0x00f3 {
                // FEC header present: conv is after 8-byte FEC header
                if buf.len() >= 12 {
                    u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]])
                } else {
                    0xDEADBEEF
                }
            } else {
                u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
            }
        } else {
            u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
        }
    } else {
        0xDEADBEEF
    };
    info!("new KCP session from {} (conv=0x{:08x})", peer, conv);
    kcp_rs::DEFAULT_SNMP.session_opened(false);
    let session = Arc::new(KcpServerSession::new(
        conv,
        *peer,
        socket,
        key_arr,
        session_cfg,
    ));
    // Insert with entry API — only locks one shard
    // If another thread inserted a session for this peer while we were
    // creating one, use the existing one and drop ours.
    match sessions.entry(*peer) {
        dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
        dashmap::mapref::entry::Entry::Vacant(e) => {
            let s = session.clone();
            e.insert(session);
            s
        }
    }
}

/// Recv loop for a TCP raw KCP session. Reads datagrams and feeds KCP+SMUX.
#[cfg(target_os = "linux")]
fn spawn_tcp_recv_loop(session: Arc<KcpServerSession>, socket: Arc<kio::DatagramSocket>) {
    let dead = session.dead.clone();
    let flush = session.flush_notify.clone();
    kio::spawn_task(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            if dead.load(Ordering::Acquire) {
                break;
            }
            match socket.recv_from(&mut buf).await {
                Ok((n, _)) if n > 0 => {
                    session.feed_data_mut(&mut buf[..n]);
                    flush.notify_one();
                }
                Err(_) => break,
                _ => {}
            }
        }
    });
}

fn main() -> Result<()> {
    kcp_rs::set_offload_profile(match kio::runtime_kind() {
        kio::RuntimeKind::Tokio => kcp_rs::OffloadProfile::Tokio,
        kio::RuntimeKind::Smol => kcp_rs::OffloadProfile::Smol,
    });
    kio::block_on(async_main())
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

    // Set up logging: redirect to file if --log is specified
    if let Some(ref log_path) = cli.log {
        // Rotate log file if it exceeds 10MB
        rotate_log(log_path, 10 * 1024 * 1024);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        // Logging: controlled by RUST_LOG env var, defaults to "info".
        // Use RUST_LOG=debug for debug output.
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .target(env_logger::Target::Pipe(Box::new(file)))
            .init();
    } else {
        // Logging: controlled by RUST_LOG env var, defaults to "info".
        // Use RUST_LOG=debug for debug output, RUST_LOG=trace for everything.
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .init();
        info!(
            "log level: {} (set RUST_LOG=debug for verbose output)",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        );
    }

    let listen = cli.listen.as_deref().unwrap_or(":29900");
    let target = cli.target.as_deref().unwrap_or("127.0.0.1:12948");

    let key_str = cli.key.as_deref().unwrap();
    let crypt_method = cli.crypt.as_deref().unwrap();
    let mode = cli.mode.as_deref().unwrap();
    let mtu = cli.mtu.unwrap_or(1350);
    let sndwnd = cli.sndwnd.unwrap_or(1024);
    let rcvwnd = cli.rcvwnd.unwrap_or(1024);
    let datashard = cli.datashard;
    let parityshard = cli.parityshard;
    let dscp_val = cli.dscp.unwrap_or(0);
    let sockbuf = cli.sockbuf.unwrap_or(4 * 1024 * 1024);
    let nocomp = cli.nocomp;
    let acknodelay = cli.acknodelay;
    let nodelay = cli.nodelay.unwrap_or(0);
    let interval = cli.interval.unwrap_or(50);
    let resend = cli.resend.unwrap_or(0);
    let nc = cli.nc.unwrap_or(0);
    let smuxver = cli.smuxver.unwrap_or(2);
    let smuxbuf = cli.smuxbuf.unwrap_or(4 * 1024 * 1024);
    let streambuf = cli.streambuf;
    let framesize = cli.framesize;
    let keepalive = cli.keepalive.unwrap_or(10);
    let ratelimit_val = cli.ratelimit;
    let close_wait_val = cli.closewait.unwrap_or(30);
    let quiet = cli.quiet;
    #[cfg(feature = "qpp")]
    let qpp_enabled = cli.qpp;
    #[cfg(not(feature = "qpp"))]
    let qpp_enabled = false;
    #[cfg(feature = "qpp")]
    let qpp_count = cli.qppcount.unwrap_or(61);
    #[cfg(not(feature = "qpp"))]
    let qpp_count: u16 = 0;

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
    info!(
        "key derived: crypt={}, key={:02x}..{:02x}",
        crypt_method, key[0], key[31]
    );

    // Bind listen address(es) — multi-port "host:min-max" matches Go ParseMultiPort.
    let listen_addrs = kcptun_common::parse_multi_port(listen).context("invalid listen address")?;

    // Prepare shared state (needed by both TCP and UDP paths).
    let stop_flag = Arc::new(AtomicBool::new(false));
    let target_str = target.to_string();
    let key_arr = key;
    let session_cfg = SessionConfig {
        crypt: crypt_method.to_string(),
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
        ratelimit: ratelimit_val,
    };

    // TCP mode: accept raw TCP connections, each is a dedicated KCP session.
    // Do NOT also bind UDP on the same ports — that was only for the UDP path
    // and confused logs / wasted sockets when --tcp is set.
    if cli.tcp {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("--tcp requires Linux (raw sockets + TCP_REPAIR)");

        #[cfg(target_os = "linux")]
        {
            let key = key_arr;
            let cfg = session_cfg.clone();
            for &addr in &listen_addrs {
                let listener = kio::tcpraw_listen(&addr)?;
                info!("listening on {} for TCP raw KCP connections", addr);
                let key = key;
                let cfg = cfg.clone();
                kio::spawn_task(async move {
                    loop {
                        let (conn, peer) = match listener.accept().await {
                            Ok(c) => c,
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::Interrupted =>
                            {
                                // Should not happen with blocking accept; retry.
                                kio::sleep_ms(10).await;
                                continue;
                            }
                            Err(e) => {
                                error!("TCP accept error on {}: {}", addr, e);
                                break;
                            }
                        };
                        info!("TCP raw session from {}", peer);
                        let socket = Arc::new(kio::DatagramSocket::TcpRaw(conn));
                        let session = Arc::new(KcpServerSession::new(0, peer, &socket, &key, &cfg));
                        spawn_tcp_recv_loop(session, socket);
                    }
                });
            }
            info!("forwarding to TCP target {}", target);
            if ratelimit_val > 0 {
                info!("ratelimit: {} bytes/sec", ratelimit_val);
            }
            info!("sockbuf: {}", sockbuf);
            // Wait for Ctrl-C.
            loop {
                kio::sleep_ms(500).await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
            }
            info!("bye");
            return Ok(());
        }
    }

    let mut udp_sockets: Vec<Arc<kio::DatagramSocket>> = Vec::with_capacity(listen_addrs.len());
    for addr in &listen_addrs {
        let socket = create_udp_socket(*addr, sockbuf, dscp_val)?;
        let socket = Arc::new(kio::DatagramSocket::Udp(socket));
        info!("listening on {} for KCP connections", addr);
        udp_sockets.push(socket);
    }
    info!("forwarding to TCP target {}", target);
    if ratelimit_val > 0 {
        info!("ratelimit: {} bytes/sec", ratelimit_val);
    }
    if dscp_val > 0 {
        info!("dscp: {}", dscp_val);
    }
    info!("sockbuf: {}", sockbuf);

    // Session map: peer address -> KcpServerSession
    let sessions: Arc<DashMap<SocketAddr, Arc<KcpServerSession>>> = Arc::new(DashMap::new());

    // Start SNMP logger if configured
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

    // Spawn Ctrl-C handler (runtime-agnostic)
    {
        let stop = stop_flag.clone();
        kio::spawn_task(async move {
            let _ = kio::ctrl_c().await;
            stop.store(true, Ordering::Relaxed);
        });
    }

    // ── Per-socket UDP recv tasks ──
    // Spawn one recv loop per UDP socket (matches Go multi-port listener pattern).
    for udp in &udp_sockets {
        let udp_recv = udp.clone();
        let target_str = target_str.clone();
        let sessions = sessions.clone();
        let session_cfg = session_cfg.clone();
        let qpp_key = key_arr.to_vec();
        let stop_flag = stop_flag.clone();

        kio::spawn_task(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            let mut batch_slots: Vec<Vec<u8>> =
                (0..16).map(|_| Vec::with_capacity(MAX_DATAGRAM)).collect();
            let mut batch_extra: Vec<(Vec<u8>, SocketAddr)> = Vec::with_capacity(16);

            // Process one encrypted datagram for a peer.
            let process_datagram = |peer: SocketAddr, data: &mut [u8]| {
                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_pkts, 1);
                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_bytes, data.len() as u64);
                let session = get_or_create_session(
                    &sessions,
                    &peer,
                    data,
                    &key_arr,
                    &udp_recv,
                    &session_cfg,
                );
                let target_str = target_str.clone();
                session.feed_data_mut(data);
                session.flush_notify.notify_one();
                for (stream_id, smux_stream) in session.drain_new_streams() {
                    if !quiet {
                        info!(
                            "accepting stream {} from {} -> target {}",
                            stream_id, peer, target_str
                        );
                    }
                    let target = target_str.clone();
                    let qpp_key = qpp_key.clone();
                    let fn_notify = session.flush_notify.clone();
                    kio::spawn_task(async move {
                        if let Err(e) = handle_stream(
                            target,
                            smux_stream,
                            stream_id,
                            qpp_enabled,
                            qpp_key,
                            qpp_count,
                            quiet,
                            close_wait_val,
                            fn_notify,
                        )
                        .await
                        {
                            error!("stream {} handler error: {:?}", stream_id, e);
                        }
                        if !quiet {
                            info!("stream {} closed", stream_id);
                        }
                    });
                }
            };

            loop {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                match kio::timeout(Duration::from_millis(500), udp_recv.recv_from(&mut buf)).await {
                    Ok(Ok((n, peer))) => {
                        if n == 0 {
                            continue;
                        }
                        process_datagram(peer, &mut buf[..n]);
                        batch_extra.clear();
                        match udp_recv.try_recv_batch_from(&mut batch_slots, &mut batch_extra) {
                            Ok(count) if count > 0 => {
                                for (mut pkt, peer) in batch_extra.drain(..) {
                                    process_datagram(peer, pkt.as_mut_slice());
                                }
                            }
                            Ok(_) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                error!("UDP try_recv_batch_from error: {}", e);
                                kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_errs, 1);
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!("UDP recv_from error: {}", e);
                        kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_errs, 1);
                        continue;
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    // Main task waits for stop signal (Ctrl-C).
    loop {
        kio::sleep_ms(500).await;
        if stop_flag.load(Ordering::Relaxed) {
            info!("received Ctrl+C, shutting down...");
            break;
        }
    }

    // Graceful shutdown
    info!("shutting down...");
    kio::sleep(Duration::from_secs(1)).await;
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
    use bytes::Bytes;

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
    fn test_parse_addr() {
        let addr = parse_addr("127.0.0.1:29900").unwrap();
        assert_eq!(addr.port(), 29900);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn test_parse_addr_ipv6() {
        let addr = parse_addr("[::1]:29900").unwrap();
        assert_eq!(addr.port(), 29900);
    }

    #[test]
    fn test_parse_addr_invalid() {
        assert!(parse_addr("not-an-address").is_err());
    }

    #[test]
    fn test_apply_mode_normal() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "normal");
        assert_eq!(kcp.interval(), 40);
    }

    #[test]
    fn test_apply_mode_fast() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "fast");
        assert_eq!(kcp.interval(), 30);
    }

    #[test]
    fn test_apply_mode_fast3() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "fast3");
        assert_eq!(kcp.interval(), 10);
    }

    #[test]
    fn test_apply_mode_unknown() {
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
        apply_mode(&mut kcp, "unknown");
        // Falls back to "fast" with interval 30
        assert_eq!(kcp.interval(), 30);
    }

    #[test]
    fn test_config_deserialize() {
        let json = r#"{
            "listen": ":29900",
            "target": "127.0.0.1:8080",
            "key": "test-key",
            "crypt": "aes-128",
            "mode": "fast2",
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
        assert_eq!(cfg.listen.as_deref(), Some(":29900"));
        assert_eq!(cfg.target.as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(cfg.mode.as_deref(), Some("fast2"));
        assert_eq!(cfg.smuxver, Some(2));
    }

    #[test]
    fn test_empty_config() {
        let cfg: Config = serde_json::from_str("{}").unwrap();
        assert!(cfg.listen.is_none());
        assert!(cfg.target.is_none());
    }

    #[test]
    fn test_cli_merge() {
        let cli = Cli {
            listen: Some("0.0.0.0:29900".into()),
            target: None,
            key: None,
            crypt: None,
            mode: None,
            ratelimit: 0,
            mtu: None,
            sndwnd: None,
            rcvwnd: None,
            datashard: 10,
            parityshard: 3,
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
            streambuf: 2097152,
            framesize: 8192,
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
            target: Some("127.0.0.1:8080".into()),
            key: Some("cfg-key".into()),
            mtu: Some(1400),
            ..Default::default()
        };
        let merged = Cli::merge(cli, cfg);
        assert_eq!(merged.listen.as_deref(), Some("0.0.0.0:29900"));
        assert_eq!(merged.target.as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(merged.key.as_deref(), Some("cfg-key"));
        assert_eq!(merged.mtu, Some(1400));
    }

    #[test]
    fn test_cli_merge_cli_precedence() {
        let cli = Cli {
            listen: Some("0.0.0.0:29900".into()),
            target: Some("10.0.0.1:8080".into()),
            key: Some("cli-key".into()),
            crypt: None,
            mode: None,
            ratelimit: 0,
            mtu: None,
            sndwnd: None,
            rcvwnd: None,
            datashard: 10,
            parityshard: 3,
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
            streambuf: 2097152,
            framesize: 8192,
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
            target: Some("cfg-target:8080".into()),
            key: Some("cfg-key".into()),
            ..Default::default()
        };
        let merged = Cli::merge(cli, cfg);
        // CLI values take precedence
        assert_eq!(merged.target.as_deref(), Some("10.0.0.1:8080"));
        assert_eq!(merged.key.as_deref(), Some("cli-key"));
    }

    #[test]
    fn test_smux_frame_encode_decode() {
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
        let frame = Frame::new(Cmd::Syn, 0, Bytes::new());
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let (decoded, _) = Frame::decode(&buf).unwrap();
        assert_eq!(decoded.cmd, Cmd::Syn);
        assert_eq!(decoded.stream_id, 0);
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn test_smux_fin_frame() {
        use smux_rs::{Cmd, Frame};
        let frame = Frame::new(Cmd::Fin, 2, Bytes::new());
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let (decoded, _) = Frame::decode(&buf).unwrap();
        assert_eq!(decoded.cmd, Cmd::Fin);
        assert_eq!(decoded.stream_id, 2);
    }

    #[test]
    fn test_kcp_roundtrip() {
        // Verify that KCP can send and receive data via the output callback
        let output_data = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = output_data.clone();

        let mut sender = KCP::new(
            1,
            0,
            Box::new(move |data: bytes::Bytes| {
                out.lock().unwrap().extend_from_slice(&data);
            }),
        );

        sender.set_stream_mode(true);
        sender.send(b"hello kcp server").unwrap();

        // Flush to trigger output callback
        sender.update(100);
        sender.flush();

        // Verify something was emitted
        let sent = output_data.lock().unwrap().clone();
        assert!(!sent.is_empty(), "KCP should have produced output bytes");

        // Feed back into a receiver
        let mut receiver = KCP::new(1, 0, Box::new(|_| {}));
        receiver.set_stream_mode(true);
        receiver.input(&sent, false).unwrap();
        receiver.update(200);

        let recvd = receiver.recv().unwrap();
        assert_eq!(&recvd[..], b"hello kcp server");
    }

    #[test]
    fn test_smux_server_session() {
        let cfg = smux_rs::DEFAULT_CONFIG.clone();
        let session = smux_rs::Session::new_server(&cfg).unwrap();
        assert!(!session.is_closed());
        assert_eq!(session.stream_count(), 0);
    }

    #[test]
    fn test_smux_server_accept_stream() {
        let cfg = smux_rs::DEFAULT_CONFIG.clone();
        let session = smux_rs::Session::new_server(&cfg).unwrap();
        let stream = session.accept_stream(0).unwrap();
        assert_eq!(stream.id(), 0);
        assert!(stream.is_ready());
    }

    #[test]
    fn test_smux_server_process_syn() {
        let cfg = smux_rs::DEFAULT_CONFIG.clone();
        let session = smux_rs::Session::new_server(&cfg).unwrap();

        // Encode a Syn frame
        let syn = smux_rs::Frame::new(smux_rs::Cmd::Syn, 0, Bytes::new());
        let mut buf = BytesMut::new();
        syn.encode(&mut buf);

        // Process it
        let results = session.process_data(&buf).unwrap();
        assert!(results.is_empty()); // Syn doesn't return data

        // Stream should be accepted
        assert_eq!(session.stream_count(), 1);
    }

    #[test]
    fn test_qpp_port_smoke() {
        // Test that QPPPort can encrypt/decrypt a round-trip over a real TCP pair.
        use kio::{AsyncReadExt, AsyncWriteExt};

        let key = b"test-key-for-qpp-smoke-test-32bytes!";
        let listen_addr: SocketAddr = "127.0.0.1:18888".parse().unwrap();
        let connect_addr = listen_addr.to_string();

        kio::block_on(async {
            // Create a TCP listener on a fixed port.
            let listener = kio::TcpListener::bind(listen_addr).await.unwrap();

            let writer = kio::spawn_task(async move {
                let mut a = kio::TcpStream::connect(&connect_addr).await.unwrap();
                a.write_all(b"hello qpp").await.unwrap();
                // Dropping `a` sends a FIN, signaling EOF to the reader.
            });

            let (b, _) = listener.accept().await.unwrap();
            let qpp = QPPPort::new(b, key, 61);
            let mut qpp = qpp;
            let mut result = Vec::new();
            qpp.read_to_end(&mut result).await.unwrap();
            assert!(!result.is_empty(), "should have decrypted data");

            let _ = writer.await;
        });
    }

    fn now_ms() -> u32 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32
    }

    #[test]
    fn test_now_ms() {
        let t1 = now_ms();
        std::thread::sleep(Duration::from_millis(10));
        let t2 = now_ms();
        assert!(t2.wrapping_sub(t1) >= 10);
    }

    #[test]
    fn test_smux_stream_write_read() {
        let stream = smux_rs::stream::Stream::with_buffer(1, 65536);
        stream.push_data(b"hello from server").unwrap();

        let mut buf = [0u8; 64];
        let (n, _) = stream.read(&mut buf).unwrap();
        assert_eq!(n, 17);
        assert_eq!(&buf[..n], b"hello from server");

        stream.write(b"response data").unwrap();
        assert_eq!(stream.pending_send(), 13);
    }

    #[test]
    fn test_kcp_default_config() {
        // Verify KCP starts with reasonable defaults
        let kcp = KCP::new(42, 0, Box::new(|_| {}));
        assert_eq!(kcp.conv(), 42);
        assert!(kcp.mtu() >= 50);
        assert!(kcp.snd_wnd() > 0);
        assert!(kcp.rcv_wnd() > 0);
    }
}

#[test]
fn test_snappy_framing_comparison() {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut enc = snap::write::FrameEncoder::new(&mut buf);
        enc.write_all(b"OK\n").unwrap();
        enc.flush().unwrap();
    }
    eprintln!(
        "Rust framed: {}",
        buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
    );
    // Go produces: ff060000734e61507059010700002598a89a4f4b0a
}
