//! Client DialOptions, session building, stream handler, and expiry logic.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use log::{info, warn};

use crate::socket;

/// CLI-derived options retained for initial dial and reconnect.
#[derive(Clone, Debug)]
pub(crate) struct ClientDialOptions {
    pub(crate) crypt: String,
    pub(crate) mode: String,
    pub(crate) mtu: u32,
    pub(crate) sndwnd: u32,
    pub(crate) rcvwnd: u32,
    pub(crate) datashard: u32,
    pub(crate) parityshard: u32,
    pub(crate) acknodelay: bool,
    pub(crate) nodelay: u32,
    pub(crate) interval: u32,
    pub(crate) resend: u32,
    pub(crate) nc: u32,
    pub(crate) smuxver: u8,
    pub(crate) smuxbuf: usize,
    pub(crate) streambuf: usize,
    pub(crate) framesize: usize,
    pub(crate) keepalive: u64,
    pub(crate) nocomp: bool,
    pub(crate) ratelimit: u32,
}

/// Build a [`KcptunSession`] for the given remote address and options.
pub(crate) async fn build_session(
    remote: SocketAddr,
    key: &[u8; 32],
    cfg: &ClientDialOptions,
    socket: Arc<kio::DatagramSocket>,
) -> Result<kcptun_common::KcptunSession> {
    let params = kcptun_common::KcpCliParams {
        mode: cfg.mode.clone(),
        mtu: cfg.mtu,
        sndwnd: cfg.sndwnd,
        rcvwnd: cfg.rcvwnd,
        datashard: cfg.datashard,
        parityshard: cfg.parityshard,
        acknodelay: cfg.acknodelay,
        nodelay: cfg.nodelay,
        interval: cfg.interval,
        resend: cfg.resend,
        nc: cfg.nc,
        conv: crate::DEFAULT_CONV,
        token: 0,
    };
    let smux = smux_rs::Config {
        version: cfg.smuxver,
        max_receive_buffer: cfg.smuxbuf,
        max_stream_buffer: cfg.streambuf,
        max_frame_size: cfg.framesize,
        keepalive_interval: cfg.keepalive,
        keepalive_timeout: if cfg.keepalive == 0 {
            0
        } else {
            cfg.keepalive.saturating_mul(3).max(1)
        },
    };
    let config = kcptun_common::KcptunConfig {
        kcp: params.to_kcp_config(),
        smux,
        nocomp: cfg.nocomp,
        rate_limit: cfg.ratelimit,
        offload_profile: match kio::runtime_kind() {
            kio::RuntimeKind::Tokio => kcrypt_rs::OffloadProfile::Tokio,
            kio::RuntimeKind::Smol => kcrypt_rs::OffloadProfile::Smol,
        },
    };
    kcptun_common::KcptunSession::connect(socket, remote, key, &cfg.crypt, &config).await
}

/// Check if a session has expired past its auto-expire + scavenge TTL.
pub(crate) fn is_session_expired(
    session: &kcptun_common::KcptunSession,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
) -> bool {
    is_activity_expired(
        session.last_activity_ms(),
        autoexpire_secs,
        scavengettl_secs,
    )
}

/// Check if `last_activity_ms` is past the deadline.
pub(crate) fn is_activity_expired(
    last_activity_ms: u64,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
) -> bool {
    is_activity_expired_at(
        last_activity_ms,
        autoexpire_secs,
        scavengettl_secs,
        kio::mono_ms(),
    )
}

/// Check if `last_activity_ms` is past the deadline at `now_ms`.
pub(crate) fn is_activity_expired_at(
    last_activity_ms: u64,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
    now_ms: u64,
) -> bool {
    let deadline = last_activity_ms + (autoexpire_secs + scavengettl_secs) * 1000;
    now_ms > deadline
}

/// Handle a single client connection: pipe between local TCP and SMUX stream
/// with optional QPP. Compression is handled at the KCP/SMUX session level
/// (matching Go kcptun architecture).
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "qpp"), allow(unused_variables))]
pub(crate) async fn handle_client(
    local: kio::TcpStream,
    smux_stream: Arc<smux_rs::stream::Stream>,
    qpp_enabled: bool,
    qpp_key: Vec<u8>,
    qpp_count: u16,
    quiet: bool,
    flush_notify: Arc<kio::Notify>,
    closewait: u64,
) -> Result<()> {
    smux_stream.set_flush_notify(flush_notify.clone());
    let smux_async = smux_rs::SmuxIo::new(smux_stream.clone(), flush_notify);

    // Use closewait (from --closewait) as the post-copy grace period.
    // Matches Go kcptun semantics: after both sides reach EOF, wait closewait
    // seconds before tearing down. closewait=0 means no wait (Go client default).
    let pipe_result = if qpp_enabled {
        #[cfg(feature = "qpp")]
        {
            let qpp_port = kcptun_common::QPPPort::new(smux_async, &qpp_key, qpp_count);
            let mut local_pin = local;
            let mut qpp_pin = qpp_port;
            kcptun_common::pipe(&mut local_pin, &mut qpp_pin, closewait).await
        }
        #[cfg(not(feature = "qpp"))]
        {
            unreachable!("qpp_enabled should be false when qpp feature disabled")
        }
    } else {
        let mut local_pin = local;
        let mut smux_pin = smux_async;
        kcptun_common::pipe(&mut local_pin, &mut smux_pin, closewait).await
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
            if !quiet {
                info!("pipe completed: {} sent, {} recv{}", a, b, qpp_suffix);
            }
        }
        Err(e) => {
            warn!("pipe error: {}", e);
        }
    }

    Ok(())
}

/// Reconnect a dead session at the given index, returning true on success.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconnect_session(
    conns: &Arc<parking_lot::Mutex<Vec<kcptun_common::KcptunSession>>>,
    idx: usize,
    remote_addrs: &[SocketAddr],
    key: &[u8; 32],
    session_cfg: &ClientDialOptions,
    tcp: bool,
    sockbuf: u32,
    dscp: u32,
) -> bool {
    let remote = remote_addrs[idx % remote_addrs.len()];
    info!("connection {} is dead, reconnecting to {}...", idx, remote);
    let socket = match socket::create_client_socket(remote, tcp, sockbuf, dscp) {
        Ok(s) => s,
        Err(e) => {
            log::error!("reconnect socket creation failed: {:#}", e);
            return false;
        }
    };
    match build_session(remote, key, session_cfg, socket).await {
        Ok(new_conn) => {
            conns.lock()[idx] = new_conn;
            kcp_rs::DEFAULT_SNMP.session_opened(true);
            info!("connection {} reconnected", idx);
            true
        }
        Err(e) => {
            log::error!("reconnect connection {} failed: {:#}", idx, e);
            false
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_expired_when_recently_active() {
        assert!(!is_activity_expired_at(1_000, 3600, 600, 1_001));
    }

    #[test]
    fn test_is_expired_with_zero_autoexpire() {
        assert!(is_activity_expired_at(1_000, 0, 0, 1_001));
    }

    #[test]
    fn test_is_expired_respects_scavengettl() {
        assert!(!is_activity_expired_at(1_000, 0, 3600, 1_001));
        assert!(is_activity_expired_at(1_000, 0, 0, 1_001));
    }
}
