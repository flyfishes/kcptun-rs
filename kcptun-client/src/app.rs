//! Application lifecycle: async_main, configuration, and main accept loop.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyContext, Result};
use clap::Parser;
use log::{error, info};

use crate::cli::{Cli, Config};
use crate::client::{self, ClientDialOptions};
use crate::socket;

/// Main async entry point — configuration, session setup, accept loop, and
/// graceful shutdown.  Mirrors the Go kcptun client lifecycle.
pub(crate) async fn async_main() -> Result<()> {
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
    if let Some(ref log_path) = cli.log.as_ref().filter(|s| !s.is_empty()) {
        crate::rotate_log(log_path, 10 * 1024 * 1024);
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
    let remote_addr_str = cli.remoteaddr.as_deref().unwrap_or("vps:29900");

    let key_str = cli.key.as_deref().unwrap_or("it's a secrect");
    let crypt = cli.crypt.as_deref().unwrap_or("aes");
    let mode = cli.mode.as_deref().unwrap_or("fast");
    let conn_count = cli.conn.unwrap_or(1).max(1);
    let mtu = cli.mtu.unwrap_or(1350);
    let sndwnd = cli.sndwnd.unwrap_or(128);
    let rcvwnd = cli.rcvwnd.unwrap_or(512);
    let datashard = cli.datashard.unwrap_or(10);
    let parityshard = cli.parityshard.unwrap_or(3);
    let nocomp = cli.nocomp;
    let quiet = cli.quiet;
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
    let closewait = cli.closewait.unwrap_or(0).max(0) as u64;
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
                    log::warn!("{}", w);
                }
            }
            Err(e) => {
                error!("QPP configuration error: {}", e);
                return Err(anyhow::anyhow!("QPP: {}", e));
            }
        }
    }

    // Derive encryption key
    let key = kcptun_common::derive_key(key_str);
    let session_cfg = ClientDialOptions {
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
        keepalive: keepalive.max(0) as u64,
        nocomp,
        ratelimit,
    };

    info!(
        "key derived: crypt={}, key={:02x}..{:02x}",
        crypt, key[0], key[31]
    );

    // Parse remote addresses (supports multi-port format)
    let remote_addrs = kcptun_common::parse_multi_port(remote_addr_str)?;

    if !cli.tcp {
        info!("using shared kcptun session stack");
    }

    // Create KCP connection pool (shared with scavenger for auto-expire)
    let conns: Arc<parking_lot::Mutex<Vec<kcptun_common::KcptunSession>>> = Arc::new(
        parking_lot::Mutex::new(Vec::with_capacity(conn_count as usize)),
    );
    if cli.tcp {
        // TCP mode: single connection (TCP is point-to-point).
        let remote = remote_addrs[0];
        info!("creating TCP raw KCP connection -> {}", remote);
        let socket = socket::create_client_socket(remote, true, sockbuf, dscp)?;
        let conn = client::build_session(remote, &key, &session_cfg, socket).await?;
        kcp_rs::DEFAULT_SNMP.session_opened(true);
        conns.lock().push(conn);
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
            let socket = socket::create_client_udp_socket(remote, sockbuf, dscp)?;
            let socket = Arc::new(kio::DatagramSocket::Udp(socket));
            let conn = client::build_session(remote, &key, &session_cfg, socket).await?;
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
    if let Some(ref snmplog_path) = cli.snmplog {
        let secs = cli.snmpperiod.unwrap_or(60).max(0) as u64;
        if secs > 0 && !snmplog_path.is_empty() {
            kcp_rs::snmp_enable();
            let period = Duration::from_secs(secs);
            let s = stop_flag.clone();
            let p = snmplog_path.clone();
            kio::spawn_task(async move {
                kcptun_common::snmp_logger(p, period, s).await;
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
        let scavenge_autoexpire = autoexpire.max(0) as u64;
        let scavenge_ttl = scavengettl.max(0) as u64;
        kio::spawn_task(async move {
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
                    if !conn.is_dead()
                        && client::is_session_expired(conn, scavenge_autoexpire, scavenge_ttl)
                    {
                        info!("scavenger: closing expired connection");
                        conn.close();
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

        let (local, peer) = match kio::timeout(Duration::from_millis(500), listener.accept()).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                error!("accept error: {}", e);
                continue;
            }
            Err(_) => continue, // timeout, loop back to check stop_flag
        };

        // Process the accepted connection, then drain any already-queued
        // connections in the same wakeup. Without this, a burst of concurrent
        // connections is serialized behind per-connection reactor wakeups
        // (measured ~90ms stall on the 2nd accept under the first burst).
        let mut pending = Some((local, peer));
        while let Some((local, peer)) = pending.take() {
            if stop_flag.load(Ordering::Relaxed) {
                info!("shutting down, rejecting new connection from {}", peer);
                break;
            }

            let idx = round_robin.fetch_add(1, Ordering::Relaxed) % conn_count_usize;

            // Ensure a live KCP/SMUX session (Go muxSession.Open auto-redial).
            let mut opened: Option<Arc<smux_rs::stream::Stream>> = None;
            for _attempt in 0..2 {
                let needs_reconnect = {
                    let guard = conns.lock();
                    guard[idx].is_dead()
                };
                if needs_reconnect {
                    let ok = client::reconnect_session(
                        &conns,
                        idx,
                        &remote_addrs,
                        &key,
                        &session_cfg,
                        cli.tcp,
                        sockbuf,
                        dscp,
                    )
                    .await;
                    if !ok {
                        break;
                    }
                }

                let stream_result = {
                    let guard = conns.lock();
                    let c = &guard[idx];
                    match c.open_stream() {
                        Ok(stream) => Some(stream),
                        Err(e) => {
                            error!("failed to open SMUX stream: {:?}", e);
                            c.close();
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
            if !quiet {
                info!("accepted connection from {} (stream {})", peer, stream_id);
            }

            let flush_notify_ref = {
                let guard = conns.lock();
                guard[idx].flush_notify()
            };

            let qpp_key = key.to_vec();
            kio::spawn_task(async move {
                if let Err(e) = client::handle_client(
                    local,
                    smux_stream,
                    qpp_enabled,
                    qpp_key,
                    qpp_count,
                    quiet,
                    flush_notify_ref,
                    closewait,
                )
                .await
                {
                    error!("client handler error (stream {}): {:?}", stream_id, e);
                }
                if !quiet {
                    info!("stream {} closed", stream_id);
                }
            });

            // Drain any other connections already queued in the same wakeup.
            pending = listener.try_accept().ok();
        }
    }

    // Graceful shutdown
    info!("shutting down...");
    kio::sleep_ms(1000).await;
    info!("bye");

    Ok(())
}
