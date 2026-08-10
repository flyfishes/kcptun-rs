//! Acceptor + Worker sharding prototype (macOS / non-SO_REUSEPORT platforms).
//!
//! One shared UDP fd is read by a single **Acceptor** task, which hashes each
//! peer to one of `G` workers. Each worker runs its own current-thread tokio
//! runtime (`kio::block_on_local`) and drives its peers' KCP input via
//! [`kcp_rs::KcpConn::feed_input`] (`background_input(false)`), so:
//!
//! - a peer's whole task set (input / read_loop / flush / SMUX / TCP pipe)
//!   stays on one worker → smol-style serial I/O, no cross-thread scheduling;
//! - `G` workers run in parallel;
//! - only `G` workers ever call `send` on the shared fd → multi-worker socket
//!   contention is bounded by `G` (vs 16 for the multi-thread runtime).
//!
//! This is the user-space analogue of SO_REUSEPORT sharding for platforms that
//! do not hash-distribute (Darwin), and is a proof-of-concept for the
//! Acceptor → channel → Worker design.
//!
//! Usage (null crypt only, matching `--crypt null --nocomp` bench config):
//!
//! ```sh
//! cargo run --release --example acceptor_worker -- -l :29900 -t 127.0.0.1:12948 -w 4
//! ```

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kcp_rs::{KcpConn, KcpMode, PacketTransport};

/// Stable, cheap peer → worker hash (IPv4 fast path; string hash for v6).
fn hash_peer(peer: SocketAddr) -> usize {
    let ip: u64 = match peer {
        SocketAddr::V4(a) => u32::from(*a.ip()) as u64,
        SocketAddr::V6(a) => match a.ip().to_ipv4_mapped() {
            Some(v4) => u32::from(v4) as u64,
            None => {
                let oct = a.ip().octets();
                (oct[0] as u64)
                    .wrapping_add((oct[8] as u64) << 8)
                    .wrapping_add((oct[15] as u64) << 16)
            }
        },
    };
    let mut h = 2166136261u64;
    h = (h ^ ip).wrapping_mul(16777619);
    h = (h ^ peer.port() as u64).wrapping_mul(16777619);
    (h >> 32) as usize ^ h as usize
}

/// Transport for one worker's peer: sends go out the shared fd to `peer`; recv
/// is never used because the Acceptor feeds input via `KcpConn::feed_input`.
struct WorkerTransport {
    socket: Arc<kio::DatagramSocket>,
    peer: SocketAddr,
}

#[async_trait::async_trait]
impl PacketTransport for WorkerTransport {
    async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "acceptor feeds input",
        ))
    }
    fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "acceptor feeds input",
        ))
    }
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.socket.send_batch_to(packets, self.peer).await
    }
    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        self.socket.send_batch_to(packets, target).await
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

/// Accept loop for one session: accept SMUX streams, connect the TCP target,
/// and pipe bidirectionally. Runs on the worker's runtime (`kio::spawn_task`
/// inside a `block_on_local` context stays on that worker).
fn spawn_stream_loop(
    session: Arc<kcptun_common::KcptunSession>,
    target: String,
    flush_notify: Arc<kio::Notify>,
    close_wait: u64,
) {
    kio::spawn_task(async move {
        loop {
            let stream = match session.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };
            let target = target.clone();
            let notify = flush_notify.clone();
            kio::spawn_task(async move {
                match kio::TcpStream::connect(&target).await {
                    Ok(tcp) => {
                        stream.set_flush_notify(notify.clone());
                        let smux_io = smux_rs::SmuxIo::new(stream.clone(), notify);
                        let mut a = tcp;
                        let mut b = smux_io;
                        let _ = kcptun_common::pipe(&mut a, &mut b, close_wait).await;
                    }
                    Err(_) => {}
                }
            });
        }
        session.close();
    });
}

/// One worker: current-thread runtime, drives the KCP input of its peers.
fn spawn_worker(
    rx: async_channel::Receiver<(SocketAddr, Vec<u8>)>,
    socket: Arc<kio::DatagramSocket>,
    config: kcptun_common::KcptunConfig,
    target: String,
    close_wait: u64,
) {
    std::thread::Builder::new()
        .name("acceptor-worker".into())
        .spawn(move || {
            kio::block_on_local(async move {
                let mut peers: HashMap<SocketAddr, Arc<KcpConn>> = HashMap::new();
                let mut sessions: HashMap<SocketAddr, Arc<kcptun_common::KcptunSession>> =
                    HashMap::new();
                loop {
                    // Gather a batch: block for the first datagram, then drain
                    // up to 31 more non-blocking, so each peer's conn gets a
                    // whole burst in one KCP lock + one ACK flush.
                    let mut batch: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
                    match rx.recv().await {
                        Ok(v) => batch.push(v),
                        Err(_) => break,
                    }
                    while batch.len() < 32 {
                        match rx.try_recv() {
                            Ok(v) => batch.push(v),
                            Err(_) => break,
                        }
                    }
                    // Group by peer → build conn on first sight → feed_batch.
                    let mut groups: HashMap<SocketAddr, Vec<Vec<u8>>> = HashMap::new();
                    for (peer, data) in batch {
                        groups.entry(peer).or_default().push(data);
                    }
                    for (peer, datas) in groups {
                        if !peers.contains_key(&peer) {
                            let transport: Arc<dyn PacketTransport> = Arc::new(WorkerTransport {
                                socket: socket.clone(),
                                peer,
                            });
                            let conn = match KcpConn::with_transport(transport, peer)
                                .connected(false)
                                .adopt_conv(true)
                                .background_input(false)
                                .config(config.kcp.clone())
                                .build()
                                .await
                            {
                                Ok(c) => Arc::new(c),
                                Err(e) => {
                                    log::warn!("peer {} conn build failed: {}", peer, e);
                                    continue;
                                }
                            };
                            let session = Arc::new(
                                kcptun_common::KcptunSession::server((*conn).clone(), &config)
                                    .expect("server session"),
                            );
                            let flush_notify = session.flush_notify();
                            spawn_stream_loop(
                                session.clone(),
                                target.clone(),
                                flush_notify,
                                close_wait,
                            );
                            sessions.insert(peer, session);
                            peers.insert(peer, conn.clone());
                        }
                        // Drive the burst through decrypt→FEC→KCP→read_buf. The
                        // session read_loop (same worker) picks up user data.
                        if let Some(conn) = peers.get(&peer) {
                            if let Err(e) = conn.feed_batch(datas) {
                                log::warn!("feed_batch {}: {}", peer, e);
                            }
                        }
                    }
                }
            });
        })
        .expect("spawn worker");
}

/// Single Acceptor task: read the shared fd, hash the peer, fan out to workers.
async fn acceptor_task(
    socket: Arc<kio::DatagramSocket>,
    senders: Vec<async_channel::Sender<(SocketAddr, Vec<u8>)>>,
) {
    let mut buf = vec![0u8; 65536];
    loop {
        let (n, peer) =
            match kio::timeout(Duration::from_millis(100), socket.recv_from(&mut buf)).await {
                Ok(Ok(v)) => v,
                _ => continue, // 100ms tick; closed check handled by caller
            };
        let data = buf[..n].to_vec();
        let g = hash_peer(peer) % senders.len();
        let _ = senders[g].try_send((peer, data));
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut listen = ":29900".to_string();
    let mut target = "127.0.0.1:12948".to_string();
    let mut workers = 4usize;
    let close_wait = 0u64;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-l" | "--listen" => {
                i += 1;
                listen = args[i].clone();
            }
            "-t" | "--target" => {
                i += 1;
                target = args[i].clone();
            }
            "-w" | "--workers" => {
                i += 1;
                workers = args[i].parse()?;
            }
            "-h" | "--help" => {
                println!("acceptor_worker -l :PORT -t host:port -w WORKERS");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let listen: SocketAddr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("bad listen: {}", e))?;

    let config = kcptun_common::KcptunConfig {
        kcp: kcp_rs::KcpConfig {
            sndwnd: 2048,
            rcvwnd: 2048,
            datashard: 0,
            parityshard: 0,
            mode: KcpMode::Fast,
            ..kcp_rs::KcpConfig::default()
        },
        smux: smux_rs::Config {
            keepalive_interval: 0,
            keepalive_timeout: 0,
            ..smux_rs::DEFAULT_CONFIG.clone()
        },
        nocomp: true,
        rate_limit: 0,
        offload_profile: kcrypt_rs::OffloadProfile::Tokio,
    };

    // Bind inside a runtime context: tokio UdpSocket needs a reactor.
    kio::block_on(async move {
        let socket = Arc::new(kio::DatagramSocket::Udp(kio::UdpSocket::bind(listen)?));
        log::info!(
            "listening on {} for KCP connections ({} workers, null crypt)",
            listen,
            workers
        );

        let mut senders = Vec::with_capacity(workers);
        let mut receivers = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = async_channel::unbounded();
            senders.push(tx);
            receivers.push(rx);
        }

        // Acceptor on the process-wide runtime.
        let acc_socket = socket.clone();
        kio::spawn_task(acceptor_task(acc_socket, senders));

        // Workers: one OS thread + current-thread runtime each.
        for rx in receivers {
            spawn_worker(
                rx,
                socket.clone(),
                config.clone(),
                target.clone(),
                close_wait,
            );
        }

        // Keep the process alive.
        loop {
            kio::sleep_ms(500).await;
        }
    })
}
