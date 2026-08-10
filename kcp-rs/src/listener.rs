//! KCP server listeners: shared-socket demux ([KcpListener]) and raw-TCP
//! ([KcpTcpListener]), each accepting per-peer [crate::KcpConn]s.
//!
//! [`KcpListener`] supports opt-in resource caps via [`KcpListenerLimits`]
//! (session count, per-peer inbound queue, pending-accept backlog, per-wakeup
//! connection builds and drain, Building/pending timeouts). The defaults are
//! **unlimited** (`0`/`Duration::ZERO`) to match the legacy demux behavior;
//! applications exposing a public socket should set explicit caps to bound
//! memory under attack/churn (see [`KcpListenerBuilder::limits`]).

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::config::{KcpConfig, KcpMode};
use crate::conn::{kcp_config_setters, resolve_one, KcpConn};
use crate::transport::{PacketTransport, PeerQueue, PeerTransport, TransportWrapper, MAX_DATAGRAM};
use kio::CancellationToken;

/// Target size of the non-blocking recv slot pool for one reader drain batch.
/// Linux `recvmmsg` takes up to this many datagrams per syscall.
const RECV_BATCH: usize = 16;
/// Even in unlimited mode, return to the executor after a bounded packet/time
/// quantum so peer input loops and acceptors get runtime service under flood.
const UNLIMITED_DRAIN_QUANTUM: usize = 1_024;
const UNLIMITED_DRAIN_QUANTUM_MS: u128 = 5;
/// Wakeups between periodic lifecycle sweeps (Building/pending timeout reaping).
const SWEEP_INTERVAL: u32 = 64;

/// One connection build collected for the batch commit: peer, generation, its
/// queue, and the `KcpConn::build()` outcome.
type BuildResult = (SocketAddr, u64, Arc<PeerQueue>, Result<KcpConn, ()>);

// ─── KcpListener (multi-peer server) ─────────────────────────────────────────

/// Resource limits for [`KcpListener`], applied by the reader/demux loop.
///
/// **Unlimited by default** (`0` = no limit for count-based fields;
/// `Duration::ZERO` = no timeout): the listener behaves like the pre-limits
/// demux unless the application opts into caps. When a limit is set, it is an
/// advisory drop-tail — the reader never blocks on a peer's queue, so one slow
/// peer cannot stall the shared socket.
#[derive(Debug, Clone, Copy)]
pub struct KcpListenerLimits {
    /// Max concurrent peer sessions (Building + Ready). `0` = unlimited. New
    /// peers beyond this are admission-dropped (their first datagram is
    /// discarded; KCP retransmit retries later, so the drop is recoverable).
    pub max_sessions: usize,
    /// Max accepted-but-un-`accept()`ed connections held in the accept backlog.
    /// `0` = unlimited. When full, a newly built connection is closed and its
    /// session reaped.
    pub max_pending_accepts: usize,
    /// Drop-tail cap on each peer's inbound queue (packets). `0` = unbounded.
    pub max_peer_queue_packets: usize,
    /// Max `KcpConn::build()` calls the reader performs per wakeup (bounds the
    /// connect-storm cost on the demux path without a per-peer task storm).
    /// `0` = build everything queued each wakeup.
    pub max_builds_per_wakeup: usize,
    /// Max wall-clock time the reader spends building connections per wakeup
    /// (bounds a single slow `KcpConn::build` from starving the demux loop).
    /// `Duration::ZERO` = no time limit (only `max_builds_per_wakeup` applies).
    pub max_build_time_per_wakeup: Duration,
    /// Max datagrams routed per reader wakeup (bounds the single `sessions`
    /// critical-section hold and gives KCP/SMUX/accept tasks on the same
    /// runtime room to run under a sustained flood). `0` = unlimited.
    pub max_drain_packets: usize,
    /// A peer stuck in `Building` longer than this is reaped (queue closed,
    /// session dropped; a later reconnect creates a fresh session).
    /// `Duration::ZERO` = no timeout.
    pub building_timeout: Duration,
    /// A built connection sitting un-`accept()`ed longer than this is closed
    /// and its session reaped (application is not draining the backlog).
    /// `Duration::ZERO` = no timeout.
    pub pending_timeout: Duration,
}

impl Default for KcpListenerLimits {
    fn default() -> Self {
        Self {
            max_sessions: 0,
            max_pending_accepts: 0,
            max_peer_queue_packets: 0,
            max_builds_per_wakeup: 0,
            max_build_time_per_wakeup: Duration::ZERO,
            max_drain_packets: 0,
            building_timeout: Duration::ZERO,
            pending_timeout: Duration::ZERO,
        }
    }
}

/// Live snapshot of [`KcpListener`] resource accounting.
#[derive(Debug, Default, Clone, Copy)]
pub struct ListenerStatsSnapshot {
    /// Current peer sessions (Building + Ready).
    pub sessions: usize,
    /// Current accepted-but-un-`accept()`ed connections.
    pub pending: usize,
    /// New sessions rejected because `max_sessions` was reached.
    pub session_drops: u64,
    /// Datagrams dropped by a peer queue at its drop-tail cap.
    pub queue_drops: u64,
    /// Connections closed because `max_pending_accepts` was reached.
    pub pending_drops: u64,
    /// `KcpConn::build` failures.
    pub build_failures: u64,
    /// Building peers reaped by `building_timeout`.
    pub build_timeouts: u64,
    /// Un-accepted connections reaped by `pending_timeout`.
    pub pending_timeouts: u64,
    /// Datagrams that reached the per-wakeup `affected` dedup check.
    pub affected_comparisons: u64,
}

/// Atomic counters behind [`KcpListener::stats`]. Increments are `Relaxed` and
/// only occur on the rare drop/timeout/failure paths, so they add no hot-path
/// contention. `queue_drops` is an `Arc` because it is shared with every
/// [`PeerQueue`].
#[derive(Default)]
struct ListenerStats {
    session_drops: AtomicU64,
    queue_drops: Arc<AtomicU64>,
    pending_drops: AtomicU64,
    build_failures: AtomicU64,
    build_timeouts: AtomicU64,
    pending_timeouts: AtomicU64,
    affected_comparisons: AtomicU64,
}

/// Per-peer demux entry with an explicit lifecycle.
///
/// A new peer is inserted as `Building` *before* its `KcpConn` is built (one
/// sessions critical section), so concurrent datagrams for the same peer route
/// to its queue without triggering a second build. The build completes by
/// atomically upgrading `Building → Ready` under the same generation guard;
/// build failure/timeout only removes a matching generation, never a later
/// reconnect's fresh session.
enum PeerState {
    Building {
        generation: u64,
        queue: Arc<PeerQueue>,
        created_at: Instant,
    },
    Ready {
        queue: Arc<PeerQueue>,
    },
}

/// One accepted-but-not-yet-`accept()`ed connection in the backlog.
struct PendingAccept {
    conn: KcpConn,
    peer: SocketAddr,
    created_at: Instant,
}

/// KCP server listener: binds one UDP socket and demultiplexes inbound
/// datagrams by source address, exposing each peer as a [`KcpConn`] via
/// [`accept`](KcpListener::accept).
pub struct KcpListener {
    socket: Arc<kio::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<kio::Notify>,
    closed: Arc<AtomicBool>,
    /// Cancels the reader's socket `recv` on `close()` so the demux task exits
    /// immediately instead of waiting out the 100ms poll tick (removes the
    /// per-idle-listener timer churn).
    cancel_token: CancellationToken,
    /// Last transport error from the demux reader, surfaced by [`take_error`](KcpListener::take_error).
    last_error: Arc<Mutex<Option<io::Error>>>,
    stats: Arc<ListenerStats>,
    _reader: kio::JoinHandle<()>,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        self.close();
    }
}

impl KcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
        match resolve_one(addr) {
            Ok(a) => KcpListenerBuilder {
                addr: Some(a),
                socket: None,
                config: KcpConfig::default(),
                resolve_err: None,
                transport_wrapper: None,
                limits: None,
            },
            Err(e) => KcpListenerBuilder {
                addr: None,
                socket: None,
                config: KcpConfig::default(),
                resolve_err: Some(e),
                transport_wrapper: None,
                limits: None,
            },
        }
    }

    /// Use an already-bound datagram socket, preserving caller socket options.
    pub fn from_socket(socket: Arc<kio::DatagramSocket>) -> KcpListenerBuilder {
        KcpListenerBuilder {
            addr: None,
            socket: Some(socket),
            config: KcpConfig::default(),
            resolve_err: None,
            transport_wrapper: None,
            limits: None,
        }
    }

    /// Local address of the listen socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Remove a peer from the demultiplexer after its accepted connection ends.
    ///
    /// A later datagram from the same address creates a fresh connection and is
    /// surfaced by [`accept`](Self::accept), matching kcptun reconnect behavior.
    pub fn remove_peer(&self, peer: SocketAddr) -> bool {
        let mut sessions = self.sessions.lock();
        if let Some(state) = sessions.remove(&peer) {
            match state {
                PeerState::Building { queue, .. } | PeerState::Ready { queue, .. } => {
                    queue.mark_closed();
                }
            }
            true
        } else {
            false
        }
    }

    /// Accept the next client connection, returning the per-peer `KcpConn` and
    /// the peer's `SocketAddr`. Returns `ConnectionAborted` once closed.
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)> {
        loop {
            if let Some(v) = self.pending.lock().pop_front() {
                return Ok((v.conn, v.peer));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "KcpListener closed",
                ));
            }
            let notified = self.accept_notify.notified();
            if let Some(v) = self.pending.lock().pop_front() {
                return Ok((v.conn, v.peer));
            }
            notified.await;
        }
    }

    /// Stop accepting new connections. Existing accepted [`KcpConn`]s are
    /// unaffected; the reader task exits on its next tick.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.cancel_token.cancel();
            self.accept_notify.notify_waiters();
        }
    }

    /// Accept the next client connection within `timeout`, or fail with
    /// [`io::ErrorKind::TimedOut`]. Mirrors `tokio::net::TcpListener::accept_timeout`.
    pub async fn accept_timeout(&self, timeout: Duration) -> io::Result<(KcpConn, SocketAddr)> {
        kio::timeout(timeout, self.accept())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timed out"))?
    }

    /// Non-blocking accept: return the next already-pending connection, or
    /// `Ok(None)` when nothing is ready yet. Mirrors
    /// `std::net::TcpListener::try_accept`.
    pub fn try_accept(&self) -> io::Result<Option<(KcpConn, SocketAddr)>> {
        if let Some(v) = self.pending.lock().pop_front() {
            return Ok(Some((v.conn, v.peer)));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpListener closed",
            ));
        }
        Ok(None)
    }

    /// Surface and clear the last transport error from the demux reader.
    /// Mirrors `std::net::TcpListener::take_error`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.last_error.lock().take())
    }

    /// Current number of known peer sessions (Building + Ready).
    pub fn session_count(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Current accepted-but-un-`accept()`ed backlog size.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Live resource-accounting snapshot.
    pub fn stats(&self) -> ListenerStatsSnapshot {
        ListenerStatsSnapshot {
            sessions: self.sessions.lock().len(),
            pending: self.pending.lock().len(),
            session_drops: self.stats.session_drops.load(Ordering::Relaxed),
            queue_drops: self.stats.queue_drops.load(Ordering::Relaxed),
            pending_drops: self.stats.pending_drops.load(Ordering::Relaxed),
            build_failures: self.stats.build_failures.load(Ordering::Relaxed),
            build_timeouts: self.stats.build_timeouts.load(Ordering::Relaxed),
            pending_timeouts: self.stats.pending_timeouts.load(Ordering::Relaxed),
            affected_comparisons: self.stats.affected_comparisons.load(Ordering::Relaxed),
        }
    }
}

/// Builder for [`KcpListener`].
pub struct KcpListenerBuilder {
    addr: Option<SocketAddr>,
    socket: Option<Arc<kio::DatagramSocket>>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
    transport_wrapper: Option<TransportWrapper>,
    limits: Option<KcpListenerLimits>,
}

impl KcpListenerBuilder {
    kcp_config_setters!();

    /// Wrap each accepted peer transport before constructing its [`KcpConn`].
    ///
    /// This is used by upper layers to add encryption while retaining the
    /// listener's single shared-socket reader and per-peer inbound queues.
    pub fn transport_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(Arc<dyn PacketTransport>, SocketAddr) -> Arc<dyn PacketTransport>
            + Send
            + Sync
            + 'static,
    {
        self.transport_wrapper = Some(Arc::new(wrapper));
        self
    }

    /// Override the resource limits (defaults are unlimited; see
    /// [`KcpListenerLimits::default`] and [`KcpListenerLimits`] for the
    /// per-field `0` / `Duration::ZERO` "no limit" semantics).
    pub fn limits(mut self, limits: KcpListenerLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Bind the listen socket, spawn the demux reader, and return the listener.
    pub async fn build(self) -> io::Result<KcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let socket = match self.socket {
            Some(socket) => socket,
            None => {
                let addr = self.addr.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "KcpListener: bind address required",
                    )
                })?;
                Arc::new(kio::DatagramSocket::Udp(kio::UdpSocket::bind(addr)?))
            }
        };
        let limits = self.limits.unwrap_or_default();
        let stats = Arc::new(ListenerStats::default());

        let sessions = Arc::new(Mutex::new(HashMap::<SocketAddr, PeerState>::new()));
        let pending = Arc::new(Mutex::new(VecDeque::<PendingAccept>::new()));
        let accept_notify = Arc::new(kio::Notify::new());
        let closed = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let last_error = Arc::new(Mutex::new(None::<io::Error>));

        let reader = spawn_listener_reader(
            socket.clone(),
            self.config,
            sessions.clone(),
            pending.clone(),
            accept_notify.clone(),
            closed.clone(),
            cancel_token.clone(),
            last_error.clone(),
            self.transport_wrapper,
            limits,
            stats.clone(),
        );

        Ok(KcpListener {
            socket,
            sessions,
            pending,
            accept_notify,
            closed,
            cancel_token,
            last_error,
            stats,
            _reader: reader,
        })
    }
}

/// `KcpListener::bind(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpListenerBuilder {
    type Output = io::Result<KcpListener>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Shared demux state + limits owned by the reader task.
struct ListenerCtx {
    socket: Arc<kio::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<kio::Notify>,
    last_error: Arc<Mutex<Option<io::Error>>>,
    config: KcpConfig,
    transport_wrapper: Option<TransportWrapper>,
    limits: KcpListenerLimits,
    stats: Arc<ListenerStats>,
    cancel_token: CancellationToken,
    generation: AtomicU64,
}

impl ListenerCtx {
    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed)
    }

    /// Route one inbound datagram to its peer's queue (drop-tail bounded) under
    /// an **already-held** sessions lock, recording the queue for a single
    /// end-of-wakeup notify.
    ///
    /// The reader acquires the sessions lock once per wakeup and calls this per
    /// datagram, so a whole burst costs one critical section instead of one per
    /// packet. A brand-new peer is inserted as `Building` before this datagram
    /// is enqueued, so a same-burst second datagram from the same source cannot
    /// trigger a duplicate build.
    ///
    /// Returns a spare buffer to refill the caller's recv slot (allocation-free
    /// steady state — dropped datagrams are recycled, not re-allocated).
    fn route_inner(
        &self,
        sessions: &mut HashMap<SocketAddr, PeerState>,
        peer: SocketAddr,
        data: Vec<u8>,
        build_work: &mut VecDeque<(SocketAddr, u64)>,
        affected: &mut Vec<Arc<PeerQueue>>,
        affected_seen: &mut HashSet<SocketAddr>,
    ) -> Vec<u8> {
        let queue = {
            match sessions.get_mut(&peer) {
                Some(PeerState::Ready { queue, .. }) if !queue.is_closed() => Some(queue.clone()),
                Some(PeerState::Building { queue, .. }) if !queue.is_closed() => {
                    Some(queue.clone())
                }
                _ => {
                    sessions.remove(&peer);
                    None
                }
            }
        };
        let queue = match queue {
            Some(q) => q,
            None => {
                if self.limits.max_sessions > 0 && sessions.len() >= self.limits.max_sessions {
                    // Admission: session table full — drop this datagram but
                    // recycle its buffer as the next recv slot (no fresh
                    // alloc). KCP retransmission retries later, so the drop
                    // itself is recoverable.
                    self.stats.session_drops.fetch_add(1, Ordering::Relaxed);
                    let mut spare = data;
                    spare.resize(MAX_DATAGRAM, 0);
                    return spare;
                }
                let gen = self.next_generation();
                let queue = Arc::new(PeerQueue::new(
                    self.limits.max_peer_queue_packets,
                    self.stats.queue_drops.clone(),
                ));
                sessions.insert(
                    peer,
                    PeerState::Building {
                        generation: gen,
                        queue: queue.clone(),
                        created_at: Instant::now(),
                    },
                );
                build_work.push_back((peer, gen));
                queue
            }
        };
        let (spare, queued) = queue.push_and_reuse(data);
        self.stats
            .affected_comparisons
            .fetch_add(1, Ordering::Relaxed);
        // O(1) per-peer dedup via a reused HashSet (capacity survives
        // `clear()`), replacing the O(n) linear scan on large bursts.
        if queued && affected_seen.insert(peer) {
            affected.push(queue);
        }
        spare
    }

    /// Build queued connections (Building → Ready) in a three-stage batch:
    ///
    /// 1. **Collect** — one `sessions` lock snapshots up to
    ///    `max_builds_per_wakeup` queued `(peer, generation, queue)` entries
    ///    (stale generations dropped);
    /// 2. **Build** — constructs each `KcpConn` lock-free, respecting
    ///    `max_build_time_per_wakeup`;
    /// 3. **Commit** — one `sessions` + `pending` critical section upgrades the
    ///    whole batch to Ready; closed queues and the accept notify run outside
    ///    the lock.
    ///
    /// Batching turns ~2–3 lock acquisitions per peer into ~2 per whole batch,
    /// so a connect storm's demux cost no longer scales linearly with the
    /// number of queued peers.
    async fn process_builds(&self, build_work: &mut VecDeque<(SocketAddr, u64)>) {
        let count_budget = if self.limits.max_builds_per_wakeup == 0 {
            usize::MAX
        } else {
            self.limits.max_builds_per_wakeup
        };
        let time_budget = self.limits.max_build_time_per_wakeup;
        let start = Instant::now();

        // Stage 1 — Collect.
        let mut to_build: Vec<(SocketAddr, u64, Arc<PeerQueue>)> = Vec::new();
        {
            let sessions = self.sessions.lock();
            for _ in 0..count_budget {
                let Some((peer, gen)) = build_work.pop_front() else {
                    break;
                };
                match sessions.get(&peer) {
                    Some(PeerState::Building {
                        generation, queue, ..
                    }) if *generation == gen => to_build.push((peer, gen, queue.clone())),
                    _ => continue, // removed or replaced while queued — skip
                }
            }
        }

        // Stage 2 — Build (lock-free; the reader is single-threaded and builds
        // are mostly synchronous setup + task spawn).
        let mut results: Vec<BuildResult> = Vec::new();
        for (peer, gen, queue) in to_build {
            if !time_budget.is_zero() && start.elapsed() > time_budget {
                // Defer the rest to the next wakeup, preserving their order.
                build_work.push_front((peer, gen));
                continue;
            }
            let transport: Arc<dyn PacketTransport> = Arc::new(PeerTransport {
                queue: queue.clone(),
                socket: self.socket.clone(),
                peer,
            });
            let transport = match &self.transport_wrapper {
                Some(wrapper) => wrapper(transport, peer),
                None => transport,
            };
            let conn = match KcpConn::with_transport(transport, peer)
                .connected(false)
                .adopt_conv(true)
                .config(self.config.clone())
                .build()
                .await
            {
                Ok(conn) => Ok(conn),
                Err(_) => Err(()),
            };
            results.push((peer, gen, queue, conn));
        }

        // Stage 3 — Commit: one sessions + pending critical section for the
        // whole batch; closed queues + accept notify run outside the lock.
        let mut to_close: Vec<Arc<PeerQueue>> = Vec::new();
        let mut notify = false;
        {
            let mut sessions = self.sessions.lock();
            let mut pending = self.pending.lock();
            for (peer, gen, queue, conn) in results {
                // Generation must still be Building; a peer replaced while we
                // were building is not resurrected.
                let still_building = matches!(
                    sessions.get(&peer),
                    Some(PeerState::Building { generation, .. }) if *generation == gen
                );
                if !still_building {
                    to_close.push(queue);
                    continue;
                }
                match conn {
                    Err(_) => {
                        self.stats.build_failures.fetch_add(1, Ordering::Relaxed);
                        sessions.remove(&peer);
                        to_close.push(queue);
                    }
                    Ok(conn) => {
                        if self.limits.max_pending_accepts > 0
                            && pending.len() >= self.limits.max_pending_accepts
                        {
                            // Backlog full: close the connection and reap it.
                            self.stats.pending_drops.fetch_add(1, Ordering::Relaxed);
                            sessions.remove(&peer);
                            to_close.push(queue);
                        } else {
                            sessions.insert(
                                peer,
                                PeerState::Ready {
                                    queue: queue.clone(),
                                },
                            );
                            pending.push_back(PendingAccept {
                                conn,
                                peer,
                                created_at: Instant::now(),
                            });
                            notify = true;
                        }
                    }
                }
            }
        }
        for queue in to_close {
            queue.mark_closed();
        }
        if notify {
            self.accept_notify.notify_one();
        }
    }

    /// Reap stale resources: Building entries past `building_timeout` and
    /// un-accepted connections past `pending_timeout`.
    fn sweep(&self, build_work: &mut VecDeque<(SocketAddr, u64)>) {
        let now = Instant::now();
        // Un-built Building entries past building_timeout. One sessions
        // critical section for the whole sweep (not per entry).
        let mut kept = VecDeque::with_capacity(build_work.len());
        let mut to_close: Vec<Arc<PeerQueue>> = Vec::new();
        {
            let mut sessions = self.sessions.lock();
            for (peer, gen) in build_work.drain(..) {
                // Clone the queue handle out of the borrow before mutating.
                let reap = match sessions.get(&peer) {
                    Some(PeerState::Building {
                        generation,
                        created_at,
                        queue,
                        ..
                    }) if *generation == gen
                        && !self.limits.building_timeout.is_zero()
                        && now.duration_since(*created_at) > self.limits.building_timeout =>
                    {
                        Some(queue.clone())
                    }
                    _ => None,
                };
                if let Some(queue) = reap {
                    sessions.remove(&peer);
                    to_close.push(queue);
                } else {
                    kept.push_back((peer, gen));
                }
            }
        }
        for queue in to_close {
            queue.mark_closed();
            self.stats.build_timeouts.fetch_add(1, Ordering::Relaxed);
        }
        *build_work = kept;

        // Un-accepted Ready conns past pending_timeout.
        let mut expired: Vec<SocketAddr> = Vec::new();
        {
            let mut pending = self.pending.lock();
            pending.retain(|pa| {
                if !self.limits.pending_timeout.is_zero()
                    && now.duration_since(pa.created_at) > self.limits.pending_timeout
                {
                    expired.push(pa.peer);
                    false
                } else {
                    true
                }
            });
        }
        if !expired.is_empty() {
            let mut sessions = self.sessions.lock();
            for peer in expired {
                if let Some(state) = sessions.remove(&peer) {
                    let queue = match state {
                        PeerState::Ready { queue, .. } | PeerState::Building { queue, .. } => queue,
                    };
                    queue.mark_closed();
                    self.stats.pending_timeouts.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Reader task: `recv_from` the shared listen socket, demux by source address,
/// and feed each peer's inbound queue. New / reconnecting peers get a fresh
/// [`KcpConn`] pushed onto the accept queue (bounded, staged build).
fn spawn_listener_reader(
    socket: Arc<kio::DatagramSocket>,
    config: KcpConfig,
    sessions: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<kio::Notify>,
    closed: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    last_error: Arc<Mutex<Option<io::Error>>>,
    transport_wrapper: Option<TransportWrapper>,
    limits: KcpListenerLimits,
    stats: Arc<ListenerStats>,
) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        let ctx = ListenerCtx {
            socket,
            sessions,
            pending,
            accept_notify,
            last_error,
            config,
            transport_wrapper,
            limits,
            stats,
            cancel_token,
            generation: AtomicU64::new(0),
        };
        let mut buf = vec![0u8; MAX_DATAGRAM];
        // Recycled recv-slot pool. On Linux the non-blocking drain is one
        // `recvmmsg` into these slots (allocation-free); routing refills each
        // consumed slot from the peer queue's spare pool. Built once up front
        // (cold-start prealloc) so the first burst is already allocation-free.
        let mut spares: Vec<Vec<u8>> = (0..RECV_BATCH).map(|_| vec![0u8; MAX_DATAGRAM]).collect();
        let mut peers: Vec<SocketAddr> = Vec::with_capacity(8);
        let mut affected: Vec<Arc<PeerQueue>> = Vec::new();
        // Per-wakeup dedup set, reader-task-local. Capacity is retained across
        // `clear()`, so the O(1) dedup does not reallocate per wakeup.
        let mut affected_seen: HashSet<SocketAddr> = HashSet::new();
        let mut build_work: VecDeque<(SocketAddr, u64)> = VecDeque::new();
        let mut sweep_counter: u32 = 0;
        loop {
            if closed.load(Ordering::Acquire) {
                break;
            }
            // Periodic lifecycle sweep + bounded staged build, before the next
            // receive: connection builds happen between drain wakeups so a
            // connect storm cannot monopolize a single drain.
            sweep_counter = sweep_counter.wrapping_add(1);
            if sweep_counter.is_multiple_of(SWEEP_INTERVAL) {
                ctx.sweep(&mut build_work);
            }
            ctx.process_builds(&mut build_work).await;

            // Safety net: a build-error path may recycle a truncated buffer.
            buf.resize(MAX_DATAGRAM, 0);
            // `close()` cancels the socket recv via the cancellation token, so
            // the demux task exits immediately instead of waiting out a 100ms
            // poll tick (no ~10 Hz timer churn per idle listener). The recv
            // itself blocks indefinitely on a quiet socket.
            let (n, peer) = match kio::race(
                Box::pin(ctx.socket.recv_from(&mut buf)),
                ctx.cancel_token.cancelled(),
            )
            .await
            {
                kio::RaceOutcome::First(Ok(v)) => v,
                kio::RaceOutcome::First(Err(e)) => {
                    *ctx.last_error.lock() = Some(e);
                    kio::sleep_ms(10).await;
                    continue;
                }
                kio::RaceOutcome::Second(_) => continue, // close() cancelled → loop exits on closed check
            };
            buf.truncate(n);

            // Route the wakeup's datagrams under ONE sessions critical section
            // per `recvmmsg` batch (v3 §5.1: per-burst lock instead of
            // per-packet), draining the socket in batches and routing each
            // batch in arrival order (preserves per-peer order). Slots are
            // refilled in place
            // after routing, so the steady state is allocation-free. The drain
            // is budgeted so a sustained flood cannot starve KCP/SMUX/accept
            // tasks on the same runtime (v3 §4.2).
            affected.clear();
            affected_seen.clear();
            while spares.len() < RECV_BATCH {
                spares.push(vec![0u8; MAX_DATAGRAM]);
            }
            // The first packet woke this iteration and counts toward any
            // configured packet budget (max_drain_packets=1 must not drain a
            // second packet before yielding).
            let mut drained: usize = 1;
            let drain_started = Instant::now();
            let mut quantum_hit =
                ctx.limits.max_drain_packets > 0 && drained >= ctx.limits.max_drain_packets;
            {
                // First packet (the blocking recv that woke us).
                let mut sessions = ctx.sessions.lock();
                buf = ctx.route_inner(
                    &mut sessions,
                    peer,
                    std::mem::take(&mut buf),
                    &mut build_work,
                    &mut affected,
                    &mut affected_seen,
                );
            }
            while !quantum_hit {
                // `recvmmsg` runs OUTSIDE the sessions lock: a syscall inside
                // the demux critical section makes accept()/stats()/remove_peer()
                // — and `process_builds`' own lock — queue behind it, which is
                // the dominant listener P999 term under a multi-peer flood.
                let recv_cap = if ctx.limits.max_drain_packets > 0 {
                    ctx.limits
                        .max_drain_packets
                        .saturating_sub(drained)
                        .min(spares.len())
                } else {
                    spares.len().min(RECV_BATCH)
                };
                if recv_cap == 0 {
                    quantum_hit = true;
                    break;
                }
                let got = match ctx
                    .socket
                    .try_recv_batch_from_into(&mut spares[..recv_cap], &mut peers)
                {
                    Ok(got) => got,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        *ctx.last_error.lock() = Some(e);
                        break;
                    }
                };
                if got == 0 {
                    break; // WouldBlock — socket drained
                }
                {
                    // Per-batch lock; routing still happens in arrival order,
                    // so per-peer ordering is unchanged.
                    let mut sessions = ctx.sessions.lock();
                    for i in 0..got {
                        let data = std::mem::take(&mut spares[i]);
                        spares[i] = ctx.route_inner(
                            &mut sessions,
                            peers[i],
                            data,
                            &mut build_work,
                            &mut affected,
                            &mut affected_seen,
                        );
                    }
                }
                drained += got;
                quantum_hit = (ctx.limits.max_drain_packets > 0
                    && drained >= ctx.limits.max_drain_packets)
                    || (ctx.limits.max_drain_packets == 0
                        && (drained >= UNLIMITED_DRAIN_QUANTUM
                            || drain_started.elapsed().as_millis() >= UNLIMITED_DRAIN_QUANTUM_MS));
                if quantum_hit {
                    // Defer the rest to the next wakeup; the socket is
                    // still readable, so the next `recv_from` returns at
                    // once and drains the remainder.
                    break;
                }
            }

            // Keep only usable recv slots; trim any admission-dropped empties.
            spares.retain(|s| s.capacity() >= MAX_DATAGRAM);

            // Notify each affected queue ONCE for the whole drain quantum.
            // Batched notify lets each peer input loop process a full burst and
            // batch its ACKs instead of waking mid-drain.
            for q in &affected {
                q.notify_one();
            }
            if quantum_hit {
                // Publish the affected queues before yielding, then give the
                // runtime a scheduling point before receiving the next batch.
                kio::yield_now().await;
            }
        }
    })
}

// ─── KcpTcpListener (server, 1 TCP conn = 1 KCP session) ─────────────────────

/// TCP-mode KCP server listener: each accepted raw-TCP connection becomes its
/// own [`KcpConn`]. Linux only (`kio::TcpRawListener`); non-Linux bind returns
/// `io::Unsupported`.
pub struct KcpTcpListener {
    listener: kio::TcpRawListener,
    config: KcpConfig,
    closed: AtomicBool,
    /// Last accept error, surfaced by [`take_error`](KcpTcpListener::take_error).
    last_error: Mutex<Option<io::Error>>,
}

impl Drop for KcpTcpListener {
    fn drop(&mut self) {
        self.close();
    }
}

impl KcpTcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpTcpListenerBuilder {
        match resolve_one(addr) {
            Ok(a) => KcpTcpListenerBuilder {
                addr: Some(a),
                config: KcpConfig::default(),
                resolve_err: None,
            },
            Err(e) => KcpTcpListenerBuilder {
                addr: None,
                config: KcpConfig::default(),
                resolve_err: Some(e),
            },
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next client connection: one [`KcpConn`] per accepted TCP
    /// connection. Returns `ConnectionAborted` once closed.
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpTcpListener closed",
            ));
        }
        let (conn, peer) = match self.listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                *self.last_error.lock() =
                    Some(io::Error::new(e.kind(), format!("accept failed: {e}")));
                return Err(e);
            }
        };
        let socket: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::TcpRaw(conn));
        let kcp = KcpConn::with_transport(socket, peer)
            .connected(true)
            .config(self.config.clone())
            .build()
            .await?;
        Ok((kcp, peer))
    }

    /// Stop accepting new connections. Existing accepted [`KcpConn`]s are
    /// unaffected (they hold their own raw-fd Arc).
    ///
    /// Limitation: this only flips the internal `closed` flag; it does NOT
    /// abort an `accept()` that is already blocked in the kernel
    /// (`TcpRawListener::accept` runs a blocking `accept(2)` inside
    /// `cpu_block`). A blocked `accept()` only returns once the underlying
    /// listener is dropped (e.g. when this `KcpTcpListener` goes out of scope),
    /// at which point the listener fd is closed and the kernel unblocks it.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {}
    }

    /// Accept the next client connection within `timeout`, or fail with
    /// [`io::ErrorKind::TimedOut`].
    pub async fn accept_timeout(&self, timeout: Duration) -> io::Result<(KcpConn, SocketAddr)> {
        kio::timeout(timeout, self.accept())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timed out"))?
    }

    /// Surface and clear the last accept error. Mirrors
    /// `std::net::TcpListener::take_error`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.last_error.lock().take())
    }
}

/// Builder for [`KcpTcpListener`].
pub struct KcpTcpListenerBuilder {
    addr: Option<SocketAddr>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
}

impl KcpTcpListenerBuilder {
    pub fn config(mut self, cfg: KcpConfig) -> Self {
        self.config = cfg;
        self
    }

    /// Bind the raw-TCP listener and return it.
    pub fn build(self) -> io::Result<KcpTcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let addr = self.addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpTcpListener: bind address required",
            )
        })?;
        let listener = kio::tcpraw_listen(&addr)?;
        Ok(KcpTcpListener {
            listener,
            config: self.config,
            closed: AtomicBool::new(false),
            last_error: Mutex::new(None),
        })
    }
}

/// `KcpTcpListener::bind(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpTcpListenerBuilder {
    type Output = io::Result<KcpTcpListener>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        // `build` is sync (bind + tcpraw_listen); wrap it so `.await` works.
        Box::pin(async move { self.build() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default limits must be unlimited (0 / Duration::ZERO) to match the
    /// legacy demux behavior; caps are opt-in via `KcpListenerBuilder::limits`.
    #[test]
    fn default_limits_are_unlimited() {
        let l = KcpListenerLimits::default();
        assert_eq!(l.max_sessions, 0);
        assert_eq!(l.max_pending_accepts, 0);
        assert_eq!(l.max_peer_queue_packets, 0);
        assert_eq!(l.max_builds_per_wakeup, 0);
        assert!(l.max_build_time_per_wakeup.is_zero());
        assert_eq!(l.max_drain_packets, 0);
        assert!(l.building_timeout.is_zero());
        assert!(l.pending_timeout.is_zero());
    }

    /// Admission drop (session table full) recycles the dropped datagram's
    /// buffer as the next recv slot (no fresh allocation) and does not add the
    /// dropped peer to `affected` (no pointless notify).
    #[test]
    fn route_inner_admission_drop_recycles_buffer() {
        // The tokio `UdpSocket::bind` needs a reactor, so create the socket
        // inside a runtime (route_inner itself is sync and does not use it).
        kio::block_on(async {
            let limits = KcpListenerLimits {
                max_sessions: 1,
                ..KcpListenerLimits::default()
            };
            let ctx = ListenerCtx {
                socket: Arc::new(kio::DatagramSocket::Udp(
                    kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap(),
                )),
                sessions: Arc::new(Mutex::new(HashMap::new())),
                pending: Arc::new(Mutex::new(VecDeque::new())),
                accept_notify: Arc::new(kio::Notify::new()),
                last_error: Arc::new(Mutex::new(None)),
                config: KcpConfig::default(),
                transport_wrapper: None,
                limits,
                stats: Arc::new(ListenerStats::default()),
                cancel_token: CancellationToken::new(),
                generation: AtomicU64::new(0),
            };
            let mut sessions = HashMap::new();
            let mut build_work = VecDeque::new();
            let mut affected: Vec<Arc<PeerQueue>> = Vec::new();
            let mut affected_seen: HashSet<SocketAddr> = HashSet::new();
            let addr_a = SocketAddr::from(([127, 0, 0, 1], 10001));
            let spare_a = ctx.route_inner(
                &mut sessions,
                addr_a,
                vec![0u8; MAX_DATAGRAM],
                &mut build_work,
                &mut affected,
                &mut affected_seen,
            );
            assert_eq!(sessions.len(), 1);
            assert!(spare_a.capacity() >= MAX_DATAGRAM);
            // Peer B is admission-dropped (max_sessions = 1); buffer recycled.
            affected.clear();
            affected_seen.clear();
            let addr_b = SocketAddr::from(([127, 0, 0, 1], 10002));
            let spare_b = ctx.route_inner(
                &mut sessions,
                addr_b,
                vec![0u8; MAX_DATAGRAM],
                &mut build_work,
                &mut affected,
                &mut affected_seen,
            );
            assert_eq!(ctx.stats.session_drops.load(Ordering::Relaxed), 1);
            assert_eq!(spare_b.len(), MAX_DATAGRAM);
            assert!(spare_b.capacity() >= MAX_DATAGRAM);
            assert!(affected.is_empty());
        });
    }

    /// A multi-datagram burst from one peer yields one affected entry (O(1)
    /// HashSet dedup); distinct peers each get an entry, in first-seen order.
    /// Every routed datagram reaches the dedup comparison counter.
    #[test]
    fn route_inner_dedups_affected_per_peer() {
        kio::block_on(async {
            let ctx = ListenerCtx {
                socket: Arc::new(kio::DatagramSocket::Udp(
                    kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap(),
                )),
                sessions: Arc::new(Mutex::new(HashMap::new())),
                pending: Arc::new(Mutex::new(VecDeque::new())),
                accept_notify: Arc::new(kio::Notify::new()),
                last_error: Arc::new(Mutex::new(None)),
                config: KcpConfig::default(),
                transport_wrapper: None,
                limits: KcpListenerLimits::default(),
                stats: Arc::new(ListenerStats::default()),
                cancel_token: CancellationToken::new(),
                generation: AtomicU64::new(0),
            };
            let mut sessions = HashMap::new();
            let mut build_work = VecDeque::new();
            let mut affected: Vec<Arc<PeerQueue>> = Vec::new();
            let mut affected_seen: HashSet<SocketAddr> = HashSet::new();
            let a = SocketAddr::from(([127, 0, 0, 1], 20001));
            let b = SocketAddr::from(([127, 0, 0, 1], 20002));
            for _ in 0..3 {
                ctx.route_inner(
                    &mut sessions,
                    a,
                    vec![0u8; MAX_DATAGRAM],
                    &mut build_work,
                    &mut affected,
                    &mut affected_seen,
                );
            }
            for _ in 0..2 {
                ctx.route_inner(
                    &mut sessions,
                    b,
                    vec![0u8; MAX_DATAGRAM],
                    &mut build_work,
                    &mut affected,
                    &mut affected_seen,
                );
            }
            // One affected entry per distinct peer, in first-seen order.
            assert_eq!(affected.len(), 2);
            assert_eq!(affected_seen.len(), 2);
            // Every routed datagram reached the dedup check.
            assert_eq!(ctx.stats.affected_comparisons.load(Ordering::Relaxed), 5);
        });
    }
}
