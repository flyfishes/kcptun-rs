//! kio — Async runtime + network I/O abstraction for kcptun.
//!
//! Provides a unified async API that compiles under either `tokio`
//! or `smol` feature. Business code calls `kio::sleep_ms`,
//! `kio::spawn_task`, etc. without knowing which runtime is active.
//!
//! ## Features
//!
//! - `tokio` (default): backed by tokio. For high-concurrency public servers.
//! - `smol`: backed by smol + async-executor. For embedded / router clients.
//!
//! The two features are **mutually exclusive**. Enabling both is a compile error.

#![allow(clippy::needless_doctest_main)]

use std::time::Duration;

// ─── Feature mutual-exclusion enforcement ──────────────────────────────────────
#[cfg(all(feature = "tokio", feature = "smol"))]
compile_error!("tokio and smol are mutually exclusive; enable only one");

#[cfg(not(any(feature = "tokio", feature = "smol")))]
compile_error!("Must enable either tokio or smol feature");

// ─── Async I/O trait re-exports ────────────────────────────────────────────────
// tokio and futures-lite define DIFFERENT AsyncRead/AsyncWrite traits (different
// poll_read signatures). Re-export the appropriate one so business code is
// runtime-agnostic at the trait-bound level. Concrete I/O wrapper impls
// (SmuxStreamAsync, QPPPort, etc.) must still be cfg-gated.
#[cfg(feature = "tokio")]
pub use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

#[cfg(feature = "smol")]
pub use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// ─── Runtime-agnostic channel re-export ───────────────────────────────────────
/// Bounded async channel — works on both tokio and smol runtimes.
///
/// Use `kio::bounded(capacity)` to create a sender/receiver pair.
/// `Sender::try_send` is non-blocking; `Receiver::recv` is async.
pub use async_channel::{bounded, Receiver, Sender};

pub mod net;
pub mod sync;
pub mod task;
pub mod time;

// ─── Convenience re-exports ────────────────────────────────────────────────────
pub use net::{tcpraw_dial, tcpraw_listen};
pub use net::{DatagramSocket, TcpListener, TcpStream, UdpSocket};
pub use net::{TcpRawConn, TcpRawListener};
pub use sync::cancel::{race, CancellationToken, Cancelled, Race, RaceOutcome};
pub use sync::Notify;
pub use task::{
    block_on, block_on_local, cpu_block, runtime_kind, spawn_task, yield_now, JoinHandle,
    RuntimeKind,
};
pub use time::{mono_ms, sleep, sleep_ms, timeout, Elapsed};

/// Read a file to a string, using a blocking thread pool to avoid stalling
/// the async runtime. Replaces `tokio::fs::read_to_string`.
pub async fn read_to_string(
    path: impl AsRef<std::path::Path> + Send + 'static,
) -> std::io::Result<String> {
    let path = path.as_ref().to_owned();
    cpu_block(move || std::fs::read_to_string(&path)).await
}

/// Bidirectionally copy data with an **idle** timeout.
///
/// Breaks gracefully when no data flows in either direction for `idle_secs`
/// seconds. The idle timer resets after every data transfer, matching Go
/// kcptun's `closeWait` semantics (an idle/cleanup period, NOT a total pipe
/// duration limit).
///
/// If `idle_secs == 0`, behaves as a plain bidirectional copy without timeout.
pub async fn copy_bidirectional_idle<A, B>(
    a: &mut A,
    b: &mut B,
    idle_secs: u64,
) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    cfg_copy_bidirectional_idle(a, b, idle_secs).await
}

/// Bidirectionally copy data, then wait `postwait_secs` after completion.
///
/// This matches Go kcptun's `closeWait` semantics: data is copied until both
/// sides reach EOF, then the function waits `postwait_secs` seconds before
/// returning. This allows the remote side to receive and acknowledge final
/// data before the connection is torn down.
///
/// If `postwait_secs == 0`, returns immediately after copy completes
/// (no wait). This is the Go default for the client side.
pub async fn copy_bidirectional_postwait<A, B>(
    a: &mut A,
    b: &mut B,
    postwait_secs: u64,
) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let result = cfg_copy_bidirectional(a, b).await;
    if postwait_secs > 0 {
        sleep(Duration::from_secs(postwait_secs)).await;
    }
    result
}

// ─── copy_bidirectional shared state ───────────────────────────────────────────

const BIDI_BUF_SIZE: usize = 65536;

/// Shared state for bidirectional copy — buffers, counters, and EOF flags.
///
/// Used by both tokio and smol backends (which differ in their async
/// concurrency patterns: `tokio::select!` vs `poll_fn`).
struct BidiState {
    buf_a: Box<[u8; BIDI_BUF_SIZE]>,
    buf_b: Box<[u8; BIDI_BUF_SIZE]>,
    /// Pending A→B data range in `buf_a`: [pending_ab, n_a).
    pending_ab: usize,
    n_a: usize,
    /// Pending B→A data range in `buf_b`: [pending_ba, n_b).
    pending_ba: usize,
    n_b: usize,
    total_a_to_b: u64,
    total_b_to_a: u64,
    a_eof: bool,
    b_eof: bool,
}

#[allow(dead_code)] // some methods only used by smol variant
impl BidiState {
    fn new() -> Self {
        Self {
            buf_a: Box::new([0u8; BIDI_BUF_SIZE]),
            buf_b: Box::new([0u8; BIDI_BUF_SIZE]),
            pending_ab: 0,
            n_a: 0,
            pending_ba: 0,
            n_b: 0,
            total_a_to_b: 0,
            total_b_to_a: 0,
            a_eof: false,
            b_eof: false,
        }
    }

    #[inline]
    fn has_pending_ab(&self) -> bool {
        self.pending_ab < self.n_a
    }

    #[inline]
    fn has_pending_ba(&self) -> bool {
        self.pending_ba < self.n_b
    }

    #[inline]
    fn pending_ab_size(&self) -> usize {
        self.n_a - self.pending_ab
    }

    #[inline]
    fn pending_ba_size(&self) -> usize {
        self.n_b - self.pending_ba
    }

    #[inline]
    fn advance_ab(&mut self, n: usize) {
        self.total_a_to_b += n as u64;
        self.pending_ab += n;
    }

    #[inline]
    fn advance_ba(&mut self, n: usize) {
        self.total_b_to_a += n as u64;
        self.pending_ba += n;
    }

    #[inline]
    fn set_ab_read(&mut self, n: usize) {
        self.pending_ab = 0;
        self.n_a = n;
    }

    #[inline]
    fn set_ba_read(&mut self, n: usize) {
        self.pending_ba = 0;
        self.n_b = n;
    }

    #[inline]
    fn reset_ab(&mut self) {
        self.pending_ab = 0;
        self.n_a = 0;
    }

    #[inline]
    fn reset_ba(&mut self) {
        self.pending_ba = 0;
        self.n_b = 0;
    }

    #[inline]
    fn pending_ab_slice(&self) -> &[u8] {
        &self.buf_a[self.pending_ab..self.n_a]
    }

    #[inline]
    fn pending_ba_slice(&self) -> &[u8] {
        &self.buf_b[self.pending_ba..self.n_b]
    }

    #[inline]
    fn a_buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf_a[..]
    }

    #[inline]
    fn b_buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf_b[..]
    }

    #[inline]
    fn both_eof(&self) -> bool {
        self.a_eof && self.b_eof
    }

    fn into_result(self) -> (u64, u64) {
        (self.total_a_to_b, self.total_b_to_a)
    }
}

#[cfg(feature = "tokio")]
async fn cfg_copy_bidirectional<A, B>(a: &mut A, b: &mut B) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    // tokio 多线程运行时优化版本：使用 tokio::select! 而非 poll_fn。
    //
    // 使用 select! 替代 write_all 的优化：
    // - 用 write 替代 write_all，避免写入缓冲区满时完全阻塞循环
    // - write 写入至少 1 字节后返回，select! 可以立即轮询另一方向
    use AsyncWriteExt;

    let mut s = BidiState::new();

    loop {
        if s.both_eof() {
            break;
        }

        // ── 写入待发数据（非阻塞，每次 select! 迭代只写入一个方向） ──
        // 如果两个方向都有待发数据，优先写入数据量较少的（更快完成）。
        if s.has_pending_ab() {
            let write_ab = !s.has_pending_ba() || s.pending_ab_size() <= s.pending_ba_size();
            if write_ab {
                let slice = &s.buf_a[s.pending_ab..s.n_a];
                match b.write(slice).await {
                    Ok(0) => {} // 写入侧满，fall through 到 select!
                    Ok(m) => {
                        s.advance_ab(m);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        if s.has_pending_ba() {
            let slice = &s.buf_b[s.pending_ba..s.n_b];
            match a.write(slice).await {
                Ok(0) => {} // 写入侧满，fall through 到 select!
                Ok(m) => {
                    s.advance_ba(m);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        // ── 无待发数据，从 A 或 B 读取 ──
        // tokio::select! 要求各分支的 borrow 不重叠，直接引用字段而非通过 &mut self 方法。
        tokio::select! {
            result = async {
                if s.a_eof { std::future::pending::<std::io::Result<usize>>().await }
                else { a.read(&mut s.buf_a[..]).await }
            } => {
                match result {
                    Ok(0) => {
                        s.a_eof = true;
                        let _ = b.shutdown().await;
                    }
                    Ok(n) => s.set_ab_read(n),
                    Err(e) => return Err(e),
                }
            }
            result = async {
                if s.b_eof { std::future::pending::<std::io::Result<usize>>().await }
                else { b.read(&mut s.buf_b[..]).await }
            } => {
                match result {
                    Ok(0) => {
                        s.b_eof = true;
                        let _ = a.shutdown().await;
                    }
                    Ok(n) => s.set_ba_read(n),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    Ok(s.into_result())
}

#[cfg(feature = "smol")]
async fn cfg_copy_bidirectional<A, B>(a: &mut A, b: &mut B) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    use futures_lite::future::poll_fn;
    use std::pin::Pin;
    use std::task::Poll;

    let mut s = BidiState::new();
    let mut error: Option<std::io::Error> = None;

    while !s.both_eof() && error.is_none() {
        poll_fn(|cx| {
            let mut progress = false;

            // Write pending A→B data to B
            while s.has_pending_ab() {
                match Pin::new(&mut *b).poll_write(cx, s.pending_ab_slice()) {
                    Poll::Ready(Ok(n)) if n > 0 => {
                        s.advance_ab(n);
                        progress = true;
                    }
                    Poll::Ready(Ok(_)) => break, // n == 0
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.a_eof = true;
                        progress = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            // Read from A if no pending data
            if !s.has_pending_ab() && !s.a_eof && error.is_none() {
                s.reset_ab();
                match Pin::new(&mut *a).poll_read(cx, s.a_buf_mut()) {
                    Poll::Ready(Ok(0)) => {
                        s.a_eof = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(n)) => {
                        s.set_ab_read(n);
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.a_eof = true;
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }

            // Write pending B→A data to A
            while s.has_pending_ba() {
                match Pin::new(&mut *a).poll_write(cx, s.pending_ba_slice()) {
                    Poll::Ready(Ok(n)) if n > 0 => {
                        s.advance_ba(n);
                        progress = true;
                    }
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.b_eof = true;
                        progress = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            // Read from B if no pending data
            if !s.has_pending_ba() && !s.b_eof && error.is_none() {
                s.reset_ba();
                match Pin::new(&mut *b).poll_read(cx, s.b_buf_mut()) {
                    Poll::Ready(Ok(0)) => {
                        s.b_eof = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(n)) => {
                        s.set_ba_read(n);
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.b_eof = true;
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }

            // Close write side when the corresponding read side hits EOF
            if s.a_eof {
                let _ = Pin::new(&mut *b).poll_close(cx);
            }
            if s.b_eof {
                let _ = Pin::new(&mut *a).poll_close(cx);
            }

            if progress {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    if let Some(e) = error {
        return Err(e);
    }
    Ok(s.into_result())
}

// ─── copy_bidirectional_idle backend implementations ──────────────────────────

#[cfg(feature = "tokio")]
async fn cfg_copy_bidirectional_idle<A, B>(
    a: &mut A,
    b: &mut B,
    idle_secs: u64,
) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    if idle_secs == 0 {
        return cfg_copy_bidirectional(a, b).await;
    }

    use AsyncReadExt;
    use AsyncWriteExt;

    let mut s = BidiState::new();
    let idle_duration = Duration::from_secs(idle_secs);
    let mut idle_deadline = tokio::time::Instant::now() + idle_duration;

    loop {
        if s.both_eof() {
            break;
        }

        let mut data_flowed = false;

        tokio::select! {
            result = async {
                if s.a_eof { std::future::pending::<std::io::Result<usize>>().await }
                else { a.read(&mut s.buf_a[..]).await }
            } => {
                match result {
                    Ok(0) => {
                        s.a_eof = true;
                        let _ = b.shutdown().await;
                    }
                    Ok(n) => {
                        b.write_all(&s.buf_a[..n]).await?;
                        s.total_a_to_b += n as u64;
                        data_flowed = true;
                    }
                    Err(e) => return Err(e),
                }
            }
            result = async {
                if s.b_eof { std::future::pending::<std::io::Result<usize>>().await }
                else { b.read(&mut s.buf_b[..]).await }
            } => {
                match result {
                    Ok(0) => {
                        s.b_eof = true;
                        let _ = a.shutdown().await;
                    }
                    Ok(n) => {
                        a.write_all(&s.buf_b[..n]).await?;
                        s.total_b_to_a += n as u64;
                        data_flowed = true;
                    }
                    Err(e) => return Err(e),
                }
            }
            _ = tokio::time::sleep_until(idle_deadline) => {
                break;
            }
        }

        if data_flowed {
            idle_deadline = tokio::time::Instant::now() + idle_duration;
        }
    }

    Ok(s.into_result())
}

#[cfg(feature = "smol")]
async fn cfg_copy_bidirectional_idle<A, B>(
    a: &mut A,
    b: &mut B,
    idle_secs: u64,
) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    if idle_secs == 0 {
        return cfg_copy_bidirectional(a, b).await;
    }

    use futures_lite::future::poll_fn;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Poll;

    let mut s = BidiState::new();
    let mut error: Option<std::io::Error> = None;
    let idle_duration = Duration::from_secs(idle_secs);
    let mut idle = async_io::Timer::after(idle_duration);

    while !s.both_eof() && error.is_none() {
        let result = poll_fn(|cx| -> Poll<Option<bool>> {
            let mut progress = false;
            let mut data_flowed = false;

            while s.has_pending_ab() {
                match Pin::new(&mut *b).poll_write(cx, s.pending_ab_slice()) {
                    Poll::Ready(Ok(n)) if n > 0 => {
                        s.advance_ab(n);
                        data_flowed = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.a_eof = true;
                        progress = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            if !s.has_pending_ab() && !s.a_eof && error.is_none() {
                s.reset_ab();
                match Pin::new(&mut *a).poll_read(cx, s.a_buf_mut()) {
                    Poll::Ready(Ok(0)) => {
                        s.a_eof = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(n)) => {
                        s.set_ab_read(n);
                        data_flowed = true;
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.a_eof = true;
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }

            while s.has_pending_ba() {
                match Pin::new(&mut *a).poll_write(cx, s.pending_ba_slice()) {
                    Poll::Ready(Ok(n)) if n > 0 => {
                        s.advance_ba(n);
                        data_flowed = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.b_eof = true;
                        progress = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            if !s.has_pending_ba() && !s.b_eof && error.is_none() {
                s.reset_ba();
                match Pin::new(&mut *b).poll_read(cx, s.b_buf_mut()) {
                    Poll::Ready(Ok(0)) => {
                        s.b_eof = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(n)) => {
                        s.set_ba_read(n);
                        data_flowed = true;
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        error = Some(e);
                        s.b_eof = true;
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }

            if s.a_eof {
                let _ = Pin::new(&mut *b).poll_close(cx);
            }
            if s.b_eof {
                let _ = Pin::new(&mut *a).poll_close(cx);
            }

            if progress {
                return Poll::Ready(Some(data_flowed));
            }

            match Pin::new(&mut idle).poll(cx) {
                Poll::Ready(_) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;

        match result {
            Some(true) => idle.set_after(idle_duration),
            Some(false) => {}
            None => break,
        }
    }

    if let Some(e) = error {
        return Err(e);
    }
    Ok(s.into_result())
}

/// Wait for Ctrl-C (SIGINT). Uses a dedicated blocking thread with a libc
/// signal handler, so it works on both runtimes without tokio::signal.
pub async fn ctrl_c() -> std::io::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};

    static CTRL_C_FIRED: AtomicBool = AtomicBool::new(false);
    static INSTALLED: std::sync::Once = std::sync::Once::new();

    INSTALLED.call_once(|| {
        // Install a minimal SIGINT handler that sets a flag.
        // On non-Unix targets this is a no-op.
        #[cfg(unix)]
        // SAFETY: the installed handler only performs an atomic store. It does
        // not allocate, lock, or perform I/O while running in signal context.
        unsafe {
            libc::signal(
                libc::SIGINT,
                sigint_handler as *const () as libc::sighandler_t,
            );
        }
    });

    #[cfg(unix)]
    extern "C" fn sigint_handler(_sig: i32) {
        CTRL_C_FIRED.store(true, Ordering::SeqCst);
    }

    // Poll the flag — cheap and avoids complex async signal machinery.
    loop {
        if CTRL_C_FIRED.load(Ordering::SeqCst) {
            return Ok(());
        }
        sleep_ms(100).await;
    }
}

/// Ignore SIGPIPE to prevent process termination when writing to a closed
/// socket/pipe. Matches Go kcptun's `signal.Ignore(syscall.SIGPIPE)`.
///
/// Call once at process startup. On non-Unix targets this is a no-op.
pub fn ignore_sigpipe() {
    #[cfg(unix)]
    // SAFETY: SIG_IGN is a valid process-wide disposition for SIGPIPE and no
    // Rust data is accessed by a signal callback.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

/// Install a handler for SIGUSR1 that logs KCP SNMP statistics.
/// Matches Go kcptun's SIGUSR1 → `log.Printf("KCP SNMP:%+v", kcp.DefaultSnmp.Copy())`.
///
/// Call once at process startup. On non-Unix targets this is a no-op.
pub fn install_sigusr1_handler() {
    #[cfg(unix)]
    {
        extern "C" fn sigusr1_handler(_sig: i32) {
            // Read process-wide counters — minimal work inside signal handler.
            // The actual log output is deferred to the next SNMP poll or
            // handled by reading the static counters from user code.
            // SAFETY: only reads AtomicU64 fields; safe in signal context.
            crate::SIGUSR1_FIRED.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        // SAFETY: the installed handler only performs an atomic store. It does
        // not allocate, lock, or perform I/O while running in signal context.
        unsafe {
            libc::signal(
                libc::SIGUSR1,
                sigusr1_handler as *const () as libc::sighandler_t,
            );
        }
    }
}

/// True if SIGUSR1 was received since the last call to this function.
/// Resets the flag on each call (one-shot semantics matching Go).
pub fn sigusr1_received() -> bool {
    SIGUSR1_FIRED.swap(false, std::sync::atomic::Ordering::SeqCst)
}

#[cfg(unix)]
static SIGUSR1_FIRED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(not(unix))]
static SIGUSR1_FIRED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod tests;
