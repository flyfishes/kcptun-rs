//! Post-copy wait bidirectional pipe (Go `closeWait` semantics).

use std::io;

use kio::AsyncRead;
use kio::AsyncWrite;

/// Bidirectional copy between two AsyncRead + AsyncWrite streams.
///
/// Copies data until both sides reach EOF, then waits `closewait_secs`
/// seconds before returning. This matches Go kcptun's `closeWait` behavior:
/// data is fully transferred, then a grace period allows the remote side
/// to receive and acknowledge final data before the connection closes.
///
/// If `closewait_secs == 0`, returns immediately after copy completes.
pub async fn pipe<A, B>(a: &mut A, b: &mut B, closewait_secs: u64) -> io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    kio::copy_bidirectional_postwait(a, b, closewait_secs).await
}
