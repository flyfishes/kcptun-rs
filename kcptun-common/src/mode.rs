use kcp_rs::{KcpMode, KCP};
use log::{info, warn};

/// Apply a mode profile to a KCP instance.
///
/// Mode curves live in `kcp-rs` ([`KcpMode`] / [`KCP::set_mode`] / [`KCP::apply`]).
/// This helper maps CLI strings and keeps the historical unknown→`fast` fallback.
/// Prefer `KCP::apply(&KcpConfig)` for new library code.
pub fn apply_mode(kcp: &mut KCP, mode: &str) {
    let (mode_enum, nodelay, interval, resend, nc) = match mode {
        "normal" => (KcpMode::Normal, 0, 40, 2, 1),
        "fast" => (KcpMode::Fast, 0, 30, 2, 1),
        "fast2" => (KcpMode::Fast2, 1, 20, 2, 1),
        "fast3" => (KcpMode::Fast3, 1, 10, 2, 1),
        _ => {
            // Keep historical fallback to *fast* (not Manual) for unknown names.
            warn!("unknown mode '{}', falling back to 'fast'", mode);
            (KcpMode::Fast, 0, 30, 2, 1)
        }
    };
    info!(
        "applying mode '{}': nodelay={}, interval={}, resend={}, nc={}",
        mode, nodelay, interval, resend, nc
    );
    kcp.set_mode(mode_enum, nodelay, interval, resend, nc);
}
