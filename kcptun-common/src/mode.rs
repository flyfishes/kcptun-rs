use kcp_rs::KCP;
use log::{info, warn};

/// Apply a mode profile to a KCP instance.
pub fn apply_mode(kcp: &mut KCP, mode: &str) {
    let (nodelay, interval, resend, nc) = match mode {
        "normal" => (0, 40, 2, 1),
        "fast" => (0, 30, 2, 1),
        "fast2" => (1, 20, 2, 1),
        "fast3" => (1, 10, 2, 1),
        _ => {
            warn!("unknown mode '{}', falling back to 'fast'", mode);
            (0, 30, 2, 1)
        }
    };
    info!(
        "applying mode '{}': nodelay={}, interval={}, resend={}, nc={}",
        mode, nodelay, interval, resend, nc
    );
    kcp.set_nodelay(nodelay, interval, resend, nc);
}
