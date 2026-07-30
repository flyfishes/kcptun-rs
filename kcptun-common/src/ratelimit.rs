//! Token-bucket rate limiter for per-connection packet pacing.
//!
//! Matches Go kcptun's use of `golang.org/x/time/rate` applied in the KCP
//! flush loop before `tx()`. Tokens refill at `rate` bytes/second; bursts up
//! to `rate` bytes (1 second of capacity) are permitted. Zero rate = unlimited.

use std::time::{Duration, Instant};

/// A rate limiter that blocks the caller until enough tokens are available.
///
/// Thread-safe (`Mutex`-protected). Designed for per-connection use in the
/// KCP flush/send path. The granularity is ~1ms (spin-wait), matching Go's
/// `rate.Limiter.WaitN` with a comparable sleep resolution.
pub struct RateLimiter {
    inner: parking_lot::Mutex<Inner>,
}

struct Inner {
    /// Configured rate in bytes/sec. 0 = unlimited.
    rate: f64,
    /// Burst size in bytes (typically == rate for 1s capacity).
    burst: f64,
    /// Current token balance (capped at burst).
    tokens: f64,
    /// Last time tokens were refilled.
    last: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// `bytes_per_sec` — maximum sustained rate. 0 means no rate limit.
    /// When non-zero, burst size is set equal to `bytes_per_sec` (matching Go).
    pub fn new(bytes_per_sec: u32) -> Self {
        let rate = bytes_per_sec as f64;
        RateLimiter {
            inner: parking_lot::Mutex::new(Inner {
                rate,
                burst: rate,
                tokens: rate, // start full
                last: Instant::now(),
            }),
        }
    }

    /// Block until `n` bytes can be sent under the rate limit.
    ///
    /// Returns the time spent waiting (0 if unlimited or no wait needed).
    /// Returns immediately if the rate is 0 (unlimited).
    pub fn acquire(&self, n: usize) -> Duration {
        let nf = n as f64;
        if nf == 0.0 {
            return Duration::ZERO;
        }

        // Fast path: check under the lock first, without sleeping.
        let wait = {
            let mut inner = self.inner.lock();
            if inner.rate <= 0.0 {
                return Duration::ZERO;
            }
            inner.refill();
            if inner.tokens >= nf {
                inner.tokens -= nf;
                return Duration::ZERO;
            }
            // Not enough tokens — compute how long to wait.
            // The deficit is (nf - tokens), refill rate is rate/sec.
            let deficit = nf - inner.tokens;
            Duration::from_secs_f64(deficit / inner.rate)
        };

        // Sleep for the computed wait.
        let t0 = Instant::now();
        // Spin-wait in ~1ms increments (matching Go's rate.Limiter resolution).
        // Use a small spin so we don't oversleep by a full scheduling tick.
        let deadline = t0 + wait;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(now);
            if remaining.as_millis() > 1 {
                std::thread::sleep(Duration::from_millis(1));
            } else {
                // Busy-wait for sub-ms precision (rare, only for very high rates).
                std::thread::yield_now();
            }
        }

        // Deduct tokens under the lock (re-fill already happened during the wait).
        {
            let mut inner = self.inner.lock();
            inner.refill();
            if inner.tokens >= nf {
                inner.tokens -= nf;
            } else {
                // Shouldn't happen if we waited correctly, but clamp to zero.
                inner.tokens = 0.0;
            }
        }

        wait
    }

    /// Reset the rate (e.g., on reconfiguration).
    pub fn set_rate(&self, bytes_per_sec: u32) {
        let mut inner = self.inner.lock();
        inner.rate = bytes_per_sec as f64;
        inner.burst = inner.rate;
        inner.tokens = inner.rate.min(inner.burst);
        inner.last = Instant::now();
    }

    /// Current configured rate in bytes/sec (0 = unlimited).
    pub fn rate(&self) -> u32 {
        self.inner.lock().rate as u32
    }
}

impl Inner {
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last);
        self.last = now;
        if elapsed.is_zero() {
            return;
        }
        let added = elapsed.as_secs_f64() * self.rate;
        self.tokens = (self.tokens + added).min(self.burst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_rate_is_unlimited() {
        let lim = RateLimiter::new(0);
        assert_eq!(lim.acquire(1_000_000), Duration::ZERO);
        assert_eq!(lim.acquire(1_000_000_000), Duration::ZERO);
    }

    #[test]
    fn small_burst_passes_immediately() {
        let lim = RateLimiter::new(1_000_000); // 1 MB/s
                                               // Should pass immediately since bucket starts full (1MB).
        assert_eq!(lim.acquire(100_000), Duration::ZERO);
    }

    #[test]
    fn rate_limit_enforces_wait() {
        let lim = RateLimiter::new(1_000_000); // 1 MB/s
                                               // Drain the bucket
        assert_eq!(lim.acquire(1_000_000), Duration::ZERO);
        // Attempt another 100KB — must wait at least ~100ms.
        let wait = lim.acquire(100_000);
        assert!(wait >= Duration::from_millis(80), "wait was {:?}", wait);
        assert!(wait <= Duration::from_millis(500), "wait was {:?}", wait);
    }

    #[test]
    fn set_rate_dynamically() {
        let lim = RateLimiter::new(1_000_000);
        lim.set_rate(2_000_000);
        assert_eq!(lim.rate(), 2_000_000);
        assert_eq!(lim.acquire(2_000_000), Duration::ZERO); // burst = 2MB
    }

    #[test]
    fn zero_n_returns_immediately() {
        let lim = RateLimiter::new(1_000);
        assert_eq!(lim.acquire(0), Duration::ZERO);
    }
}
