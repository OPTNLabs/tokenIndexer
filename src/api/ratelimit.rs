use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug)]
pub struct IpRouteRateLimiter {
    buckets: DashMap<String, Bucket>,
    cleanup_counter: AtomicU64,
    idle_ttl: Duration,
}

impl Default for IpRouteRateLimiter {
    fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }
}

impl IpRouteRateLimiter {
    pub fn new(idle_ttl: Duration) -> Self {
        Self {
            buckets: DashMap::new(),
            cleanup_counter: AtomicU64::new(0),
            idle_ttl,
        }
    }

    pub fn check(&self, client_ip: &str, route_key: &str, rps: u32) -> bool {
        if rps == 0 {
            return false;
        }
        self.maybe_cleanup();

        let now = Instant::now();
        let burst = (rps * 2).max(5) as f64;
        let rate = rps as f64;
        let key = format!("{route_key}:{client_ip}");

        let mut entry = self.buckets.entry(key).or_insert_with(|| Bucket {
            tokens: burst,
            last_refill: now,
        });

        let elapsed = now.duration_since(entry.last_refill).as_secs_f64();
        entry.tokens = (entry.tokens + elapsed * rate).min(burst);
        entry.last_refill = now;

        if entry.tokens >= 1.0 {
            entry.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn maybe_cleanup(&self) {
        let count = self.cleanup_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count % 10_000 != 0 {
            return;
        }
        let idle_ttl = self.idle_ttl;
        let now = Instant::now();
        self.buckets
            .retain(|_, bucket| now.duration_since(bucket.last_refill) < idle_ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::IpRouteRateLimiter;
    use std::time::Duration;

    #[test]
    fn denies_after_burst_capacity() {
        let limiter = IpRouteRateLimiter::new(Duration::from_secs(300));
        let mut allowed = 0usize;
        for _ in 0..15 {
            if limiter.check("127.0.0.1", "holders", 5) {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 10);
        assert!(!limiter.check("127.0.0.1", "holders", 5));
    }
}
