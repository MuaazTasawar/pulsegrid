use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use domain::AppError;
use infra::ShardRegistry;
use tokio::task::JoinHandle;

/// This worker's claim on its shards, kept alive with a lease in Redis.
///
/// A worker "owns" a shard only while it keeps renewing the lease. If the
/// process dies, the lease expires on its own and another worker can take the
/// shard; nobody is sent to a dead host forever. Two workers can never hold
/// the same shard: the claim is a single atomic step in Redis.
#[derive(Clone)]
pub struct ShardLease {
    registry: ShardRegistry,
    prefixes: Vec<String>,
    url: String,
    ttl_secs: u64,
    healthy: Arc<AtomicBool>,
}

impl ShardLease {
    pub fn new(registry: ShardRegistry, prefixes: Vec<String>, url: String, ttl_secs: u64) -> Self {
        Self {
            registry,
            prefixes,
            url,
            ttl_secs,
            healthy: Arc::new(AtomicBool::new(false)),
        }
    }

    /// True while every shard's lease was successfully renewed on the last try.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Claims every shard, all or nothing. If another live worker owns one of
    /// them, anything claimed so far is released again and an error naming the
    /// owner is returned.
    pub async fn acquire_all(&self) -> Result<(), AppError> {
        let mut claimed: Vec<&String> = Vec::new();
        for prefix in &self.prefixes {
            match self
                .registry
                .claim_shard(prefix, &self.url, self.ttl_secs)
                .await
            {
                Ok(true) => claimed.push(prefix),
                Ok(false) => {
                    let owner = self
                        .registry
                        .lookup_shard(prefix)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "another worker".to_string());
                    self.release(&claimed).await;
                    return Err(AppError::Conflict(format!(
                        "shard {prefix} is already owned by {owner}; give each worker its own \
                         FANOUT_WORKER_SHARD_PREFIXES (or wait for the old lease to expire)"
                    )));
                }
                Err(e) => {
                    self.release(&claimed).await;
                    return Err(e);
                }
            }
        }
        self.healthy.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn release(&self, prefixes: &[&String]) {
        for prefix in prefixes {
            if let Err(e) = self.registry.release_shard(prefix, &self.url).await {
                tracing::warn!(shard = %prefix, error = ?e, "could not release shard lease");
            }
        }
    }

    /// Renews every lease once. Returns whether this worker still holds all
    /// of them. A lease that expired in the meantime (for example Redis was
    /// restarted) is claimed again; one that another worker took is lost.
    pub async fn refresh_once(&self) -> bool {
        let mut all_held = true;
        for prefix in &self.prefixes {
            match self
                .registry
                .claim_shard(prefix, &self.url, self.ttl_secs)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    all_held = false;
                    tracing::error!(shard = %prefix, "LOST the lease: another worker owns this shard now");
                }
                Err(e) => {
                    all_held = false;
                    tracing::warn!(shard = %prefix, error = ?e, "could not renew the shard lease");
                }
            }
        }
        self.healthy.store(all_held, Ordering::Relaxed);
        all_held
    }

    /// Renews the leases three times per TTL for as long as the process lives.
    pub fn spawn_refresher(&self) -> JoinHandle<()> {
        let lease = self.clone();
        let every = Duration::from_secs((self.ttl_secs / 3).max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.tick().await; // the first tick is immediate; we just acquired
            loop {
                ticker.tick().await;
                lease.refresh_once().await;
            }
        })
    }

    /// Gives the shards up so another worker can take them straight away.
    pub async fn release_all(&self) {
        self.healthy.store(false, Ordering::Relaxed);
        let all: Vec<&String> = self.prefixes.iter().collect();
        self.release(&all).await;
    }
}
