//! fairshare: fair-share rate limiting straight from Redis.
//!
//! The decision logic is the Lua in `scripts/`, identical to what the rate
//! limiter service runs, so an app using this crate and an app behind the
//! service share the same buckets and the same answers.
//!
//! ```no_run
//! # async fn demo() -> Result<(), fairshare::Error> {
//! let client = redis::Client::open("redis://127.0.0.1/")?;
//! let limiter = fairshare::FairShare::connect(&client, "thirdparty").await?;
//! let d = limiter.acquire("my-app", 1.0, std::time::Duration::from_secs(4)).await?;
//! if !d.allowed { /* back off for d.retry_after */ }
//! # Ok(()) }
//! ```

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[cfg(feature = "axum")]
pub mod axum;

pub const CONFIG_SCHEMA: u32 = 1;

const ACQUIRE: &str = include_str!("../scripts/acquire.lua");
const COOLDOWN: &str = include_str!("../scripts/cooldown.lua");
const SYNC: &str = include_str!("../scripts/sync.lua");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown service: {0}")]
    UnknownService(String),
    #[error("no published config for upstream: {0}")]
    NoConfig(String),
    #[error("published config for {upstream} has schema {found}, expected {expected}")]
    Schema { upstream: String, found: u32, expected: u32 },
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("invalid published config: {0}")]
    Json(#[from] serde_json::Error),
}

/// One service's share of the upstream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Share {
    pub rate: f64,
    pub burst: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
}

/// An upstream's budget and the shares of its services.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Spec {
    pub rate: f64,
    pub burst: f64,
    pub services: BTreeMap<String, Share>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Published {
    schema: u32,
    #[serde(default)]
    upstream: String,
    #[serde(flatten)]
    spec: Spec,
    #[serde(default)]
    published_at: String,
    #[serde(default)]
    published_by: String,
}

/// The outcome of an acquire.
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub allowed: bool,
    /// own, borrow, share, global or cooldown
    pub mode: String,
    pub retry_after: Duration,
    pub waited: Duration,
    pub global_tokens: f64,
    pub service_tokens: f64,
}

pub fn config_key(upstream: &str) -> String {
    format!("rl:{{{upstream}}}:config")
}
fn global_key(upstream: &str) -> String {
    format!("rl:{{{upstream}}}:g")
}
fn service_key(upstream: &str, service: &str) -> String {
    format!("rl:{{{upstream}}}:s:{service}")
}

struct Cached {
    spec: Spec,
    loaded: Instant,
}

/// Admits requests for one upstream. Cheap to clone; clones share the config cache.
#[derive(Clone)]
pub struct FairShare {
    conn: MultiplexedConnection,
    upstream: String,
    config_ttl: Duration,
    cache: Arc<Mutex<Option<Cached>>>,
    acquire: Arc<Script>,
    cooldown: Arc<Script>,
    sync: Arc<Script>,
}

impl FairShare {
    /// Connect to Redis and create a limiter for `upstream`.
    pub async fn connect(client: &redis::Client, upstream: &str) -> Result<Self, Error> {
        Ok(Self::new(client.get_multiplexed_async_connection().await?, upstream))
    }

    /// Create a limiter on an existing connection.
    pub fn new(conn: MultiplexedConnection, upstream: &str) -> Self {
        Self {
            conn,
            upstream: upstream.to_string(),
            config_ttl: Duration::from_secs(10),
            cache: Arc::new(Mutex::new(None)),
            acquire: Arc::new(Script::new(ACQUIRE)),
            cooldown: Arc::new(Script::new(COOLDOWN)),
            sync: Arc::new(Script::new(SYNC)),
        }
    }

    /// How long the published config is cached (default 10 s).
    pub fn with_config_ttl(mut self, ttl: Duration) -> Self {
        self.config_ttl = ttl;
        self
    }

    /// The upstream's shares as published to Redis (cached).
    pub async fn spec(&self) -> Result<Spec, Error> {
        let mut cache = self.cache.lock().await;
        if let Some(c) = cache.as_ref() {
            if c.loaded.elapsed() < self.config_ttl {
                return Ok(c.spec.clone());
            }
        }
        let mut conn = self.conn.clone();
        let raw: Option<String> = match conn.get(config_key(&self.upstream)).await {
            Ok(v) => v,
            Err(e) => {
                if let Some(c) = cache.as_ref() {
                    return Ok(c.spec.clone()); // Redis hiccup: keep what we have
                }
                return Err(e.into());
            }
        };
        let raw = raw.ok_or_else(|| Error::NoConfig(self.upstream.clone()))?;
        let doc: Published = serde_json::from_str(&raw)?;
        if doc.schema != CONFIG_SCHEMA {
            return Err(Error::Schema { upstream: self.upstream.clone(), found: doc.schema, expected: CONFIG_SCHEMA });
        }
        *cache = Some(Cached { spec: doc.spec.clone(), loaded: Instant::now() });
        Ok(doc.spec)
    }

    /// Ask once; never waits.
    pub async fn try_acquire(&self, service: &str, cost: f64) -> Result<Decision, Error> {
        let spec = self.spec().await?;
        let me = spec.services.get(service).ok_or_else(|| Error::UnknownService(service.to_string()))?;
        // Same KEYS/ARGV layout as the service: global, mine, then the others by name.
        let mut inv = self.acquire.prepare_invoke();
        inv.key(global_key(&self.upstream)).key(service_key(&self.upstream, service));
        inv.arg(cost).arg(spec.rate).arg(spec.burst).arg(me.rate).arg(me.burst);
        for (name, other) in &spec.services {
            if name == service {
                continue;
            }
            inv.key(service_key(&self.upstream, name));
            inv.arg(other.rate).arg(other.burst);
        }
        let mut conn = self.conn.clone();
        let (allowed, retry_ms, mode, g, s): (i64, i64, String, String, String) = inv.invoke_async(&mut conn).await?;
        Ok(Decision {
            allowed: allowed == 1,
            mode,
            retry_after: Duration::from_millis(retry_ms.max(0) as u64),
            waited: Duration::ZERO,
            global_tokens: g.parse().unwrap_or(0.0),
            service_tokens: s.parse().unwrap_or(0.0),
        })
    }

    /// Keep asking until a token is granted or `wait` has elapsed.
    pub async fn acquire(&self, service: &str, cost: f64, wait: Duration) -> Result<Decision, Error> {
        let start = Instant::now();
        let mut d = self.try_acquire(service, cost).await?;
        while !d.allowed {
            let elapsed = start.elapsed();
            if elapsed >= wait {
                break;
            }
            let remaining = wait - elapsed;
            let pause = d.retry_after.max(Duration::from_millis(5)).min(remaining).min(Duration::from_secs(1));
            tokio::time::sleep(pause).await;
            d = self.try_acquire(service, cost).await?;
        }
        d.waited = start.elapsed();
        Ok(d)
    }

    /// The upstream answered 429: every service backs off for `retry_after`.
    pub async fn report_rate_limited(&self, retry_after: Duration) -> Result<(), Error> {
        let spec = self.spec().await?;
        let mut conn = self.conn.clone();
        let _: i64 = self
            .cooldown
            .key(global_key(&self.upstream))
            .arg(retry_after.as_millis().max(1) as i64)
            .arg(spec.rate)
            .arg(spec.burst)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Lower the global bucket to what the upstream says is left. Never raises it.
    pub async fn sync_remaining(&self, remaining: f64) -> Result<(), Error> {
        let spec = self.spec().await?;
        let mut conn = self.conn.clone();
        let _: String = self
            .sync
            .key(global_key(&self.upstream))
            .arg(remaining)
            .arg(spec.rate)
            .arg(spec.burst)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Refill every bucket. For demos and tests.
    pub async fn reset(&self) -> Result<(), Error> {
        let spec = self.spec().await?;
        let mut keys = vec![global_key(&self.upstream)];
        keys.extend(spec.services.keys().map(|s| service_key(&self.upstream, s)));
        let mut conn = self.conn.clone();
        let _: i64 = conn.del(keys).await?;
        Ok(())
    }
}

/// Write an upstream's shares to Redis without running the service.
pub async fn publish(conn: &mut MultiplexedConnection, upstream: &str, spec: &Spec) -> Result<(), Error> {
    let doc = Published {
        schema: CONFIG_SCHEMA,
        upstream: upstream.to_string(),
        spec: spec.clone(),
        published_at: String::new(),
        published_by: "fairshare-rust".to_string(),
    };
    let _: () = conn.set(config_key(upstream), serde_json::to_string(&doc)?).await?;
    Ok(())
}

/// Split rate and burst by weight, the way the service does.
pub fn shares_from_weights(rate: f64, burst: f64, weights: &BTreeMap<String, f64>) -> BTreeMap<String, Share> {
    let sum: f64 = weights.values().sum();
    weights
        .iter()
        .map(|(n, w)| (n.clone(), Share { rate: rate * w / sum, burst: (burst * w / sum).max(1.0), weight: Some(*w) }))
        .collect()
}
