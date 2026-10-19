//! axum middleware. Enable with the `axum` feature.
//!
//! ```ignore
//! use axum::middleware::from_fn_with_state;
//! let gate = fairshare::axum::Gate::new(limiter).service_from_header("x-service").wait(Duration::from_secs(4));
//! let app = Router::new()
//!     .route("/api/me", get(me))
//!     .layer(from_fn_with_state(gate.shared(), fairshare::axum::middleware));
//! ```

use crate::FairShare;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::{HeaderValue, StatusCode};
use std::sync::Arc;
use std::time::Duration;

type ServiceFn = dyn Fn(&Request) -> Option<String> + Send + Sync;

/// Configuration for the middleware.
pub struct Gate {
    limiter: FairShare,
    service: Box<ServiceFn>,
    wait: Duration,
    cost: f64,
    fail_open: bool,
}

impl Gate {
    pub fn new(limiter: FairShare) -> Self {
        Self { limiter, service: Box::new(|r| header(r, "x-service")), wait: Duration::ZERO, cost: 1.0, fail_open: true }
    }
    /// Resolve the calling service from a request header.
    pub fn service_from_header(mut self, name: &'static str) -> Self {
        self.service = Box::new(move |r| header(r, name));
        self
    }
    /// Resolve the calling service however you like; `None` lets the request through unmanaged.
    pub fn service_with(mut self, f: impl Fn(&Request) -> Option<String> + Send + Sync + 'static) -> Self {
        self.service = Box::new(f);
        self
    }
    /// How long a request may wait for a token before 429.
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
    pub fn cost(mut self, cost: f64) -> Self {
        self.cost = cost;
        self
    }
    /// Let requests through when Redis is down or the service is unknown (default true).
    pub fn fail_open(mut self, v: bool) -> Self {
        self.fail_open = v;
        self
    }
    /// The state to pass to `axum::middleware::from_fn_with_state(.., fairshare::axum::middleware)`.
    pub fn shared(self) -> Arc<Gate> {
        Arc::new(self)
    }
}

fn header(r: &Request, name: &str) -> Option<String> {
    r.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// The middleware function: admit or refuse the request on behalf of its service.
pub async fn middleware(State(gate): State<Arc<Gate>>, req: Request, next: Next) -> Response {
    let Some(service) = (gate.service)(&req) else {
        return next.run(req).await;
    };
    let d = match gate.limiter.acquire(&service, gate.cost, gate.wait).await {
        Ok(d) => d,
        Err(_) if gate.fail_open => {
            let mut res = next.run(req).await;
            res.headers_mut().insert("x-fair-share-mode", HeaderValue::from_static("unavailable"));
            return res;
        }
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, "rate limiter unavailable").into_response(),
    };
    let mut res = if d.allowed {
        next.run(req).await
    } else {
        let body = format!(
            "{{\"error\":\"Too Many Requests\",\"service\":\"{service}\",\"mode\":\"{}\",\"retryAfterMs\":{}}}",
            d.mode,
            d.retry_after.as_millis()
        );
        let mut res = Response::new(Body::from(body));
        *res.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        res.headers_mut().insert("content-type", HeaderValue::from_static("application/json"));
        res.headers_mut().insert("retry-after", HeaderValue::from((d.retry_after.as_secs()).max(1)));
        res
    };
    let h = res.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&service) {
        h.insert("x-fair-share-service", v);
    }
    if let Ok(v) = HeaderValue::from_str(&d.mode) {
        h.insert("x-fair-share-mode", v);
    }
    h.insert("x-fair-share-wait-ms", HeaderValue::from(d.waited.as_millis() as u64));
    res
}
