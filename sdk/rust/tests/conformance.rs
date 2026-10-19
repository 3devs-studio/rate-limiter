//! Runs sdk/conformance/cases.json against a real Redis (REDIS_ADDR, default localhost:6379).
use fairshare::{publish, Error, FairShare, Spec};
use serde_json::Value;
use std::time::Duration;

async fn redis() -> Option<redis::aio::MultiplexedConnection> {
    let addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "localhost:6379".into());
    let client = redis::Client::open(format!("redis://{addr}/")).ok()?;
    client.get_multiplexed_async_connection().await.ok()
}

fn f(v: &Value, k: &str, def: f64) -> f64 {
    v.get(k).and_then(Value::as_f64).unwrap_or(def)
}

#[tokio::test]
async fn conformance() {
    let Some(conn) = redis().await else {
        eprintln!("no Redis, skipping");
        return;
    };
    let suite: Value = serde_json::from_str(include_str!("../../conformance/cases.json")).unwrap();
    let tol = f(&suite, "tolerance", 0.1);
    for case in suite["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let upstream = format!("conformance-rust-{name}");
        let spec: Spec = serde_json::from_value(case["spec"].clone()).unwrap();
        let mut c = conn.clone();
        publish(&mut c, &upstream, &spec).await.unwrap();
        let lim = FairShare::new(conn.clone(), &upstream);
        lim.reset().await.unwrap();
        for (i, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let where_ = format!("{name} step {i}");
            let service = step["service"].as_str().unwrap_or("");
            match step["op"].as_str().unwrap() {
                "acquire" => {
                    let e = &step["expect"];
                    let d = lim.acquire(service, f(step, "cost", 1.0), Duration::from_millis(f(step, "wait_ms", 0.0) as u64)).await.unwrap();
                    assert_eq!(d.allowed, e["allowed"].as_bool().unwrap(), "{where_}: {d:?}");
                    if let Some(m) = e["mode"].as_str() {
                        assert_eq!(d.mode, m, "{where_}");
                    }
                    if let Some(v) = e["service_tokens"].as_f64() {
                        assert!((d.service_tokens - v).abs() <= tol, "{where_}: service_tokens {}", d.service_tokens);
                    }
                    if let Some(v) = e["global_tokens"].as_f64() {
                        assert!((d.global_tokens - v).abs() <= tol, "{where_}: global_tokens {}", d.global_tokens);
                    }
                    if let Some(v) = e["min_waited_ms"].as_f64() {
                        assert!(d.waited.as_millis() as f64 >= v, "{where_}: waited {:?}", d.waited);
                    }
                    if let Some(v) = e["min_retry_after_ms"].as_f64() {
                        assert!(d.retry_after.as_millis() as f64 >= v, "{where_}");
                    }
                    if let Some(v) = e["max_retry_after_ms"].as_f64() {
                        assert!(d.retry_after.as_millis() as f64 <= v, "{where_}");
                    }
                }
                "drain" => {
                    let max = f(step, "max", 1000.0) as usize;
                    let (mut count, mut last) = (0usize, String::new());
                    while count < max {
                        let d = lim.try_acquire(service, 1.0).await.unwrap();
                        last = d.mode.clone();
                        if !d.allowed {
                            break;
                        }
                        count += 1;
                    }
                    assert_eq!(count, f(step, "expect_count", -1.0) as usize, "{where_}");
                    if let Some(m) = step["expect_last_mode"].as_str() {
                        assert_eq!(last, m, "{where_}");
                    }
                }
                "acquire_error" => {
                    assert!(matches!(lim.try_acquire(service, 1.0).await, Err(Error::UnknownService(_))), "{where_}");
                }
                "cooldown" => lim.report_rate_limited(Duration::from_millis(f(step, "ms", 0.0) as u64)).await.unwrap(),
                "sync" => lim.sync_remaining(f(step, "remaining", 0.0)).await.unwrap(),
                "sleep" => tokio::time::sleep(Duration::from_millis(f(step, "ms", 0.0) as u64)).await,
                "reset" => lim.reset().await.unwrap(),
                op => panic!("unknown op {op}"),
            }
        }
    }
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_middleware() {
    use axum::{body::Body, http::Request, routing::get, Router};
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    let Some(conn) = redis().await else {
        return;
    };
    let mut c = conn.clone();
    let spec = Spec { rate: 1.0, burst: 2.0, services: BTreeMap::from([("web".to_string(), fairshare::Share { rate: 1.0, burst: 2.0, weight: None })]) };
    publish(&mut c, "conformance-axum", &spec).await.unwrap();
    let lim = FairShare::new(conn, "conformance-axum");
    lim.reset().await.unwrap();
    let gate = fairshare::axum::Gate::new(lim).shared();
    let app = Router::new()
        .route("/api/x", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(gate, fairshare::axum::middleware));
    let mut codes = vec![];
    let mut mode = String::new();
    for _ in 0..3 {
        let res = app.clone().oneshot(Request::get("/api/x").header("x-service", "web").body(Body::empty()).unwrap()).await.unwrap();
        codes.push(res.status().as_u16());
        mode = res.headers().get("x-fair-share-mode").unwrap().to_str().unwrap().to_string();
    }
    assert_eq!(codes, [200, 200, 429]);
    assert_eq!(mode, "global");
    let open = app.oneshot(Request::get("/api/x").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(open.status(), 200);
}
