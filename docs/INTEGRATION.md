# Integration guide

How to put an application behind the rate limiter, per language. Every snippet
is the bare minimum: no SDK, just the HTTP API. All of them assume the limiter
runs at `http://localhost:8080` with an upstream called `thirdparty` and a
service called `my-app` registered in its config:

```yaml
upstreams:
  thirdparty:
    base_url: https://api.example.com
    rate: 90
    burst: 10
    services:
      my-app: { weight: 1 }
```

> Prefer a package over HTTP calls? Every language below also has an SDK that
> runs the same Lua straight against Redis, with a middleware included. See
> [../sdk/README.md](../sdk/README.md). The snippets here need no package at all.

## Contents

- [Proxy mode: change a URL](#proxy-mode-change-a-url) (no code)
- [Acquire mode: the contract](#acquire-mode-the-contract)
- Snippets by language:
  [Go](#go) ·
  [TypeScript](#typescript-node-or-bun) ·
  [Python](#python) ·
  [Java](#java-spring-boot-java-11-httpclient) ·
  [Kotlin](#kotlin-ktor) ·
  [Rust](#rust-reqwest--axum) ·
  [Ruby on Rails](#ruby-on-rails-rack-middleware)
- [Things every integration should do](#things-every-integration-should-do)

There are two ways in. Pick one per call site.

| | Proxy mode | Acquire mode |
|---|---|---|
| What you change | The base URL of the HTTP client you already use, plus one header | Add one POST before the protected work |
| Code | None | A few lines |
| Queueing when the budget is spent | The limiter stores and replays the request | The limiter holds your POST (`wait`) until a token is free |
| Use when | You call a third party over HTTP | You are the thing being protected (a gateway or API admitting its own callers), or the call cannot be proxied |

## Proxy mode: change a URL

Instead of `https://api.example.com/v1/things`, call
`http://localhost:8080/v1/proxy/thirdparty/v1/things` and send `X-Service: my-app`.
Method, path, query, headers and body are forwarded unchanged; you get the
third party's real response back, with `X-RateLimiter-Queued` and
`X-RateLimiter-Wait-Ms` added. If the request had to wait longer than
`X-Wait` (default 30 s) you get `202` and a `Location` to poll.

```bash
curl -H 'X-Service: my-app' http://localhost:8080/v1/proxy/thirdparty/v1/things?limit=10
```

That is the whole integration for most languages: configure the client's base
URL and default headers, which every HTTP library supports.

## Acquire mode: the contract

```
POST /v1/acquire/{upstream}
{"service": "my-app", "cost": 1, "wait": "4s"}

200 {"allowed": true,  "mode": "own" | "borrow", "waited_ms": 0, ...}   proceed
429 {"allowed": false, "retry_after_ms": 180, ...}  + Retry-After header  back off
403 unknown service, 404 unknown upstream
```

`wait` is optional. Without it the answer is immediate. With it, the limiter
holds the call until a token is free or the wait elapses, so the caller gets
fair queueing without proxying its request through the limiter.

The snippets below each define one `acquire` function and then use it either
as middleware (admit or refuse incoming callers, like a gateway does) or as a
guard before an outbound call. The Go, TypeScript and Python snippets were run
against a live limiter; the Java, Kotlin, Rust and Ruby ones follow the same
shape but were not executed, so treat them as a starting point.

### Go

```go
package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Decision struct {
	Allowed      bool   `json:"allowed"`
	Mode         string `json:"mode"`
	RetryAfterMs int64  `json:"retry_after_ms"`
	WaitedMs     int64  `json:"waited_ms"`
}

var client = &http.Client{Timeout: 10 * time.Second}

func Acquire(ctx context.Context, limiter, upstream, service string, wait time.Duration) (Decision, error) {
	body, _ := json.Marshal(map[string]any{"service": service, "cost": 1, "wait": wait.String()})
	req, _ := http.NewRequestWithContext(ctx, "POST", limiter+"/v1/acquire/"+upstream, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return Decision{}, err
	}
	defer resp.Body.Close()
	var d Decision
	if resp.StatusCode != 200 && resp.StatusCode != 429 {
		return d, fmt.Errorf("limiter: %s", resp.Status)
	}
	return d, json.NewDecoder(resp.Body).Decode(&d)
}

// Middleware admits or refuses incoming callers identified by a header.
func Middleware(limiter, upstream string, wait time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		service := r.Header.Get("X-Service")
		d, err := Acquire(r.Context(), limiter, upstream, service, wait)
		if err != nil { // limiter unreachable: fail open
			next.ServeHTTP(w, r)
			return
		}
		if !d.Allowed {
			w.Header().Set("Retry-After", fmt.Sprint(d.RetryAfterMs/1000+1))
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("X-Fair-Share-Mode", d.Mode)
		next.ServeHTTP(w, r)
	})
}
```

```go
// Guard an outbound call instead:
d, err := ratelimit.Acquire(ctx, "http://localhost:8080", "thirdparty", "my-app", 4*time.Second)
if err == nil && !d.Allowed {
	return fmt.Errorf("rate limited, retry in %dms", d.RetryAfterMs)
}
resp, err := http.Get("https://api.example.com/v1/things")
```

[Back to contents](#contents)

### TypeScript (Node or Bun)

```ts
// ratelimit.ts
export type Decision = { allowed: boolean; mode: string; retry_after_ms: number; waited_ms: number };

export async function acquire(limiter: string, upstream: string, service: string, waitMs = 0): Promise<Decision> {
  const res = await fetch(`${limiter}/v1/acquire/${upstream}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ service, cost: 1, wait: `${waitMs}ms` }),
    signal: AbortSignal.timeout(waitMs + 5000),
  });
  if (res.status !== 200 && res.status !== 429) throw new Error(`limiter: ${res.status}`);
  return (await res.json()) as Decision;
}
```

```ts
// Hono middleware (Express is the same idea with req/res/next)
import { Hono } from "hono";
import { acquire } from "./ratelimit";

const app = new Hono();
app.use("/api/*", async (c, next) => {
  let d;
  try {
    d = await acquire("http://localhost:8080", "thirdparty", c.req.header("x-service") ?? "", 4000);
  } catch {
    return next(); // limiter unreachable: fail open
  }
  if (!d.allowed) {
    c.header("Retry-After", String(Math.ceil(d.retry_after_ms / 1000)));
    return c.json({ error: "Too Many Requests" }, 429);
  }
  await next();
  c.res.headers.set("x-fair-share-mode", d.mode);
});
```

```ts
// Guard an outbound call instead:
const d = await acquire("http://localhost:8080", "thirdparty", "my-app", 4000);
if (!d.allowed) throw new Error(`rate limited, retry in ${d.retry_after_ms}ms`);
const resp = await fetch("https://api.example.com/v1/things");
```

[Back to contents](#contents)

### Python

```python
# ratelimit.py  (requests or httpx work the same; this is stdlib only)
import json
import urllib.request
import urllib.error

def acquire(limiter: str, upstream: str, service: str, wait_s: float = 0) -> dict:
    body = json.dumps({"service": service, "cost": 1, "wait": f"{int(wait_s * 1000)}ms"}).encode()
    req = urllib.request.Request(f"{limiter}/v1/acquire/{upstream}", data=body,
                                 headers={"content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=wait_s + 5) as res:
            return json.load(res)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return json.load(e)
        raise
```

```python
# FastAPI middleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from ratelimit import acquire

app = FastAPI()

@app.middleware("http")
async def fair_share(request: Request, call_next):
    try:
        d = acquire("http://localhost:8080", "thirdparty", request.headers.get("x-service", ""), wait_s=4)
    except Exception:
        return await call_next(request)  # limiter unreachable: fail open
    if not d["allowed"]:
        return JSONResponse({"error": "Too Many Requests"}, status_code=429,
                            headers={"Retry-After": str(d["retry_after_ms"] // 1000 + 1)})
    response = await call_next(request)
    response.headers["x-fair-share-mode"] = d["mode"]
    return response
```

For an async app use `httpx.AsyncClient` so the wait does not block the event
loop. Before an outbound call: `if not acquire(..., "my-app", 4)["allowed"]: raise ...`.

[Back to contents](#contents)

### Java (Spring Boot, Java 11+ HttpClient)

```java
// RateLimit.java
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import com.fasterxml.jackson.databind.*;

public final class RateLimit {
    public record Decision(boolean allowed, String mode, long retry_after_ms, long waited_ms) {}

    private static final HttpClient http = HttpClient.newHttpClient();
    private static final ObjectMapper json = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static Decision acquire(String limiter, String upstream, String service, Duration wait) throws Exception {
        String body = json.writeValueAsString(java.util.Map.of("service", service, "cost", 1, "wait", wait.toMillis() + "ms"));
        HttpRequest req = HttpRequest.newBuilder(URI.create(limiter + "/v1/acquire/" + upstream))
                .header("content-type", "application/json")
                .timeout(wait.plusSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200 && res.statusCode() != 429) throw new IllegalStateException("limiter: " + res.statusCode());
        return json.readValue(res.body(), Decision.class);
    }
}
```

```java
// Spring interceptor
@Component
public class FairShareInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws Exception {
        RateLimit.Decision d;
        try {
            d = RateLimit.acquire("http://localhost:8080", "thirdparty", req.getHeader("X-Service"), Duration.ofSeconds(4));
        } catch (Exception e) {
            return true; // limiter unreachable: fail open
        }
        if (!d.allowed()) {
            res.setHeader("Retry-After", String.valueOf(d.retry_after_ms() / 1000 + 1));
            res.sendError(429, "Too Many Requests");
            return false;
        }
        res.setHeader("X-Fair-Share-Mode", d.mode());
        return true;
    }
}

// register it:
@Configuration
class WebConfig implements WebMvcConfigurer {
    @Override public void addInterceptors(InterceptorRegistry r) {
        r.addInterceptor(new FairShareInterceptor()).addPathPatterns("/api/**");
    }
}
```

[Back to contents](#contents)

### Kotlin (Ktor)

```kotlin
// RateLimit.kt
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class Decision(val allowed: Boolean, val mode: String = "", val retry_after_ms: Long = 0, val waited_ms: Long = 0)

private val http = HttpClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

suspend fun acquire(limiter: String, upstream: String, service: String, waitMs: Long = 0): Decision {
    val res = http.post("$limiter/v1/acquire/$upstream") {
        contentType(ContentType.Application.Json)
        setBody(mapOf("service" to service, "cost" to 1, "wait" to "${waitMs}ms"))
    }
    check(res.status.value == 200 || res.status.value == 429) { "limiter: ${res.status}" }
    return res.body()
}
```

```kotlin
// Ktor plugin
val FairShare = createApplicationPlugin("FairShare") {
    onCall { call ->
        val d = runCatching {
            acquire("http://localhost:8080", "thirdparty", call.request.headers["X-Service"] ?: "", 4000)
        }.getOrNull() ?: return@onCall // limiter unreachable: fail open
        if (!d.allowed) {
            call.response.headers.append("Retry-After", (d.retry_after_ms / 1000 + 1).toString())
            call.respond(HttpStatusCode.TooManyRequests, mapOf("error" to "Too Many Requests"))
        } else {
            call.response.headers.append("X-Fair-Share-Mode", d.mode)
        }
    }
}

// in Application.module(): install(FairShare)
```

[Back to contents](#contents)

### Rust (reqwest + axum)

```rust
// ratelimit.rs
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Clone)]
pub struct Decision {
    pub allowed: bool,
    #[serde(default)] pub mode: String,
    #[serde(default)] pub retry_after_ms: u64,
    #[serde(default)] pub waited_ms: u64,
}

pub async fn acquire(limiter: &str, upstream: &str, service: &str, wait: Duration) -> anyhow::Result<Decision> {
    let res = reqwest::Client::new()
        .post(format!("{limiter}/v1/acquire/{upstream}"))
        .timeout(wait + Duration::from_secs(5))
        .json(&serde_json::json!({ "service": service, "cost": 1, "wait": format!("{}ms", wait.as_millis()) }))
        .send()
        .await?;
    anyhow::ensure!(res.status() == 200 || res.status() == 429, "limiter: {}", res.status());
    Ok(res.json().await?)
}
```

```rust
// axum middleware
use axum::{extract::Request, http::{HeaderValue, StatusCode}, middleware::Next, response::{IntoResponse, Response}};

pub async fn fair_share(req: Request, next: Next) -> Response {
    let service = req.headers().get("x-service").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let d = match acquire("http://localhost:8080", "thirdparty", &service, Duration::from_secs(4)).await {
        Ok(d) => d,
        Err(_) => return next.run(req).await, // limiter unreachable: fail open
    };
    if !d.allowed {
        let mut res = (StatusCode::TOO_MANY_REQUESTS, "Too Many Requests").into_response();
        res.headers_mut().insert("retry-after", HeaderValue::from(d.retry_after_ms / 1000 + 1));
        return res;
    }
    let mut res = next.run(req).await;
    res.headers_mut().insert("x-fair-share-mode", HeaderValue::from_str(&d.mode).unwrap());
    res
}

// Router::new().route("/api/me", get(me)).layer(axum::middleware::from_fn(fair_share))
```

[Back to contents](#contents)

### Ruby on Rails (Rack middleware)

```ruby
# lib/rate_limit.rb
require "net/http"
require "json"

module RateLimit
  def self.acquire(limiter, upstream, service, wait_s: 0)
    uri = URI("#{limiter}/v1/acquire/#{upstream}")
    http = Net::HTTP.new(uri.host, uri.port)
    http.read_timeout = wait_s + 5
    res = http.post(uri.path, { service: service, cost: 1, wait: "#{(wait_s * 1000).to_i}ms" }.to_json,
                    "Content-Type" => "application/json")
    raise "limiter: #{res.code}" unless %w[200 429].include?(res.code)
    JSON.parse(res.body)
  end
end
```

```ruby
# app/middleware/fair_share.rb
class FairShare
  def initialize(app) = @app = app

  def call(env)
    d = begin
      RateLimit.acquire("http://localhost:8080", "thirdparty", env["HTTP_X_SERVICE"].to_s, wait_s: 4)
    rescue StandardError
      return @app.call(env) # limiter unreachable: fail open
    end
    unless d["allowed"]
      return [429, { "Content-Type" => "application/json", "Retry-After" => (d["retry_after_ms"] / 1000 + 1).to_s },
              [{ error: "Too Many Requests" }.to_json]]
    end
    status, headers, body = @app.call(env)
    headers["X-Fair-Share-Mode"] = d["mode"]
    [status, headers, body]
  end
end

# config/application.rb
config.middleware.use FairShare
```

[Back to contents](#contents)

## Things every integration should do

- **Fail open** when the limiter is unreachable (all snippets do). The limiter
  is there to protect a third party's budget, not to take your app down.
- **Resolve the service name deliberately.** A gateway takes it from the
  caller (header, API key, tenant). A single app protecting its own outbound
  calls hardcodes its own name.
- **Pass `wait` on paths where latency is acceptable** and leave it out where a
  fast 429 is better than a slow 200 (interactive endpoints, health checks).
- **Report upstream 429s you see yourself** in acquire mode with
  `POST /v1/upstreams/{upstream}/cooldown {"retry_after_ms": ...}` so the
  other services back off too. Proxy mode does this for you.
