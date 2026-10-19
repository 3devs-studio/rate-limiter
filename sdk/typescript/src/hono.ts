import type { Context, MiddlewareHandler } from "hono";
import type { FairShare } from "./index";

export interface HonoOptions {
  /** Which service is calling. Return undefined to let the request through unmanaged. */
  service: (c: Context) => string | undefined;
  /** How long a request may wait for a token before 429 (default 0). */
  waitMs?: number;
  cost?: number;
  /** Let requests through when Redis is down or the service is unknown (default true). */
  failOpen?: boolean;
}

/** Admit or refuse incoming requests on behalf of the service that sent them. */
export function fairShare(limiter: FairShare, opts: HonoOptions): MiddlewareHandler {
  const failOpen = opts.failOpen ?? true;
  return async (c, next) => {
    const service = opts.service(c);
    if (!service) return next();
    let d;
    try {
      d = await limiter.acquire(service, opts.cost ?? 1, opts.waitMs ?? 0);
    } catch (err) {
      if (!failOpen) return c.json({ error: "rate limiter unavailable" }, 503);
      await next();
      c.res.headers.set("x-fair-share-mode", "unavailable");
      return;
    }
    if (!d.allowed) {
      c.header("Retry-After", String(Math.ceil(d.retryAfterMs / 1000) || 1));
      c.header("x-fair-share-mode", d.mode);
      return c.json({ error: "Too Many Requests", service, mode: d.mode, retryAfterMs: d.retryAfterMs }, 429);
    }
    await next();
    // After the handler: it may have returned its own Response object.
    c.res.headers.set("x-fair-share-service", service);
    c.res.headers.set("x-fair-share-mode", d.mode);
    c.res.headers.set("x-fair-share-wait-ms", String(d.waitedMs));
  };
}
