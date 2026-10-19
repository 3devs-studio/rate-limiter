import type { FairShare } from "./index";

// Structural types so this file does not depend on @types/express.
interface Req { header?(name: string): string | undefined; headers: Record<string, string | string[] | undefined> }
interface Res {
  setHeader(name: string, value: string): unknown;
  status(code: number): Res;
  json(body: unknown): unknown;
}
type Next = (err?: unknown) => void;

export interface ExpressOptions {
  service: (req: Req) => string | undefined;
  waitMs?: number;
  cost?: number;
  failOpen?: boolean;
}

/** Express / Connect middleware: admit or refuse incoming requests per service. */
export function fairShare(limiter: FairShare, opts: ExpressOptions) {
  const failOpen = opts.failOpen ?? true;
  return async (req: Req, res: Res, next: Next) => {
    const service = opts.service(req);
    if (!service) return next();
    let d;
    try {
      d = await limiter.acquire(service, opts.cost ?? 1, opts.waitMs ?? 0);
    } catch {
      if (!failOpen) return res.status(503).json({ error: "rate limiter unavailable" });
      res.setHeader("x-fair-share-mode", "unavailable");
      return next();
    }
    res.setHeader("x-fair-share-service", service);
    res.setHeader("x-fair-share-mode", d.mode);
    res.setHeader("x-fair-share-wait-ms", String(d.waitedMs));
    if (!d.allowed) {
      res.setHeader("Retry-After", String(Math.ceil(d.retryAfterMs / 1000) || 1));
      return res.status(429).json({ error: "Too Many Requests", service, mode: d.mode, retryAfterMs: d.retryAfterMs });
    }
    next();
  };
}
