import { describe, expect, test } from "bun:test";
import Redis from "ioredis";
import { FairShare, UnknownServiceError } from "./index";
import { fairShare } from "./hono";
import { Hono } from "hono";

const suite = await Bun.file(`${import.meta.dir}/../../conformance/cases.json`).json();
const addr = process.env.REDIS_ADDR ?? "localhost:6379";
const [host, port] = addr.split(":");
const redis = new Redis({ host, port: Number(port), lazyConnect: true, maxRetriesPerRequest: 1 });
let available = true;
try {
  await redis.connect();
} catch {
  available = false;
}
const tol = suite.tolerance ?? 0.1;
const near = (a: number, b: number) => Math.abs(a - b) <= tol;

describe.skipIf(!available)("conformance", () => {
  for (const c of suite.cases) {
    test(c.name, async () => {
      const upstream = `conformance-ts-${c.name}`;
      await FairShare.publish(redis, upstream, c.spec);
      const lim = new FairShare(redis, upstream);
      await lim.reset();
      for (const [i, step] of c.steps.entries()) {
        const where = `${c.name} step ${i}`;
        switch (step.op) {
          case "acquire": {
            const d = await lim.acquire(step.service, step.cost ?? 1, step.wait_ms ?? 0);
            expect(d.allowed, where).toBe(step.expect.allowed);
            if (step.expect.mode) expect(d.mode, where).toBe(step.expect.mode);
            if ("service_tokens" in step.expect) expect(near(d.serviceTokens, step.expect.service_tokens), `${where} service_tokens=${d.serviceTokens}`).toBe(true);
            if ("global_tokens" in step.expect) expect(near(d.globalTokens, step.expect.global_tokens), `${where} global_tokens=${d.globalTokens}`).toBe(true);
            if ("min_waited_ms" in step.expect) expect(d.waitedMs, where).toBeGreaterThanOrEqual(step.expect.min_waited_ms);
            if ("min_retry_after_ms" in step.expect) expect(d.retryAfterMs, where).toBeGreaterThanOrEqual(step.expect.min_retry_after_ms);
            if ("max_retry_after_ms" in step.expect) expect(d.retryAfterMs, where).toBeLessThanOrEqual(step.expect.max_retry_after_ms);
            break;
          }
          case "drain": {
            let count = 0;
            let last = "";
            const max = step.max ?? 1000;
            while (count < max) {
              const d = await lim.tryAcquire(step.service, 1);
              last = d.mode;
              if (!d.allowed) break;
              count++;
            }
            expect(count, where).toBe(step.expect_count);
            if (step.expect_last_mode) expect(last, where).toBe(step.expect_last_mode);
            break;
          }
          case "acquire_error":
            await expect(lim.tryAcquire(step.service, 1)).rejects.toBeInstanceOf(UnknownServiceError);
            break;
          case "cooldown":
            await lim.reportRateLimited(step.ms);
            break;
          case "sync":
            await lim.syncRemaining(step.remaining);
            break;
          case "sleep":
            await new Promise((r) => setTimeout(r, step.ms));
            break;
          case "reset":
            await lim.reset();
            break;
          default:
            throw new Error(`unknown op ${step.op}`);
        }
      }
    });
  }

  test("hono middleware", async () => {
    const upstream = "conformance-hono";
    await FairShare.publish(redis, upstream, { rate: 1, burst: 2, services: { web: { rate: 1, burst: 2 } } });
    const lim = new FairShare(redis, upstream);
    await lim.reset();
    const app = new Hono();
    app.use("/api/*", fairShare(lim, { service: (c) => c.req.header("x-service") }));
    app.get("/api/x", (c) => c.text("ok"));
    const codes: number[] = [];
    let mode = "";
    for (let i = 0; i < 3; i++) {
      const res = await app.request("/api/x", { headers: { "x-service": "web" } });
      codes.push(res.status);
      mode = res.headers.get("x-fair-share-mode") ?? "";
    }
    expect(codes).toEqual([200, 200, 429]);
    expect(mode).toBe("global");
    const open = await app.request("/api/x");
    expect(open.status).toBe(200); // no service header: unmanaged
  });
});
