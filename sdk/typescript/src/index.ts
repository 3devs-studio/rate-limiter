// fairshare-client: fair-share rate limiting straight from Redis.
//
// The decision logic is the Lua in ./scripts.ts, identical to what the rate
// limiter service runs, so an app using this package and an app behind the
// service share the same buckets and the same answers.
import type { Cluster, Redis } from "ioredis";
import { ACQUIRE, COOLDOWN, SYNC } from "./scripts";

export type Mode = "own" | "borrow" | "share" | "global" | "cooldown";

export interface Decision {
  allowed: boolean;
  mode: Mode;
  retryAfterMs: number;
  waitedMs: number;
  globalTokens: number;
  serviceTokens: number;
}

export interface Share {
  rate: number;
  burst: number;
  weight?: number;
}

export interface Spec {
  rate: number;
  burst: number;
  services: Record<string, Share>;
}

export const CONFIG_SCHEMA = 1;

export class UnknownServiceError extends Error {
  constructor(service: string) {
    super(`unknown service: ${service}`);
    this.name = "UnknownServiceError";
  }
}

export class NoConfigError extends Error {
  constructor(upstream: string) {
    super(`no published config for upstream: ${upstream}`);
    this.name = "NoConfigError";
  }
}

export type RedisClient = Redis | Cluster;

export const configKey = (upstream: string) => `rl:{${upstream}}:config`;
const globalKey = (upstream: string) => `rl:{${upstream}}:g`;
const serviceKey = (upstream: string, service: string) => `rl:{${upstream}}:s:${service}`;

type Scripted = RedisClient & {
  fairshareAcquire(numKeys: number, ...keysAndArgs: (string | number)[]): Promise<[number, number, string, string, string]>;
  fairshareCooldown(key: string, ms: number, rate: number, burst: number): Promise<number>;
  fairshareSync(key: string, remaining: number, rate: number, burst: number): Promise<string>;
};

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export interface FairShareOptions {
  /** How long the published config is cached, in ms (default 10 000). */
  configTtlMs?: number;
}

export class FairShare {
  private readonly redis: Scripted;
  private readonly ttlMs: number;
  private spec?: Spec;
  private loadedAt = 0;

  constructor(redis: RedisClient, readonly upstream: string, opts: FairShareOptions = {}) {
    this.ttlMs = opts.configTtlMs ?? 10_000;
    this.redis = redis as Scripted;
    if (typeof this.redis.fairshareAcquire !== "function") {
      // numberOfKeys is dynamic for acquire: the first argument carries it.
      redis.defineCommand("fairshareAcquire", { lua: ACQUIRE });
      redis.defineCommand("fairshareCooldown", { lua: COOLDOWN, numberOfKeys: 1 });
      redis.defineCommand("fairshareSync", { lua: SYNC, numberOfKeys: 1 });
    }
  }

  /** The upstream's shares as published to Redis (cached). */
  async getSpec(): Promise<Spec> {
    if (this.spec && Date.now() - this.loadedAt < this.ttlMs) {
      return this.spec;
    }
    const raw = await this.redis.get(configKey(this.upstream));
    if (!raw) {
      throw new NoConfigError(this.upstream);
    }
    const doc = JSON.parse(raw) as Spec & { schema?: number };
    if (doc.schema !== CONFIG_SCHEMA) {
      throw new Error(`published config for ${this.upstream} has schema ${doc.schema}, expected ${CONFIG_SCHEMA}`);
    }
    this.spec = { rate: doc.rate, burst: doc.burst, services: doc.services };
    this.loadedAt = Date.now();
    return this.spec;
  }

  /** Ask once; never waits. */
  async tryAcquire(service: string, cost = 1): Promise<Decision> {
    const spec = await this.getSpec();
    const me = spec.services[service];
    if (!me) {
      throw new UnknownServiceError(service);
    }
    // Same KEYS/ARGV layout as the service: global, mine, then the others in name order.
    const keys = [globalKey(this.upstream), serviceKey(this.upstream, service)];
    const args: number[] = [cost, spec.rate, spec.burst, me.rate, me.burst];
    for (const name of Object.keys(spec.services).sort()) {
      if (name === service) continue;
      keys.push(serviceKey(this.upstream, name));
      args.push(spec.services[name].rate, spec.services[name].burst);
    }
    const [allowed, retryMs, mode, g, s] = await this.redis.fairshareAcquire(keys.length, ...keys, ...args);
    return {
      allowed: Number(allowed) === 1,
      mode: mode as Mode,
      retryAfterMs: Number(retryMs),
      waitedMs: 0,
      globalTokens: Number(g),
      serviceTokens: Number(s),
    };
  }

  /** Keep asking until a token is granted or waitMs has elapsed. */
  async acquire(service: string, cost = 1, waitMs = 0): Promise<Decision> {
    const start = Date.now();
    let d = await this.tryAcquire(service, cost);
    while (!d.allowed) {
      const remaining = waitMs - (Date.now() - start);
      if (remaining <= 0) break;
      await sleep(Math.min(Math.max(d.retryAfterMs, 5), remaining, 1000));
      d = await this.tryAcquire(service, cost);
    }
    return { ...d, waitedMs: Date.now() - start };
  }

  /** The upstream answered 429: every service backs off for retryAfterMs. */
  async reportRateLimited(retryAfterMs: number): Promise<void> {
    const spec = await this.getSpec();
    await this.redis.fairshareCooldown(globalKey(this.upstream), Math.max(1, Math.round(retryAfterMs)), spec.rate, spec.burst);
  }

  /** Lower the global bucket to what the upstream says is left. Never raises it. */
  async syncRemaining(remaining: number): Promise<void> {
    const spec = await this.getSpec();
    await this.redis.fairshareSync(globalKey(this.upstream), remaining, spec.rate, spec.burst);
  }

  /** Refill every bucket. For demos and tests. */
  async reset(): Promise<void> {
    const spec = await this.getSpec();
    await this.redis.del(globalKey(this.upstream), ...Object.keys(spec.services).map((s) => serviceKey(this.upstream, s)));
  }

  /** Write an upstream's shares to Redis without running the service. */
  static async publish(redis: RedisClient, upstream: string, spec: Spec, by = "fairshare-typescript"): Promise<void> {
    const doc = { schema: CONFIG_SCHEMA, upstream, ...spec, published_at: new Date().toISOString(), published_by: by };
    await redis.set(configKey(upstream), JSON.stringify(doc));
  }

  /** Split rate and burst by weight, the way the service does. */
  static sharesFromWeights(rate: number, burst: number, weights: Record<string, number>): Record<string, Share> {
    const sum = Object.values(weights).reduce((a, b) => a + b, 0);
    const out: Record<string, Share> = {};
    for (const [name, w] of Object.entries(weights)) {
      out[name] = { rate: (rate * w) / sum, burst: Math.max(1, (burst * w) / sum), weight: w };
    }
    return out;
  }
}
