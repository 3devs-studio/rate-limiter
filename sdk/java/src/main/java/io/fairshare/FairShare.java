package io.fairshare;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fair-share rate limiting straight from Redis.
 *
 * <p>The decision logic is the Lua in src/main/resources/fairshare, identical to
 * what the rate limiter service runs, so an app using this library and an app
 * behind the service share the same buckets and the same answers.
 *
 * <pre>{@code
 * FairShare limiter = new FairShare(new JedisPooled("localhost", 6379), "thirdparty");
 * Decision d = limiter.acquire("my-app", 1, Duration.ofSeconds(4));
 * if (!d.allowed()) { /* back off for d.retryAfter() *\/ }
 * }</pre>
 *
 * <p>Thread-safe. One instance per upstream.
 */
public final class FairShare {
    public static final int CONFIG_SCHEMA = 1;

    private static final ObjectMapper JSON = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String ACQUIRE = resource("acquire.lua");
    private static final String COOLDOWN = resource("cooldown.lua");
    private static final String SYNC = resource("sync.lua");

    private final UnifiedJedis redis;
    private final String upstream;
    private final Duration configTtl;
    private final Map<String, String> shas = new ConcurrentHashMap<>();
    private volatile Spec spec;
    private volatile Instant loadedAt = Instant.EPOCH;

    /** @param redis a {@code JedisPooled} or {@code JedisCluster}; this class never closes it. */
    public FairShare(UnifiedJedis redis, String upstream) {
        this(redis, upstream, Duration.ofSeconds(10));
    }

    public FairShare(UnifiedJedis redis, String upstream, Duration configTtl) {
        this.redis = redis;
        this.upstream = upstream;
        this.configTtl = configTtl;
    }

    public String upstream() {
        return upstream;
    }

    public static String configKey(String upstream) {
        return "rl:{" + upstream + "}:config";
    }

    private String globalKey() {
        return "rl:{" + upstream + "}:g";
    }

    private String serviceKey(String service) {
        return "rl:{" + upstream + "}:s:" + service;
    }

    /** The upstream's shares as published to Redis (cached). */
    public Spec spec() {
        Spec current = spec;
        if (current != null && Duration.between(loadedAt, Instant.now()).compareTo(configTtl) < 0) {
            return current;
        }
        String raw;
        try {
            raw = redis.get(configKey(upstream));
        } catch (RuntimeException e) {
            if (current != null) {
                return current; // Redis hiccup: keep what we have
            }
            throw e;
        }
        if (raw == null) {
            throw new NoConfigException(upstream);
        }
        try {
            JsonNode doc = JSON.readTree(raw);
            if (doc.path("schema").asInt(-1) != CONFIG_SCHEMA) {
                throw new FairShareException("published config for " + upstream + " has schema " + doc.path("schema") + ", expected " + CONFIG_SCHEMA);
            }
            Map<String, Share> services = new TreeMap<>();
            doc.path("services").fields().forEachRemaining(e -> {
                JsonNode s = e.getValue();
                services.put(e.getKey(), new Share(s.path("rate").asDouble(), s.path("burst").asDouble(), s.hasNonNull("weight") ? s.get("weight").asDouble() : null));
            });
            current = new Spec(doc.path("rate").asDouble(), doc.path("burst").asDouble(), services);
        } catch (IOException e) {
            throw new FairShareException("invalid published config for " + upstream, e);
        }
        spec = current;
        loadedAt = Instant.now();
        return current;
    }

    /** Ask once; never waits. */
    public Decision tryAcquire(String service, double cost) {
        Spec s = spec();
        Share me = s.services().get(service);
        if (me == null) {
            throw new UnknownServiceException(service);
        }
        // Same KEYS/ARGV layout as the service: global, mine, then the others by name.
        List<String> keys = new ArrayList<>(List.of(globalKey(), serviceKey(service)));
        List<String> args = new ArrayList<>(List.of(num(cost), num(s.rate()), num(s.burst()), num(me.rate()), num(me.burst())));
        new TreeMap<>(s.services()).forEach((name, other) -> {
            if (!name.equals(service)) {
                keys.add(serviceKey(name));
                args.add(num(other.rate()));
                args.add(num(other.burst()));
            }
        });
        List<?> reply = (List<?>) run("acquire", ACQUIRE, keys, args);
        return new Decision(
                ((Number) reply.get(0)).longValue() == 1,
                String.valueOf(reply.get(2)),
                Duration.ofMillis(Math.max(0, ((Number) reply.get(1)).longValue())),
                Duration.ZERO,
                Double.parseDouble(String.valueOf(reply.get(3))),
                Double.parseDouble(String.valueOf(reply.get(4))));
    }

    /** Keep asking until a token is granted or {@code wait} has elapsed. */
    public Decision acquire(String service, double cost, Duration wait) {
        Instant start = Instant.now();
        Decision d = tryAcquire(service, cost);
        while (!d.allowed()) {
            Duration remaining = wait.minus(Duration.between(start, Instant.now()));
            if (remaining.isNegative() || remaining.isZero()) {
                break;
            }
            long pauseMs = Math.min(Math.min(Math.max(d.retryAfter().toMillis(), 5), remaining.toMillis()), 1000);
            try {
                Thread.sleep(pauseMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            d = tryAcquire(service, cost);
        }
        return d.withWaited(Duration.between(start, Instant.now()));
    }

    /** The upstream answered 429: every service backs off for {@code retryAfter}. */
    public void reportRateLimited(Duration retryAfter) {
        Spec s = spec();
        run("cooldown", COOLDOWN, List.of(globalKey()), List.of(String.valueOf(Math.max(1, retryAfter.toMillis())), num(s.rate()), num(s.burst())));
    }

    /** Lower the global bucket to what the upstream says is left. Never raises it. */
    public void syncRemaining(double remaining) {
        Spec s = spec();
        run("sync", SYNC, List.of(globalKey()), List.of(num(remaining), num(s.rate()), num(s.burst())));
    }

    /** Refill every bucket. For demos and tests. */
    public void reset() {
        List<String> keys = new ArrayList<>();
        keys.add(globalKey());
        spec().services().keySet().forEach(n -> keys.add(serviceKey(n)));
        redis.del(keys.toArray(new String[0]));
    }

    /** Write an upstream's shares to Redis without running the service. */
    public static void publish(UnifiedJedis redis, String upstream, Spec spec) {
        ObjectNode doc = JSON.createObjectNode();
        doc.put("schema", CONFIG_SCHEMA).put("upstream", upstream).put("rate", spec.rate()).put("burst", spec.burst());
        ObjectNode services = doc.putObject("services");
        spec.services().forEach((name, s) -> {
            ObjectNode n = services.putObject(name).put("rate", s.rate()).put("burst", s.burst());
            if (s.weight() != null) {
                n.put("weight", s.weight());
            }
        });
        doc.put("published_at", Instant.now().toString()).put("published_by", "fairshare-java");
        redis.set(configKey(upstream), doc.toString());
    }

    /** EVALSHA with a one-time SCRIPT LOAD when Redis does not know the script yet. */
    private Object run(String name, String lua, List<String> keys, List<String> args) {
        String sha = shas.computeIfAbsent(name, n -> redis.scriptLoad(lua));
        try {
            return redis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException e) {
            sha = redis.scriptLoad(lua);
            shas.put(name, sha);
            return redis.evalsha(sha, keys, args);
        }
    }

    private static String num(double v) {
        return v == Math.rint(v) ? Long.toString((long) v) : Double.toString(v);
    }

    private static String resource(String file) {
        try (InputStream in = FairShare.class.getResourceAsStream("/fairshare/" + file)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + file);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
