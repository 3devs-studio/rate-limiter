package io.fairshare;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import redis.clients.jedis.JedisPooled;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs sdk/conformance/cases.json against a real Redis (REDIS_ADDR, default localhost:6379). */
class ConformanceTest {

    @TestFactory
    List<DynamicTest> conformance() throws Exception {
        JsonNode suite = new ObjectMapper().readTree(Files.readString(Path.of("../conformance/cases.json")));
        double tol = suite.path("tolerance").asDouble(0.1);
        String[] addr = System.getenv().getOrDefault("REDIS_ADDR", "localhost:6379").split(":");
        JedisPooled redis = new JedisPooled(addr[0], Integer.parseInt(addr[1]));
        try {
            redis.ping();
        } catch (RuntimeException e) {
            Assumptions.abort("no Redis: " + e.getMessage());
        }
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : suite.path("cases")) {
            tests.add(DynamicTest.dynamicTest(c.path("name").asText(), () -> runCase(redis, c, tol)));
        }
        return tests;
    }

    private static void runCase(JedisPooled redis, JsonNode c, double tol) {
        String upstream = "conformance-java-" + c.path("name").asText();
        TreeMap<String, Share> services = new TreeMap<>();
        c.path("spec").path("services").fields().forEachRemaining(e -> services.put(e.getKey(), new Share(e.getValue().path("rate").asDouble(), e.getValue().path("burst").asDouble())));
        FairShare.publish(redis, upstream, new Spec(c.path("spec").path("rate").asDouble(), c.path("spec").path("burst").asDouble(), services));
        FairShare lim = new FairShare(redis, upstream);
        lim.reset();
        int i = 0;
        for (JsonNode step : c.path("steps")) {
            String where = c.path("name").asText() + " step " + i++;
            String service = step.path("service").asText("");
            switch (step.path("op").asText()) {
                case "acquire" -> {
                    JsonNode e = step.path("expect");
                    Decision d = lim.acquire(service, step.path("cost").asDouble(1), Duration.ofMillis(step.path("wait_ms").asLong(0)));
                    assertEquals(e.path("allowed").asBoolean(), d.allowed(), where + ": " + d);
                    if (e.has("mode")) assertEquals(e.path("mode").asText(), d.mode(), where);
                    if (e.has("service_tokens")) assertTrue(Math.abs(d.serviceTokens() - e.path("service_tokens").asDouble()) <= tol, where + ": service_tokens " + d.serviceTokens());
                    if (e.has("global_tokens")) assertTrue(Math.abs(d.globalTokens() - e.path("global_tokens").asDouble()) <= tol, where + ": global_tokens " + d.globalTokens());
                    if (e.has("min_waited_ms")) assertTrue(d.waited().toMillis() >= e.path("min_waited_ms").asLong(), where);
                    if (e.has("min_retry_after_ms")) assertTrue(d.retryAfter().toMillis() >= e.path("min_retry_after_ms").asLong(), where);
                    if (e.has("max_retry_after_ms")) assertTrue(d.retryAfter().toMillis() <= e.path("max_retry_after_ms").asLong(), where);
                }
                case "drain" -> {
                    int max = step.path("max").asInt(1000);
                    int count = 0;
                    String last = "";
                    while (count < max) {
                        Decision d = lim.tryAcquire(service, 1);
                        last = d.mode();
                        if (!d.allowed()) break;
                        count++;
                    }
                    assertEquals(step.path("expect_count").asInt(), count, where);
                    if (step.has("expect_last_mode")) assertEquals(step.path("expect_last_mode").asText(), last, where);
                }
                case "acquire_error" -> assertThrows(UnknownServiceException.class, () -> lim.tryAcquire(service, 1), where);
                case "cooldown" -> lim.reportRateLimited(Duration.ofMillis(step.path("ms").asLong()));
                case "sync" -> lim.syncRemaining(step.path("remaining").asDouble());
                case "sleep" -> {
                    try {
                        Thread.sleep(step.path("ms").asLong());
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                case "reset" -> lim.reset();
                default -> throw new AssertionError("unknown op " + step.path("op"));
            }
        }
    }
}
