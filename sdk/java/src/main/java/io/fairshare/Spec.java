package io.fairshare;

import java.util.Map;
import java.util.TreeMap;

/** An upstream's budget and the shares of its services. */
public record Spec(double rate, double burst, Map<String, Share> services) {

    /** Split rate and burst by weight, the way the service does. */
    public static Spec fromWeights(double rate, double burst, Map<String, Double> weights) {
        double sum = weights.values().stream().mapToDouble(Double::doubleValue).sum();
        Map<String, Share> services = new TreeMap<>();
        weights.forEach((name, w) -> services.put(name, new Share(rate * w / sum, Math.max(1.0, burst * w / sum), w)));
        return new Spec(rate, burst, services);
    }
}
