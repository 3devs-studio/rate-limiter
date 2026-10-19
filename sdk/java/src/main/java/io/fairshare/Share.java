package io.fairshare;

/** One service's share of the upstream. weight may be null. */
public record Share(double rate, double burst, Double weight) {
    public Share(double rate, double burst) {
        this(rate, burst, null);
    }
}
