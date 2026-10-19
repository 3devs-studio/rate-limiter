package io.fairshare;

public final class NoConfigException extends FairShareException {
    public final String upstream;

    public NoConfigException(String upstream) {
        super("no published config for upstream: " + upstream);
        this.upstream = upstream;
    }
}
