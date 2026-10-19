package io.fairshare;

public final class UnknownServiceException extends FairShareException {
    public final String service;

    public UnknownServiceException(String service) {
        super("unknown service: " + service);
        this.service = service;
    }
}
