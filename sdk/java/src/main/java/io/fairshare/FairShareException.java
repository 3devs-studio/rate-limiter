package io.fairshare;

public class FairShareException extends RuntimeException {
    public FairShareException(String message) {
        super(message);
    }

    public FairShareException(String message, Throwable cause) {
        super(message, cause);
    }
}
