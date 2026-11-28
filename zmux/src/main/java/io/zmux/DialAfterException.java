package io.zmux;

import java.io.IOException;

/** The host prohibited a carrier open before the specified reading of the controller clock. */
public final class DialAfterException extends IOException {
    @java.io.Serial private static final long serialVersionUID = 1L;
    private final String candidate;
    private final long retryAt;

    DialAfterException(String candidate, long retryAt) {
        super("host requires a later start for " + candidate);
        this.candidate = candidate;
        this.retryAt = retryAt;
    }

    public String candidate() { return candidate; }
    public long retryAt() { return retryAt; }
}
