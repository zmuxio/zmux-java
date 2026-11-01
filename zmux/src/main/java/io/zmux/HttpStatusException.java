package io.zmux;

import java.io.IOException;

/**
 * An HTTP carrier's request was answered with a status other than 2xx: the server or an
 * intermediary refused the carrier.
 */
public final class HttpStatusException extends IOException {
    private final int status;

    public HttpStatusException(int status, String reason) {
        super("HTTP carrier status " + status + (reason == null || reason.isEmpty() ? "" : " " + reason));
        this.status = status;
    }

    /** The response's status code. */
    public int status() { return status; }
}
