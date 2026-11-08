package io.zmux;

import java.io.IOException;

/**
 * A redirect that an HTTP carrier did not follow: to another host, a downgrade from https to
 * http, another port without a scheme upgrade, credentials in the Location, one that would turn
 * a POST into a GET, past the bound of hops, after more uplink bytes than a repeat can resend, or
 * without a valid Location. A mode dialer counts the candidate as refused.
 */
public final class HttpRedirectException extends IOException {
    private final int status;
    private final String location;
    private final String reason;

    public HttpRedirectException(int status, String location, String reason) {
        super("HTTP carrier redirect " + status + " to \"" + location + "\" refused: " + reason);
        this.status = status;
        this.location = location == null ? "" : location;
        this.reason = reason;
    }

    /** The redirect's status code. */
    public int status() { return status; }

    /** Its Location, as the response gave it ("" without one). */
    public String location() { return location; }

    /** Why it was not followed. */
    public String reason() { return reason; }
}
