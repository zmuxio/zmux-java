package io.zmux;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The URL of a carrier candidate and the URL that redirects last led it to: later carriers of
 * the candidate (joins, redials) start from the final URL, and a mode dialer keeps it per server
 * in its {@link CarrierCache} until the kind fails there, after which it starts at the base URL
 * again. Safe for concurrent use.
 */
public final class HttpTarget {
    private final String base;
    private final AtomicReference<String> current = new AtomicReference<>();

    /** A target at url. */
    public HttpTarget(String url) { base = Objects.requireNonNull(url, "url"); }

    /** The URL the next carrier starts from. */
    public String url() {
        String c = current.get();
        return c != null ? c : base;
    }

    /** The URL the target was made with. */
    public String base() { return base; }

    /** Records the final URL of a carrier that redirects led to. */
    public void redirected(String finalUrl) {
        current.set(finalUrl == null || finalUrl.equals(base) ? null : finalUrl);
    }

    /** Starts the next carrier from the base URL again. */
    public void reset() { current.set(null); }
}
