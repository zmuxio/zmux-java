package io.zmux.http;

import io.zmux.JoinHint;
import io.zmux.RequestHead;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * The carrier class of an HTTP request on a unified endpoint,
 * decided by {@link #classify} on the request head alone, before any body byte is read.
 */
public enum HttpClass {
    /** A request that is not a carrier: the host's. */
    NONE,
    /** A WebSocket opening handshake: a GET with Upgrade: websocket, or an extended CONNECT for websocket. */
    WEBSOCKET,
    /** An extended CONNECT for webtransport-h3 (or webtransport, the name of earlier drafts). */
    WEBTRANSPORT,
    /** A GET with Zmux-Pipe-Pair or Zmux-Pipe-Join. */
    DOWNLINK,
    /** A POST with Zmux-Pipe-Pair (a split bootstrap's uplink), or one without a Content-Length that carries Zmux-Pipe-Join. */
    UPLINK,
    /** A POST with a Content-Length: one exchange of the exchange carrier. */
    EXCHANGE,
    /** A POST without a Content-Length. */
    BIDIRECTIONAL;

    /** The class's name in lower case, as reports use it ("websocket", "downlink", ...). */
    @Override public String toString() { return name().toLowerCase(Locale.ROOT); }

    /**
     * The head of an HTTP request, as far as classification needs it.
     *
     * @param method the request method, such as "GET"
     * @param version the major HTTP version: 1, 2 or 3
     * @param protocol the :protocol pseudo-header of an extended CONNECT (RFC 8441); empty without one
     * @param contentLength the request's Content-Length; -1 when it has none (a chunked body on
     *     HTTP/1.1, a body of unknown length on HTTP/2 and HTTP/3)
     * @param headers the request's header fields
     */
    public record Head(String method, int version, String protocol, long contentLength, RequestHead headers) {
        public Head {
            Objects.requireNonNull(method);
            Objects.requireNonNull(protocol);
            Objects.requireNonNull(headers);
            if (contentLength < -1) throw new IllegalArgumentException("contentLength below -1");
        }
    }

    /**
     * Classifies a request by the first of its rules that matches. It is
     * the one classification of every HTTP adapter: each serves the classes it carries and the
     * unified endpoint dispatches by it.
     */
    public static HttpClass classify(Head r) {
        RequestHead h = r.headers();
        switch (r.method()) {
            case "GET" -> {
                if (r.version() == 1 && hasToken(h.header("Upgrade"), "websocket")) return WEBSOCKET;
                if (present(h, JoinHint.PAIR_HEADER) || present(h, JoinHint.JOIN_HEADER)) return DOWNLINK;
            }
            case "CONNECT" -> {
                String p = r.protocol();
                if (p.equalsIgnoreCase("websocket")) return WEBSOCKET;
                // webtransport is the name of earlier drafts.
                if (p.equalsIgnoreCase("webtransport-h3") || p.equalsIgnoreCase("webtransport")) {
                    return WEBTRANSPORT;
                }
            }
            case "POST" -> {
                if (present(h, JoinHint.PAIR_HEADER)) return UPLINK;
                if (r.contentLength() >= 0) return EXCHANGE;
                if (present(h, JoinHint.JOIN_HEADER)) return UPLINK;
                return BIDIRECTIONAL;
            }
            default -> { }
        }
        return NONE;
    }

    /** Whether the field has a non-empty value (the first of its values, as Go's Header.Get). */
    private static boolean present(RequestHead h, String name) {
        List<String> v = h.header(name);
        return !v.isEmpty() && !v.getFirst().isEmpty();
    }

    /** Whether a comma-separated field holds token, case-insensitively. */
    static boolean hasToken(List<String> values, String token) {
        for (String v : values) {
            for (String t : v.split(",", -1)) if (t.strip().equalsIgnoreCase(token)) return true;
        }
        return false;
    }
}
