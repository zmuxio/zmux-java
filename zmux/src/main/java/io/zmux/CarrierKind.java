package io.zmux;

import java.util.Locale;

/** A kind of carrier that {@link SessionController#open} may use. */
public enum CarrierKind {
    /** A WebSocket: one binary message per record. */
    WEBSOCKET,
    /** A bidirectional streaming request: one request whose body is the uplink and whose response body the downlink. */
    STREAMING,
    /** A split bootstrap: an uplink POST and a downlink GET. */
    SPLIT,
    /** The exchange carrier of bounded requests. */
    EXCHANGE,
    /** A QUIC stream, direct only. */
    QUIC,
    /** A streaming request over HTTP/3, direct only. */
    HTTP3,
    /** A WebTransport stream, direct only. */
    WEBTRANSPORT,
    /**
     * A reliable ordered byte connection that carries the records with no framing of its own: TCP, TLS, a Unix
     * socket, a stream of another session.
     */
    RAW;

    /** The kind's name in lower case: "websocket", "streaming", "split", "exchange", "quic", "http3", "webtransport" or "raw". */
    @Override public String toString() { return name().toLowerCase(Locale.ROOT); }
}
