package io.zmux;

import java.util.Locale;

/** A kind of carrier that {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} may use (API.md Section 4.8). */
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
    WEBTRANSPORT;

    /** The kind's name in lower case: "websocket", "streaming", "split", "exchange", "quic", "http3" or "webtransport". */
    @Override public String toString() { return name().toLowerCase(Locale.ROOT); }
}
