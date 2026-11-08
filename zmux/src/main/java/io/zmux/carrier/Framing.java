package io.zmux.carrier;

/** How records travel on a carrier. */
public enum Framing {
    /** Records are concatenated on a byte stream (TCP, TLS, HTTP bodies). */
    BYTE_STREAM,
    /** Each message carries one record's frames without its Size (WebSocket binary messages). */
    MESSAGES
}
