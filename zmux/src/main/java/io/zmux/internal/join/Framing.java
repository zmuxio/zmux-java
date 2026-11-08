package io.zmux.internal.join;

/** How handshake messages and sealed records sit on a carrier. */
public enum Framing {
    /** A byte stream: units carry a two-byte big-endian length. */
    STREAM,
    /** A message carrier: one unit per message, no length. */
    MESSAGE
}
