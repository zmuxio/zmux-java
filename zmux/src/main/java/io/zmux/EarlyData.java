package io.zmux;

/** Stream data written before the peer's parameters arrive. */
public enum EarlyData {
    /** Establishment waits for the peer's parameters. */
    OFF,
    /**
     * The peer is declared a zmux endpoint: data goes out at once under the zmux floor
     * (SPEC.md Section 6.1.2), and establishment returns without waiting.
     */
    ZMUX_PEER
}
