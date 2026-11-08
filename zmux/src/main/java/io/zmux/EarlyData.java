package io.zmux;

/** Stream data written before the peer's parameters arrive. */
public enum EarlyData {
    /** Establishment waits for the peer's parameters. */
    OFF,
    /**
     * The peer is declared a zmux endpoint: stream data goes out at once under the zmux floor
     * (early data of the opening pipe), behind the PIPE_ACK fence, and establishment returns without
     * waiting. There is no carrier early data (TLS 1.3 early data or QUIC 0-RTT): no Java
     * carrier offers it.
     */
    ZMUX_PEER
}
