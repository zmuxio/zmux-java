/** zmux: QMux-compatible multiplexing of streams and datagrams over reliable byte pipes. */
module io.zmux {
    requires java.logging;

    exports io.zmux;
    exports io.zmux.carrier;
    exports io.zmux.helper;
    exports io.zmux.shaping;
    // The transport SPI the QUIC carrier implements (internal.loop) names core and wire types
    // in its signatures; they are exported to it so the SPI is well-formed, not for direct use.
    exports io.zmux.internal.core to io.zmux.quic;
    exports io.zmux.internal.diag to io.zmux.quic;
    exports io.zmux.internal.loop to io.zmux.quic;
    exports io.zmux.internal.wire to io.zmux.http, io.zmux.quic;
}
