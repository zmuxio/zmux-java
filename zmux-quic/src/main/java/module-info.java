/** QUIC carrier integration. */
module io.zmux.quic {
    requires transitive io.zmux;
    requires transitive io.netty.codec.classes.quic;
    requires transitive io.netty.transport;
    requires io.netty.buffer;
    requires io.netty.common;
    requires io.netty.handler;
    exports io.zmux.quic;
}
