/** HTTP/3 request carriers on Netty QUIC. */
@SuppressWarnings("module") // Keep the module name aligned with the HTTP/3 artifact.
module io.zmux.http3 {
    requires transitive io.zmux.http;
    requires transitive io.zmux.quic;
    requires io.netty.codec.http3;
    requires io.netty.buffer;
    requires io.netty.codec;
    exports io.zmux.http3;
}
