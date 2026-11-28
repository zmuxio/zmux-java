/** HTTP/3 request and WebTransport carriers on Netty QUIC. */
// The HTTP/3 module name follows its artifact.
@SuppressWarnings("module")
module io.zmux.http3 {
    requires transitive io.zmux.http;
    requires transitive io.zmux.quic;
    requires transitive io.netty.codec.http3;
    requires io.netty.buffer;
    requires io.netty.codec;
    exports io.zmux.http3;
}
