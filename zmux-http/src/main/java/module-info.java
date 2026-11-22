/** HTTP carriers: streaming requests, the exchange carrier and WebSockets, on Netty. */
@SuppressWarnings("module") // Qualified exports name the sibling compiled next.
module io.zmux.http {
    requires transitive io.zmux;
    requires transitive io.netty.transport;
    requires transitive io.netty.codec.http;
    requires io.netty.codec.http2;
    requires io.netty.buffer;
    requires transitive io.netty.common;
    requires transitive io.netty.handler;
    requires io.netty.codec;
    exports io.zmux.http;
    exports io.zmux.http.internal to io.zmux.http3;
}
