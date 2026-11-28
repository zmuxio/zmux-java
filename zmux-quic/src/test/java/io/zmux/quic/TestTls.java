package io.zmux.quic;

import io.netty.handler.codec.quic.QuicSslContextBuilder;

final class TestTls {
    static QuicSslContextBuilder server;
    static QuicSslContextBuilder client;
    private TestTls() {}
    static synchronized void init() throws Exception {
        if (server != null) return;
        var keys = io.zmux.nettytest.TestTls.kmf();
        var trust = io.zmux.nettytest.TestTls.tmf();
        server = QuicSslContextBuilder.forServer(keys, "changeit").applicationProtocols("qmux-02").earlyData(false);
        client = QuicSslContextBuilder.forClient().trustManager(trust).endpointIdentificationAlgorithm("HTTPS")
                .applicationProtocols("qmux-02").earlyData(false);
    }
}
