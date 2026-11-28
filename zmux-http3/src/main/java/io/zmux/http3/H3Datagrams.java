package io.zmux.http3;

import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.util.AttributeKey;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Observes connection-level HTTP Datagram SETTINGS on a shared connection. */
final class H3Datagrams {
    private static final AttributeKey<H3Datagrams> STATE = AttributeKey.valueOf(H3Datagrams.class, "state");

    private final CompletableFuture<Boolean> peerEnabled = new CompletableFuture<>();

    static H3Datagrams install(QuicChannel connection) {
        var state = new H3Datagrams();
        connection.attr(STATE).set(state);
        connection.closeFuture().addListener(ignored -> state.peerEnabled.complete(false));
        return state;
    }

    Http3SettingsFrame settings() { return new DefaultHttp3SettingsFrame(new Http3Settings().enableH3Datagram(true)); }

    void peerSettings(Http3SettingsFrame settings) {
        peerEnabled.complete(Boolean.TRUE.equals(settings.settings().h3DatagramEnabled()));
    }

    static CompletionStage<Boolean> peerEnabled(QuicChannel connection) {
        var state = connection.attr(STATE).get();
        return state == null ? CompletableFuture.completedStage(false) : state.peerEnabled.minimalCompletionStage();
    }
}
