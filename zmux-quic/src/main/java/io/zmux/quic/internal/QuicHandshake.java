package io.zmux.quic.internal;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
import io.zmux.Admission.OuterSecurity;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

/** QUIC handshake evidence for prefix admission. */
public final class QuicHandshake {
    private static final AttributeKey<OuterSecurity> STREAM = AttributeKey.valueOf(QuicHandshake.class, "admissionSecurity");

    private QuicHandshake() {}

    public static OuterSecurity security(QuicChannel channel) {
        var engine = channel == null ? null : channel.sslEngine();
        if (engine == null || !"TLSv1.3".equals(engine.getSession().getProtocol())) return OuterSecurity.EARLY_DATA;
        var status = engine.getHandshakeStatus();
        return status == HandshakeStatus.FINISHED || status == HandshakeStatus.NOT_HANDSHAKING
                ? OuterSecurity.QUIC : OuterSecurity.EARLY_DATA;
    }

    /** Remembers the evidence at the first delivery of a stream, before it can wait for the handshake. */
    public static OuterSecurity observe(QuicStreamChannel stream) {
        var value = stream.attr(STREAM);
        var earlier = value.get();
        if (earlier != null) return earlier;
        var current = security(stream.parent());
        return value.compareAndSet(null, current) ? current : value.get();
    }
}
