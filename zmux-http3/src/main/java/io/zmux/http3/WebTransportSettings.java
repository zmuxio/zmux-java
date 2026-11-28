package io.zmux.http3;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;

/** WebTransport capabilities advertised on the peer's HTTP/3 control stream. */
record WebTransportSettings(boolean enabled, boolean datagrams, boolean connect) {
    static final long WT_ENABLED = 0x2c7cf000L;
    private static final long LEGACY_ENABLE = 0x2b603742L;
    private static final long LEGACY_MAX_SESSIONS_DRAFT02 = 0x2b603743L;
    private static final long LEGACY_MAX_SESSIONS_CHROMIUM = 0x14e9cd29L;
    private static final long LEGACY_MAX_SESSIONS = 0xc671706aL;
    private static final long INITIAL_MAX_UNI = 0x2b64L;
    private static final long INITIAL_MAX_BIDI = 0x2b65L;

    static Http3Settings advertise(boolean server) {
        var settings = new Http3Settings(WebTransportSettings::validate);
        settings.enableH3Datagram(true);
        if (server) settings.enableConnectProtocol(true);
        settings.put(WT_ENABLED, 1L);
        settings.put(LEGACY_ENABLE, 1L);
        settings.put(LEGACY_MAX_SESSIONS_DRAFT02, 1L);
        settings.put(LEGACY_MAX_SESSIONS_CHROMIUM, 1L);
        settings.put(LEGACY_MAX_SESSIONS, 1L);
        return settings;
    }

    static void controlFailure(ChannelHandlerContext ctx, Throwable cause) {
        var error = cause instanceof DecoderException ? cause.getCause() : cause;
        if (error instanceof IllegalArgumentException) {
            ((QuicChannel) ctx.channel().parent()).close(true,
                    Http3ErrorCode.H3_SETTINGS_ERROR.code(), Unpooled.EMPTY_BUFFER);
        }
    }

    static boolean validate(long key, long value) {
        if ((key == WT_ENABLED && value > 1)
                || ((key == INITIAL_MAX_UNI || key == INITIAL_MAX_BIDI) && value > 1L << 60)) {
            throw new IllegalArgumentException("invalid WebTransport setting " + key);
        }
        return key == WT_ENABLED || key == LEGACY_ENABLE || key == LEGACY_MAX_SESSIONS
                || key == LEGACY_MAX_SESSIONS_DRAFT02 || key == LEGACY_MAX_SESSIONS_CHROMIUM;
    }

    static WebTransportSettings read(Http3Settings settings) {
        boolean enabled = settings.getOrDefault(WT_ENABLED, 0) == 1
                || settings.getOrDefault(LEGACY_ENABLE, 0) > 0
                || settings.getOrDefault(LEGACY_MAX_SESSIONS_DRAFT02, 0) > 0
                || settings.getOrDefault(LEGACY_MAX_SESSIONS_CHROMIUM, 0) > 0
                || settings.getOrDefault(LEGACY_MAX_SESSIONS, 0) > 0;
        return new WebTransportSettings(enabled, Boolean.TRUE.equals(settings.h3DatagramEnabled()),
                Boolean.TRUE.equals(settings.connectProtocolEnabled()));
    }

    boolean permits(boolean peerIsServer, boolean quicDatagrams, boolean legacyRequest) {
        return (enabled || legacyRequest) && datagrams && (!peerIsServer || connect) && quicDatagrams;
    }
}
