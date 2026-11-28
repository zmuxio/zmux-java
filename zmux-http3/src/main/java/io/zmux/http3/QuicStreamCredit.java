package io.zmux.http3;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.QuicTransportError;

/** Stream allocation can disagree with the cached peer credit until its next update. */
final class QuicStreamCredit {
    private final QuicStreamType type;
    private boolean blocked;

    QuicStreamCredit(QuicStreamType type) { this.type = type; }

    boolean available(QuicChannel channel) { return !blocked && channel.peerAllowedStreams(type) > 0; }

    boolean waitForCredit(Throwable failure) {
        if (!(failure instanceof QuicException error) || error.error() != QuicTransportError.STREAM_LIMIT_ERROR) return false;
        blocked = true;
        return true;
    }

    void changed() { blocked = false; }

    boolean blocked() { return blocked; }
}
