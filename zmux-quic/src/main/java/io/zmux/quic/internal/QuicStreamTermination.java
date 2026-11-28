package io.zmux.quic.internal;

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;

/** Protocol-specific half termination on an externally routed stream. */
public interface QuicStreamTermination {
    AttributeKey<QuicStreamTermination> KEY = AttributeKey.valueOf(QuicStreamTermination.class, "termination");

    ChannelFuture stop(long applicationCode);
    ChannelFuture reset(long applicationCode);

    /** The peer reset the incoming direction with this cause. */
    record InputReset(Throwable cause) { }

    /** A byte-stream write failed after earlier slices of the same batch completed. */
    final class PartialWrite extends java.io.IOException {
        private final int confirmed;

        public PartialWrite(Throwable cause, int confirmed) {
            super(cause);
            this.confirmed = confirmed;
        }

        public int confirmed() { return confirmed; }
    }

    static ChannelFuture stop(QuicStreamChannel stream, int code) {
        var extension = stream.attr(KEY).get();
        return extension == null ? stream.shutdownInput(code) : extension.stop(Integer.toUnsignedLong(code));
    }

    static ChannelFuture reset(QuicStreamChannel stream, int code) {
        var extension = stream.attr(KEY).get();
        return extension == null ? stream.shutdownOutput(code) : extension.reset(Integer.toUnsignedLong(code));
    }
}
