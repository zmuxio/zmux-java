package io.zmux.http3;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.zmux.quic.internal.QuicStreamTermination;

/** Translates WT stream errors and preserves independent read and write halves. */
final class WebTransportData extends ChannelOutboundHandlerAdapter implements QuicStreamTermination {
    private final QuicStreamChannel stream;
    private final WebTransportProvider provider;
    private WebTransportStreamException stopped;

    WebTransportData(QuicStreamChannel stream, WebTransportProvider provider) {
        this.stream = stream;
        this.provider = provider;
    }

    @Override public void handlerAdded(ChannelHandlerContext ctx) {
        stream.attr(QuicStreamTermination.KEY).set(this);
        provider.observeTermination(stream, error -> {
            if (error.stopSending()) stopped = error;
            else ctx.fireUserEventTriggered(new QuicStreamTermination.InputReset(error));
        });
    }

    @Override public ChannelFuture stop(long code) { return provider.stop(stream, WebTransportStreamException.wireCode(code)); }
    @Override public ChannelFuture reset(long code) { return provider.reset(stream, WebTransportStreamException.wireCode(code)); }

    @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise completion) {
        if (stopped == null) ctx.write(message, completion);
        else {
            ReferenceCountUtil.release(message);
            completion.tryFailure(stopped);
        }
    }
}
