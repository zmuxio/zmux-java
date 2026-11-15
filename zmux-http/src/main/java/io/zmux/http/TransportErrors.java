package io.zmux.http;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * The last word on a connection's transport failure (a reset, a broken pipe): while it is the
 * pipeline's last handler (an HTTP/2 connection, an idle pooled HTTP/1.1 connection) nobody else
 * would take the failure, so it closes the connection; a handler added behind it (an exchange
 * in progress) gets the failure instead. Other failures go on.
 */
@ChannelHandler.Sharable
final class TransportErrors extends ChannelInboundHandlerAdapter {
    static final TransportErrors INSTANCE = new TransportErrors();

    private TransportErrors() {}

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException && ctx.pipeline().last() == this) ctx.close();
        else ctx.fireExceptionCaught(cause);
    }
}
