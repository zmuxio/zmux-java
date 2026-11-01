package io.zmux.http;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.LinkedBlockingQueue;

/** A host's handler: answers every request with 200 "host" and records method, target and body. */
final class HostHandler extends ChannelInboundHandlerAdapter {
    record Seen(String method, String uri, byte[] body) {}

    final LinkedBlockingQueue<Seen> seen;
    private HttpRequest req;
    private final ByteArrayOutputStream body = new ByteArrayOutputStream();

    HostHandler(LinkedBlockingQueue<Seen> seen) { this.seen = seen; }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof HttpRequest r) { req = r; body.reset(); }
            if (msg instanceof HttpContent c) body.writeBytes(ByteBufUtil.getBytes(c.content()));
            if (msg instanceof LastHttpContent && req != null) {
                seen.add(new Seen(req.method().name(), req.uri(), body.toByteArray()));
                var resp = new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.OK, Unpooled.copiedBuffer("host".getBytes()));
                HttpUtil.setContentLength(resp, 4);
                ctx.writeAndFlush(resp);
                req = null;
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }
}
