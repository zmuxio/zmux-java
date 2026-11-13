package io.zmux.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayDeque;

/**
 * Keeps an HTTP/1.1 connection's answers in the order of its requests: while an answer is
 * outstanding ({@link #hold}), requests the decoder already parsed (pipelined in the same read)
 * wait here, and go on once it was written ({@link #release}). Loop only.
 */
final class PipelineGate extends ChannelInboundHandlerAdapter {
    private final ArrayDeque<Object> waiting = new ArrayDeque<>();
    private ChannelHandlerContext ctx;
    private boolean held;

    @Override public void handlerAdded(ChannelHandlerContext ctx) { this.ctx = ctx; }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (held || !waiting.isEmpty()) waiting.add(msg);
        else ctx.fireChannelRead(msg);
    }

    /** An answer is outstanding: later requests wait. */
    void hold() { held = true; }

    /** The outstanding answer was written: waiting requests go on, until one holds again. */
    void release() {
        held = false;
        Object m;
        while (!held && (m = waiting.poll()) != null) ctx.fireChannelRead(m);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        drop();
        ctx.fireChannelInactive();
    }

    @Override public void handlerRemoved(ChannelHandlerContext ctx) { drop(); }

    private void drop() {
        for (Object m; (m = waiting.poll()) != null; ) ReferenceCountUtil.release(m);
    }

    /** The gate of ctx's HTTP/1.1 connection; null on HTTP/2 streams. */
    static PipelineGate of(ChannelHandlerContext ctx) { return ctx.pipeline().get(PipelineGate.class); }
}
