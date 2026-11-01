package io.zmux.http;

import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.net.ProtocolException;

/**
 * The inbound messages of one WebSocket carrier, server or client: a
 * binary message (its fragments joined) is one record's Frames field and goes to the carrier;
 * a close frame is the graceful end of the inbound half; a text message, or a message above
 * {@link #READ_LIMIT}, fails the carrier. Pings are answered. Every call is on the channel's
 * event loop.
 */
final class WebSocketFrames {
    /** The largest message: zmux's largest record (65535 bytes) plus one, so that an oversized record reaches zmux as what it is. */
    static final int READ_LIMIT = 65536;

    final BodyCarrier carrier;
    private final Channel ch;
    private CompositeByteBuf partial;

    WebSocketFrames(BodyCarrier carrier) {
        this.carrier = carrier;
        ch = carrier.ch;
    }

    /** The outbound side of a WebSocket carrier: binary messages, and a close frame as the graceful end. */
    static final BodyCarrier.Out OUT = new BodyCarrier.Out() {
        @Override public Object content(io.netty.buffer.ByteBuf data) { return new BinaryWebSocketFrame(data); }
        @Override public Object end() { return new CloseWebSocketFrame(1000, ""); }
    };

    void read(Object msg) {
        try {
            switch (msg) {
                case BinaryWebSocketFrame f -> data(f, false);
                case ContinuationWebSocketFrame f -> data(f, true);
                case CloseWebSocketFrame _ -> { dropPartial(); carrier.inEnd(); }
                case PingWebSocketFrame f -> ch.writeAndFlush(new PongWebSocketFrame(f.content().retain()));
                case PongWebSocketFrame _ -> { }
                case WebSocketFrame _ -> fail("a text message on a zmux carrier");
                default -> { }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void data(WebSocketFrame f, boolean continuation) {
        if (continuation == (partial == null)) { fail("an unexpected continuation frame"); return; }
        if (!continuation && f.isFinalFragment()) {
            carrier.in(f.content().retain());
            return;
        }
        if (partial == null) partial = ch.alloc().compositeBuffer(64);
        partial.addComponent(true, f.content().retain());
        if (partial.readableBytes() > READ_LIMIT) { fail("a message above " + READ_LIMIT + " bytes"); return; }
        if (f.isFinalFragment()) {
            CompositeByteBuf m = partial;
            partial = null;
            carrier.in(m);
        }
    }

    private void dropPartial() {
        if (partial != null) { partial.release(); partial = null; }
    }

    private void fail(String why) {
        dropPartial();
        carrier.lost(new ProtocolException(why));
        ch.close();
    }

    /** The connection ended. */
    void closed() { dropPartial(); }
}
