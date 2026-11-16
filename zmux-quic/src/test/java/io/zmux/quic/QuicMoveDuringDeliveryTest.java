package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamChannelConfig;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.loop.Inbound;
import io.zmux.internal.loop.Transport;
import io.zmux.internal.loop.TransportInfo;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** A transport that moves while it delivers a read hands the reads still held to the next owner, in order. */
class QuicMoveDuringDeliveryTest {
    /** A QUIC stream channel with nothing behind it: configuration calls chain, other calls do nothing. */
    private static QuicStreamChannel stream(EventLoop loop) {
        QuicStreamChannel[] self = new QuicStreamChannel[1];
        QuicStreamChannelConfig config = (QuicStreamChannelConfig) Proxy.newProxyInstance(QuicStreamChannelConfig.class.getClassLoader(),
                new Class<?>[] {QuicStreamChannelConfig.class}, (p, m, a) -> m.getReturnType().isInstance(p) ? p : defaultOf(m.getReturnType()));
        DefaultChannelPromise[] closed = new DefaultChannelPromise[1];
        self[0] = (QuicStreamChannel) Proxy.newProxyInstance(QuicStreamChannel.class.getClassLoader(), new Class<?>[] {QuicStreamChannel.class}, (p, m, a) -> switch (m.getName()) {
            case "config" -> config;
            case "type" -> QuicStreamType.BIDIRECTIONAL;
            case "eventLoop" -> loop;
            case "closeFuture" -> closed[0];
            case "hashCode" -> System.identityHashCode(p);
            case "equals" -> p == a[0];
            case "toString" -> "test stream";
            default -> m.getReturnType().isInstance(p) ? p : defaultOf(m.getReturnType());
        });
        closed[0] = new DefaultChannelPromise(self[0], loop);
        return self[0];
    }

    private static Object defaultOf(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == int.class || t == long.class || t == short.class || t == byte.class) return 0;
        return null;
    }

    private static ByteBuf read(String s) { return Unpooled.directBuffer().writeBytes(s.getBytes(java.nio.charset.StandardCharsets.US_ASCII)); }

    @Test void readsHeldAtAMoveReachTheNextOwner() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            EventLoop loop = group.next();
            ByteBuf first = read("first"), second = read("second");
            StringBuilder next = new StringBuilder();
            AtomicReference<Transport> moved = new AtomicReference<>();
            loop.submit(() -> {
                NettyHost host = new NettyHost(loop);
                StreamCarrier carrier = new StreamCarrier(stream(loop), host, true, true);
                // Both reads arrive before an owner: they wait in the carrier.
                carrier.channelRead(null, first);
                carrier.channelRead(null, second);
                var info = new TransportInfo(new io.zmux.internal.core.CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false), "", null, 0, 0);
                QuicTransport t = new QuicTransport(host, carrier, info, null);
                t.start(new Inbound() {
                    // The first read makes the owner move the transport (a listener routing it): it leaves.
                    @Override public int bytes(Transport tr, ReadBuf buf, ByteBuffer data, int pos, int lim) {
                        moved.set(tr.moveTo(host));
                        return GONE;
                    }
                    @Override public void ended(Transport tr, boolean graceful, boolean partial) {}
                });
                moved.get().start(new Inbound() {
                    @Override public int bytes(Transport tr, ReadBuf buf, ByteBuffer data, int pos, int lim) {
                        byte[] b = new byte[lim - pos];
                        data.get(pos, b);
                        next.append(new String(b, java.nio.charset.StandardCharsets.US_ASCII));
                        return lim - pos;
                    }
                    @Override public void ended(Transport tr, boolean graceful, boolean partial) {}
                });
            }).get(5, TimeUnit.SECONDS);
            assertNotNull(moved.get());
            assertEquals("firstsecond", next.toString(), "the read in delivery and the one held at the move reach the next owner, in order");
            assertEquals(0, first.refCnt());
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }
}
