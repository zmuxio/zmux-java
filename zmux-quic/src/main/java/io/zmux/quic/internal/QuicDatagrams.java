package io.zmux.quic.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicDatagramExtensionEvent;
import io.netty.handler.codec.quic.QuicPathEvent;
import io.netty.util.AttributeKey;
import io.zmux.carrier.CarrierChannel;
import io.zmux.internal.spi.ChannelDemux;
import io.zmux.internal.spi.Loops;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/** The negotiated datagram capacity and stream routes of one physical connection. */
public final class QuicDatagrams extends ChannelInboundHandlerAdapter {
    private static final AttributeKey<QuicDatagrams> STATE = AttributeKey.valueOf(QuicDatagrams.class, "state");
    // Minimum UDP payload, allowing a short header, a 20-byte destination ID, a four-byte
    // packet number, the authentication tag and the DATAGRAM type and length.
    private static final int MIGRATED_CAPACITY = 1200 - 1 - 20 - 4 - 16 - 3;

    private final QuicChannel connection;
    private final ChannelDemux demux = new ChannelDemux();
    private volatile int capacity;
    private final java.util.concurrent.CopyOnWriteArrayList<Runnable> watchers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicInteger owners = new AtomicInteger();

    private QuicDatagrams(QuicChannel connection) { this.connection = connection; }

    /** Call on the connection loop before its activation to observe Netty's one capacity event. */
    public static QuicDatagrams observe(QuicChannel connection) {
        var attribute = connection.attr(STATE);
        var state = attribute.get();
        if (state == null) {
            state = new QuicDatagrams(connection);
            attribute.set(state);
            connection.pipeline().addFirst(state);
        }
        return state;
    }

    public static QuicDatagrams get(QuicChannel connection) { return connection.attr(STATE).get(); }

    /** Claims the connection's incoming datagrams for this protocol until the returned action runs. */
    public Runnable claim() {
        owners.incrementAndGet();
        return owners::decrementAndGet;
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        // Netty delivers all capacity changes on the connection event loop.
        int before = capacity;
        int next = before;
        if (event instanceof QuicDatagramExtensionEvent negotiated) next = Math.max(0, negotiated.maxLength());
        if (event instanceof QuicPathEvent.PeerMigrated) next = Math.min(before, MIGRATED_CAPACITY);
        if (before != next) {
            capacity = next;
            changed();
        }
        ctx.fireUserEventTriggered(event);
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
        boolean claimed = owners.get() != 0;
        if (!(message instanceof ByteBuf datagram) || (!claimed && !demux.hasRoutes())) {
            ctx.fireChannelRead(message);
            return;
        }
        boolean forward = false;
        try {
            ByteBuffer view = datagram.nioBuffer();
            if (claimed) demux.deliver(view, view.position(), view.limit());
            else forward = !demux.route(view, view.position(), view.limit());
        } finally {
            if (!forward) datagram.release();
        }
        if (forward) ctx.fireChannelRead(message);
    }

    public Runnable watch(Runnable changed) {
        watchers.add(changed);
        return () -> watchers.remove(changed);
    }

    private void changed() {
        for (Runnable watcher : watchers) watcher.run();
    }

    @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        changed();
        ctx.fireChannelWritabilityChanged();
    }

    public int capacity() { return connection.isActive() ? capacity : 0; }
    public int room() { return connection.isWritable() ? capacity() : 0; }
    public long dropped() { return demux.dropped(); }

    /** Receives a borrowed view of a routed datagram's payload. */
    @FunctionalInterface
    public interface Receiver {
        void received(ByteBuffer bytes, int from, int to);
    }

    public Runnable register(long id, Receiver receiver) { return demux.register(id, receiver::received); }

    public static int prefixLength(long id) { return VarInt.length(id); }

    /** Submits a datagram to Netty, with an optional outer stream ID. Successful submission is not delivery. */
    public void send(byte[] bytes, long prefix, long expiry, java.util.function.Consumer<CarrierChannel.Outcome> completed) {
        var loop = connection.eventLoop();
        Loops.dispatch(loop, loop.inEventLoop(), () -> {
            int overhead = prefix < 0 ? 0 : prefixLength(prefix);
            int available = capacity() - overhead;
            if (CarrierChannel.expired(expiry)) {
                completed.accept(CarrierChannel.Outcome.EXPIRED);
                return;
            }
            if (available <= 0 || !connection.isWritable() || bytes.length > available) {
                completed.accept(CarrierChannel.Outcome.REJECTED);
                return;
            }
            ByteBuf data = overhead == 0 ? Unpooled.wrappedBuffer(bytes) : prefixed(bytes, prefix, overhead);
            // Netty can report a successful promise after discarding on native queue saturation.
            // Only peer feedback confirms arrival. A failed write is never retried as a record.
            connection.writeAndFlush(data).addListener(done -> completed.accept(
                    done.isSuccess() ? CarrierChannel.Outcome.ACCEPTED : CarrierChannel.Outcome.REJECTED));
        }, () -> completed.accept(CarrierChannel.Outcome.REJECTED));
    }

    private ByteBuf prefixed(byte[] bytes, long prefix, int overhead) {
        ByteBuf encoded = connection.alloc().ioBuffer(overhead + bytes.length);
        QuicVarInts.write(encoded, prefix);
        return encoded.writeBytes(bytes);
    }
}
