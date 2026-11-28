package io.zmux.http3;

import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.carrier.CarrierChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.ChannelDemux;
import io.zmux.quic.internal.QuicDatagrams;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** One datagram namespace and lane opener per CONNECT. Mutation runs on the connection loop. */
final class WebTransportBinding {
    private final WebTransportSession session;
    private final QuicDatagrams transport;
    private final ChannelDemux pipes = new ChannelDemux();
    private final long quarterId;
    private final int prefix;
    private volatile boolean active = true;

    WebTransportBinding(WebTransportSession session, QuicDatagrams transport) {
        this.session = session;
        this.transport = transport;
        quarterId = session.id() / 4;
        prefix = QuicDatagrams.prefixLength(quarterId);
    }

    void stop() {
        active = false;
    }

    void datagram(ByteBuffer input) {
        pipes.deliver(input, input.position(), input.limit());
    }

    PipeIo pipe(WebTransportSession.Stream stream) {
        return stream.io().withCapabilities(stream.io().capabilities().toBuilder()
                .laneOpener(() -> openLane(stream.channel()))
                .datagramChannel(new PipeChannel(stream.channel())).build());
    }

    private CompletableFuture<PipeWriter> openLane(QuicStreamChannel pipe) {
        var result = new CompletableFuture<PipeWriter>();
        session.dispatch(() -> {
            if (!active || !pipe.isActive()) {
                result.completeExceptionally(new ClosedChannelException());
                return;
            }
            var opening = session.openStream(QuicStreamType.UNIDIRECTIONAL);
            result.whenComplete((writer, failure) -> opening.cancel(false));
            opening.whenComplete((lane, failure) -> {
                if (failure != null) result.completeExceptionally(failure);
                else if (!pipe.isActive() || !result.complete(lane.io().writer())) {
                    session.cancelStream(lane.channel());
                    result.completeExceptionally(new ClosedChannelException());
                }
            });
        }, () -> result.completeExceptionally(new ClosedChannelException()));
        return result;
    }

    private final class PipeChannel implements CarrierChannel {
        private final QuicStreamChannel stream;

        PipeChannel(QuicStreamChannel stream) { this.stream = stream; }

        @Override public long channelStreamId() { return stream.streamId(); }

        @Override public int maxChannelDatagram() {
            return usable() ? Math.max(0, transport.room() - prefix) : 0;
        }

        @Override public int channelDatagramLimit(int room) {
            if (room > 0) return room;
            return usable() ? Math.max(0, transport.capacity() - prefix) : 0;
        }

        @Override public void send(byte[] payload, long expiry, Consumer<Outcome> completed) {
            session.dispatch(() -> {
                if (CarrierChannel.expired(expiry)) completed.accept(Outcome.EXPIRED);
                else if (!usable()) completed.accept(Outcome.REJECTED);
                else transport.send(payload, quarterId, expiry, completed);
            }, () -> completed.accept(Outcome.REJECTED));
        }

        @Override public Runnable watch(Runnable changed) { return transport.watch(changed); }

        @Override public Runnable receive(Sink sink) {
            var subscription = new Subscription(stream);
            session.dispatch(() -> subscription.install(sink), () -> {});
            return subscription::stop;
        }

        private boolean usable() { return active && stream.isActive(); }
    }

    private final class Subscription {
        private final QuicStreamChannel stream;
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final ChannelFutureListener closed = ignored -> stop();
        private Runnable unregister;

        Subscription(QuicStreamChannel stream) { this.stream = stream; }

        void install(CarrierChannel.Sink sink) {
            if (!active || stopped.get() || !stream.isOpen()) return;
            unregister = pipes.register(stream.streamId(), (bytes, from, to) -> {
                if (stream.isActive() && !stopped.get()) sink.datagram(bytes, from, to);
            });
            stream.closeFuture().addListener(closed);
        }

        void stop() {
            if (!stopped.compareAndSet(false, true)) return;
            session.dispatch(() -> {
                if (unregister != null) unregister.run();
                stream.closeFuture().removeListener(closed);
            }, () -> {});
        }
    }
}
