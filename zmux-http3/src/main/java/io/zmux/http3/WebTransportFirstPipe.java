package io.zmux.http3;

import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.CarrierKind;
import io.zmux.carrier.CarrierChannel;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.AdapterPorts;
import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.quic.internal.QuicCarrierStats;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

/** Late binding of the head admission's pipe to the first actual bidirectional stream. */
final class WebTransportFirstPipe implements CarrierChannel {
    final CompletableFuture<PipeIo> first = new CompletableFuture<>();
    private final Runnable withdraw;

    WebTransportFirstPipe(Runnable withdraw) { this.withdraw = withdraw; }

    PipeIo pipe(Object identity) {
        return new PipeIo(new Input(), new Output(identity), Framing.BYTE_STREAM, "WebTransport", identity)
                .withCarrier(CarrierKind.WEBTRANSPORT).withDatagramChannel(this)
                .withLaneOpener(() -> first.thenCompose(io -> io.capabilities().laneOpener().open()));
    }

    private abstract class Half implements AdapterPorts.ReleasingHalf {
        final CompletableFuture<Void> released = new CompletableFuture<>();

        void closeHalf(Consumer<PipeIo> close, boolean input) {
            if (first.completeExceptionally(new ClosedChannelException())) withdraw.run();
            first.whenComplete((io, error) -> {
                if (io == null) released.complete(null);
                else {
                    close.accept(io);
                    AdapterPorts.releasedHalf(input ? io.reader() : io.writer()).whenComplete((v, e) -> {
                        if (e == null) released.complete(null);
                        else released.completeExceptionally(e);
                    });
                }
            });
        }

        @Override public CompletionStage<Void> released() { return released.minimalCompletionStage(); }
    }

    private final class Input extends Half implements PipeReader {
        @Override public void start(PipeReader.Sink sink) {
            first.whenComplete((io, error) -> {
                if (error != null) sink.onError(error);
                else io.reader().start(sink);
            });
        }

        @Override public void close() { closeHalf(io -> io.reader().close(), true); }
    }

    private final class Output extends Half implements PipeWriter, CarrierProvider {
        private final Object physical;

        Output(Object namespace) { physical = CarrierBudget.physical(namespace); }
        @Override public Object carrierIdentity() { return physical; }
        @Override public Query carrierQuery(Object identity) {
            return physical instanceof QuicChannel channel
                    ? QuicCarrierStats.query(channel, identity) : null;
        }

        @Override public void write(OutboundBatch batch, Completion done) {
            first.whenComplete((io, error) -> {
                if (error != null) done.failed(error);
                else io.writer().write(batch, done);
            });
        }

        @Override public void close() { closeHalf(io -> io.writer().close(), false); }
        @Override public void abort(Throwable cause) { closeHalf(io -> io.writer().abort(cause), false); }
    }

    private CarrierChannel channel() {
        var io = first.isCompletedExceptionally() ? null : first.getNow(null);
        return io == null ? null : io.capabilities().datagramChannel();
    }

    @Override public long channelStreamId() { var ch = channel(); return ch == null ? -1 : ch.channelStreamId(); }
    @Override public int maxChannelDatagram() { var ch = channel(); return ch == null ? 0 : ch.maxChannelDatagram(); }
    @Override public int channelDatagramLimit(int room) {
        var ch = channel();
        return ch == null ? -1 : ch.channelDatagramLimit(room);
    }
    @Override public void send(byte[] data, long expiry, Consumer<Outcome> completed) {
        var ch = channel();
        if (ch == null) completed.accept(Outcome.REJECTED);
        else ch.send(data, expiry, completed);
    }
    @Override public Runnable receive(CarrierChannel.Sink sink) { return subscribe(ch -> ch.receive(sink)); }
    @Override public Runnable watch(Runnable changed) { return subscribe(ch -> ch.watch(changed)); }

    private Runnable subscribe(Function<CarrierChannel, Runnable> register) {
        var stop = first.thenApply(io -> register.apply(io.capabilities().datagramChannel()));
        return () -> stop.thenAccept(action -> { if (action != null) action.run(); });
    }
}
