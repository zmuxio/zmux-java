package io.zmux;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.AdapterPorts.ReleasingHalf;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

final class SessionTestSupport {
    private SessionTestSupport() {}
    @FunctionalInterface interface Checked { boolean get() throws Exception; }
    static void await(ZmuxSession session, Checked condition) throws Exception {
        await(condition, session.driver.loop()::observeRoundsForTests);
    }

    static void await(EventLoops loops, Checked condition) throws Exception {
        await(condition, loops.group()::observeRoundsForTests);
    }

    private static void await(Checked condition, Function<Runnable, Runnable> subscribe) throws Exception {
        // Coalesce completed rounds. Registration itself also ends in a round notification.
        var published = new ArrayBlockingQueue<Boolean>(1);
        Runnable unsubscribe = subscribe.apply(() -> published.offer(Boolean.TRUE));
        try {
            while (!condition.get())
                assertTrue(published.poll(30, TimeUnit.SECONDS) != null, "no owner round after the last snapshot");
        } finally {
            unsubscribe.run();
        }
    }
    static void inactive(ZmuxPipe pipe) throws Exception {
        assertTrue(isInactive(pipe), "pipe still active");
    }
    static boolean isInactive(ZmuxPipe pipe) throws Exception {
        // An empty snapshot: retirement completed already.
        return pipe.stats().map(state -> (state.outbound() == null || java.util.Set.of(PipeState.Outbound.FAILED, PipeState.Outbound.CLOSED).contains(state.outbound()))
                && (state.inbound() == null || state.inbound() == PipeState.Inbound.ENDED)).orElse(true);
    }
    static ZmuxConfig config(EventLoops loops, int cap) {
        return configBuilder(loops, cap).build();
    }
    static ZmuxConfig.Builder configBuilder(EventLoops loops, int cap) {
        return ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(cap)))
                .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation().stabilityHold(Duration.ZERO);
    }

    /** A connected memory pipe whose first write can be staged before listener admission. */
    static final class MemoryBootstrap implements PipeWriter, ReleasingHalf {
        private final PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        final CompletableFuture<Void> firstRecord = new CompletableFuture<>();

        PipeIo client() { return pair[0].withHalves(pair[0].reader(), this); }

        void admit(ZmuxListener listener) {
            admit(listener, UnaryOperator.identity());
        }

        void admit(ZmuxListener listener, UnaryOperator<PipeIo> observe) {
            listener.adapter().handle(observe.apply(pair[1]), JoinHint.inBand(TestAddr.peer("bootstrap")));
        }

        @Override public void write(OutboundBatch batch, Completion done) {
            pair[0].writer().write(batch, done);
            firstRecord.complete(null);
        }

        @Override public void close() { pair[0].writer().close(); }
        @Override public void abort(Throwable cause) { pair[0].writer().abort(cause); }
        @Override public CompletionStage<Void> released() { return ((ReleasingHalf) pair[0].writer()).released(); }
    }

    static final class Fixture implements AutoCloseable {
        final ZmuxListener listener; final ZmuxSession client, server;
        Fixture(ZmuxConfig cfg) throws Exception {
            this(cfg, new MemoryBootstrap());
        }
        private Fixture(ZmuxConfig cfg, MemoryBootstrap bootstrap) throws Exception {
            this(cfg, bootstrap.client(), listener -> bootstrap.firstRecord.thenRun(() -> bootstrap.admit(listener)));
        }
        Fixture(ZmuxConfig cfg, PipeIo[] io) throws Exception {
            this(cfg, io[0], listener -> listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("bootstrap"))));
        }
        private Fixture(ZmuxConfig cfg, PipeIo io, Consumer<ZmuxListener> admit) throws Exception {
            listener = Zmux.listener(ListenerConfig.of(cfg));
            ZmuxSession opened = null;
            try {
                admit.accept(listener);
                opened = Zmux.open(io, Role.CLIENT, cfg);
                server = listener.accept(Duration.ofSeconds(30));
                client = opened;
            } catch (Exception | Error failure) {
                if (opened != null) opened.abort(0, "");
                try { listener.close(); }
                catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        @Override public void close() throws IOException { client.close(); server.close(); client.awaitClosed(); server.awaitClosed(); listener.close(); }
    }

    static CapacitySnapshot awaitCapacity(ZmuxSession session, Predicate<CapacitySnapshot> condition) throws Exception {
        var capacity = session.capacity();
        while (!condition.test(capacity)) capacity = session.awaitCapacityChange(capacity.observed()).get(30, TimeUnit.SECONDS);
        return capacity;
    }

    static SessionControl.Snapshot observe(SessionControl control, Predicate<SessionControl.Snapshot> condition) throws Exception {
        var snapshot = control.snapshot();
        while (!condition.test(snapshot)) snapshot = control.awaitSnapshotChange(snapshot).get(30, TimeUnit.SECONDS);
        return snapshot;
    }

    static void leaveReceiveOnly(Fixture fixture) throws Exception {
        add(fixture, new MemberSpec("receive", List.of(new AttachmentSpec(Directions.RECEIVE, PipeOptions.defaults()))));
        fixture.client.pipes().stream().filter(pipe -> pipe.id().equals(PipeId.BOOTSTRAP)).findFirst().orElseThrow().abort();
        awaitCapacity(fixture.client, capacity -> capacity.covered() == Directions.RECEIVE);
    }

    static SessionTarget sendRecoveryTarget(String candidate) {
        var quota = new SessionTarget.Quota("send", 1, Directions.SEND, List.of(candidate));
        return new SessionTarget(1, List.of(new SessionTarget.Group("repair", List.of(quota), SessionTarget.Rotation.NONE)), List.of());
    }

    static CapacityChange change(ZmuxSession session, MemberSpec member, Set<MemberId> drain) throws IOException {
        var capacity = session.capacity();
        return session.beginChange(new ChangeRequest(capacity.policyRevision(), capacity.observed(), Duration.ofMinutes(1),
                List.of(member), drain, 0));
    }

    static CarrierOpener memoryOpener(ZmuxListener listener, BiFunction<AttachmentSpec, io.zmux.carrier.PipeIo,
            io.zmux.carrier.PipeIo> edit) {
        return (scope, request) -> {
            var carriers = new ArrayList<io.zmux.carrier.PipeIo>();
            for (var attachment : request.layout()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(pair[1], JoinHint.inBand(null));
                carriers.add(edit.apply(attachment, pair[0]));
            }
            return new CarrierOpener.Opened(carriers);
        };
    }

    static Attempt.Result add(Fixture fixture, MemberSpec member) throws Exception {
        try (var change = change(fixture.client, member, Set.of())) {
            var result = change.startMember(0, memoryOpener(fixture.listener, (attachment, carrier) -> carrier))
                    .completion().get(30, TimeUnit.SECONDS);
            org.junit.jupiter.api.Assertions.assertEquals(Attempt.Disposition.CONFIRMED, result.disposition(),
                    () -> String.valueOf(result.error()));
            org.junit.jupiter.api.Assertions.assertEquals(CapacityChange.Status.COMMITTED, change.commit().status());
            return result;
        }
    }

    /**
     * Reports the carrier's second write (the joiner's Confirm, after its Hello) as taken at once
     * and delivers it delay later, as a socket does with bytes still in flight.
     */
    static final class LateConfirm implements io.zmux.carrier.PipeWriter {
        final io.zmux.carrier.PipeWriter delegate; final java.util.concurrent.Executor delivery; final AtomicInteger writes = new AtomicInteger();
        final CompletableFuture<Void> delivered = new CompletableFuture<>();
        LateConfirm(io.zmux.carrier.PipeWriter delegate, long delay) { this(delegate, CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)); }
        LateConfirm(io.zmux.carrier.PipeWriter delegate, java.util.concurrent.Executor delivery) {
            this.delegate = delegate;
            this.delivery = delivery;
        }
        @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) {
            int n = writes.incrementAndGet();
            if (n != 2) {
                // Later writes keep their order behind the held Confirm.
                if (n > 2) delivered.thenRun(() -> delegate.write(batch, done)); else delegate.write(batch, done);
                return;
            }
            var copy = java.nio.ByteBuffer.allocate(batch.bytes());
            for (int i = 0; i < batch.segmentCount(); i++) copy.put(batch.segment(i).duplicate());
            done.completed();
            delivery.execute(() -> delegate.write(io.zmux.carrier.OutboundBatch.of(copy.flip()), new Completion() {
                @Override public void completed() { delivered.complete(null); }
                @Override public void failed(Throwable cause) { delivered.complete(null); }
            }));
        }
        @Override public void close() { delivered.thenRun(delegate::close); }
        @Override public void abort(Throwable error) { delegate.abort(error); }
    }

}
