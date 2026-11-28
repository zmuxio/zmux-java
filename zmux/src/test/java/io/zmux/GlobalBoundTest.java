package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The listener-wide bound of carriers before a session ({@link ListenerConfig#maxPending()}), through its real entries. */
@Timeout(60)
class GlobalBoundTest {
    private static final long WATCHDOG_SECONDS = 20;

    private interface ReleasedWriter extends PipeWriter, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {}

    @FunctionalInterface
    interface Occupy { void awaitStage(ZmuxListener listener) throws Exception; }

    /** A carrier fed by the test; it records when the listener closes it. */
    static final class Fake {
        final CompletableFuture<PipeReader.Sink> sink = new CompletableFuture<>();
        final CountDownLatch closed = new CountDownLatch(1);
        final AtomicInteger written = new AtomicInteger();
        final PipeIo io;
        io.zmux.adapter.Handled handled;

        Fake(boolean read, boolean write) {
            PipeReader r = new PipeReader() {
                @Override public void start(Sink s) { sink.complete(s); }
                @Override public void close() { closed.countDown(); }
            };
            PipeWriter w = new ReleasedWriter() {
                private final CompletableFuture<Void> released = new CompletableFuture<>();
                @Override public void write(OutboundBatch data, Completion done) { written.addAndGet(data.bytes()); done.completed(); }
                @Override public void close() { closed.countDown(); released.complete(null); }
                @Override public void abort(Throwable cause) { close(); }
                @Override public CompletionStage<Void> released() { return released; }
            };
            io = PipeIo.of(read ? r : null, write ? w : null);
        }

        Fake() { this(true, true); }

        void sendZeros(int size) throws Exception { send(new byte[size]); }

        void send(byte... b) throws Exception { sink.get(5, TimeUnit.SECONDS).onLent(ByteBuffer.wrap(b), () -> {}); }
        boolean isClosed() { return closed.getCount() == 0; }
        void awaitClosed() throws InterruptedException { assertTrue(closed.await(5, TimeUnit.SECONDS), "closed"); }
    }

    static JoinHint from(String label) { return JoinHint.inBand(TestAddr.peer(label)); }

    static Fake handle(ZmuxListener l, String source) {
        Fake f = new Fake();
        f.handled = l.adapter().handle(f.io, from(source));
        return f;
    }

    static ListenerConfig.Builder base(EventLoops loops) {
        return ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build()).firstRecordTimeout(Duration.ofSeconds(30));
    }

    /** With maxPending 1, a carrier occupied in one stage leaves no slot: a new carrier is closed at once, counted rejected, nothing written. */
    static void holdsTheSlot(String stage, ListenerConfig.Builder b, Occupy occupy) throws Exception {
        try (var l = Zmux.listener(b.maxPending(1).build())) {
            occupy.awaitStage(l);
            long rejected = l.stats().rejected();
            Fake late = handle(l, "late");
            late.handled.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
            assertEquals(rejected + 1, l.stats().rejected(), stage);
            assertEquals(0, late.written.get(), stage);
        }
    }

    @Test void everyStageBeforeASessionHoldsASlot() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var deciding = new CompletableFuture<Void>();
            Admission decision = Admission.builder(1, (info, prefix, deadline) -> {
                deciding.complete(null);
                return new CompletableFuture<>();
            }).peekTimeout(Duration.ofSeconds(30)).build();
            var observingMore = new CompletableFuture<Void>();
            var more = new CompletableFuture<Admission.Decision>() {
                @Override public CompletableFuture<Admission.Decision> whenComplete(
                        BiConsumer<? super Admission.Decision, ? super Throwable> listener) {
                    var result = super.whenComplete(listener);
                    observingMore.complete(null);
                    return result;
                }
            };
            Admission longerPrefix = Admission.builder(1, (info, prefix, deadline) -> more)
                    .maxPeek(2).peekTimeout(Duration.ofSeconds(30)).build();

            holdsTheSlot("handshake", base(loops), l -> assertNotNull(l.adapter().reserve(TestAddr.peer("a"), null)));
            holdsTheSlot("prefix", base(loops).admission(decision), l -> {
                handle(l, "a").sink.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                assertFalse(deciding.isDone(), "waiting for the first prefix");
            });
            holdsTheSlot("decision", base(loops).admission(decision), l -> {
                handle(l, "a").send((byte) 1);
                deciding.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
            });
            holdsTheSlot("more", base(loops).admission(longerPrefix), l -> {
                var first = handle(l, "a");
                first.send((byte) 1);
                observingMore.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                more.complete(Admission.Decision.more(2));
                afterOwnerTasks(loops);
                assertFalse(first.handled.verdict().isDone(), "waiting for the second prefix byte");
            });
            var transforming = new CompletableFuture<Void>();
            holdsTheSlot("transform", base(loops).inboundTransform((io, ctx) -> {
                transforming.complete(null);
                return new CompletableFuture<>();
            }), l -> {
                handle(l, "a");
                transforming.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
            });
            holdsTheSlot("pairing", base(loops), l -> {
                Fake half = new Fake(true, false);
                l.adapter().handle(half.io, JoinHint.pair("AAAAAAAAAAAAAAAAAAAAAA", true, TestAddr.peer("a")));
                half.sink.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
            });
            holdsTheSlot("classification", base(loops), l -> handle(l, "a").sink.get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
        }
    }

    private static void afterOwnerTasks(EventLoops loops) throws Exception {
        var done = new CompletableFuture<Void>();
        loops.next().execute(() -> done.complete(null));
        done.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
    }

    @Test void stallsHoldHalfAndTheOldestMakesRoom() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var never = new CompletableFuture<Admission.Decision>();
            // Byte 1 stalls; anything else waits for a decision that never comes.
            Admission adm = Admission.builder(1, (info, prefix, deadline) -> prefix.get(0) == 1
                            ? CompletableFuture.completedFuture(Admission.Decision.STALL) : never)
                    .peekTimeout(Duration.ofSeconds(30)).stall(Duration.ofSeconds(30), Duration.ofSeconds(30)).build();
            try (var l = Zmux.listener(base(loops).maxPending(4).admission(adm).build())) {
                Fake s1 = handle(l, "s1"); s1.send((byte) 1);
                assertEquals(AdmissionVerdict.STALL, s1.handled.verdict().get(5, TimeUnit.SECONDS));
                Fake s2 = handle(l, "s2"); s2.send((byte) 1);
                assertEquals(AdmissionVerdict.STALL, s2.handled.verdict().get(5, TimeUnit.SECONDS));
                // The third stall is beyond half of 4.
                Fake s3 = handle(l, "s3"); s3.send((byte) 1);
                s3.handled.closed().get(30, TimeUnit.SECONDS);
                assertEquals(1, l.stats().admission().stallOverflow());
                assertEquals(2, l.stats().admission().stalled());
                Fake d1 = handle(l, "d1"); d1.send((byte) 0);
                Fake d2 = handle(l, "d2"); d2.send((byte) 0);
                assertFalse(s1.isClosed() || s2.isClosed() || d1.isClosed() || d2.isClosed());
                long rejected = l.stats().rejected();
                // Every slot is taken: a new carrier closes the oldest stall and proceeds.
                Fake d3 = handle(l, "d3");
                s1.awaitClosed();
                d3.sink.get(5, TimeUnit.SECONDS);
                assertFalse(s2.isClosed() || d3.isClosed());
                Fake d4 = handle(l, "d4");
                s2.awaitClosed();
                d4.sink.get(5, TimeUnit.SECONDS);
                assertFalse(d4.isClosed());
                assertEquals(rejected, l.stats().rejected(), "an evicted stall is not counted again");
                // None stalled: the new carrier is closed at once, without a byte.
                Fake d5 = handle(l, "d5");
                d5.awaitClosed();
                assertEquals(rejected + 1, l.stats().rejected());
                assertEquals(0, d5.written.get());
                assertFalse(d1.isClosed() || d2.isClosed() || d3.isClosed() || d4.isClosed());
                // A closed carrier gives its slot back: d3 ends before its prefix, and its decision closes it.
                d3.sink.get(5, TimeUnit.SECONDS).onEnd();
                d3.handled.closed().get(5, TimeUnit.SECONDS);
                Fake d6 = handle(l, "d6");
                d6.sink.get(5, TimeUnit.SECONDS);
                assertFalse(d6.isClosed());
            }
        }
    }

    @SuppressWarnings("try") // Stop the listener while the first TCP connection still holds its slot.
    @Test void socketsTakeTheSameBound() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var entered = new CompletableFuture<Void>();
            var decision = new CompletableFuture<Admission.Decision>();
            var admission = Admission.builder(0, (info, prefix, deadline) -> {
                entered.complete(null);
                return decision;
            }).peekTimeout(Duration.ofMinutes(1)).build();
            try (var listener = Zmux.listen(NetworkTest.any(),
                    base(loops).maxPending(1).maxPendingPerSource(4).admission(admission).build());
                 var first = java.nio.channels.SocketChannel.open(listener.localAddress())) {
                entered.get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                try (var second = java.nio.channels.SocketChannel.open(listener.localAddress())) {
                    second.socket().setSoTimeout((int) TimeUnit.SECONDS.toMillis(WATCHDOG_SECONDS));
                    assertEquals(-1, second.socket().getInputStream().read(), "the held admission takes the only slot");
                    listener.close();
                }
                first.socket().setSoTimeout((int) TimeUnit.SECONDS.toMillis(WATCHDOG_SECONDS));
                assertEquals(-1, first.socket().getInputStream().read());
                assertEquals(1, listener.stats().rejected());
                assertEquals(0, listener.stats().refused());
            }
        }
    }

    @Test void unlimitedNeverClosesForTheBound() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var l = Zmux.listener(base(loops).noMaxPending().build())) {
            assertTrue(base(loops).noMaxPending().build().maxPending().isEmpty());
            assertEquals(1024, base(loops).build().maxPending().orElseThrow());
            var all = new java.util.ArrayList<Fake>();
            for (int i = 0; i < 2000; i++) all.add(handle(l, "u" + i));
            var started = all.stream().map(f -> f.sink).toArray(CompletableFuture<?>[]::new);
            CompletableFuture.allOf(started).get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
            for (Fake f : all) assertFalse(f.isClosed());
            assertEquals(0, l.stats().rejected());
        }
    }
}
