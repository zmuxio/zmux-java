package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The listener-wide bound of carriers before a session ({@link ListenerConfig#maxPending()}), through its real entries. */
@Timeout(60)
class GlobalBoundTest {
    /** A carrier fed by the test; it records when the listener closes it. */
    static final class Fake {
        final CompletableFuture<PipeReader.Sink> sink = new CompletableFuture<>();
        final CountDownLatch closed = new CountDownLatch(1);
        final AtomicInteger written = new AtomicInteger();
        final PipeIo io;

        Fake(boolean read, boolean write) {
            PipeReader r = new PipeReader() {
                @Override public void start(Sink s) { sink.complete(s); }
                @Override public void close() { closed.countDown(); }
            };
            PipeWriter w = new PipeWriter() {
                @Override public void write(OutboundBatch data, Completion done) { written.addAndGet(data.bytes()); done.completed(); }
                @Override public void close() { closed.countDown(); }
                @Override public void abort(Throwable cause) { closed.countDown(); }
            };
            io = PipeIo.of(read ? r : null, write ? w : null);
        }

        Fake() { this(true, true); }

        void send(byte... b) throws Exception { sink.get(5, TimeUnit.SECONDS).onLent(ByteBuffer.wrap(b), () -> {}); }
        boolean isClosed() { return closed.getCount() == 0; }
        void awaitClosed() throws InterruptedException { assertTrue(closed.await(5, TimeUnit.SECONDS), "closed"); }
    }

    static JoinHint from(String label) { return JoinHint.inBand(TestAddr.peer(label)); }

    static Fake handle(ZmuxListener l, String source) {
        Fake f = new Fake();
        l.adapter().handle(f.io, from(source));
        return f;
    }

    static ListenerConfig.Builder base(EventLoops loops) {
        return ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build()).firstRecordTimeout(Duration.ofSeconds(30));
    }

    /** With maxPending 1, a carrier occupied in one stage leaves no slot: a new carrier is closed at once, counted rejected, nothing written. */
    static void holdsTheSlot(String stage, ListenerConfig.Builder b, Consumer<ZmuxListener> occupy) throws Exception {
        try (var l = ZmuxListener.of(b.maxPending(1).build())) {
            occupy.accept(l);
            Thread.sleep(50); // the occupying carrier reaches its stage
            long rejected = l.stats().rejected();
            Fake late = handle(l, "late");
            late.awaitClosed();
            assertEquals(rejected + 1, l.stats().rejected(), stage);
            assertEquals(0, late.written.get(), stage);
        }
    }

    @Test void everyStageBeforeASessionHoldsASlot() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var never = new CompletableFuture<Admission.Decision>();
            Admission deciding = Admission.of(1, (info, prefix, deadline) -> never).withPeekTimeout(Duration.ofSeconds(30));
            Admission more = Admission.of(1, Admission.Decider.immediate((info, prefix) -> Admission.Decision.more(2)))
                    .withMaxPeek(2).withPeekTimeout(Duration.ofSeconds(30));
            // The handshake: a slot reserved by an entry before its outer handshake.
            holdsTheSlot("handshake", base(loops), l -> assertNotNull(l.adapter().reserve(TestAddr.peer("a"), null)));
            // The admission's prefix read, then its decision.
            holdsTheSlot("prefix", base(loops).admission(deciding), l -> handle(l, "a"));
            holdsTheSlot("decision", base(loops).admission(deciding), l -> {
                try { handle(l, "a").send((byte) 1); } catch (Exception e) { throw new AssertionError(e); }
            });
            holdsTheSlot("more", base(loops).admission(more), l -> {
                try { handle(l, "a").send((byte) 1); } catch (Exception e) { throw new AssertionError(e); }
            });
            // The inbound transform.
            holdsTheSlot("transform", base(loops).inboundTransform((io, ctx) -> new CompletableFuture<>()), l -> handle(l, "a"));
            // A split half waiting for its pair.
            holdsTheSlot("pairing", base(loops), l -> {
                Fake half = new Fake(true, false);
                l.adapter().handle(half.io, JoinHint.pair("AAAAAAAAAAAAAAAAAAAAAA", true, TestAddr.peer("a")));
            });
            // Classification: no first record yet.
            holdsTheSlot("classification", base(loops), l -> handle(l, "a"));
        }
    }

    @Test void stallsHoldHalfAndTheOldestMakesRoom() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var never = new CompletableFuture<Admission.Decision>();
            // Byte 1 stalls; anything else waits for a decision that never comes.
            Admission adm = Admission.of(1, (info, prefix, deadline) -> prefix.get(0) == 1
                            ? CompletableFuture.completedFuture(Admission.Decision.STALL) : never)
                    .withPeekTimeout(Duration.ofSeconds(30)).withStall(Duration.ofSeconds(30), Duration.ofSeconds(30));
            try (var l = ZmuxListener.of(base(loops).maxPending(4).admission(adm).build())) {
                Fake s1 = handle(l, "s1"); s1.send((byte) 1);
                PipePoolTest.await(() -> l.stats().admission().stalled() == 1);
                Fake s2 = handle(l, "s2"); s2.send((byte) 1);
                PipePoolTest.await(() -> l.stats().admission().stalled() == 2);
                // The third stall is beyond half of 4.
                Fake s3 = handle(l, "s3"); s3.send((byte) 1);
                s3.awaitClosed();
                assertEquals(1, l.stats().admission().stallOverflow());
                assertEquals(2, l.stats().admission().stalled());
                Fake d1 = handle(l, "d1"); d1.send((byte) 0);
                Fake d2 = handle(l, "d2"); d2.send((byte) 0);
                Thread.sleep(50);
                assertFalse(s1.isClosed() || s2.isClosed() || d1.isClosed() || d2.isClosed());
                long rejected = l.stats().rejected();
                // Every slot is taken: a new carrier closes the oldest stall and proceeds.
                Fake d3 = handle(l, "d3");
                s1.awaitClosed();
                Thread.sleep(50);
                assertFalse(s2.isClosed() || d3.isClosed());
                Fake d4 = handle(l, "d4");
                s2.awaitClosed();
                Thread.sleep(50);
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
                d3.awaitClosed();
                Fake d6 = handle(l, "d6");
                Thread.sleep(50);
                assertFalse(d6.isClosed());
            }
        }
    }

    @Test void socketsTakeTheSameBound() throws Exception {
        try (EventLoops loops = EventLoops.create(1);
             var l = Zmux.listen(new java.net.InetSocketAddress("127.0.0.1", 0), base(loops).maxPending(1).maxPendingPerSource(4).build());
             var _ = java.nio.channels.SocketChannel.open(l.localAddress());
             var second = java.nio.channels.SocketChannel.open(l.localAddress())) {
            second.socket().setSoTimeout(5000);
            assertEquals(-1, second.socket().getInputStream().read(), "no slot left, closed at once");
            PipePoolTest.await(() -> l.stats().rejected() == 1);
            assertEquals(0, l.stats().refused());
        }
    }

    @Test void unlimitedNeverClosesForTheBound() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var l = ZmuxListener.of(base(loops).noMaxPending().build())) {
            assertTrue(base(loops).noMaxPending().build().maxPending().isEmpty());
            assertEquals(1024, base(loops).build().maxPending().orElseThrow());
            var all = new java.util.ArrayList<Fake>();
            for (int i = 0; i < 2000; i++) all.add(handle(l, "u" + i));
            Thread.sleep(100);
            for (Fake f : all) assertFalse(f.isClosed());
            assertEquals(0, l.stats().rejected());
        }
    }
}
