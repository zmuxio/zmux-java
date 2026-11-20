package io.zmux;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The host's takeover of a carrier: {@link Takeover#detach()} hands it over for good, during the fallback only;
 * every half of the carrier is closed exactly once whatever closes it; {@link Takeover#value()} is the fallback
 * decision's value; {@link Takeover#messages()} types a message-framed carrier's halves; the pipe view forwards every
 * capability of the carrier; an empty first message closes the carrier without a decision.
 */
@Timeout(60)
class TakeoverTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    /** The closes and aborts each half of a carrier got. */
    static final class Counted {
        final AtomicInteger readerCloses = new AtomicInteger(), writerCloses = new AtomicInteger();

        PipeIo wrap(PipeIo io) {
            PipeReader r = io.reader();
            PipeWriter w = io.writer();
            PipeReader reader = r == null ? null : new PipeReader() {
                @Override public void start(Sink s) { r.start(s); }
                @Override public void limitFirstMessage(int limit) { r.limitFirstMessage(limit); }
                @Override public void close() { readerCloses.incrementAndGet(); r.close(); }
            };
            PipeWriter writer = w == null ? null : new PipeWriter() {
                @Override public void write(OutboundBatch b, Completion c) { w.write(b, c); }
                @Override public OptionalLong unsent() { return OptionalLong.of(7); }
                @Override public OptionalLong retransmitted() { return OptionalLong.of(11); }
                @Override public void close() { writerCloses.incrementAndGet(); w.close(); }
                @Override public void abort(Throwable cause) { writerCloses.incrementAndGet(); w.abort(cause); }
            };
            return io.withHalves(reader, writer);
        }

        void assertOnce() throws InterruptedException {
            long end = System.nanoTime() + WAIT.toNanos();
            while ((readerCloses.get() == 0 || writerCloses.get() == 0) && System.nanoTime() < end) Thread.sleep(5);
            Thread.sleep(100); // a second close would follow the first at once
            assertEquals(1, readerCloses.get(), "the reading half closed once");
            assertEquals(1, writerCloses.get(), "the writing half closed once");
        }
    }

    /** What the peer end of a carrier reads: each chunk as text, then "end" or "error". */
    static BlockingQueue<String> reading(PipeIo peer) {
        var got = new LinkedBlockingQueue<String>();
        peer.reader().start(new PipeReader.Sink() {
            @Override public void onChunk(Chunk chunk, Runnable consumed) {
                ByteBuffer b = chunk.buffer();
                byte[] m = new byte[b.remaining()];
                b.get(m);
                chunk.close();
                consumed.run();
                got.add(new String(m, US_ASCII));
            }
            @Override public void onEnd() { got.add("end"); }
            @Override public void onError(Throwable cause) { got.add("error"); }
        });
        return got;
    }

    static void write(PipeWriter w, String s) {
        var done = new CompletableFuture<Void>();
        w.write(OutboundBatch.of(ByteBuffer.wrap(s.getBytes(US_ASCII))), new PipeWriter.Completion() {
            @Override public void completed() { done.complete(null); }
            @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
        });
        done.orTimeout(5, TimeUnit.SECONDS).join();
    }

    /** A listener whose admission hands every carrier to fallback with Decision.fallback("route"). */
    private static void withListener(Admission.Fallback fallback, ListenerTest body) throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.builder(1, Admission.Decider.immediate((info, prefix) -> Admission.Decision.fallback("route")))
                    .peekTimeout(Duration.ofSeconds(5)).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.builder().session(cfg).admission(adm).fallback(fallback).build())) {
                body.run(l);
            }
        }
    }

    @FunctionalInterface
    interface ListenerTest { void run(ZmuxListener l) throws Exception; }

    /** Hands a memory carrier to l, its halves counted; returns the peer end. */
    private static PipeIo hand(ZmuxListener l, Counted counted, Framing framing, Function<PipeIo, PipeIo> with) {
        PipeIo[] pair = MemoryPipe.pair(framing);
        l.adapter().handle(counted.wrap(with.apply(pair[1])), JoinHint.inBand(TestAddr.peer("peer")));
        return pair[0];
    }

    @Test void aDetachedCarrierOutlivesTheFallbackAndIsTheHostsToClose() throws Exception {
        var taken = new LinkedBlockingQueue<Takeover>();
        var detached = new LinkedBlockingQueue<Boolean>();
        withListener(t -> { taken.add(t); detached.add(t.detach()); detached.add(t.detach()); return null; }, l -> {
            var counted = new Counted();
            PipeIo peer = hand(l, counted, Framing.BYTE_STREAM, p -> p);
            write(peer.writer(), "x");
            Takeover t = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals(true, detached.poll(5, TimeUnit.SECONDS));
            assertEquals(true, detached.poll(5, TimeUnit.SECONDS), "a second detach");
            assertEquals("route", t.value(), "the fallback decision's value");
            assertTrue(t.messages().isEmpty(), "a byte stream has no message halves");
            Thread.sleep(200);
            assertEquals(0, counted.readerCloses.get() + counted.writerCloses.get(), "zmux did not close a detached carrier");
            // Served after the fallback returned, on another thread.
            var got = reading(peer);
            var in = reading(t.pipe());
            assertEquals("x", in.poll(5, TimeUnit.SECONDS), "the replayed byte");
            write(t.pipe().writer(), "late");
            assertEquals("late", got.poll(5, TimeUnit.SECONDS));
            t.pipe().writer().close();
            assertEquals("end", got.poll(5, TimeUnit.SECONDS), "half-closed");
            write(peer.writer(), "more");
            assertEquals("more", in.poll(5, TimeUnit.SECONDS), "the receiving half reads on");
            t.pipe().reader().close();
            counted.assertOnce();
            assertTrue(t.detach(), "detached stays detached");
        });
    }

    @Test void detachAfterTheFallbackFailsAndTheCarrierWasClosed() throws Exception {
        var taken = new LinkedBlockingQueue<Takeover>();
        withListener(t -> { taken.add(t); return null; }, l -> {
            var counted = new Counted();
            PipeIo peer = hand(l, counted, Framing.BYTE_STREAM, p -> p);
            var got = reading(peer);
            write(peer.writer(), "x");
            Takeover t = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals("end", got.poll(5, TimeUnit.SECONDS), "zmux closed the carrier when the fallback returned");
            assertThrows(IllegalStateException.class, t::detach);
            counted.assertOnce();
        });
    }

    @Test void eachHalfClosesOnceWhoeverClosesIt() throws Exception {
        // The host closes both halves in the call, then the stage completes: no second close; a detach then
        // reports the closed carrier.
        var detach = new LinkedBlockingQueue<Boolean>();
        withListener(t -> {
            t.pipe().writer().close();
            t.pipe().reader().close();
            detach.add(t.detach());
            return null;
        }, l -> {
            var counted = new Counted();
            hand(l, counted, Framing.BYTE_STREAM, p -> p).writer().close();
            assertEquals(false, detach.poll(5, TimeUnit.SECONDS), "detach reports a carrier the host closed");
            counted.assertOnce();
        });
        // The host closes one half, zmux the other on return.
        withListener(t -> { t.pipe().writer().close(); return null; }, l -> {
            var counted = new Counted();
            hand(l, counted, Framing.BYTE_STREAM, p -> p).writer().close();
            counted.assertOnce();
        });
        // A failed stage aborts the carrier, once.
        withListener(t -> CompletableFuture.failedFuture(new java.io.IOException("served badly")), l -> {
            var counted = new Counted();
            hand(l, counted, Framing.BYTE_STREAM, p -> p).writer().close();
            counted.assertOnce();
        });
        // Detached, aborted by the host.
        var taken = new LinkedBlockingQueue<Takeover>();
        withListener(t -> { t.detach(); taken.add(t); return null; }, l -> {
            var counted = new Counted();
            hand(l, counted, Framing.BYTE_STREAM, p -> p).writer().close();
            Takeover t = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(t);
            t.pipe().writer().abort(new java.io.IOException("done"));
            t.pipe().reader().close();
            t.pipe().writer().close();
            counted.assertOnce();
        });
    }

    @Test void aMessageCarriersHalvesAreTyped() throws Exception {
        var taken = new LinkedBlockingQueue<Takeover>();
        withListener(t -> { taken.add(t); t.detach(); return null; }, l -> {
            var counted = new Counted();
            PipeIo peer = hand(l, counted, Framing.MESSAGES, p -> p);
            write(peer.writer(), "first");
            Takeover t = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(t);
            var m = t.messages().orElseThrow();
            assertSame(t.pipe().reader(), m.reader());
            assertSame(t.pipe().writer(), m.writer());
            var in = reading(t.pipe());
            assertEquals("first", in.poll(5, TimeUnit.SECONDS), "the first message, whole");
            write(peer.writer(), "second");
            assertEquals("second", in.poll(5, TimeUnit.SECONDS));
            var got = reading(peer);
            write(m.writer(), "answer");
            assertEquals("answer", got.poll(5, TimeUnit.SECONDS));
            m.reader().close();
            m.writer().close();
            counted.assertOnce();
        });
    }

    @Test void anEmptyFirstMessageClosesWithoutADecision() throws Exception {
        var decided = new AtomicInteger();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.builder(1, Admission.Decider.immediate((info, prefix) -> {
                decided.incrementAndGet();
                return Admission.Decision.FALLBACK;
            })).peekTimeout(Duration.ofSeconds(5)).build();
            var served = new AtomicInteger();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0),
                    ListenerConfig.builder().session(cfg).admission(adm).fallback(t -> { served.incrementAndGet(); return null; }).build())) {
                var counted = new Counted();
                PipeIo peer = hand(l, counted, Framing.MESSAGES, p -> p);
                var got = reading(peer);
                // An empty message: a batch of one message with no bytes.
                var done = new CompletableFuture<Void>();
                peer.writer().write(OutboundBatch.of(ByteBuffer.allocate(0)), new PipeWriter.Completion() {
                    @Override public void completed() { done.complete(null); }
                    @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
                });
                assertEquals("end", got.poll(5, TimeUnit.SECONDS), "the carrier is closed");
                counted.assertOnce();
                assertEquals(0, decided.get(), "no decision on an empty first message");
                assertEquals(0, served.get());
                assertEquals(1, l.stats().admission().closed());
            }
        }
    }

    /**
     * Every capability of a pipe input is classified: forwarded by the takeover's pipe view (its value on the view is
     * the carrier's), the view's own, or not forwarded. A capability added to {@link PipeIo.Capabilities} or to the
     * halves fails this test until it is classified and, if forwarded, handed through by the view.
     */
    @Test void theViewForwardsEveryCapability() throws Exception {
        Set<String> forwarded = Set.of("exporter", "applicationProtocol", "serverName", "fateGroup", "writeOverhead", "maxRecord",
                "isTerminatedTls", "isCarrierSecure", "reportsFailure", "handshakePadding");
        // The view copies the input it reads, so its receive memory is its own; lanes run zmux sessions.
        Set<String> own = Set.of("receiveMemory"), notForwarded = Set.of("laneOpener");
        Set<String> declared = new TreeSet<>();
        for (Method m : PipeIo.Capabilities.class.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers()) && m.getParameterCount() == 0
                    && !Set.of("toBuilder", "hashCode", "toString").contains(m.getName())) declared.add(m.getName());
        }
        Set<String> classified = new TreeSet<>(forwarded);
        classified.addAll(own);
        classified.addAll(notForwarded);
        assertEquals(classified, declared, "every capability of PipeIo.Capabilities is classified");
        Set<String> halves = new TreeSet<>();
        for (Method m : PipeWriter.class.getMethods()) if (m.isDefault() && m.getParameterCount() == 0) halves.add(m.getName());
        for (Method m : PipeReader.class.getMethods()) if (m.isDefault()) halves.add(m.getName());
        // unsent and retransmitted are forwarded; a reader's first-message bound only applies before zmux starts it.
        assertEquals(new TreeSet<>(Set.of("unsent", "retransmitted", "limitFirstMessage")), halves, "every half capability is classified");

        byte[] key = {1, 2, 3};
        String protocol = ZmuxConfig.builder().build().alpn().getFirst();
        Function<PipeIo, PipeIo> full = p -> p.withCapabilities(PipeIo.Capabilities.builder()
                        .exporter((label, context, length) -> Arrays.copyOf(key, length)).receiveMemory(1 << 20).applicationProtocol(protocol)
                        .serverName("example.test").fateGroup(5).writeOverhead(9).maxRecord(1 << 15).terminatedTls(true)
                        .carrierSecure(true).reportsFailure(true).handshakePadding(13).laneOpener(() -> new CompletableFuture<>()).build())
                .withLabel("label").withCarrier(CarrierKind.WEBSOCKET).withVariant("v");
        PipeIo source = full.apply(MemoryPipe.pair(Framing.BYTE_STREAM)[1]);
        var taken = new LinkedBlockingQueue<Takeover>();
        withListener(t -> { taken.add(t); return new CompletableFuture<>().completeOnTimeout(null, 1, TimeUnit.SECONDS); }, l -> {
            PipeIo peer = hand(l, new Counted(), Framing.BYTE_STREAM, full);
            write(peer.writer(), "x");
            PipeIo view = taken.poll(5, TimeUnit.SECONDS).pipe();
            PipeIo.Capabilities a = source.capabilities(), b = view.capabilities();
            for (String name : forwarded) {
                Method m = PipeIo.Capabilities.class.getMethod(name);
                Object want = m.invoke(a), have = m.invoke(b);
                if (name.equals("exporter")) {
                    assertNotNull(have, name);
                    assertArrayEquals(((PipeIo.Exporter) want).export("l", new byte[0], 3), ((PipeIo.Exporter) have).export("l", new byte[0], 3), name);
                } else assertEquals(want, have, name);
            }
            assertNull(b.laneOpener(), "lanes are not forwarded");
            assertEquals("label", view.label());
            assertEquals(CarrierKind.WEBSOCKET, view.carrier());
            assertEquals("v", view.variant());
            assertEquals(OptionalLong.of(7), view.writer().unsent());
            assertEquals(OptionalLong.of(11), view.writer().retransmitted());
        });
    }
}
