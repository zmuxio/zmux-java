package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** N17: the close forms on a pair of in-memory sessions. */
@Timeout(30)
class CloseFormsTest {
    /** A carrier that takes each batch only after a delay: a flush through it takes a while. */
    static final class Slow implements PipeWriter {
        final PipeWriter delegate;
        final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().daemon().unstarted(r));
        Slow(PipeWriter delegate) { this.delegate = delegate; }
        @Override public void write(OutboundBatch batch, Completion done) {
            timer.schedule(() -> delegate.write(batch, done), 150, TimeUnit.MILLISECONDS);
        }
        @Override public void close() { delegate.close(); }
        @Override public void abort(Throwable error) { delegate.abort(error); }
    }

    /** A pair whose client writes through a Slow carrier. */
    private static ZmuxSession[] slowPair(EventLoops loops) throws Exception {
        var cfg = ZmuxConfig.builder().eventLoops(loops).build();
        var io = MemoryPipe.pair(Framing.BYTE_STREAM);
        var server = CompletableFuture.supplyAsync(() -> {
            try { return Zmux.open(io[1], Role.SERVER, cfg); } catch (Exception e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
        var client = Zmux.open(PipeIo.of(io[0].reader(), new Slow(io[0].writer())), Role.CLIENT, cfg);
        return new ZmuxSession[] {client, server.get(5, TimeUnit.SECONDS)};
    }

    @Test void anAbortDuringACloseEndsTheFlushAndTheCloseDecides() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.SECONDS.toNanos(60); // The flush through the slow carrier could take minutes.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var p = slowPair(loops);
            var client = p[0]; var server = p[1];
            var in = workers.submit(() -> {
                byte[] b = new byte[1 << 16];
                try {
                    var st = server.acceptStream(Duration.ofSeconds(5));
                    while (st.read(b, 0, b.length) > 0) { /* Keeps the window open. */ }
                } catch (java.io.IOException ended) { /* The session's end, possibly before the stream arrived. */ }
                return null;
            });
            var st = client.openStream();
            st.write(new byte[4 << 20]); // Many batches through the slow carrier: the flush runs for seconds.
            var close = workers.submit(() -> { client.close(0x2a, "first"); return null; });
            Thread.sleep(100);
            client.abort(7, "second");
            close.get(30, TimeUnit.SECONDS);
            // The close returned with data still unsent: the abort ended the flush, not its completion.
            assertTrue(server.stats().bytesReceived() < 4 << 20, "the flush ran to its end");
            assertEquals(new SessionError.ApplicationClosed(0x2a, "first", true), server.closed().get(5, TimeUnit.SECONDS));
            assertEquals(new SessionError.ApplicationClosed(0x2a, "first", false), client.closed().get(5, TimeUnit.SECONDS));
            in.get(5, TimeUnit.SECONDS);
        } finally {
            io.zmux.internal.core.Session.closeLimit = saved;
        }
    }

    @Test void aCloseAfterAnAbortReturnsAtOnceAndChangesNothing() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            p.client().abort(7, "abort");
            long t0 = System.nanoTime();
            p.client().close(0x2a, "close");
            p.client().close();
            assertTrue(System.nanoTime() - t0 < io.zmux.internal.core.Session.closeLimit, "a close after an abort waited for its bound");
            assertEquals(new SessionError.ApplicationClosed(7, "abort", true), p.server().closed().get(5, TimeUnit.SECONDS));
            assertEquals(new SessionError.ApplicationClosed(7, "abort", false), p.client().closed().get(5, TimeUnit.SECONDS));
        }
    }

    @Test void aCloseBehindAFullPeerWindowGivesUpTheDataWithinTheBound() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var st = p.client().openStream();
            st.setSendBuffer(8L << 20);
            st.write(new byte[4 << 20]); // The server never reads: its windows hold most of it back.
            var in = p.server().acceptStream(Duration.ofSeconds(5));
            assertNotNull(in);
            long t0 = System.nanoTime();
            p.client().close(0x2a, "bye");
            long took = System.nanoTime() - t0;
            // It waits for credit until a tenth before its bound, never past the bound (a loose guard).
            assertTrue(took >= io.zmux.internal.core.Session.closeLimit * 9 / 10, "close returned before its flush bound: " + took / 1_000_000 + " ms");
            assertTrue(took < io.zmux.internal.core.Session.closeLimit + TimeUnit.SECONDS.toNanos(5), "close is bounded");
            assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", true), p.server().closed().get(5, TimeUnit.SECONDS));
            assertTrue(p.server().stats().bytesReceived() < 4 << 20, "the data behind the window was given up");
        }
    }

    @Test void codesAndReasonsAreChecked() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            assertThrows(IllegalArgumentException.class, () -> p.client().close(-1, ""));
            assertThrows(IllegalArgumentException.class, () -> p.client().close(1L << 62, ""));
            assertThrows(NullPointerException.class, () -> p.client().close(1, null));
            assertFalse(p.client().isClosed(), "a refused call closes nothing");
            p.client().close(0x2a, "x".repeat(2000));
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, p.server().closed().get(5, TimeUnit.SECONDS));
            assertEquals("x".repeat(1024), why.reason(), "the reason is cut to 1024 bytes");
        }
    }

    /** A 5000-byte reason of 3-byte characters is cut at a character boundary at or below 1024 bytes, locally and on the wire. */
    @Test void aLongMultiByteReasonIsCutAtACharacter() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            String reason = "\u20ac".repeat(1667); // 5001 bytes of UTF-8
            p.client().close(0x2a, reason);
            String cut = "\u20ac".repeat(341); // 1023 bytes: the 342nd character would end at 1026
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, p.server().closed().get(5, TimeUnit.SECONDS));
            assertEquals(cut, why.reason());
            var local = assertInstanceOf(SessionError.ApplicationClosed.class, p.client().closed().get(5, TimeUnit.SECONDS));
            assertEquals(cut, local.reason());
        }
    }

    /** The core cuts every Reason Phrase it is handed, whichever caller gave it. */
    @Test void theCoreCutsEveryReason() {
        byte[] b = "\u00e9".repeat(600).getBytes(java.nio.charset.StandardCharsets.UTF_8); // 1200 bytes
        byte[] cut = io.zmux.internal.wire.Utf8.truncate(b, io.zmux.internal.wire.Utf8.MAX_REASON);
        assertEquals(1024, cut.length);
        assertEquals("\u00e9".repeat(512), new String(cut, java.nio.charset.StandardCharsets.UTF_8));
        byte[] three = "a\u20ac\u20ac".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(4, io.zmux.internal.wire.Utf8.truncate(three, 5).length);
    }

    /**
     * The peer's CONNECTION_CLOSE and its carrier's end both arrive while this side's own CONNECTION_CLOSE is still
     * being written: the carrier gets the bounded graceful close once that write completes, never an abort.
     */
    @Test void aCloseStillWritingWhenThePeerEndedClosesGracefully() throws Exception {
        for (int round = 0; round < Integer.getInteger("zmux.closeRounds", 20); round++) {
            try (EventLoops loops = EventLoops.create(2)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).build();
                var gate = new NonBlockingCallsTest.Gate();
                io.zmux.carrier.PipeIo[] p = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                var serverF = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    try { return Zmux.open(gate.wrap(p[1]), Role.SERVER, cfg); } catch (Exception e) { throw new RuntimeException(e); }
                }, r -> Thread.ofVirtual().start(r));
                try (var client = Zmux.open(p[0], Role.CLIENT, cfg); var server = serverF.get(5, TimeUnit.SECONDS)) {
                    client.ping(java.time.Duration.ofSeconds(5));
                    synchronized (gate) { gate.closed = true; }
                    // The server closes first: its CONNECTION_CLOSE write stays in progress.
                    var closing = java.util.concurrent.CompletableFuture.runAsync(() -> {
                        try { server.close(7, "server"); } catch (Exception e) { throw new RuntimeException(e); }
                    }, r -> Thread.ofVirtual().start(r));
                    // The client's close and the end of its carrier reach the server meanwhile.
                    client.close(9, "client");
                    Thread.sleep(50 + round % 5 * 20);
                    gate.open();
                    closing.get(10, TimeUnit.SECONDS);
                    server.closed().get(10, TimeUnit.SECONDS);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!gate.writerClosed && gate.aborted == null && System.nanoTime() < until) Thread.sleep(5);
                    assertNull(gate.aborted, "round " + round + ": the carrier was aborted");
                    assertTrue(gate.writerClosed, "round " + round + ": the carrier was closed gracefully");
                }
            }
        }
    }
}
