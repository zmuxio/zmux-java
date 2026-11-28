package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The close forms on a pair of in-memory sessions. */
@Timeout(30)
class CloseFormsTest {
    @Test void anAbortDuringACloseEndsTheFlushAndTheCloseDecides() throws Exception {
        try (var loops = EventLoops.create(2); var pair = Pair.memory(NonBlockingCallsTest.small(loops))) {
            var client = pair.client();
            var server = pair.server();
            var st = client.openStream();
            st.write(new byte[2 * NonBlockingCallsTest.WINDOW]);
            try (var incoming = server.acceptStream()) {
                assertEquals(st.id(), incoming.id());
                client.run(op -> {
                    assertTrue(client.core.flushable(), "the suffix is held by the peer's fixed credit");
                    client.close(0x2a, "first"); // Installs the first close before handing the flush to a worker.
                    client.abort(7, "second");
                    return null;
                });
                assertEquals(new SessionError.ApplicationClosed(0x2a, "first", true), server.closed().get(20, TimeUnit.SECONDS));
                assertEquals(new SessionError.ApplicationClosed(0x2a, "first", false), client.closed().get(20, TimeUnit.SECONDS));
                assertTrue(server.stats().bytesReceived() <= NonBlockingCallsTest.WINDOW);
            }
        }
    }

    @Test void aCloseAfterAnAbortReturnsAtOnceAndChangesNothing() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            p.client().abort(7, "abort");
            p.client().close(0x2a, "close");
            p.client().close();
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
            assertEquals(true, p.client().run(op -> p.client().core.flushable()));
            p.client().close(0x2a, "bye");
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
        try (var loops = EventLoops.create(2)) {
            var clock = new SpliceHooksTest.ManualClock();
            var cfg = ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain()).build();
            var gate = new NonBlockingCallsTest.Gate();
            var pipes = MemoryPipe.pair(Framing.BYTE_STREAM);
            var end = new CompletableFuture<Void>();
            var input = pipes[1].reader();
            PipeReader watched = new PipeReader() {
                @Override public void start(Sink sink) {
                    input.start(new Sink() {
                        @Override public void onChunk(Chunk chunk, Runnable consumed) { sink.onChunk(chunk, consumed); }
                        @Override public void onEnd() { sink.onEnd(); end.complete(null); }
                        @Override public void onError(Throwable cause) { sink.onError(cause); end.completeExceptionally(cause); }
                    });
                }
                @Override public void close() { input.close(); }
            };
            pipes[1] = gate.wrap(pipes[1].withHalves(watched, pipes[1].writer()));
            try (var pair = Pair.open(pipes, cfg, cfg)) {
                var server = pair.server();
                pair.client().ping(Duration.ofSeconds(20));
                synchronized (gate) { gate.closed = true; }
                try {
                    CloseLingerTest.closeOnOwner(server, 7, "server");
                    gate.writeHeld.get(20, TimeUnit.SECONDS);
                    pair.client().close(9, "client");
                    end.get(20, TimeUnit.SECONDS);
                    server.run(op -> null); // The EOF was posted before signalling.
                    assertFalse(gate.writerTerminated.isDone(), "the local CONNECTION_CLOSE still owns the writer");
                } finally { gate.open(); }
                gate.writerTerminated.get(20, TimeUnit.SECONDS);
                server.closed().get(20, TimeUnit.SECONDS);
            }
        }
    }
}
