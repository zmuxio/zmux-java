package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A closing session ends a pipe's linger once its own CONNECTION_CLOSE on it is complete and the peer's was read on
 * it, on a carrier with a half-close (TCP) and on one without; a peer that never answers leaves the linger bound.
 */
@Timeout(120)
class CloseLingerTest {
    /** One channel that cannot shut a direction down: the peer's end shows only as the close of the whole channel. */
    private record Whole(SocketChannel ch) implements ByteChannel {
        @Override public int read(ByteBuffer dst) throws IOException { return ch.read(dst); }
        @Override public int write(ByteBuffer src) throws IOException { return ch.write(src); }
        @Override public boolean isOpen() { return ch.isOpen(); }
        @Override public void close() throws IOException { ch.close(); }
    }

    /** Two sessions over a connected socket pair; whole: each end is a channel without a half-close. */
    private record Linked(ZmuxSession client, ZmuxSession server, SocketChannel a, SocketChannel b) {
        boolean bothClosed() { return !a.isOpen() && !b.isOpen(); }
    }

    private static PipeIo pipe(SocketChannel ch, boolean whole) {
        ByteChannel c = whole ? new Whole(ch) : ch;
        return PipeChannel.pipe(c, c, Framing.BYTE_STREAM, 1 << 16);
    }

    /**
     * A server channel whose close is held until released: the peer sees no end of the carrier, so this side's own
     * write side is shut (and reported) before anything ends its inbound direction.
     */
    private static final class Held implements ByteChannel {
        final SocketChannel ch;
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean asked = new java.util.concurrent.atomic.AtomicBoolean();
        Held(SocketChannel ch) { this.ch = ch; }
        @Override public int read(ByteBuffer dst) throws IOException { return ch.read(dst); }
        @Override public int write(ByteBuffer src) throws IOException { return ch.write(src); }
        @Override public boolean isOpen() { return ch.isOpen(); }
        @Override public void close() {
            if (!asked.compareAndSet(false, true)) return;
            Thread.ofVirtual().start(() -> {
                try { release.await(); ch.close(); } catch (InterruptedException | IOException e) { Thread.currentThread().interrupt(); }
            });
        }
    }

    private static Linked link(ZmuxConfig cfg, boolean whole, java.util.function.UnaryOperator<io.zmux.carrier.PipeWriter> serverWriter) throws Exception {
        return link(cfg, whole, serverWriter, null);
    }

    private static Linked link(ZmuxConfig cfg, boolean whole, java.util.function.UnaryOperator<io.zmux.carrier.PipeWriter> serverWriter, Held[] held) throws Exception {
        try (var listener = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            SocketChannel a = SocketChannel.open(listener.getLocalAddress());
            SocketChannel b = listener.accept();
            PipeIo ia = pipe(a, whole);
            PipeIo ib;
            if (held == null) ib = pipe(b, whole);
            else { held[0] = new Held(b); ib = PipeChannel.pipe(held[0], held[0], Framing.BYTE_STREAM, 1 << 16); }
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(PipeIo.of(ib.reader(), serverWriter.apply(ib.writer())), Role.SERVER, cfg); }
                catch (Exception e) { throw new CompletionException(e); }
            }, r -> Thread.ofVirtual().start(r));
            var client = Zmux.open(ia, Role.CLIENT, cfg);
            client.awaitEstablished(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
            return new Linked(client, server.get(5, TimeUnit.SECONDS), a, b);
        }
    }

    private static void awaitClosed(Linked l, long limitMillis) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(limitMillis);
        while (!l.bothClosed() && System.nanoTime() < until) Thread.sleep(1);
    }

    /** Both ends close their carriers well under the linger, and the peer is told the code. */
    @Test void aCloseExchangeEndsTheLingerAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofSeconds(30));
            try {
                for (boolean whole : new boolean[] {false, true}) {
                    long[] took = new long[100];
                    for (int i = 0; i < 105; i++) {
                        var l = link(cfg, whole, w -> w);
                        long t0 = System.nanoTime();
                        l.client().close(0x2a, "bye");
                        awaitClosed(l, 5_000);
                        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                        assertTrue(l.bothClosed(), "carriers closed, whole=" + whole + " round " + i);
                        if (i >= 5) took[i - 5] = ms;
                        assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", true), l.server().closed().get(5, TimeUnit.SECONDS));
                        assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", false), l.client().closed().get(5, TimeUnit.SECONDS));
                    }
                    // Ending at once, not on a timer: the typical close is
                    // immediate, and none reaches the next timer the close
                    // path has, the 1 s hand-off limit (the linger is 30 s).
                    // A bound on the worst round alone would measure the
                    // host's scheduling stalls.
                    java.util.Arrays.sort(took);
                    long p95 = took[94], worst = took[99];
                    assertTrue(p95 < 100 && worst < 500,
                            "close p95 " + p95 + " ms, worst " + worst + " ms, whole=" + whole);
                }
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
        }
    }

    /** A peer whose CONNECTION_CLOSE never goes out leaves the linger as the bound. */
    @Test void aPeerThatNeverClosesLeavesTheLingerBound() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.SECONDS.toNanos(3);
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofMillis(400));
            try {
                for (boolean whole : new boolean[] {false, true}) {
                    for (int i = 0; i < 5; i++) {
                        var held = new CloseHandOffTest.Stalling[1];
                        var l = link(cfg, whole, w -> held[0] = new CloseHandOffTest.Stalling(w));
                        held[0].held.set(true);
                        long t0 = System.nanoTime();
                        l.client().close(7, "x");
                        long until = t0 + TimeUnit.SECONDS.toNanos(2);
                        while (l.a().isOpen() && System.nanoTime() < until) Thread.sleep(1);
                        long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                        assertFalse(l.a().isOpen(), "the linger ended");
                        assertTrue(took >= 350 && took < 2_900, "the linger bounded the wait: " + took + " ms, whole=" + whole);
                        l.server().close();
                    }
                }
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
                io.zmux.internal.core.Session.closeLimit = saved;
            }
        }
    }

    /**
     * The read drain keeps a stream's end, not only its bytes (SPEC.md Section 6.5): a finish or a reset received
     * before the peer's close holds the session until the application reads it, though its carriers may have ended.
     */
    @Test void aFinishReceivedBeforeTheCloseIsStillReadAsTheEndOfTheStream() throws Exception {
        endBeforeTheClose(false);
    }

    @Test void aResetReceivedBeforeTheCloseIsStillReadAsAReset() throws Exception {
        endBeforeTheClose(true);
    }

    /**
     * This side's CONNECTION_CLOSE is written and its write side shut while the peer's carrier has not ended yet
     * (its close is held): the pipe has both closes and its linger ends (SPEC.md Section 6.5 step 3), but the read
     * drain is the session's, which stays closing until the unread end is read.
     */
    @Test void anUnreadEndHoldsThePipeWhoseWriteSideShutBeforeThePeerEnded() throws Exception {
        for (boolean reset : new boolean[] {true, false}) {
            try (EventLoops loops = EventLoops.create(2)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).build();
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofSeconds(30));
                var held = new Held[1];
                try {
                    var l = link(cfg, false, w -> w, held);
                    var s = l.client().openStream();
                    s.write(new byte[] {7});
                    var r = l.server().acceptStream(Duration.ofSeconds(5));
                    if (reset) r.reset(9); else r.finish();
                    l.server().close();
                    l.server().awaitClosed(); // the client's CONNECTION_CLOSE and FIN were read; the server's carrier is still open
                    assertTrue(held[0].asked.get(), "the server let its carrier go");
                    // Both closes are done on the client's pipe: it may end (step 3), the session may not.
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (l.a().isOpen() && System.nanoTime() < until) Thread.sleep(1);
                    assertFalse(l.a().isOpen(), "the pipe's linger ended with both closes, reset=" + reset);
                    assertFalse(l.client().closed().isDone(), "the unread end holds the closing session, reset=" + reset);
                    if (reset) assertThrows(StreamResetException.class, () -> s.read(new byte[16], 0, 16));
                    else assertEquals(-1, s.read(new byte[16], 0, 16));
                    held[0].release.countDown();
                    awaitClosed(l, 5_000);
                    assertTrue(l.bothClosed(), "the session ends once the end was read, reset=" + reset);
                } finally {
                    if (held[0] != null) held[0].release.countDown();
                    io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
                }
            }
        }
    }

    private static void endBeforeTheClose(boolean reset) throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofSeconds(30));
            try {
                var l = link(cfg, false, w -> w);
                var s = l.client().openStream();
                s.write(new byte[] {7});
                var r = l.server().acceptStream(Duration.ofSeconds(5));
                if (reset) r.reset(9); else r.finish(); // the end alone: the client's stream never receives a byte
                l.server().close();
                l.server().awaitClosed();
                awaitClosed(l, 1_500);
                // The carriers may end first, once both closes are done on the pipe (SPEC.md Section 6.5 step 3).
                assertFalse(l.client().closed().isDone(), "the unread end holds the closing session");
                if (reset) assertThrows(StreamResetException.class, () -> s.read(new byte[16], 0, 16));
                else assertEquals(-1, s.read(new byte[16], 0, 16));
                awaitClosed(l, 5_000);
                assertTrue(l.bothClosed(), "the session ends once the end was read");
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
        }
    }
}
