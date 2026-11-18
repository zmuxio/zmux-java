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

    private static Linked link(ZmuxConfig cfg, boolean whole, java.util.function.UnaryOperator<io.zmux.carrier.PipeWriter> serverWriter) throws Exception {
        try (var listener = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            SocketChannel a = SocketChannel.open(listener.getLocalAddress());
            SocketChannel b = listener.accept();
            PipeIo ia = pipe(a, whole);
            PipeIo ib = pipe(b, whole);
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
                    long worst = 0;
                    for (int i = 0; i < 105; i++) {
                        var l = link(cfg, whole, w -> w);
                        long t0 = System.nanoTime();
                        l.client().close(0x2a, "bye");
                        awaitClosed(l, 5_000);
                        long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                        assertTrue(l.bothClosed(), "carriers closed, whole=" + whole + " round " + i);
                        if (i >= 5) worst = Math.max(worst, took);
                        assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", true), l.server().closed().get(5, TimeUnit.SECONDS));
                        assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", false), l.client().closed().get(5, TimeUnit.SECONDS));
                    }
                    assertTrue(worst < 100, "worst close " + worst + " ms, whole=" + whole);
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
     * before the peer's close holds the session until the application reads it, though every carrier has ended.
     */
    @Test void aFinishReceivedBeforeTheCloseIsStillReadAsTheEndOfTheStream() throws Exception {
        endBeforeTheClose(false);
    }

    @Test void aResetReceivedBeforeTheCloseIsStillReadAsAReset() throws Exception {
        endBeforeTheClose(true);
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
                assertFalse(l.bothClosed(), "the unread end holds the closing session");
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
