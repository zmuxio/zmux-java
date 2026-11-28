package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.SpliceHooksTest.ManualClock;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.spi.AdapterPorts;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Close exchange, physical linger and read drain over carriers with and without half-close. */
@Timeout(120)
class CloseLingerTest {
    private static final Duration WATCHDOG = Duration.ofSeconds(20);

    /** One channel that cannot shut a direction down. */
    private record Whole(SocketChannel ch) implements ByteChannel {
        @Override public int read(ByteBuffer dst) throws IOException { return ch.read(dst); }
        @Override public int write(ByteBuffer src) throws IOException { return ch.write(src); }
        @Override public boolean isOpen() { return ch.isOpen(); }
        @Override public void close() throws IOException { ch.close(); }
    }

    private record Linked(ZmuxSession client, ZmuxSession server, SocketChannel a, SocketChannel b,
                          CompletionStage<Void> clientRelease, CompletionStage<Void> serverRelease) implements AutoCloseable {
        @Override public void close() throws IOException {
            try (a; b) {
                // The sockets also clean up a failed scenario whose protocol clocks stay frozen.
            }
        }

        void awaitCarrierRelease() throws Exception {
            await(clientRelease);
            await(serverRelease);
            assertFalse(a.isOpen(), "the client socket was released");
            assertFalse(b.isOpen(), "the server socket was released");
        }
    }

    /** Holds the actual server socket open after both logical halves release it. */
    private static final class Held implements ByteChannel {
        private final SocketChannel socket;
        private final AtomicBoolean closing = new AtomicBoolean();
        private final CompletableFuture<Void> requested = new CompletableFuture<>();
        private final CompletableFuture<Void> allowClose = new CompletableFuture<>();

        Held(SocketChannel socket) { this.socket = socket; }

        @Override public int read(ByteBuffer dst) throws IOException { return socket.read(dst); }
        @Override public int write(ByteBuffer src) throws IOException { return socket.write(src); }
        @Override public boolean isOpen() { return socket.isOpen(); }

        @Override public void close() {
            if (!closing.compareAndSet(false, true)) return;
            allowClose.thenRun(() -> {
                try { socket.close(); }
                catch (IOException failed) { throw new CompletionException(failed); }
            });
            requested.complete(null);
        }
    }

    /** Holds the peer's next write until cleanup can return its batch, without sending it. */
    private static final class BlockedWriter implements PipeWriter, AdapterPorts.ReleasingHalf {
        private final PipeWriter lower;
        private final AtomicBoolean blocked = new AtomicBoolean();
        private final CompletableFuture<Completion> held = new CompletableFuture<>();

        BlockedWriter(PipeWriter lower) { this.lower = lower; }

        @Override public void write(OutboundBatch batch, Completion done) {
            if (blocked.get()) {
                if (!held.complete(done)) throw new IllegalStateException("second blocked carrier write");
            } else lower.write(batch, done);
        }

        @Override public void close() { lower.close(); }
        @Override public void abort(Throwable cause) { lower.abort(cause); }
        @Override public CompletionStage<Void> released() { return AdapterPorts.releasedHalf(lower); }

        void discard() {
            held.thenAccept(write -> write.failed(new IOException("blocked write discarded by the fixture")));
        }
    }

    private static ZmuxConfig config(EventLoops loops, ManualClock clock) {
        return ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain()).build();
    }

    private static PipeIo pipe(ByteChannel channel) {
        return PipeChannel.pipe(channel, channel, Framing.BYTE_STREAM, 1 << 16);
    }

    private static Linked link(ZmuxConfig clientConfig, ZmuxConfig serverConfig, boolean whole,
                               UnaryOperator<PipeWriter> serverWriter, Held[] held) throws Exception {
        try (var listener = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            SocketChannel a = SocketChannel.open(listener.getLocalAddress());
            try {
                SocketChannel b = listener.accept();
                try {
                    var ia = pipe(whole ? new Whole(a) : a);
                    if (held != null) held[0] = new Held(b);
                    var ib = pipe(held != null ? held[0] : whole ? new Whole(b) : b);
                    var accepting = CompletableFuture.supplyAsync(() -> {
                        try { return Zmux.open(ib.withHalves(ib.reader(), serverWriter.apply(ib.writer())), Role.SERVER, serverConfig); }
                        catch (IOException e) { throw new CompletionException(e); }
                    }, task -> Thread.ofVirtual().start(task));
                    var client = Zmux.open(ia, Role.CLIENT, clientConfig);
                    var server = await(accepting);
                    return new Linked(client, server, a, b, attachment(client), attachment(server));
                } catch (Exception | Error failure) {
                    closeAfterFailure(b, failure);
                    throw failure;
                }
            } catch (Exception | Error failure) {
                closeAfterFailure(a, failure);
                throw failure;
            }
        }
    }

    private static void closeAfterFailure(SocketChannel socket, Throwable failure) {
        try { socket.close(); }
        catch (IOException cleanup) { failure.addSuppressed(cleanup); }
    }

    private static CompletionStage<Void> attachment(ZmuxSession session) {
        var release = session.driver.attachmentClosed(PipeId.BOOTSTRAP.value());
        assertFalse(release.toCompletableFuture().isDone(), "a live bootstrap attachment has a release receipt");
        return release;
    }

    private static <T> T await(CompletionStage<T> signal) throws Exception {
        return signal.toCompletableFuture().get(WATCHDOG.toNanos(), TimeUnit.NANOSECONDS);
    }

    static void closeOnOwner(ZmuxSession session, long code, String reason) {
        // Exercise the real close exchange without the public caller's independent wall-clock flush bound.
        assertTrue(session.driver.executeCarrier(() -> session.core.close(code, ZmuxSession.reasonBytes(reason))));
    }

    private static void awaitHandoff(ZmuxSession session) throws Exception {
        await(session.<Void>async(op -> session.core.waitCloseHandedOff(op) ? true : Operation.PENDING,
                session.core::cancelCloseHandOffWait, null, Long.MAX_VALUE, ignored -> null));
    }

    static void afterRound(ZmuxSession session) throws Exception {
        var loop = session.driver.loop();
        var done = new CompletableFuture<Void>();
        loop.execute(() -> loop.execute(() -> done.complete(null)));
        await(done);
    }

    @Test void aCloseExchangeEndsTheLingerAtOnce() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var cfg = config(loops, new ManualClock());
            for (boolean whole : new boolean[] {false, true}) {
                try (var link = link(cfg, cfg, whole, writer -> writer, null)) {
                    closeOnOwner(link.client, 0x2a, "bye");
                    link.awaitCarrierRelease();
                    assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", true), await(link.server.closed()));
                    assertEquals(new SessionError.ApplicationClosed(0x2a, "bye", false), await(link.client.closed()));
                }
            }
        }
    }

    @Test void aPeerThatNeverClosesLeavesTheLingerBound() throws Exception {
        try (var loops = EventLoops.create(2)) {
            for (boolean whole : new boolean[] {false, true}) {
                var clock = new ManualClock();
                // Freeze the peer's own hand-off timer while advancing just the client's linger.
                var serverConfig = config(loops, new ManualClock());
                var writer = new BlockedWriter[1];
                try (var link = link(config(loops, clock), serverConfig, whole,
                        lower -> writer[0] = new BlockedWriter(lower), null)) {
                    try {
                        writer[0].blocked.set(true);
                        closeOnOwner(link.client, 7, "x");
                        awaitHandoff(link.client);
                        await(writer[0].held);
                        long linger = (Long) link.client.run(op -> link.client.core.lingerTimeout());
                        clock.advance(Duration.ofNanos(linger - 1));
                        afterRound(link.client);
                        assertFalse(link.clientRelease.toCompletableFuture().isDone());
                        assertTrue(link.a.isOpen());

                        clock.advance(Duration.ofNanos(1));
                        await(link.clientRelease);
                        assertFalse(link.a.isOpen());
                        assertEquals(new SessionError.ApplicationClosed(7, "x", false), await(link.client.closed()));
                    } finally {
                        writer[0].discard();
                    }
                }
            }
        }
    }

    @Test void aFinishReceivedBeforeTheCloseIsStillReadAsTheEndOfTheStream() throws Exception {
        endBeforeTheClose(false, false);
    }

    @Test void aResetReceivedBeforeTheCloseIsStillReadAsAReset() throws Exception {
        endBeforeTheClose(true, false);
    }

    @Test void aPeerCloseCanReleaseTheCarrierBeforeEofDuringReadDrain() throws Exception {
        endBeforeTheClose(false, true);
        endBeforeTheClose(true, true);
    }

    private static void endBeforeTheClose(boolean reset, boolean holdPeer) throws Exception {
        try (var loops = EventLoops.create(2)) {
            var cfg = config(loops, new ManualClock());
            var held = holdPeer ? new Held[1] : null;
            try (var link = link(cfg, cfg, false, writer -> writer, held)) {
                try {
                    var stream = link.client.openStream();
                    stream.write(new byte[] {7});
                    var remote = link.server.acceptStream(WATCHDOG);
                    if (reset) remote.reset(9); else remote.finish();
                    // Observe the end without consuming it before submitting the close.
                    stream.awaitReadable(WATCHDOG);
                    closeOnOwner(link.server, 0, "");
                    if (holdPeer) {
                        await(held[0].requested);
                        assertTrue(link.b.isOpen(), "the peer has not closed its physical carrier");
                        await(link.clientRelease);
                        assertFalse(link.a.isOpen(), "both close frames end the pipe's linger");
                    } else {
                        await(link.server.closed());
                        await(link.clientRelease);
                        assertFalse(link.a.isOpen(), "the carrier is released with the stream end still unread");
                    }
                    assertFalse(link.client.closed().isDone(), "the unread end still holds the session");
                    if (reset) assertThrows(StreamResetException.class, () -> stream.read(new byte[16], 0, 16));
                    else assertEquals(-1, stream.read(new byte[16], 0, 16));
                    if (holdPeer) held[0].allowClose.complete(null);
                    await(link.client.closed());
                    await(link.server.closed());
                    link.awaitCarrierRelease();
                } finally {
                    if (holdPeer) held[0].allowClose.complete(null);
                }
            }
        }
    }
}
