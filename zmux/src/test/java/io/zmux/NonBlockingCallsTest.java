package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The partial and non-waiting forms: tryWrite, a timed-out writeAsync, sendWait, tryOpenStream and tryOpenSendStream. */
@Timeout(30)
class NonBlockingCallsTest {
    static final int WINDOW = 64 << 10;

    static ZmuxConfig small(EventLoops loops) {
        return smallBuilder(loops).build();
    }

    static ZmuxConfig.Builder smallBuilder(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(WINDOW).maxStreamReceiveWindow(WINDOW)
                .enableDatagrams(true);
    }

    @Test void tryWriteTakesWhatFitsAndAdvancesTheBuffer() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(small(loops))) {
            var s = pair.client().openStream();
            ByteBuffer src = ByteBuffer.allocate(64 << 20);
            int n = s.tryWrite(src);
            assertTrue(n > 0 && n < src.capacity(), "took " + n);
            assertEquals(n, src.position());
            assertEquals(0, s.tryWrite(ByteBuffer.allocate(0)));
        }
    }

    @Test void timedOutWriteAsyncReportsItsPrefix() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        try (var loops = EventLoops.create(1);
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain())
                     .initialStreamReceiveWindow(WINDOW).maxStreamReceiveWindow(WINDOW)
                     .sendBuffer(WINDOW).sendBufferMax(WINDOW).build())) {
            var s = pair.client().openStream();
            Duration bound = Duration.ofMinutes(1);
            s.setWriteTimeout(bound);
            ByteBuffer src = ByteBuffer.allocate(2 << 20);
            var written = s.writeAsync(src);
            assertTrue(src.position() > 0 && src.hasRemaining(), "the first attempt copied a prefix");
            expireWrite(pair.client(), bound);
            var e = assertThrows(ExecutionException.class, () -> written.get(20, TimeUnit.SECONDS));
            var timeout = assertInstanceOf(SocketTimeoutException.class, e.getCause());
            assertEquals(src.position(), timeout.bytesTransferred);
            assertTrue(timeout.bytesTransferred > 0 && timeout.bytesTransferred < src.capacity());
        }
    }

    @Test void aWriteDeadlineFiringDuringTheAttemptSettlesItsPrefixWhenItWaits() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        try (var loops = EventLoops.create(1);
             var pair = Pair.memory(smallBuilder(loops).clockDomain(clock.domain())
                     .sendBuffer(WINDOW).sendBufferMax(WINDOW).build())) {
            var stream = pair.client().openStream();
            var entered = new CompletableFuture<Void>();
            var resume = new CompletableFuture<Void>();
            var ownerHeld = new CompletableFuture<Void>();
            var releaseOwner = new CompletableFuture<Void>();
            var bound = Duration.ofMinutes(1);
            var src = ByteBuffer.allocate(2 << 20);
            var write = CompletableFuture.supplyAsync(() -> stream.out.writeAsync(src, null, null,
                    ZmuxSession.until(bound), null, 0, () -> {
                        entered.complete(null);
                        resume.join();
                        return true;
                    }), Thread::startVirtualThread);
            try {
                entered.get(20, TimeUnit.SECONDS);
                expireWrite(pair.client(), bound);
                // Keep the owner from granting more lease and handing the write to the
                // completion thread while the first attempt is publishing its waiter.
                pair.client().driver.loop().execute(() -> {
                    ownerHeld.complete(null);
                    releaseOwner.join();
                });
                ownerHeld.get(20, TimeUnit.SECONDS);
                resume.complete(null);
                var operation = write.get(20, TimeUnit.SECONDS);
                assertTrue(operation.isDone(), "the expired operation must settle on its first wait");
                assertInstanceOf(SocketTimeoutException.class, operation.outcome());
                assertTrue(src.position() > 0 && src.hasRemaining());
                assertEquals(src.position(), io.zmux.internal.loop.SendLane.written(operation));
            } finally {
                resume.complete(null);
                releaseOwner.complete(null);
            }
        }
    }

    private static void expireWrite(ZmuxSession session, Duration bound) throws Exception {
        // The deadline is registered through the session mailbox. Order the timer turn after it.
        session.run(op -> null);
        session.driver.loop().advanceTimersForTests(bound.toNanos()).get(20, TimeUnit.SECONDS);
    }

    @Test void tryOpenFailsWithoutCreditInsteadOfWaiting() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var ccfg = ZmuxConfig.builder().eventLoops(loops).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).maxIncomingStreams(4).maxIncomingUniStreams(4).build();
            try (var pair = Pair.memory(ccfg, scfg, Framing.BYTE_STREAM)) {
                var c = pair.client();
                // The peer's credit arrives with its transport parameters.
                var first = c.openStream();
                first.write(new byte[1]); first.flush();
                assertNotNull(pair.server().acceptStream(Duration.ofSeconds(5)));
                for (int i = 1; i < 4; i++) c.tryOpenStream().flush();
                assertThrows(StreamLimitException.class, c::tryOpenStream);
                for (int i = 0; i < 4; i++) (i % 2 == 0 ? c.tryOpenSendStream() : c.tryOpenSendStream(Preamble.EMPTY)).flush();
                assertThrows(StreamLimitException.class, c::tryOpenSendStream);
            }
        }
    }

    /** A writer that holds every completion while closed. */
    static final class Gate {
        final List<Runnable> held = new ArrayList<>();
        final CompletableFuture<Void> writeHeld = new CompletableFuture<>();
        boolean closed;
        final CompletableFuture<Void> writerTerminated = new CompletableFuture<>();

        private interface ObservedWriter extends PipeWriter, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {}

        PipeIo wrap(PipeIo io) {
            PipeWriter w = io.writer();
            return io.withHalves(io.reader(), new ObservedWriter() {
                @Override public java.util.concurrent.CompletionStage<Void> released() {
                    return ((io.zmux.internal.spi.AdapterPorts.ReleasingHalf) w).released();
                }
                @Override public void write(OutboundBatch batch, Completion done) {
                    w.write(batch, new Completion() {
                        @Override public void completed() {
                            synchronized (Gate.this) {
                                if (closed) {
                                    held.add(done::completed);
                                    writeHeld.complete(null);
                                    return;
                                }
                            }
                            done.completed();
                        }
                        @Override public void failed(Throwable cause) { done.failed(cause); }
                    });
                }
                @Override public OptionalLong unsent() { return w.unsent(); }
                @Override public void close() {
                    w.close();
                    writerTerminated.complete(null);
                }
                @Override public void abort(Throwable cause) {
                    w.abort(cause);
                    writerTerminated.completeExceptionally(cause);
                }
            });
        }

        void open() {
            List<Runnable> go;
            synchronized (this) { closed = false; go = new ArrayList<>(held); held.clear(); }
            go.forEach(Runnable::run);
        }
    }

    @Test void sendWaitWaitsForQueueSpaceWhereSendDrops() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        Duration lifetime = Duration.ofMillis(1);
        var waitOptions = new DatagramSendOptions(new DatagramSendOptions.Deadline.After(lifetime), null, null, null, null);
        var noDeadline = new DatagramSendOptions(DatagramSendOptions.Deadline.None.INSTANCE, null, null, null, null);
        try (var loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain())
                    .enableDatagrams(true).datagramSendQueue(2).noDatagramQueueDelay().datagramDefaults(waitOptions).build();
            var gate = new Gate();
            PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(p[1], Role.SERVER, cfg); }
                catch (Exception e) { throw new CompletionException(e); }
            }, r -> Thread.ofVirtual().start(r));
            try (var c = Zmux.open(gate.wrap(p[0]), Role.CLIENT, cfg); var s = server.get(20, TimeUnit.SECONDS)) {
                c.datagrams().send(ByteBuffer.allocate(1), noDeadline);
                assertEquals(1, s.datagrams().receive(Duration.ofSeconds(20)).length);
                synchronized (gate) { gate.closed = true; }
                try {
                    c.datagrams().send(ByteBuffer.allocate(1), noDeadline);
                    gate.writeHeld.get(20, TimeUnit.SECONDS);
                    for (int i = 0; i < 8; i++) c.datagrams().send(ByteBuffer.allocate(1), noDeadline);
                    assertThrows(SocketTimeoutException.class,
                            () -> c.datagrams().sendWait(ByteBuffer.allocate(1), Duration.ZERO));

                    var timedOut = waitForDatagram(c, ByteBuffer.wrap(new byte[] {7}));
                    clock.whenScheduled(lifetime.toNanos()).get(20, TimeUnit.SECONDS);
                    assertFalse(timedOut.isDone());
                    clock.advance(lifetime);
                    var error = assertThrows(ExecutionException.class, () -> timedOut.get(20, TimeUnit.SECONDS));
                    assertEquals(DatagramDroppedException.Reason.DEADLINE,
                            assertInstanceOf(DatagramDroppedException.class, error.getCause()).reason());

                    var queued = waitForDatagram(c, ByteBuffer.wrap(new byte[] {9}));
                    clock.whenScheduled(2 * lifetime.toNanos()).get(20, TimeUnit.SECONDS);
                    assertFalse(queued.isDone());
                    gate.open();
                    queued.get(20, TimeUnit.SECONDS);
                    for (int i = 0; i < 3; i++) assertArrayEquals(new byte[] {0}, s.datagrams().receive(Duration.ofSeconds(20)));
                    assertArrayEquals(new byte[] {9}, s.datagrams().receive(Duration.ofSeconds(20)));
                } finally {
                    gate.open();
                }
            }
        }
    }

    private static CompletableFuture<Void> waitForDatagram(ZmuxSession session, ByteBuffer payload) {
        return CompletableFuture.runAsync(() -> {
            try { session.datagrams().sendWait(payload); }
            catch (Exception e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
    }
}
