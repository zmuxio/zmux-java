package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The partial and non-waiting forms: tryWrite, a timed-out writeAsync, sendWait, tryOpenStream and tryOpenSendStream. */
@Timeout(30)
class NonBlockingCallsTest {
    static final int WINDOW = 64 << 10;

    static ZmuxConfig small(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(WINDOW).maxStreamReceiveWindow(WINDOW)
                .enableDatagrams(true).build();
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
        try (var loops = EventLoops.create(1); var pair = Pair.memory(small(loops))) {
            var s = pair.client().openStream();
            s.setWriteTimeout(Duration.ofMillis(200));
            ByteBuffer src = ByteBuffer.allocate(64 << 20);
            var e = assertThrows(ExecutionException.class, () -> s.writeAsync(src).get(10, TimeUnit.SECONDS));
            var timeout = assertInstanceOf(SocketTimeoutException.class, e.getCause());
            assertTrue(timeout.bytesTransferred > 0 && timeout.bytesTransferred < src.capacity(), "prefix " + timeout.bytesTransferred);
        }
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
                long started = System.nanoTime();
                for (int i = 1; i < 4; i++) c.tryOpenStream().flush();
                assertThrows(StreamLimitException.class, c::tryOpenStream);
                for (int i = 0; i < 4; i++) (i % 2 == 0 ? c.tryOpenSendStream() : c.tryOpenSendStream(Preamble.EMPTY)).flush();
                assertThrows(StreamLimitException.class, c::tryOpenSendStream);
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
            }
        }
    }

    /** A writer that holds every completion while closed. */
    static final class Gate {
        final List<Runnable> held = new ArrayList<>();
        boolean closed;
        /** The writer's abort cause, or null; whether its graceful close was called. */
        volatile Throwable aborted;
        volatile boolean writerClosed;

        PipeIo wrap(PipeIo io) {
            PipeWriter w = io.writer();
            return io.withHalves(io.reader(), new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) {
                    w.write(batch, new Completion() {
                        @Override public void completed() {
                            synchronized (Gate.this) {
                                if (closed) { held.add(done::completed); return; }
                            }
                            done.completed();
                        }
                        @Override public void failed(Throwable cause) { done.failed(cause); }
                    });
                }
                @Override public OptionalLong unsent() { return w.unsent(); }
                @Override public OptionalLong retransmitted() { return w.retransmitted(); }
                @Override public void close() { writerClosed = true; w.close(); }
                @Override public void abort(Throwable cause) { aborted = cause; w.abort(cause); }
            });
        }

        void open() {
            List<Runnable> go;
            synchronized (this) { closed = false; go = new ArrayList<>(held); held.clear(); }
            go.forEach(Runnable::run);
        }
    }

    @Test void sendWaitWaitsForQueueSpaceWhereSendDrops() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramSendQueue(2).build();
            var gate = new Gate();
            PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
            var server = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(p[1], Role.SERVER, cfg); } catch (Exception e) { throw new RuntimeException(e); }
            }, r -> Thread.ofVirtual().start(r));
            try (var c = Zmux.open(gate.wrap(p[0]), Role.CLIENT, cfg); var s = server.get(5, TimeUnit.SECONDS)) {
                assertTrue(c.datagrams().send(ByteBuffer.allocate(1)));
                assertEquals(1, s.datagrams().receive(Duration.ofSeconds(5)).length);
                synchronized (gate) { gate.closed = true; }
                // The carrier's write stays in progress, so the queue fills behind it; send drops instead of waiting.
                for (int i = 0; i < 8; i++) assertTrue(c.datagrams().send(ByteBuffer.allocate(1)));
                assertThrows(SocketTimeoutException.class, () -> c.datagrams().sendWait(ByteBuffer.allocate(1), Duration.ofMillis(200)));
                var waiting = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    try { return c.datagrams().sendWait(ByteBuffer.allocate(1), Duration.ofSeconds(5)); }
                    catch (Exception e) { throw new RuntimeException(e); }
                }, r -> Thread.ofVirtual().start(r));
                Thread.sleep(100);
                assertFalse(waiting.isDone());
                gate.open();
                assertTrue(waiting.get(5, TimeUnit.SECONDS));
            }
        }
    }
}
