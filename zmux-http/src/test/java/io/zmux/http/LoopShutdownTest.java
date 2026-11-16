package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.zmux.Chunk;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * zmux's calls on a carrier whose event loop has shut down: each fails what it brought on the
 * calling thread (a completion, a sink) and none throws or hangs.
 */
class LoopShutdownTest {
    /** A sink that records its error and refuses data. */
    static final class ErrorSink implements PipeReader.Sink {
        final List<Throwable> errors = new CopyOnWriteArrayList<>();

        @Override public void onChunk(Chunk chunk, Runnable consumed) { chunk.close(); throw new AssertionError("no data expected"); }
        @Override public void onEnd() { throw new AssertionError("no end expected"); }
        @Override public void onError(Throwable cause) { errors.add(cause); }
    }

    /** A completion that records its failure. */
    static final class Recorded implements PipeWriter.Completion {
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        boolean completed;

        @Override public void completed() { completed = true; }
        @Override public void failed(Throwable cause) { failures.add(cause); }
    }

    /** Every call of zmux on p, after its loop terminated, fails or does nothing at once. */
    static void assertCallsFailAtOnce(PipeIo p) {
        ErrorSink sink = new ErrorSink();
        p.reader().start(sink);
        assertEquals(1, sink.errors.size(), "the sink fails on the calling thread");
        assertInstanceOf(ClosedChannelException.class, sink.errors.getFirst());
        Recorded w = new Recorded();
        p.writer().write(OutboundBatch.of(ByteBuffer.wrap(new byte[] {1, 2, 3})), w);
        assertEquals(1, w.failures.size(), "the write fails on the calling thread");
        assertInstanceOf(ClosedChannelException.class, w.failures.getFirst());
        assertTrue(!w.completed);
        assertDoesNotThrow(() -> p.reader().close());
        assertDoesNotThrow(() -> p.writer().close());
        assertDoesNotThrow(() -> p.writer().abort(new ClosedChannelException()));
    }

    /** The caller's group terminates under an exchange carrier: zmux's later calls fail at once, and the client lets the carrier go. */
    @Test void exchangeClientAfterGroupTerminated() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        HttpCarrierClient hc = HttpCarrierClient.builder().group(group).timeout(Duration.ofSeconds(5)).build();
        // A closed port: attempts fail and repeat, the carrier stays up until the group goes.
        PipeIo p = hc.exchangePipe("http://127.0.0.1:9/x", ExchangeTest.cfg().build());
        assertEquals(1, hc.exchanges.size());
        assertTrue(group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS));
        assertCallsFailAtOnce(p);
        assertTrue(hc.exchanges.isEmpty(), "a refused call lets the carrier go");
        // A carrier opened on the terminated group fails the same way and is never counted.
        PipeIo late = hc.exchangePipe("http://127.0.0.1:9/x", ExchangeTest.cfg().build());
        assertTrue(hc.exchanges.isEmpty());
        assertCallsFailAtOnce(late);
        assertDoesNotThrow(hc::close);
    }

    /** A sink that records errors and closes what it gets. */
    static final class Draining implements PipeReader.Sink {
        final List<Throwable> errors = new CopyOnWriteArrayList<>();

        @Override public void onChunk(Chunk chunk, Runnable consumed) { chunk.close(); consumed.run(); }
        @Override public void onEnd() {}
        @Override public void onError(Throwable cause) { errors.add(cause); }
    }

    /**
     * On the caller's group, close comes first: when it returns, every connection the client
     * opened is closed and its exchange carriers have failed, while the group runs on.
     */
    @Test void clientClosesBeforeTheCallersGroup() throws Exception {
        var c = ExchangeTest.cfg().build();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (var f = Fixture.plain(b -> b.exchange(c))) {
            HttpCarrierClient hc = HttpCarrierClient.builder().group(group).timeout(Duration.ofSeconds(5)).build();
            PipeIo streaming = hc.pipe(f.base, java.util.Map.of(), io.zmux.PipeOptions.Direction.BIDIRECTIONAL, null);
            PipeIo exchange = hc.exchangePipe(f.base.toString(), c);
            Draining s = new Draining(), x = new Draining();
            streaming.reader().start(s);
            exchange.reader().start(x);
            assertTrue(hc.conns.open() > 0);
            hc.close();
            assertEquals(0, hc.conns.open(), "every connection is closed");
            assertEquals(1, x.errors.size(), "the exchange carrier failed");
            assertTrue(hc.exchanges.isEmpty());
            assertTrue(!group.isShuttingDown());
            // The connections' ends reach their handlers on the loop: one more task there sees them done.
            group.submit(() -> {}).sync();
            assertEquals(1, s.errors.size(), "the streaming carrier was lost");
            Recorded w = new Recorded();
            streaming.writer().write(OutboundBatch.of(ByteBuffer.wrap(new byte[] {1})), w);
            // The write fails on the loop, and the request reports it in a task of its own there.
            group.submit(() -> {}).sync();
            group.submit(() -> {}).sync();
            assertEquals(1, w.failures.size());
            assertThrowsClosed(hc);
        } finally {
            assertTrue(group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS));
        }
    }

    /** A reader started before the caller's group terminated learns of it from the termination, with no call of zmux. */
    @Test void exchangeReaderFailsWhenTheGroupTerminates() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        HttpCarrierClient hc = HttpCarrierClient.builder().group(group).timeout(Duration.ofSeconds(5)).build();
        PipeIo p = hc.exchangePipe("http://127.0.0.1:9/x", ExchangeTest.cfg().build());
        var failed = new java.util.concurrent.CompletableFuture<Throwable>();
        Recorded w = new Recorded();
        p.reader().start(new PipeReader.Sink() {
            @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); }; release.run(); }
            @Override public void onEnd() { failed.completeExceptionally(new AssertionError("end")); }
            @Override public void onError(Throwable cause) { failed.complete(cause); }
        });
        assertTrue(group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS));
        assertInstanceOf(ClosedChannelException.class, failed.get(5, TimeUnit.SECONDS));
        p.writer().write(OutboundBatch.of(ByteBuffer.wrap(new byte[] {1})), w);
        assertEquals(1, w.failures.size());
        assertTrue(hc.exchanges.isEmpty());
        hc.close();
    }

    private static void assertThrowsClosed(HttpCarrierClient hc) {
        org.junit.jupiter.api.Assertions.assertThrows(io.zmux.CarrierClientClosedException.class, () -> hc.exchangePipe("http://127.0.0.1:9/x", ExchangeTest.cfg().build()));
    }
}
