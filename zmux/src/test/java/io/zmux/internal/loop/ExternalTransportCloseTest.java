package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class ExternalTransportCloseTest {
    /** The writer closed by an earlier shutdown of the output still counts: the carrier is closed once its close returned. */
    @Test void closedWaitsForAWriterAShutdownIsStillClosing() throws Exception {
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        PipeWriter w = new PipeWriter() {
            @Override public void write(OutboundBatch b, Completion d) { d.completed(); }
            @Override public void close() {
                entered.countDown();
                try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        };
        PipeReader r = new PipeReader() {
            @Override public void start(Sink sink) { }
            @Override public void close() { }
        };
        EventLoop loop = EventLoopGroup.nextShared();
        Transport t = Transport.of(loop, new PipeIo(r, w, Framing.BYTE_STREAM, "memory", null));
        assertNotNull(t);
        var closed = t.whenClosed().toCompletableFuture();
        var ran = new CountDownLatch(1);
        assertTrue(loop.tryExecute(() -> {
            t.shutdownOutput();
            t.reject(null);
            ran.countDown();
        }));
        assertTrue(ran.await(5, TimeUnit.SECONDS));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        assertFalse(closed.isDone(), "the writer's close has not returned");
        release.countDown();
        closed.get(5, TimeUnit.SECONDS);
    }
    // A lane writer and a previously shut unused writer each require abort to leave close.
    @Test void blockingCloseAndEscalationMustBothExitBeforeRetirement() throws Exception {
        // HS-51.
        for (boolean unused : new boolean[]{false, true}) {
            var close = new java.util.concurrent.CompletableFuture<Void>();
            var abort = new java.util.concurrent.CompletableFuture<Void>();
            var release = new java.util.concurrent.CompletableFuture<Void>();
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var loop = new EventLoop("abort-gate", true, BufferPool.DEFAULT_IDLE_BYTES);
            io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ofMillis(50));
            try {
                var physical = new ExternalTransport(loop, PipeIo.of(null, new PipeWriter() {
                    @Override public void write(OutboundBatch b, Completion done) { done.completed(); }
                    @Override public void close() { close.complete(null); abort.join(); }
                    @Override public void abort(Throwable e) {
                        calls.incrementAndGet();
                        abort.complete(null);
                        release.join();
                    }
                }));
                ExternalTransportTest.run(loop, () -> {
                    var driver = new SessionRuntime(loop, new io.zmux.internal.core.Session(
                            io.zmux.internal.core.CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                    var member = driver.carriers().attach(physical, 0, unused ? 0 : 1, null);
                    if (unused) physical.shutdownOutput();
                    driver.carriers().remove(member, null);
                });
                close.get(3, TimeUnit.SECONDS);
                abort.get(3, TimeUnit.SECONDS);
                assertFalse(physical.whenClosed().toCompletableFuture().isDone());
                release.complete(null);
                physical.whenClosed().toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertEquals(1, calls.get());
            } finally {
                abort.complete(null);
                release.complete(null);
                loop.close();
                io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ZERO);
            }
        }
    }

}
