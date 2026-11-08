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
            t.close(null);
            ran.countDown();
        }));
        assertTrue(ran.await(5, TimeUnit.SECONDS));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertFalse(closed.isDone(), "the writer's close has not returned");
        release.countDown();
        closed.get(5, TimeUnit.SECONDS);
    }
}
