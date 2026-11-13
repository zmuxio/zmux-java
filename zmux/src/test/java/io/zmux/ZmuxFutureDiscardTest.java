package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A value that arrives after the future was decided is released, not dropped. */
@Timeout(30)
class ZmuxFutureDiscardTest {
    static final class Resource implements AutoCloseable {
        final CountDownLatch closed = new CountDownLatch(1);
        @Override public void close() { closed.countDown(); }
    }

    @Test void aStageValueAfterCancelIsClosed() throws Exception {
        CompletableFuture<Resource> stage = new CompletableFuture<>();
        ZmuxFuture<Resource> f = ZmuxFuture.of(stage, null);
        assertTrue(f.cancel(false));
        Resource r = new Resource();
        stage.complete(r);
        assertTrue(r.closed.await(3, TimeUnit.SECONDS));
    }

    @Test void aStageValueAfterAnOutsideCompletionIsClosed() throws Exception {
        CompletableFuture<Resource> stage = new CompletableFuture<>();
        ZmuxFuture<Resource> f = ZmuxFuture.of(stage, null);
        assertTrue(f.completeExceptionally(new TimeoutException()));
        Resource r = new Resource();
        stage.complete(r);
        assertTrue(r.closed.await(3, TimeUnit.SECONDS));
    }

    @Test void aBodyThatFinishesAfterCancelHasItsValueClosed() throws Exception {
        CountDownLatch proceed = new CountDownLatch(1);
        Resource r = new Resource();
        ZmuxFuture<Resource> f = ZmuxFuture.onThread("discard-test", () -> {
            // Ignores the interrupt, as a body that finished its last blocking step does.
            while (true) {
                try { if (proceed.await(10, TimeUnit.SECONDS)) break; }
                catch (InterruptedException e) { /* Keep waiting. */ }
            }
            return r;
        });
        assertTrue(f.cancel(true));
        proceed.countDown();
        assertTrue(r.closed.await(3, TimeUnit.SECONDS));
        assertTrue(f.isCancelled());
    }

    @Test void aPublishedValueIsNotClosed() throws Exception {
        Resource r = new Resource();
        ZmuxFuture<Resource> f = ZmuxFuture.of(CompletableFuture.completedFuture(r), null);
        assertSame(r, f.get(1, TimeUnit.SECONDS));
        assertEquals(1, r.closed.getCount());
    }
}
