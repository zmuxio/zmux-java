package io.zmux.internal.loop;

import io.zmux.internal.memory.MemoryBudget;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Session;

import io.zmux.internal.core.Time;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
final class SendBackingWaitTest {
    private static final class Host extends SessionHost {
        private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();

        Host() { super(0); }
        @Override public boolean inLoop() { return true; }
        @Override public boolean tryExecute(Runnable task) { commands.add(task); return true; }
        @Override protected void requestRoundEnd() {}
        @Override protected void wakeAt(long deadline) {}
        @Override protected long timerNow() { return 0; }

        void deliver() {
            for (Runnable task; (task = commands.poll()) != null;) task.run();
        }
    }

    @Test void aWriteWaitingForBackingCanBeCancelledTimedOutAndClosed() throws Exception {
        var budget = new MemoryBudget(CoreConfig.UNLIMITED, null);
        var config = CoreConfig.builder().budget(budget);
        config.local().maxStreamsBidi = config.local().maxStreamsUni = 0;
        var core = new Session(config.build(), 0);
        var host = new Host();
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        var stream = core.streams().openStream(false);
        var lane = driver.lanes.of(stream).send();
        budget.limit(budget.used());
        var held = new ArrayList<ReadBuf>();
        var backing = core.sending().backing(stream);
        for (ReadBuf block; (block = backing.block()) != null;) held.add(block);
        try {
            var cancelled = lane.writeAsync(ByteBuffer.allocate(1), null, null, Time.NEVER);
            host.deliver();
            assertFalse(cancelled.isDone());
            assertTrue(cancelled.cancel());
            assertTrue(cancelled.isCancelled());

            var expires = lane.writeAsync(ByteBuffer.allocate(1), null, null, System.nanoTime() + 600 * Time.SECOND);
            host.deliver();
            assertFalse(expires.isDone());
            host.runTimers(host.earliestTimer());
            host.deliver();
            assertInstanceOf(SocketTimeoutException.class, expires.outcome());

            var closing = lane.writeAsync(ByteBuffer.allocate(1), null, null, Time.NEVER);
            host.deliver();
            assertFalse(closing.isDone());
            var done = new CompletableFuture<>();
            assertTrue(closing.onDoneIndependent(() -> done.complete(closing.outcome())));
            driver.failInternalNow(new IllegalStateException("test close"));
            assertInstanceOf(CoreException.class, done.get(25, TimeUnit.SECONDS));
        } finally {
            driver.failInternalNow(new IllegalStateException("test cleanup"));
            host.deliver();
            held.forEach(ReadBuf::unref);
        }
        assertEquals(0, budget.used());
    }
}
