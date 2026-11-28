package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ListenerLimitsTest {
    @Test void splitBootstrapRequiresTwoSourceSlots() {
        assertThrows(IllegalArgumentException.class, () -> ListenerLimits.builder().maxPendingPerSource(1));
        assertEquals(2, ListenerLimits.builder().maxPendingPerSource(2).build().maxPendingPerSource());
    }

    @Test void setLimitsRoundTripsAndALoweredMaxSuspendedClosesSuspendedSessions() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ListenerLimits.builder().maxPendingPerSource(0));
        assertThrows(IllegalArgumentException.class, () -> ListenerLimits.builder().firstRecordTimeout(Duration.ZERO));
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerResourcesTest.config(loops);
            var lc = ListenerConfig.builder().session(cfg).noMaxPending().build();
            try (var listener = Zmux.listen(NetworkTest.any(), lc)) {
                assertEquals(lc.limits(), listener.limits());
                assertTrue(listener.limits().maxPending().isEmpty());
                var changed = listener.limits().toBuilder().maxPending(5).firstRecordTimeout(Duration.ofSeconds(1)).build();
                listener.setLimits(changed);
                assertEquals(changed, listener.limits());
                assertEquals(Duration.ofSeconds(1), listener.adapter().firstRecordTimeout());

                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                c.pipes().getFirst().abort();
                while (!(s.nextEvent() instanceof SessionEvent.SessionSuspended)) { /* event barrier */ }
                assertFalse(s.isClosed());
                listener.setLimits(changed.toBuilder().maxSuspended(0).build());
                assertInstanceOf(SessionError.ResumeTimeout.class, s.awaitClosed());
                c.close(); c.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    @Test void loweringMemoryBelowCommitmentsRejectsTheEntireLimitUpdate() throws Exception {
        var events = new LinkedBlockingQueue<long[]>();
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerResourcesTest.config(loops);
            var lc = ListenerConfig.builder().session(cfg).noMemoryBudget().memoryPressureListener((u, l) -> events.add(new long[] {u, l})).build();
            try (var listener = Zmux.listen(NetworkTest.any(), lc)) {
                var original = listener.limits();
                assertTrue(original.memoryBudget().isEmpty());
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                var invalid = original.toBuilder().memoryBudget(0).maxPending(5).maxSuspended(0)
                        .firstRecordTimeout(Duration.ofSeconds(1)).build();
                assertThrows(IllegalArgumentException.class, () -> listener.setLimits(invalid));
                assertEquals(original, listener.limits());
                assertEquals(original.firstRecordTimeout(), listener.adapter().firstRecordTimeout());
                assertTrue(events.isEmpty());
                var w = c.openStream(); w.write(new byte[] {42});
                var held = s.acceptStream();
                byte[] value = new byte[1];
                assertEquals(1, held.read(value, 0, 1));
                assertEquals(42, value[0]);
                var valid = invalid.toBuilder().memoryBudget(1L << 30).build();
                listener.setLimits(valid);
                assertEquals(valid, listener.limits());
                held.close(); w.close();
                c.close(); c.awaitClosed();
            }
            Leaks.none(loops);
        }
    }
}
