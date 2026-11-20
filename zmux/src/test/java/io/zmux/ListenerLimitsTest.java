package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ListenerLimitsTest {
    @Test void setLimitsRoundTripsAndALoweredMaxSuspendedClosesSuspendedSessions() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ListenerLimits.builder().maxPendingPerSource(0));
        assertThrows(IllegalArgumentException.class, () -> ListenerLimits.builder().firstRecordTimeout(Duration.ZERO));
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerResourcesTest.config(loops);
            var lc = ListenerConfig.builder().session(cfg).noMaxPending().maxPendingPerSession(3).build();
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
                ListenerResourcesTest.until(() -> s.stats().suspensions() > 0);
                assertThrows(java.net.SocketTimeoutException.class, () -> s.awaitClosed(Duration.ofMillis(200)));
                listener.setLimits(changed.toBuilder().maxSuspended(0).build());
                assertInstanceOf(SessionError.ResumeTimeout.class, s.awaitClosed());
                c.close(); c.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    @Test void setLimitsMemoryBudgetStartsAndEndsPressureAtOnce() throws Exception {
        var events = new LinkedBlockingQueue<long[]>();
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerResourcesTest.config(loops);
            var lc = ListenerConfig.builder().session(cfg).noMemoryBudget().memoryPressureListener((u, l) -> events.add(new long[] {u, l})).build();
            try (var listener = Zmux.listen(NetworkTest.any(), lc)) {
                assertTrue(listener.limits().memoryBudget().isEmpty());
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                var w = c.openStream(); w.write(new byte[64 << 10]);
                var held = s.acceptStream();
                ListenerResourcesTest.until(() -> listener.adapter().memoryUsed() >= 64 << 10);
                c.pipes().getFirst().abort();
                ListenerResourcesTest.until(() -> s.stats().suspensions() > 0);
                assertTrue(events.isEmpty());
                // unlimited to a bound below the charged sum: pressure at once, the suspended session closed
                listener.setLimits(listener.limits().toBuilder().memoryBudget(1).build());
                assertEquals(1, events.poll(5, TimeUnit.SECONDS)[1]);
                assertEquals(java.util.OptionalLong.of(1), listener.limits().memoryBudget());
                assertInstanceOf(SessionError.ResumeTimeout.class, s.awaitClosed());
                // back to unlimited: pressure ends without a charge
                listener.setLimits(listener.limits().toBuilder().noMemoryBudget().build());
                assertNotNull(events.poll(5, TimeUnit.SECONDS));
                assertTrue(listener.limits().memoryBudget().isEmpty());
                held.close(); w.close();
                c.close(); c.awaitClosed();
            }
            Leaks.none(loops);
        }
    }
}
