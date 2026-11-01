package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CancellationTest {
    static int registrations(Cancellation source) throws Exception {
        var field = Cancellation.class.getDeclaredField("listeners"); field.setAccessible(true);
        return ((Map<?, ?>)field.get(source)).size();
    }
    @Test void removedCallbacksDoNotRunAndCancellationIsIdempotent() throws Exception {
        var source = new Cancellation(); var calls = new AtomicInteger();
        var removed = source.onCancel(calls::incrementAndGet);
        removed.close(); removed.close(); assertEquals(0, registrations(source));
        source.onCancel(() -> { assertFalse(Thread.holdsLock(source)); calls.incrementAndGet(); });
        source.cancel(); source.cancel(); assertEquals(1, calls.get()); assertTrue(source.isCancelled());
        source.onCancel(() -> { assertFalse(Thread.holdsLock(source)); calls.incrementAndGet(); });
        assertEquals(2, calls.get()); assertEquals(0, registrations(source));
    }
    @Test void oneCallbackRegisteredTwiceIsTwoRegistrations() throws Exception {
        var source = new Cancellation(); var calls = new AtomicInteger();
        Runnable callback = calls::incrementAndGet;
        var first = source.onCancel(callback); source.onCancel(callback);
        assertEquals(2, registrations(source));
        first.close(); assertEquals(1, registrations(source));
        source.cancel(); assertEquals(1, calls.get());
    }
    @Test void failingCallbacksCannotHideCancellationFromOtherSubscribers() {
        var source = new Cancellation(); var calls = new AtomicInteger();
        source.onCancel(() -> { calls.incrementAndGet(); throw new IllegalStateException("broken callback"); });
        source.onCancel(calls::incrementAndGet);
        assertThrows(IllegalStateException.class, source::cancel); assertEquals(2, calls.get());
    }
}
