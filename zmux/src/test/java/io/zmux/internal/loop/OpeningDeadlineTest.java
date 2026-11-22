package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.adapter.Upgrade;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Time;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class OpeningDeadlineTest {
    /** One manual owner thread. The heap sees only advances of this clock. */
    private static final class Host extends SessionHost {
        private long now = 300 * Time.SECOND;
        Host() { super(0); }
        @Override protected long timerNow() { return now; }
        @Override public boolean inLoop() { return true; }
        @Override public boolean tryExecute(Runnable r) { r.run(); return true; }
        @Override protected void wakeAt(long deadline) { }
        @Override protected void requestRoundEnd() { }
        void advance(long elapsed) { runTimers(now += elapsed); }
    }

    private final Host host = new Host();
    private final CarrierSetup setup = new CarrierSetup();
    private final Transport carrier = setup.own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_BIDI, null, 0));
    private final LinkedBlockingQueue<System.Logger.Level> logs = new LinkedBlockingQueue<>();
    private final ListenerHub.Owner listener = new ListenerHub.Owner() {
        @Override public CoreConfig config() { return CoreConfig.defaults(); }
        @Override public DriverHooks hooks() { return DriverHooks.NONE; }
        @Override public void started(SessionRuntime d, String proto, io.zmux.CarrierCredential cred, CarrierSetup setup) { }
        @Override public void log(System.Logger.Level level, String message, Throwable error) { logs.add(level); }
    };

    private static Upgrade acknowledged(CompletableFuture<Void> released) {
        return new Upgrade() {
            @Override public CompletableFuture<Void> open() { return CompletableFuture.completedFuture(null); }
            @Override public CompletableFuture<Void> released() { return released; }
        };
    }

    @Test void anAcceptedLegacyStageHoldsOnlyOneLingerFromOpen() throws Exception {
        var never = new CompletableFuture<Void>();
        assertTrue(setup.acceptedUpgrade(carrier, () -> never, listener));
        try (var claim = setup.claim()) { claim.commit(); }
        assertEquals(host.timerNow() + Session.MAX_LINGER, host.earliestTimer());
        host.advance(Session.MAX_LINGER - 1);
        assertFalse(setup.closed().isDone());
        host.advance(1);
        setup.closed().get(1, TimeUnit.SECONDS);
        assertFalse(never.isDone());
        assertEquals(0, host.timerCount());
    }

    @Test void aWaitingLegacyHeadStartsItsLingerOnlyOnWithdrawal() throws Exception {
        // HS-16a. Its original decision deadline has a different owner.
        var write = new CompletableFuture<Void>();
        var opening = setup.openUpgrade(() -> write, listener);
        host.advance(5 * Session.MAX_LINGER);
        assertEquals(0, host.timerCount());
        opening.cancel(null);
        setup.refused();
        assertThrows(java.util.concurrent.CancellationException.class, () -> write.get(1, TimeUnit.SECONDS));
        host.advance(Session.MAX_LINGER - 1);
        assertFalse(setup.closed().isDone());
        host.advance(1);
        setup.closed().get(1, TimeUnit.SECONDS);
        assertEquals(0, host.timerCount());
    }

    @Test void aLateExplicitSignalLogsOnceAndKeepsItsShareAfterTheDeadline() throws Exception {
        // HS-16a. A completed open is independent of the outstanding physical write.
        var released = new CompletableFuture<Void>();
        setup.openUpgrade(acknowledged(released), listener);
        setup.refused();
        host.advance(Session.MAX_LINGER - 1);
        assertTrue(logs.isEmpty());
        host.advance(1);
        assertEquals(System.Logger.Level.DEBUG, logs.poll(2, TimeUnit.SECONDS));
        assertEquals(0, host.timerCount());
        host.advance(3 * Session.MAX_LINGER);
        assertFalse(setup.closed().isDone());
        released.complete(null);
        setup.closed().get(1, TimeUnit.SECONDS);
        assertTrue(logs.isEmpty());
    }

    @Test void aBrokenReleaseSubscriptionFailsBeforeOpeningAndLeavesNoTimer() {
        var failure = new IllegalStateException("release subscription failed");
        var step = new Upgrade() {
            @Override public CompletableFuture<Void> open() { throw new AssertionError("untracked write submitted"); }
            @Override public CompletableFuture<Void> released() { throw failure; }
        };
        var opening = setup.openUpgrade(step, listener);
        assertSame(failure, assertThrows(java.util.concurrent.CompletionException.class, opening.ready::join).getCause());
        assertTrue(setup.closed().isDone());
        assertEquals(0, host.timerCount());
    }

    @Test void aTimelyExplicitSignalDisarmsItsDiagnostic() {
        var released = new CompletableFuture<Void>();
        setup.openUpgrade(acknowledged(released), listener);
        assertEquals(1, host.timerCount());
        released.complete(null);
        assertEquals(0, host.timerCount());
        host.advance(3 * Session.MAX_LINGER);
        assertTrue(logs.isEmpty());
        setup.refused();
        assertTrue(setup.closed().isDone());
    }

    @Test void stoppingTheClockReleasesOnlyAnUnacknowledgedShare() {
        var waiting = setup.openUpgrade(CompletableFuture::new, listener);
        waiting.cancel(null);
        var released = new CompletableFuture<Void>();
        setup.openUpgrade(acknowledged(released), listener);
        setup.refused();
        host.runShutdownHooks();
        assertEquals(0, host.timerCount());
        assertFalse(setup.closed().isDone());
        released.complete(null);
        assertTrue(setup.closed().isDone());
        assertTrue(logs.isEmpty());
    }
}
