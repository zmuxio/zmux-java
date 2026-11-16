package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

/** Decisions that race on an operation: only the winner sets the outcome, whatever runs in between. */
class OperationRaceTest {
    /** A lane operation that runs race once, at its first decision point. */
    static final class Gated extends Operation<Object> {
        Runnable race;

        Gated() { super(null, self -> { }, Long.MAX_VALUE); }

        @Override void deciding() {
            Runnable r = race;
            race = null;
            if (r != null) r.run();
        }
    }

    @Test void aCancelThatLosesToTheAttemptLeavesItsOutcome() {
        Gated op = new Gated();
        assertTrue(op.grant()); // WAITING, as a registered wait
        Object value = new Object();
        // The cancel read WAITING; the attempt completes before the cancel claims the decision.
        op.race = () -> { assertTrue(op.begin()); op.finish(value); };
        assertFalse(op.cancel());
        assertSame(value, op.outcome());
        assertFalse(op.isCancelled());
    }

    @Test void aSettleThatLosesToACancelLeavesTheCancellation() {
        Gated op = new Gated();
        assertTrue(op.grant());
        op.race = () -> assertTrue(op.cancel());
        assertFalse(op.settle(new SocketTimeoutException("timed out")));
        assertTrue(op.isCancelled());
        assertInstanceOf(CancellationException.class, op.outcome());
    }

    @Test void aCancelThatLosesToASettleLeavesItsOutcome() {
        Gated op = new Gated();
        assertTrue(op.grant());
        var timeout = new SocketTimeoutException("timed out");
        op.race = () -> assertTrue(op.settle(timeout));
        assertFalse(op.cancel());
        assertSame(timeout, op.outcome());
    }
}
