package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A flush waits beside the part's writer: a flush that gives up leaves the writer to be woken by credit. */
class FlushWaitSlotTest {
    private static final class Named extends Waiter {
        @Override public void wake() {}
    }

    @Test void creditAfterAWithdrawnFlushWakesTheWriter() throws Exception {
        Session s = PipeTargetTest.session();
        Stream st = new Stream(true, true);
        Waiter writer = new Named(), flush = new Named();
        s.send.waitWrite(st, writer);
        assertFalse(s.send.flushDone(st, 1, flush), "nothing was sent: the flush waits");
        s.send.cancelFlushWait(st, flush); // the flush timed out
        List<Waiter> woken = new ArrayList<>();
        s.outbox.takeWakes(woken);
        woken.clear();
        assertTrue(s.send.onSendCredit(st, 1 << 20));
        s.outbox.takeWakes(woken);
        assertTrue(woken.contains(writer), "credit wakes the writer that waited for it");
        assertFalse(woken.contains(flush), "a withdrawn flush is not woken");
    }
}
