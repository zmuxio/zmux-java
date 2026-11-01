package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class DatagramsTest {
    @Test void pendingLimiterTokensStayInTheDatagramBudgetAndCannotResurrect() {
        CoreConfig config = CoreConfig.builder().datagrams(true).datagramReceiveLimited(true).datagramReceiveBytes(4).build();
        Datagrams dg = new Datagrams(config, new SessionStats(), new Outbox());
        dg.received(null, new byte[] {1, 1});
        Datagrams.Limited oldest = dg.pollLimited();
        dg.received(null, new byte[] {2, 2});
        dg.received(null, new byte[] {3, 3});
        assertFalse(oldest.active()); assertEquals(4, dg.receivedBytes());
        assertFalse(dg.deliver(oldest, false, false)); assertNull(dg.receive());
        dg.deliver(dg.pollLimited(), false, false);
        assertArrayEquals(new byte[] {2, 2}, dg.receive());
        dg.deliver(dg.pollLimited(), false, false);
        assertArrayEquals(new byte[] {3, 3}, dg.receive());
        assertEquals(0, dg.receivedBytes());
        dg.received(null, new byte[] {4});
        Datagrams.Limited pending = dg.pollLimited();
        dg.releaseAll();
        assertFalse(pending.active()); assertFalse(dg.deliver(pending, false, true));
        assertNull(dg.receive());
    }

    @Test void closingReleasesStreamDatagramsEvenWhenTheStreamHandleSurvives() throws Exception {
        CoreConfig config = CoreConfig.builder().datagrams(true).build();
        Session session = new Session(config, 0);
        Stream stream = session.streams.openStream(true);
        session.datagrams().received(stream, new byte[] {0, 1, 2, 3});
        assertEquals(4, session.datagrams().receivedBytes());
        session.close(0, null);
        assertNull(session.datagrams().receive(stream));
        assertEquals(0, session.datagrams().receivedBytes());
    }
}
