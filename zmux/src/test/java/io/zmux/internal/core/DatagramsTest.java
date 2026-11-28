package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DatagramsTest {
    @Test void pendingLimiterTokensStayInTheDatagramBudgetAndCannotResurrect() {
        CoreConfig config = CoreConfig.builder().datagrams(true).datagramReceiveLimited(true).datagramReceiveBytes(4).build();
        Datagrams dg = new Datagrams(config, new SessionStats(), new Outbox(), () -> 0L);
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

    /** A datagram queued while every pipe took it, then too large for the pipe that polls: no interest, no wait loop. */
    @Test void aDatagramThatOnlyALargerPipeHoldsIsNoInterestOfTheSmallOne() throws Exception {
        Session session = PipeTargetTest.session();
        Pipe largePipe = session.pipes().get(0);
        Pipe smallPipe = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        int[] cap = {65535};
        SessionStats stats = new SessionStats();
        Datagrams dg = new Datagrams(CoreConfig.builder().datagrams(true).build(), stats, new Outbox(), () -> 0L);
        dg.recordCap(() -> cap[0]);
        dg.peer(65535, 65535, false);
        dg.enqueueSingle(null, 0, new byte[3000], 0, Time.NEVER, io.zmux.DatagramSendOptions.INHERIT, null);
        cap[0] = 5000;
        assertFalse(dg.sendable(smallPipe, 1000), "the small pipe has nothing to write: it must not ask to be polled");
        assertTrue(dg.sendable(largePipe, 5000));
        RecordWriter small = new RecordWriter();
        small.init(new Batch(), Framing.BYTES, 1000, Session.DEFAULT_BATCH);
        dg.write(small, smallPipe);
        assertTrue(dg.queued(), "kept for the larger pipe");
        assertEquals(0, stats.datagramsDroppedSend());
        RecordWriter big = new RecordWriter();
        big.init(new Batch(), Framing.BYTES, 5000, Session.DEFAULT_BATCH);
        dg.write(big, largePipe);
        assertFalse(dg.queued());
        assertEquals(0, stats.datagramsDroppedSend());
    }

    /** No usable pipe's records hold it any more: dropped and counted at once, never queued to wait. */
    @Test void aDatagramNoUsablePipeHoldsIsDroppedPromptly() throws Exception {
        int[] caps = {65535, 65535};
        SessionStats stats = new SessionStats();
        Datagrams dg = new Datagrams(CoreConfig.builder().datagrams(true).build(), stats, new Outbox(), () -> 0L);
        dg.recordCap(() -> caps[1]);
        dg.peer(65535, 65535, false);
        dg.enqueueSingle(null, 0, new byte[3000], 0, Time.NEVER, io.zmux.DatagramSendOptions.INHERIT, null);
        caps[0] = 1000; caps[1] = 2000; // the larger pipe left
        assertFalse(dg.sendable(null, 1000), "dropping a queued datagram needs no carrier write");
        RecordWriter w = new RecordWriter();
        w.init(new Batch(), Framing.BYTES, 1000, Session.DEFAULT_BATCH);
        dg.write(w, null);
        assertFalse(dg.queued());
        assertEquals(1, stats.datagramsDroppedSend());
        assertFalse(dg.sendable(null, 1000));
    }
}
