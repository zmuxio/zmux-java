package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class LaneOwnershipTest {
    @Test void aDirectionCannotLeaveItsLaneWhileBuildingTheBatch() throws Exception {
        Session s = new Session(CoreConfig.defaults(), 0);
        var info = new CarrierInfo(Framing.BYTES, false, false, true, false);
        s.pipes().attachBootstrap(info, info, null);
        Poke.set(s, "peerSeen", true); Poke.set(s, "state", SessionState.ACTIVE);
        s.peer().maxLanes = 3; s.peer().maxStreamDataBidiRemote = 1 << 20;
        s.streams.peerLimits(10, 0); s.conn.sendLimit(1 << 20);
        Poke.set(s, "feat", new Features(false, false, false, false, false, false, true, false, true, true, false, false));
        Pipe p = s.pipes.get(0); Poke.set(p.out().lo, "offered", true);
        for (int i = 1; i <= 2; i++) {
            var lane = new Pipe.OutLane(i, info, true); p.out().add(lane);
        }
        Stream st = s.streams.openStream(true); s.streams.setPriority(st, 3, true);
        s.send.write(st, ByteBuffer.allocate(48 << 10), false);
        Batch b = new Batch(); RecordWriter w = new RecordWriter();
        w.init(b, Framing.BYTES, 65536, 65536);
        try {
            s.tx.writeData(w, p, p.out().lanes.get(1)); w.end();
            assertFalse(w.touched.isEmpty());
            assertEquals(1, st.snd.bind().lane(), "a batch cannot rebind data it already owns");
            s.tx.afterBatch(w, p, p.out().lanes.get(1));
            assertTrue(st.snd.bind().laneEnd() > p.out().lanes.get(1).ack.acked());
            assertSame(p.out().lanes.get(1), s.lanes.laneFor(p, st), "old lane must cover the batch before switching");
        } finally { b.discard(); s.streams.release(st); }
    }

    @Test void releasingAnUnopenedStreamNeverQueuesControlForAnUnassignedId() throws Exception {
        Session s = new Session(CoreConfig.defaults(), 0);
        Stream stream = s.streams.openStream(true);
        s.streams.release(stream);
        assertFalse(stream.idSet()); assertTrue(stream.snd.cancelled());
        assertNull(s.ctl.first()); assertEquals(0, ControlQueue.bits(stream)); assertEquals(0, s.streams.reserved(0));
    }
    @Test void cancellingAnUnopenedStreamEndsItsImpossibleReverseHalf() throws Exception {
        Session s = new Session(CoreConfig.defaults(), 0);
        Stream stream = s.streams.openStream(true); Waiter reader = Waiter.inert();
        Waiter capacity = Waiter.inert();
        assertFalse(s.send.waitSendCapacity(stream, 1, capacity));
        s.recv.waitRead(stream, reader); s.send.reset(stream, 91);
        var wakes = new java.util.ArrayList<Waiter>(); s.outbox().takeWakes(wakes);
        assertTrue(wakes.contains(reader)); assertTrue(wakes.contains(capacity));
        assertEquals(0, s.send.sendCapacity(stream));
        assertTrue(s.recv.readChunk(stream, 1).fin());
        assertFalse(stream.idSet()); assertNull(s.ctl.first());
    }
    @Test void closingWakesBothObserversOfProvisionalStreams() throws Exception {
        Session s = new Session(CoreConfig.defaults(), 0);
        Stream stream = s.streams.openStream(true); Waiter read = Waiter.inert(), write = Waiter.inert();
        s.recv.waitRead(stream, read); s.send.waitWriteState(stream, write); s.close(0, null);
        var wakes = new java.util.ArrayList<Waiter>(); s.outbox().takeWakes(wakes);
        assertTrue(wakes.contains(read)); assertTrue(wakes.contains(write));
    }

}
