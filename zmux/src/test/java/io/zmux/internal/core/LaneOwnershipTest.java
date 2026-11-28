package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeStats.LaneStats;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class LaneOwnershipTest {
    private static Session withLanes(int extraLanes) {
        Session s = new Session(CoreConfig.defaults(), 0);
        var info = new CarrierInfo(Framing.BYTES, false, false, true, false);
        s.pipes().attachBootstrap(info, info);
        Poke.set(s, "peerSeen", true); Poke.set(s, "state", SessionState.ACTIVE);
        s.peer().maxLanes = 3; s.peer().maxStreamDataBidiRemote = 1 << 20;
        s.streams.peerLimits(10, 0); s.conn.sendLimit(1 << 20);
        Poke.set(s, "feat", new Features(false, false, false, false, false, false, true, false, true, true, false, false, false));
        Pipe p = s.pipes.get(0); Poke.set(p.out().lo, "offered", true);
        for (int i = 1; i <= extraLanes; i++) {
            var lane = new Pipe.OutLane(i, info, true); p.out().add(lane);
        }
        return s;
    }

    @Test void aDirectionCannotLeaveItsLaneWhileBuildingTheBatch() throws Exception {
        Session s = withLanes(2);
        Pipe p = s.pipes.get(0);
        Stream st = s.streams.openStream(true); s.streams.setPriority(st, 3, true);
        s.send.write(st, ByteBuffer.allocate(48 << 10), false);
        Batch b = new Batch(); RecordWriter w = new RecordWriter();
        w.init(b, Framing.BYTES, 65536, 65536);
        try {
            s.tx.writeData(w, p, p.out().lanes.get(1), null); w.end();
            assertFalse(w.touched.isEmpty());
            assertEquals(1, st.snd.bind().lane(), "a batch cannot rebind data it already owns");
            s.tx.afterBatch(w, p, p.out().lanes.get(1));
            assertTrue(st.snd.bind().laneEnd() > p.out().lanes.get(1).ack.acked());
            assertSame(p.out().lanes.get(1), s.lanes.laneFor(p, st), "old lane must cover the batch before switching");
        } finally { b.discard(); s.streams.release(st); }
    }

    @Test void anUnchangedPacedBindingDoesNotRegisterMoreWork() throws Exception {
        Session s = withLanes(2);
        Pipe p = s.pipes.get(0);
        var lanes = p.out().lanes;
        int[] wakes = new int[lanes.size()];
        for (int i = 0; i < lanes.size(); i++) {
            int slot = i;
            s.pipes().writer(p.id, lanes.get(i).lane, () -> wakes[slot]++, null);
        }
        Stream st = s.streams.openStream(true);
        s.send.write(st, ByteBuffer.allocate(1), false);
        java.util.Arrays.fill(wakes, 0);
        int[] invalidations = {0};
        s.pipes().sharedWriter(() -> invalidations[0]++);
        Batch b = new Batch();
        RecordWriter w = new RecordWriter();
        try {
            w.init(b, Framing.BYTES, 1024, 1024);
            w.limitData(0);
            for (Pipe.OutLane lane : lanes) s.tx.writeData(w, p, lane, null);
            w.end();
            assertEquals(1, invalidations[0]);
            assertArrayEquals(new int[] {0, 0, 0}, wakes);
            assertEquals(1, st.snd.bind().lane());
            assertEquals(0, b.bytes());
        } finally {
            b.discard();
            s.streams.release(st);
        }
    }

    @Test void sharedStreamWorkGrantsItsDataLaneOnly() throws Exception {
        Session session = withLanes(2);
        Pipe pipe = session.pipes.get(0);
        Poke.set(pipe.out().lane(0).tx, "tpSent", true);
        int[] grants = new int[3];
        for (int i = 0; i < grants.length; i++) {
            int lane = i;
            session.pipes.writer(pipe.id, lane, () -> grants[lane]++, null);
        }
        var stream = session.streams.openStream(true);
        session.send.write(stream, ByteBuffer.wrap(new byte[] {7}), false);
        session.routeOutput();
        assertArrayEquals(new int[] {0, 1, 0}, grants);
        session.finishClose();
    }

    @Test void openingTimeoutOnlyRefusesAnUnconfirmedLaneAtItsDeadline() {
        Session s = withLanes(0);
        Pipe p = s.pipes.get(0);
        var info = p.out().lanes.getFirst().st.info();
        assertEquals(1, s.lanes.opened(p.id, info));
        Pipe.OutLane opening = p.out().lane(1);
        long at = s.lanes.deadline();
        assertEquals(s.config().joinDeadline(), at);
        s.lanes.onTimeout(at - 1);
        assertFalse(opening.st.refused());
        s.lanes.onTimeout(at);
        assertTrue(opening.st.refused());
        assertEquals(Time.NEVER, s.lanes.deadline());
        s.lanes.onTimeout(at);
        var actions = new java.util.ArrayList<Action>();
        s.outbox().takeActions(actions);
        assertEquals(List.of(new Action(Action.Kind.RESET_LANE, p.id, opening.lane, Action.DIR_OUT)), actions);
    }

    @Test void snapshotPreservesMissingDirectionsZeroPositionsAndWideLaneIds() {
        Session s = new Session(CoreConfig.defaults(), 0);
        var info = new CarrierInfo(Framing.BYTES, false, false, true, false);
        s.pipes().attachBootstrap(info, info);
        Pipe p = s.pipes.get(0);
        p.out().add(new Pipe.OutLane(1, info, true));
        long wide = 1L << 33;
        var wideInbound = new Pipe.InLane(wide, false, false);
        Poke.set(wideInbound.rx, "pos", wide + 3);
        p.in().add(wideInbound);
        p.in().add(new Pipe.InLane(2, false, false));

        var stats = s.pipeStats(0, 0, 0);
        var zero = OptionalLong.of(0);
        var absent = OptionalLong.empty();
        assertEquals(2L, stats.lanesOut());
        assertEquals(3L, stats.lanesIn());
        assertEquals(zero, stats.epoch());
        assertEquals(absent, stats.replaceBy());
        assertEquals(List.of(
                new LaneStats(0, zero, absent, zero),
                new LaneStats(1, zero, absent, absent),
                new LaneStats(2, absent, absent, zero),
                new LaneStats(wide, absent, absent, OptionalLong.of(wide + 3))), stats.lanes());

        Poke.set(s, "feat", new Features(false, false, false, false, false, false, true, false, true, true, false, false, false));
        assertEquals(zero, s.pipeStats(0, 0, 0).lanes().get(1).positionAcked());
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
