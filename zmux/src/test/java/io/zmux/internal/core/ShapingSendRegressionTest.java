package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;


import io.zmux.carrier.CarrierChannel.Outcome;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.Capability;
import io.zmux.shaping.DatagramView;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShapingBudget;
import io.zmux.shaping.UnitPlan;
import io.zmux.shaping.WireAccount;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class ShapingSendRegressionTest {
    private static final long US = Time.MICROSECOND;
    private static final long BUDGET = 2000 * US;

    private abstract static class Policy implements Shaper {
        @Override public Set<Capability> capabilities() { return Set.of(); }
        @Override public void onEvent(Event event, Effects effects) {}
        @Override public void plan(RecordView view, RecordPlan plan) {}
    }

    private static Shaper delayingPlans() {
        return new Policy() {
            @Override public Set<Capability> capabilities() { return Set.of(Capability.PLAN, Capability.DATAGRAM_PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) { plan.delayNanos(BUDGET); }
            @Override public void planDatagram(DatagramView view, UnitPlan plan, Effects effects) { plan.delayNanos(BUDGET); }
        };
    }

    private static ChannelDatagramTest.Pair nativePair() {
        var pair = new ChannelDatagramTest.Pair(new ChannelDatagramTest.FakeChannel(0, 1400),
                new ChannelDatagramTest.FakeChannel(0, 1400));
        pair.pump();
        return pair;
    }

    private static int recordWrites(Session session, int batchRoom, long now) {
        var batch = new Batch();
        int count = 0;
        while (session.pollTransmit(0, 0, batch, batchRoom, now)) {
            assertTrue(ChannelDatagramTest.datagramInRecords(List.of(BatchFixture.flat(batch))));
            session.writeDone(batch, null, now);
            assertTrue(++count <= 2);
        }
        return count;
    }

    @Test void consecutiveReliableRecordsUseTheWaitAlreadySpentBehindTheirFlowHead() throws Exception {
        for (boolean shaped : new boolean[] {false, true}) {
            var pair = new ChannelDatagramTest.Pair(null, null);
            pair.pump();
            if (shaped) pair.a.setPipeShaper(0, delayingPlans());
            pair.a.sendDatagram(null, 0, new byte[700]);
            pair.a.sendDatagram(null, 0, new byte[700]);
            assertEquals(shaped ? 0 : 2, recordWrites(pair.a, 900, 0));
            assertEquals(shaped ? 2 : 0, recordWrites(pair.a, 900, BUDGET));
            assertEquals(0, recordWrites(pair.a, 900, 2 * BUDGET));
            if (shaped) {
                assertEquals(Duration.ofNanos(BUDGET), pair.a.shapingMeters().maxAddedDelay());
                assertEquals(4000, pair.a.shapingMeters().stallSpentMicros());
            }
        }
    }

    @Test void structuralNativeFallbackSpendsOnlyTheRemainderOfTheFlowBudget() throws Exception {
        var pair = nativePair();
        Session session = pair.a;
        session.setPipeShaper(0, delayingPlans());
        session.sendDatagram(null, 0, new byte[] {1});
        var wire = new ArrayList<byte[]>();
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        session.pollChannel(0, wire, tickets, 0);
        assertTrue(tickets.isEmpty());
        pair.ca.max = pair.ca.limit = 0;
        session.channelChanged(0, 1500 * US);
        assertEquals(0, recordWrites(session, 900, 1500 * US));
        assertEquals(0, recordWrites(session, 900, 1999 * US));
        assertEquals(1, recordWrites(session, 900, BUDGET));
        assertEquals(Duration.ofNanos(BUDGET), session.shapingMeters().maxAddedDelay());
        assertEquals(2000, session.shapingMeters().stallSpentMicros());
    }

    @Test void anOutboundSubscriptionDoesNotTurnAnAsyncReceiptIntoAFlowGate() throws Exception {
        var session = nativePair().a;
        var events = new ArrayList<Event>();
        session.setPipeShaper(0, new Policy() {
            @Override public Set<Capability> capabilities() { return Set.of(Capability.OUTBOUND); }
            @Override public void onEvent(Event event, Effects effects) {
                if (event.kind() == EventKind.OUTBOUND) events.add(event);
            }
        });
        var wire = new ArrayList<byte[]>();
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        session.sendDatagram(null, 0, new byte[] {1});
        session.pollChannel(0, wire, tickets, 0);
        assertEquals(1, tickets.size());
        session.sendDatagram(null, 0, new byte[] {2});
        session.pollChannel(0, wire, tickets, 0);
        assertEquals(2, tickets.size());
        session.channelDone(tickets.getLast(), Outcome.ACCEPTED, 1 * US);
        assertEquals(1, events.size());
        session.channelDone(tickets.getFirst(), Outcome.ACCEPTED, 10_000 * US);
        assertEquals(2, events.size());
        assertEquals(0, session.shapingMeters().stallSpentMicros());
    }

    @Test void nativeTicketsOccupyTheQueueAcrossPolicyGenerations() throws Exception {
        var session = nativePair().a;
        var wire = new ArrayList<byte[]>();
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        for (int n = 0; n < 64; n++) {
            session.setPipeShaper(0, new Policy() {
                @Override public Set<Capability> capabilities() { return Set.of(Capability.PLAN); }
            });
            session.sendDatagram(null, 0, new byte[] {(byte) n});
            session.pollChannel(0, wire, tickets, 0);
        }
        assertEquals(64, tickets.size());
        var overflow = assertThrows(CoreException.class, () -> session.sendDatagram(null, 0, new byte[] {64}));
        assertEquals(CoreException.DatagramDrop.QUEUE_OVERFLOW, overflow.datagramDrop());
        session.channelDone(tickets.getFirst(), Outcome.REJECTED, 0);
        session.channelDone(tickets.getFirst(), Outcome.REJECTED, 0);
        session.sendDatagram(null, 0, new byte[] {64});
        session.pollChannel(0, wire, tickets, 0);
        assertEquals(65, tickets.size()); // The first ticket is already terminal.
        for (var ticket : tickets) session.channelDone(ticket, Outcome.ACCEPTED, 0);
        assertFalse(session.datagrams().sendQueueFull());
    }

    @Test void nativeTicketsKeepTheFlowSlotAndPayloadBytesUntilCompletion() throws Exception {
        for (boolean bytesOnly : new boolean[] {false, true}) {
            var pair = new ChannelDatagramTest.Pair(new ChannelDatagramTest.FakeChannel(0, 1400),
                    new ChannelDatagramTest.FakeChannel(0, 1400), 1 << 20,
                    bytesOnly ? 1 : CoreConfig.DEFAULT_DATAGRAM_STREAM_BYTES);
            pair.pump();
            var session = pair.b;
            Stream stream = session.streams().openStream(true);
            session.sending().write(stream, ByteBuffer.wrap(new byte[] {1}), false);
            pair.pump();
            var wire = new ArrayList<byte[]>();
            var tickets = new ArrayList<Datagrams.ChannelSent>();
            int limit = bytesOnly ? 1 : CoreConfig.DEFAULT_STREAM_DATAGRAMS;
            byte[] payload = bytesOnly ? new byte[] {3} : new byte[0];
            for (int i = 0; i < limit; i++) {
                session.sendDatagram(stream, 0, payload);
                session.pollChannel(0, wire, tickets, 0);
            }
            assertEquals(limit, tickets.size());
            assertFalse(session.datagrams().sendQueueFull());
            var full = assertThrows(CoreException.class, () -> session.sendDatagram(stream, 0, payload));
            assertEquals(CoreException.DatagramDrop.QUEUE_OVERFLOW, full.datagramDrop());
            session.channelDone(tickets.getFirst(), Outcome.ACCEPTED, 0);
            session.channelDone(tickets.getFirst(), Outcome.ACCEPTED, 0);
            session.sendDatagram(stream, 0, payload);
            session.pollChannel(0, wire, tickets, 0);
            assertEquals(limit + 1, tickets.size());
            for (var ticket : tickets) session.channelDone(ticket, Outcome.ACCEPTED, 0);
        }
    }

    @Test void aGateStartsChargingOnlyWithAnOrdinaryWritableOpportunity() throws Exception {
        var pair = new ChannelDatagramTest.Pair(null, null);
        pair.pump();
        var session = pair.a;
        session.setWritable(0, 0, false, 0);
        Stream stream = session.streams().openStream(true);
        session.sending().write(stream, ByteBuffer.wrap(new byte[] {1}), false);
        session.setPipeShaper(0, new Policy() {
            @Override public void onEvent(Event event, Effects effects) {
                if (event.kind() == EventKind.START) effects.gate(10 * BUDGET, 0, 0);
            }
        });
        session.handleTimeout(BUDGET);
        assertEquals(0, session.shapingMeters().stallSpentMicros());
        assertEquals(Duration.ZERO, session.shapingMeters().maxAddedDelay());
        session.setWritable(0, 0, true, BUDGET);
        Batch batch = new Batch();
        assertFalse(session.pollTransmit(0, 0, batch, 900, BUDGET));
        assertTrue(session.pollTransmit(0, 0, batch, 900, 2 * BUDGET));
        session.writeDone(batch, null, 2 * BUDGET);
        assertEquals(2000, session.shapingMeters().stallSpentMicros());
    }

    @Test void aGateSuspendsItsClockWhenTheOnlyLaneBecomesUnwritable() throws Exception {
        var pair = new ChannelDatagramTest.Pair(null, null);
        pair.pump();
        var session = pair.a;
        Stream stream = session.streams().openStream(true);
        session.sending().write(stream, ByteBuffer.wrap(new byte[] {1}), false);
        session.setPipeShaper(0, new Policy() {
            @Override public void onEvent(Event event, Effects effects) {
                if (event.kind() == EventKind.START) effects.gate(BUDGET, 0, 0);
            }
        });
        assertFalse(session.pollTransmit(0, 0, new Batch(), 900, 0));
        session.setWritable(0, 0, false, 500 * US);
        session.handleTimeout(BUDGET);
        assertEquals(500, session.shapingMeters().stallSpentMicros());
        assertEquals(Duration.ofNanos(500 * US), session.shapingMeters().maxAddedDelay());
    }

    @Test void nativeBatchDispatchesTheLastLegalSequenceOnlyOnce() throws Exception {
        var session = nativePair().a;
        var cursor = Datagrams.class.getDeclaredField("nextSequence");
        cursor.setAccessible(true);
        cursor.setLong(session.datagrams(), VarInt.MAX);
        session.sendDatagram(null, 0, new byte[] {1});
        session.sendDatagram(null, 0, new byte[] {2});
        var wire = new ArrayList<byte[]>();
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        session.pollChannel(0, wire, tickets, 0);
        assertEquals(1, tickets.size());
        assertEquals(VarInt.MAX, tickets.getFirst().sequence);
        byte[] message = wire.getFirst();
        assertEquals(VarInt.MAX, VarInt.get(ByteBuffer.wrap(message), 2, message.length));
        session.channelDone(tickets.getFirst(), Outcome.ACCEPTED, 0);
        assertFalse(session.hasChannelOutput());
    }

    @Test void duplicatePurePipeAckSnapshotsEarnZeroCoverCredit() throws Exception {
        var pair = new ChannelDatagramTest.Pair(null, null);
        pair.a.localParams().present |= TransportParams.PIPE_ACK;
        pair.b.localParams().present |= TransportParams.PIPE_ACK;
        pair.pump();
        var session = pair.a;
        session.setShaper(null, new ShapingBudget(new WireAccount(100, 0, 1_000_000, 0), null, true));
        var outbound = new ArrayList<Event>();
        session.setPipeShaper(0, new Policy() {
            @Override public Set<Capability> capabilities() { return Set.of(Capability.OUTBOUND, Capability.INCLUDE_CONTROL); }
            @Override public void onEvent(Event event, Effects effects) {
                if (event.kind() == EventKind.OUTBOUND) outbound.add(event);
            }
        });
        var engine = Session.class.getDeclaredField("ack");
        engine.setAccessible(true);
        var ack = (AckEngine) engine.get(session);
        assertNotNull(ack);
        var batch = new Batch();
        byte[] first = null;
        long baseline = session.shapingMeters().overhead().policy().baselineBytes();
        for (int i = 0; i < 2; i++) {
            ack.ackNow();
            assertTrue(session.pollTransmit(0, 0, batch, 900, 0));
            byte[] encoded = BatchFixture.flat(batch);
            if (first == null) first = encoded;
            else assertArrayEquals(first, encoded);
            session.writeDone(batch, null, 0);
        }
        assertEquals(0, session.shapingMeters().wireLeft());
        assertTrue(outbound.isEmpty());
        assertEquals(baseline + 2L * first.length, session.shapingMeters().overhead().policy().baselineBytes());
    }

    @Test void changingOnlyDefaultAdapterRoomToZeroNeverFallsBack() throws Exception {
        var pair = nativePair();
        var session = pair.a;
        pair.ca.defaultLimit = true;
        session.sendDatagram(null, 0, new byte[] {1});
        var wire = new ArrayList<byte[]>();
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        assertTrue(session.datagrams().nativePayload(1) > 0);
        pair.ca.max = 0;
        session.channelChanged(0, US);
        session.pollChannel(0, wire, tickets, US);
        assertTrue(tickets.isEmpty());
        assertEquals(0, recordWrites(session, 900, US));
        pair.ca.max = 1400;
        session.channelChanged(0, 2 * US);
        session.pollChannel(0, wire, tickets, 2 * US);
        assertEquals(1, tickets.size());
        session.channelDone(tickets.getFirst(), Outcome.ACCEPTED, 2 * US);
    }

    @Test void nativeAssociationChangeExpiresOnlyNativeLossEvidence() throws Exception {
        var pair = nativePair();
        var session = pair.a;
        assertEquals(Session.DatagramLossReport.ACCEPTED, session.reportDatagramLoss(0, true, 0, 1, 4, 2));
        assertEquals(Session.DatagramLossReport.ACCEPTED, session.reportDatagramLoss(0, false, 0, 1, 4, 2));
        pair.ca.id = 64;
        session.channelChanged(0, US);
        assertEquals(Session.DatagramLossReport.SOURCE_EXPIRED, session.reportDatagramLoss(0, true, US, 1, 5, 2));
        assertEquals(Session.DatagramLossReport.ACCEPTED, session.reportDatagramLoss(0, false, US, 1, 5, 2));
        assertEquals(Session.DatagramLossReport.ACCEPTED, session.reportDatagramLoss(0, true, US, 2, 5, 2));
    }

    @Test void cancellingAJoinAtOneMillisecondDisarmsItsTwoMillisecondWake() {
        var session = nativePair().a;
        Object join = new Object();
        session.refreshDeadlines();
        long baseline = session.nextTimeout();
        session.holdJoin(join, BUDGET, () -> true, () -> fail("cancelled Join released"));
        session.refreshDeadlines();
        assertEquals(BUDGET, session.nextTimeout());
        session.cancelJoinHold(join, 1000 * US);
        session.refreshDeadlines();
        assertEquals(baseline, session.nextTimeout());
        assertEquals(1000, session.shapingMeters().stallSpentMicros());
    }
}
