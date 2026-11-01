package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.wire.TransportParams;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** penalties, bytes_opportunistic and pings_answered (API.md Section 7.2) follow what the core does. */
class CounterSemanticsTest {
    static SessionStats.Snapshot snap(Session s) { return s.stats().snapshot(0, null); }

    @Test void aPenaltyCountsOncePerEpisode() {
        Session s = PipeTargetTest.session();
        Pipe p = PipeTargetTest.appendOut(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        Poke.set(p.out().snd, "so", 100L);
        s.mp().retention.penalize(p);
        s.mp().retention.penalize(p); // the same episode
        assertEquals(1, snap(s).penalties());
        Poke.set(p.out().snd, "cum", 100L);
        s.mp().retention.penaltyAcked(p); // the episode ends
        s.mp().retention.penalize(p);
        assertEquals(2, snap(s).penalties());
    }

    /** An established multipath session whose peer reads nothing more: what it sends stays unacknowledged. */
    static Session established() throws Exception {
        Session[] ends = new Session[2];
        for (int i = 0; i < 2; i++) {
            CoreConfig.Role role = i == 0 ? CoreConfig.Role.INITIATOR : CoreConfig.Role.RESPONDER;
            CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role).maxPipes(2).headroom(8);
            c.local().present |= TransportParams.MULTIPATH;
            c.local().secretFlags |= TransportParams.SECRET_INSECURE;
            ends[i] = new Session(c.build(), 0);
            CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
            ends[i].attachBootstrap(info, info, null);
        }
        Batch batch = new Batch();
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 2; i++) {
                Session from = ends[i], to = ends[1 - i];
                while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0)) {
                    byte[] bytes = BatchFixture.flat(batch);
                    from.writeDone(batch, null, 0);
                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                    to.receive(0, 0, new ReadBuf(bb, null), bb, 0, bytes.length, 0);
                }
            }
        }
        assertNotNull(ends[0].mp(), "multipath negotiated");
        return ends[0];
    }

    @Test void bytesBlockingTheReceiverGoAgainOnTheFastestPipeAndAreCountedThere() throws Exception {
        Session s = established();
        Pipe a = s.pipes.get(0);
        Pipe c = PipeTargetTest.appendOut(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        Stream st = s.streams.openStream(true);
        s.send.write(st, ByteBuffer.allocate(4096), false);
        // The stream goes on the pipe affinity binds it to: that one stalls.
        for (Pipe p : new Pipe[] {a, c}) {
            Batch b = new Batch(); RecordWriter w = new RecordWriter();
            try {
                w.init(b, Framing.BYTES, 65536, 65536);
                w.retainFor(p);
                s.mp().write(w, p, p.out().lanes.getFirst()); w.end();
                s.tx.afterBatch(w, p, p.out().lanes.getFirst());
            } finally { b.release(); }
        }
        Pipe slow = a.out().lanes.getFirst().log.isEmpty() ? c : a, fast = slow == a ? c : a;
        long sent = st.snd.sent();
        assertTrue(sent > 0, "the data went on one pipe");
        assertFalse(slow.out().lanes.getFirst().log.isEmpty(), "and is retained there");
        // The peer's credit ends at what was sent: the stream is blocked on bytes the slow pipe holds.
        Poke.set(st.snd, "maxData", sent);
        s.send.write(st, ByteBuffer.allocate(1024), false);
        assertTrue(Retention.streamBlocked(st));
        s.setNow(Time.add(0, 10 * Time.SECOND));

        Batch b2 = new Batch(); RecordWriter w2 = new RecordWriter();
        try {
            w2.init(b2, Framing.BYTES, 65536, 65536);
            w2.retainFor(fast);
            // One chunk per call, until the blocking bytes are all copied.
            for (long before = -1; before != snap(s).bytesOpportunistic(); ) {
                before = snap(s).bytesOpportunistic();
                assertTrue(s.mp().retention.oppSend(w2, fast, st));
            }
            w2.end();
        } finally { b2.release(); }
        long copied = snap(s).bytesOpportunistic();
        assertEquals(sent, copied, "every blocking byte, once");
        assertTrue(snap(s).bytesReinjected() >= copied, "a part of bytes_reinjected");
        assertEquals(1, snap(s).penalties(), "the pipe that held them is penalized");
        assertEquals(copied, fast.mp().ret.oppBytes(), "counted on the pipe that carried the copies");
        assertEquals(0, slow.mp().ret.oppBytes());
    }

    @Test void aPingIsAnsweredOnceItsRecordIsAcknowledgedEachOnce() throws Exception {
        Session s = established();
        Pipe p = s.pipes.get(0);
        Pipe.OutLane lane0 = p.out().lanes.getFirst();
        PathHealth h = s.mp().health;
        long base = p.mp().health.pingsAnswered();
        h.pingRecorded(p, 100);
        h.pingRecorded(p, 200);
        Poke.set(lane0.ack, "acked", 99L); h.laneAcked(p, lane0);
        assertEquals(base, p.mp().health.pingsAnswered(), "short of the end of the record");
        Poke.set(lane0.ack, "acked", 100L); h.laneAcked(p, lane0);
        assertEquals(base + 1, p.mp().health.pingsAnswered());
        h.laneAcked(p, lane0);
        assertEquals(base + 1, p.mp().health.pingsAnswered(), "each once");
        Poke.set(lane0.ack, "acked", 500L); h.laneAcked(p, lane0);
        assertEquals(base + 2, p.mp().health.pingsAnswered(), "two outstanding pings, both answered");
    }

    @Test void theNoEffectBudgetGaugeStaysWithinZeroAndItsCapacity() throws Exception {
        Session s = established();
        KeepAlive ka = s.keepAlive();
        assertEquals(KeepAlive.BUDGET_CAP, ka.budgetTokens());
        for (long i = 0; i <= KeepAlive.BUDGET_CAP; i++) ka.charge(0); // the last charge goes below 0
        assertEquals(0, ka.budgetTokens(), "the charge that closes the session leaves the gauge at 0");
    }
}
