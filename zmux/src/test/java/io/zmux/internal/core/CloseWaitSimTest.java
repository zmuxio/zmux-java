package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The close wait of a session without multipath whose pipe has a second lane (SPEC.md 6.5 step 1), on a virtual
 * clock: both ends write on lane 1 and close at once. Each end's CONNECTION_CLOSE waits for the PIPE_ACK of its lane
 * 1 data, which the other end, waiting too, keeps sending: both closes go out within a few RTTs, far below the bound.
 */
@Timeout(30)
class CloseWaitSimTest {
    private static final long MS = Time.MILLISECOND;
    /** One-way delay of every lane. */
    private static final long DELAY = 10 * MS;

    /** One direction of one lane: byte runs that arrive DELAY after they were written. */
    private static final class Link {
        final ArrayDeque<long[]> at = new ArrayDeque<>();
        final ArrayDeque<byte[]> data = new ArrayDeque<>();

        long head() {
            return at.isEmpty() ? Time.NEVER : at.peek()[0];
        }
    }

    private static final class End {
        final Session s;
        final Link[] out = {new Link(), new Link()};
        final Batch b = new Batch();
        long closeAt = Time.NEVER;

        End(Session s) {
            this.s = s;
        }

        void flush(long now) {
            for (int lane = 0; lane < 2; lane++) {
                for (int i = 0; i < 1000 && s.pollTransmit(0, lane, b, Session.DEFAULT_BATCH, now); i++) {
                    out[lane].at.add(new long[] {now + DELAY});
                    out[lane].data.add(BatchFixture.flat(b));
                    s.writeDone(b, null, now);
                }
            }
            if (closeAt == Time.NEVER && s.life.closeQueued() && !s.life.closePending()) closeAt = now;
        }

        void deliver(long now, End from) {
            for (int lane = 0; lane < 2; lane++) {
                Link in = from.out[lane];
                while (in.head() <= now) {
                    in.at.poll();
                    byte[] m = in.data.poll();
                    ByteBuffer bb = ByteBuffer.wrap(m);
                    assertEquals(m.length, s.receive(0, lane, new ReadBuf(bb, null), bb, 0, m.length, now));
                }
            }
        }
    }

    /** Gives x an outbound lane 1 toward y, and y the inbound lane 1. */
    private static void lane(Session x, Session y) {
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, true, true);
        Features f = x.features();
        Poke.set(x, "feat", new Features(f.role(), f.goAway(), f.preamble(), f.priority(), f.flows(), f.multipath(), f.pipeAck(),
                f.resume(), true, true, f.joinSecurityUnavailable(), f.carrierDatagrams()));
        x.peer().maxLanes = 3;
        Pipe p = x.pipes.get(0);
        Poke.set(p.out().lo, "offered", true);
        p.out().add(new Pipe.OutLane(1, info, true));
        y.pipes.get(0).in().add(new Pipe.InLane(1, false, false));
    }

    @Test void simultaneousCloseWithUnacknowledgedLaneData() throws Exception {
        End a = new End(HeldControlTest.session(CoreConfig.Role.INITIATOR));
        End b = new End(HeldControlTest.session(CoreConfig.Role.RESPONDER));
        HeldControlTest.pump(a.s, b.s);
        assertTrue(a.s.established() && b.s.established());
        lane(a.s, b.s);
        lane(b.s, a.s);
        long t0 = Time.SECOND;
        for (End e : new End[] {a, b}) {
            e.s.setNow(t0);
            Stream st = e.s.streams().openStream(true);
            e.s.streams.setPriority(st, 3, true);
            e.s.sending().write(st, ByteBuffer.allocate(8 << 10), false);
            e.flush(t0);
            assertTrue(e.s.pipes.get(0).out().lane(1).tx.pos() > 0, "the data went on lane 1");
        }
        a.s.close(0, new byte[0]);
        b.s.close(0, new byte[0]);
        assertTrue(a.s.life.waitingLanes() && b.s.life.waitingLanes(), "both closes wait for lane 1");
        long bound = t0 + a.s.lingerTimeout();
        long now = t0;
        while ((a.closeAt == Time.NEVER || b.closeAt == Time.NEVER) && now <= bound + 100 * MS) {
            for (End e : new End[] {a, b}) {
                e.s.setNow(now);
                if (e.s.nextTimeout() <= now) e.s.handleTimeout(now);
            }
            a.deliver(now, b);
            b.deliver(now, a);
            a.flush(now);
            b.flush(now);
            long next = Math.min(Math.min(a.s.nextTimeout(), b.s.nextTimeout()), now + MS);
            for (End e : new End[] {a, b}) for (Link l : e.out) next = Math.min(next, l.head());
            now = Math.max(next, now + 1);
        }
        long rtt = 2 * DELAY;
        assertTrue(a.closeAt - t0 <= 5 * rtt, "a's CONNECTION_CLOSE after " + (a.closeAt - t0) / MS + " ms; bound "
                + (bound - t0) / MS + " ms");
        assertTrue(b.closeAt - t0 <= 5 * rtt, "b's CONNECTION_CLOSE after " + (b.closeAt - t0) / MS + " ms; bound "
                + (bound - t0) / MS + " ms");
    }
}
