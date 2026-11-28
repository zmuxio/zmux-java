package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;

import java.util.Locale;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A virtual-clock simulation of two sessions over one carrier, the regression base of the acknowledgement rules and
 * the carrier queue limiter. The report runs with ZMUX_ACK_SIM=1: link rate x RTT x traffic x carrier kind over
 * ZMUX_ACK_SEEDS seeds (3) with delay jitter; ZMUX_ACK_ONLY limits it to one traffic mode.
 */
@Timeout(300)
class AckSimTest {
    private static final long MS = Time.MILLISECOND;
    private static final long US = Time.MICROSECOND;

    /** One direction of a carrier: a serialising bottleneck of rate bytes per second, then a fixed delay. */
    static final class Link {
        final double rate;
        long delay;
        long jitter;
        long rnd;
        long free;
        long last;
        final ArrayDeque<long[]> at = new ArrayDeque<>();
        final ArrayDeque<byte[]> data = new ArrayDeque<>();

        Link(double rate, long delay) {
            this.rate = rate;
            this.delay = delay;
        }

        long extra() {
            if (jitter <= 0) return 0;
            rnd ^= rnd << 13;
            rnd ^= rnd >>> 7;
            rnd ^= rnd << 17;
            return Long.remainderUnsigned(rnd, jitter);
        }

        void send(long now, byte[] b) {
            long start = Math.max(free, now);
            free = start + (long) (b.length * 1e9 / rate);
            long t = Math.max(free + delay + extra(), last);
            last = t;
            at.add(new long[] {t});
            data.add(b);
        }

        double queued(long now) {
            return Math.max(free - now, 0) * rate / 1e9;
        }

        long head() {
            return at.isEmpty() ? Time.NEVER : at.peek()[0];
        }
    }

    record Cfg(Framing framing, boolean queueBound, int maxRecord, double rate, long rtt, long dur, int appBytes,
               long appEvery, long seed, long jitter, long start) {
        static Cfg bulk(Framing f, double rate, long rtt, long dur) {
            return new Cfg(f, false, 0, rate, rtt, dur, 0, 0, 0, 0, 0);
        }
    }

    record Result(long srtt, long minRtt, double rate, double goodput, double acksPerSec) {
        @Override public String toString() {
            return String.format(Locale.ROOT, "srtt %.2f ms min_rtt %.2f ms R_p %.1f MB/s goodput %.1f MB/s PIPE_ACK %.0f/s",
                    srtt / 1e6, minRtt / 1e6, rate / 1e6, goodput / 1e6, acksPerSec);
        }
    }

    /** One endpoint with its outbound link. */
    static final class End {
        final Session s;
        final Link out;
        final Framing framing;
        final boolean queueBound;
        int acks;
        final Batch b = new Batch();
        final List<HeldControlTest.Sent> frames = new ArrayList<>();
        final FrameDecoder dec = new FrameDecoder();
        final Frame frame = new Frame();

        End(Session s, Link out, Framing framing, boolean queueBound) {
            this.s = s;
            this.out = out;
            this.framing = framing;
            this.queueBound = queueBound;
        }

        void flush(long now) throws Exception {
            for (int i = 0; i < 1000; i++) {
                if (queueBound && out.queued(now) >= 256 << 10) return;
                if (!s.pollReady(0, 0, b, Session.DEFAULT_BATCH, 0, now, () -> -1)) return;
                if (framing == Framing.RECORDS) {
                    for (ByteBuffer m : BatchFixture.messages(b)) {
                        byte[] a = new byte[m.remaining()];
                        m.get(a);
                        count(a, true);
                        out.send(now, a);
                    }
                } else {
                    byte[] a = BatchFixture.flat(b);
                    count(a, false);
                    out.send(now, a);
                }
                s.writeDone(b, null, now);
            }
            fail("writer spins");
        }

        private void count(byte[] a, boolean message) throws Exception {
            if (!message) {
                frames.clear();
                HeldControlTest.decode(a, frames);
                for (HeldControlTest.Sent f : frames) if (f.kind() == FrameKind.PIPE_ACK) acks++;
                return;
            }
            // A message is one record's frames, without a length prefix.
            ByteBuffer bb = ByteBuffer.wrap(a);
            for (int j = 0; j < a.length; ) {
                j += dec.decode(bb, j, a.length, frame);
                if (frame.kind == FrameKind.PIPE_ACK) acks++;
            }
        }

        void deliver(long now, Link in) {
            while (in.head() <= now) {
                in.at.poll();
                byte[] m = in.data.poll();
                ByteBuffer bb = ByteBuffer.wrap(m);
                if (framing == Framing.RECORDS) s.receiveMessage(0, 0, new ReadBuf(bb, null), bb, 0, m.length, now);
                else assertEquals(m.length, s.receive(0, 0, new ReadBuf(bb, null), bb, 0, m.length, now));
            }
        }
    }

    private static Session session(CoreConfig.Role role, CarrierInfo info, long t0) {
        Session s = new Session(CoreConfig.builder().role(role).transportRole(role).build(), t0);
        s.attachBootstrap(info, info);
        return s;
    }

    /** Sends one stream from client to server over c and reports the client's estimates. */
    static Result run(Cfg c) throws Exception {
        CarrierInfo info = new CarrierInfo(c.framing(), false, false, false, c.queueBound(), null, null, "", 0, "", c.maxRecord(), false, 0, false);
        long t0 = Time.SECOND;
        Link up = new Link(c.rate(), c.rtt() / 2), down = new Link(c.rate(), c.rtt() / 2);
        if (c.seed() != 0) {
            up.jitter = c.jitter();
            up.rnd = c.seed() * 0x9e3779b97f4a7c15L | 1;
            down.jitter = c.jitter();
            down.rnd = c.seed() * 0xc2b2ae3d27d4eb4fL | 1;
        }
        End cl = new End(session(CoreConfig.Role.INITIATOR, info, t0), up, c.framing(), c.queueBound());
        End sv = new End(session(CoreConfig.Role.RESPONDER, info, t0), down, c.framing(), c.queueBound());
        ByteBuffer chunk = ByteBuffer.allocate(256 << 10);
        ByteBuffer rbuf = ByteBuffer.allocate(1 << 20);
        Stream st = null, rs = null;
        long read = 0, readWarm = 0;
        int acksWarm = 0;
        long warm = t0 + c.dur() / 4, end = t0 + c.dur();
        boolean warmed = false;
        long now = t0, nextApp = t0 + c.start();
        while (now < end) {
            cl.s.setNow(now);
            sv.s.setNow(now);
            if (DeadlineFixture.next(cl.s) <= now) cl.s.handleTimeout(now);
            if (DeadlineFixture.next(sv.s) <= now) sv.s.handleTimeout(now);
            cl.deliver(now, down);
            sv.deliver(now, up);
            assertNull(cl.s.error());
            assertNull(sv.s.error());
            if (st == null && cl.s.established() && now >= t0 + c.start()) st = cl.s.streams().openStream(true);
            if (st != null && c.appBytes() > 0) {
                for (; nextApp <= now; nextApp += c.appEvery()) {
                    cl.s.sending().write(st, chunk.clear().limit(c.appBytes()), false);
                }
            } else if (st != null) {
                while (cl.s.sending().write(st, chunk.clear(), false) == chunk.capacity()) {
                    // keep the send buffer full
                }
            }
            if (rs == null) rs = sv.s.streams().acceptStream(true);
            if (rs != null) {
                for (int n; (n = sv.s.receiving().read(rs, rbuf.clear()).n()) > 0; ) read += n;
            }
            cl.flush(now);
            sv.flush(now);
            if (!warmed && now >= warm) {
                warmed = true;
                readWarm = read;
                acksWarm = sv.acks;
            }
            long next = now + 50 * US;
            if (c.appBytes() > 0) next = Math.min(next, nextApp);
            next = Math.min(next, DeadlineFixture.next(cl.s));
            next = Math.min(next, DeadlineFixture.next(sv.s));
            next = Math.min(next, up.head());
            next = Math.min(next, down.head());
            now = Math.max(next, now + 1);
        }
        AckEngine.Send ps = cl.s.pipes().get(0).out().snd;
        double secs = (c.dur() - c.dur() / 4) / 1e9;
        return new Result(ps.rtt().smoothed(), ps.rtt().min(), ps.re().value(), (read - readWarm) / secs, (sv.acks - acksWarm) / secs);
    }

    /**
     * A carrier paced by the limiter carries a bulk transfer at the link rate on a long path: the pacing deadline must
     * not move with the events that read it (pacing then degenerates to bursts released by acknowledgements).
     */
    @Test void pacedBulkFillsTheLink() throws Exception {
        for (long rtt : new long[] {10, 80}) {
            Cfg c = Cfg.bulk(Framing.BYTES, 20e6, rtt * MS, 3 * Time.SECOND);
            Result r = run(c);
            assertTrue(r.goodput() >= 0.95 * c.rate(), "rtt " + rtt + " ms: " + r);
        }
    }

    /**
     * A sparse record below the threshold is acknowledged at once (MULTIPATH.md 6.3 rule 2a): the smoothed and the
     * minimum RTT are the path's, not the path's plus zmux_max_ack_delay.
     */
    @Test void aSparseRecordKeepsTheRttOfThePath() throws Exception {
        Cfg c = new Cfg(Framing.RECORDS, false, 65525, 50e6, 10 * MS, 2 * Time.SECOND, 4 << 10, 40 * MS, 0, 0, 0);
        Result r = run(c);
        assertTrue(r.minRtt() <= 11 * MS && r.srtt() <= 12 * MS, r.toString());
    }

    /** The matrix report (ZMUX_ACK_SIM=1). */
    @Test void matrix() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("ZMUX_ACK_SIM") != null, "set ZMUX_ACK_SIM=1");
        String seedsEnv = System.getenv("ZMUX_ACK_SEEDS");
        int seeds = seedsEnv != null ? Integer.parseInt(seedsEnv) : 3;
        String only = System.getenv("ZMUX_ACK_ONLY");
        String[] modes = {"bulk", "app-4k/10ms", "app-60k/30ms", "msg-64k"};
        for (boolean qb : new boolean[] {false, true}) {
            for (String m : modes) {
                if (only != null && !only.equals(m)) continue;
                for (double rate : new double[] {5e6, 20e6, 50e6, 200e6}) {
                    for (long rtt : new long[] {1, 10, 20, 80}) {
                        StringBuilder line = new StringBuilder();
                        double minG = Double.MAX_VALUE, maxG = 0;
                        for (int seed = 1; seed <= seeds; seed++) {
                            Framing f = m.startsWith("bulk") || m.startsWith("app-4k") ? Framing.BYTES : Framing.RECORDS;
                            int maxRec = f == Framing.RECORDS ? 65525 : 0;
                            int app = m.equals("app-4k/10ms") ? 4 << 10 : m.equals("app-60k/30ms") ? 60 << 10 : 0;
                            long every = m.equals("app-4k/10ms") ? 10 * MS : m.equals("app-60k/30ms") ? 30 * MS : 0;
                            Cfg c = new Cfg(f, qb, maxRec, rate, rtt * MS, 4 * Time.SECOND, app, every, seed,
                                    (seed - 1L) * 150 * US, (seed - 1L) * 7 * MS);
                            Result r = run(c);
                            minG = Math.min(minG, r.goodput());
                            maxG = Math.max(maxG, r.goodput());
                            line.append(String.format(Locale.ROOT, " | srtt %5.1f min %5.1f R %6.1f G %6.1f ack %4.0f",
                                    r.srtt() / 1e6, r.minRtt() / 1e6, r.rate() / 1e6, r.goodput() / 1e6, r.acksPerSec()));
                        }
                        System.out.printf("qb=%-5s %-12s %3.0fMB/s %2dms G[%6.1f..%6.1f]%s%n", qb, m, rate / 1e6, rtt,
                                minG / 1e6, maxG / 1e6, line);
                    }
                }
            }
        }
    }
}
