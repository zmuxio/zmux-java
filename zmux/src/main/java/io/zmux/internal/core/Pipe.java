package io.zmux.internal.core;

import java.util.ArrayList;

/**
 * One attachment: an inbound and/or an outbound direction, each with its lanes. The state of a
 * direction or lane is split by owner: each component keeps its part in a slot it alone writes
 * ({@link Transmitter.LaneTx}, {@link AckEngine.LaneAck}, {@link Lanes.LaneState}, {@link RecordReceiver.LaneRx},
 * {@link AckEngine.InAck}, {@link AckEngine.Send}, {@link Lanes.Out}, {@link Pacer.Slot}, {@link QualityMonitor.PipeQ});
 * the few fields kept here change only through this class's transitions.
 */
final class Pipe {
    final long id;
    /** The carrier connection of lane 0, compared by identity (lanes). */
    private Object conn;
    /** The kind of the pipe's carrier, null when undeclared. */
    private io.zmux.CarrierKind carrier;
    private InDir in;
    private OutDir out;
    /** Multipath side storage; null outside multipath. */
    private Attach mp;
    /** Record shaping of the outbound direction; null until a shaper was set. */
    private Shaping.PipeShape shape;

    /** When a record carrying a traffic frame last went out or came in completely; MIN_VALUE: never. */
    private long lastTraffic = Long.MIN_VALUE;

    long lastTraffic() { return lastTraffic; }

    void traffic(long now) { lastTraffic = now; }

    Shaping.PipeShape shape() { return shape; }

    /** The admission value of the carrier that joined the pipe, opaque; null without one. */
    private Object admitted;

    Object admitted() { return admitted; }

    void admitted(Object v) { admitted = v; }

    /** Installs the outbound direction's shaping state (Shaping only). */
    void shape(Shaping.PipeShape ps) { shape = ps; }

    Pipe(long id, InDir in, OutDir out, Attach mp, io.zmux.CarrierKind carrier) {
        this.id = id;
        this.carrier = carrier;
        this.in = in;
        this.out = out;
        this.mp = mp;
    }

    InDir in() {
        return in;
    }

    OutDir out() {
        return out;
    }

    Attach mp() {
        return mp;
    }

    Object conn() {
        return conn;
    }

    io.zmux.CarrierKind carrier() {
        return carrier;
    }

    /** The label the application gave the pipe's carrier, or null. */
    private String label;

    String label() {
        return label;
    }

    void label(String l) {
        label = l;
    }

    /** The pipe's actual carrier was bound (a reservation). */
    void carrier(io.zmux.CarrierKind k) {
        carrier = k;
    }

    /** Records the carrier connection of lane 0. */
    void conn(Object c) {
        conn = c;
    }

    /** The session moved to multipath: a pipe attached before it gets its attachment state, once. */
    void attach(Attach a) {
        if (mp != null) throw new IllegalStateException("pipe " + id + " already attached");
        mp = a;
    }

    /** Replaces both directions of the bootstrap pipe (early data rejected by the carrier). */
    void restart(InDir in, OutDir out) {
        this.in = in;
        this.out = out;
    }

    static final class InDir {
        /** In arrival order, lane 0 first; add only through {@link #add}. */
        final ArrayList<InLane> lanes = new ArrayList<>(1);
        /** Lanes other than 0 by number; null until the peer opens one. */
        private LongMap<InLane> byLane;
        private boolean ended;

        /** An inbound direction with lane 0. */
        InDir(InLane first) {
            add(first);
        }

        InLane lane(long l) {
            if (l == 0) return lanes.isEmpty() ? null : lanes.getFirst();
            return byLane == null ? null : byLane.get(l);
        }

        void add(InLane il) {
            lanes.add(il);
            if (il.lane == 0) return;
            if (byLane == null) byLane = new LongMap<>(4);
            byLane.put(il.lane, il);
        }

        boolean ended() {
            return ended;
        }

        /** The inbound direction ended: every lane ended, or the attachment failed. */
        void end() {
            ended = true;
        }
    }

    static final class InLane {
        final long lane;
        final RecordReceiver.LaneRx rx;
        final AckEngine.InAck ack = new AckEngine.InAck();

        InLane(long lane, boolean expectTp, boolean expectLane, boolean discardTp) {
            this.lane = lane;
            rx = new RecordReceiver.LaneRx(expectTp, expectLane, discardTp);
        }
    }

    static final class OutDir {
        final Lanes.Out lo;
        /** Indexed by lane number: lanes are numbered in opening order from 0. */
        final ArrayList<OutLane> lanes = new ArrayList<>(1);
        final AckEngine.Send snd = new AckEngine.Send();
        final Pacer.Slot pace = new Pacer.Slot();
        final QualityMonitor.PipeQ q = new QualityMonitor.PipeQ();
        /** The writer of lane 0. */
        final Waiter sig;
        private long records;
        private boolean failed;
        /** Own lanes of this direction the peer refused (MULTIPATH.md 5.8). */
        private long lanesRefused;
        /** Bytes sent and not acknowledged when the pipe failed (MULTIPATH.md 7.2). */
        private long bytesLost;
        /** The carrier's retransmitted bytes, as its adapter last reported them; -1: never. */
        private long retransmitted = -1;
        /** The last loss-rate sample: its time (MIN_VALUE: none), the bytes sent and retransmitted then. */
        private long lossAt = Long.MIN_VALUE, lossSent, lossRetrans;
        private double lossRate;
        private boolean lossKnown;

        /** The bytes sent over all lanes. */
        long sent() {
            long n = 0;
            for (OutLane l : lanes) n += l.tx.pos();
            return n;
        }

        long bytesLost() { return bytesLost; }

        /** The pipe failed: what was sent and not acknowledged is lost. */
        void lost() { long s = sent(); bytesLost = s - Math.min(s, snd.cum()); }

        long retransmitted() { return retransmitted; }

        boolean lossKnown() { return lossKnown; }

        double lossRate() { return lossRate; }

        /** Whether the retransmission count is due for a query (at most once per gap). */
        boolean retransmittedDue(long t, long gap) { return lossAt == Long.MIN_VALUE || Time.sub(t, lossAt) >= gap; }

        /**
         * The carrier reported n retransmitted bytes (-1: it reports none) at t: an EWMA with gain
         * 1/8 of the bytes retransmitted per byte sent between samples at least gap apart; a
         * count that moves back counts as no retransmission.
         */
        void retransmitted(long n, long t, long gap) {
            if (n < 0) { lossAt = t; return; } // no count: asked again after the gap
            long sent = sent();
            boolean first = retransmitted < 0;
            retransmitted = n;
            if (first) { lossAt = t; lossSent = sent; lossRetrans = n; return; }
            if (Time.sub(t, lossAt) < gap || sent <= lossSent) return;
            double x = (double) (n - Math.min(n, lossRetrans)) / (sent - lossSent);
            if (lossKnown) lossRate += (x - lossRate) / 8;
            else { lossRate = x; lossKnown = true; }
            lossAt = t; lossSent = sent; lossRetrans = n;
        }

        void laneRefused() {
            lanesRefused++;
        }

        long lanesRefused() {
            return lanesRefused;
        }

        /** An outbound direction with its confirmed lane 0 on the carrier. */
        OutDir(CarrierInfo info, Waiter sig) {
            this.sig = sig;
            lo = new Lanes.Out(info.lanes());
            add(new OutLane(0, info, true));
        }

        OutLane lane(long l) {
            return l >= 0 && l < lanes.size() ? lanes.get((int) l) : null;
        }

        void add(OutLane ol) {
            if (ol.lane != lanes.size()) throw new IllegalStateException("lane " + ol.lane + " out of order");
            lanes.add(ol);
        }

        long records() {
            return records;
        }

        /** Records went out on the pipe. */
        void recordsWritten(long n) {
            records += n;
        }

        boolean failed() {
            return failed;
        }

        /** Writes on the outbound direction failed: nothing more goes out on it. */
        void fail() {
            failed = true;
        }
    }

    static final class OutLane {
        final long lane;
        final Transmitter.LaneTx tx = new Transmitter.LaneTx();
        final AckEngine.LaneAck ack = new AckEngine.LaneAck();
        final Lanes.LaneState st;
        /** Unacknowledged bytes in send order: (pos, end, so) triples. */
        final LongTriples segs = new LongTriples();
        /** End positions of records with activity frames. */
        final LongList activity = new LongList();
        final ArrayList<Stream> boundSt = new ArrayList<>(0);
        /** Multipath retention log, in sending order. */
        final java.util.ArrayDeque<Retained> log = new java.util.ArrayDeque<>(0);

        OutLane(long lane, CarrierInfo info, boolean confirmed) {
            this.lane = lane;
            st = new Lanes.LaneState(info, confirmed);
        }
    }

    record Sample(long end, long sent, long delivered, long deliveredAt, long firstSentAt, long epoch,
                  boolean bubble, long queue) {}
}
