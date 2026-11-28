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
    private SessionCapacity.Member member;

    SessionCapacity.Member member() { return member; }

    void member(SessionCapacity.Member value) { member = value; }

    /** The carrier connection of lane 0, compared by identity (lanes). */
    private Object conn;
    /** The kind of the pipe's carrier, null when undeclared. */
    private final io.zmux.CarrierKind carrier;
    private final InDir in;
    private final OutDir out;
    /** The session queued this carrier's terminal action, independently of inbound EOF. */
    private boolean carrierCloseRequested;

    boolean requestCarrierClose() {
        if (carrierCloseRequested) return false;
        carrierCloseRequested = true;
        return true;
    }

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
        shareRtt();
    }

    /**
     * One RTT estimator per pipe: an attachment and the outbound direction share it. A pipe
     * attached once it had samples keeps them; otherwise the attachment's initial estimate stands.
     */
    private void shareRtt() {
        if (mp == null || out == null) return;
        if (out.snd.rtt().has()) mp.useRtt(out.snd.rtt());
        else out.snd.useRtt(mp.rtt());
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

    /** The host's label of the transform the pipe's carrier runs, "" when none. */
    private String variant = "";

    String variant() { return variant; }

    void variant(String v) { variant = v; }

    /** The label the application gave the pipe's carrier, or null. */
    private String label;

    String label() {
        return label;
    }

    void label(String l) {
        label = l;
    }

    /** Records the carrier connection of lane 0. */
    void conn(Object c) {
        conn = c;
    }

    /** The session moved to multipath: a pipe attached before it gets its attachment state, once. */
    void attach(Attach a) {
        if (mp != null) throw new IllegalStateException("pipe " + id + " already attached");
        mp = a;
        shareRtt();
    }

    static final class InDir {
        /** In arrival order, lane 0 first; add only through {@link #add}. */
        final ArrayList<InLane> lanes = new ArrayList<>(1);
        /** Lanes other than 0 by number; null until the peer opens one. */
        private LongMap<InLane> byLane;
        private boolean ended;
        private boolean closeRead;
        final AckEngine.InPipeAck ack = new AckEngine.InPipeAck();

        /** An inbound direction with lane 0. */
        InDir(InLane first) {
            add(first);
        }

        /** Lane 0, present from construction on. */
        InLane first() { return lanes.getFirst(); }

        InLane lane(long l) {
            if (l == 0) return first();
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

        /** Whether the peer's CONNECTION_CLOSE was read on this pipe: it sends nothing after it. */
        boolean closeRead() {
            return closeRead;
        }

        void readClose() {
            closeRead = true;
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

        InLane(long lane, boolean expectTp, boolean expectLane) {
            this.lane = lane;
            rx = new RecordReceiver.LaneRx(expectTp, expectLane);
        }
    }

    static final class OutDir {
        final Lanes.Out lo;
        /** Indexed by lane number: lanes are numbered in opening order from 0. */
        final ArrayList<OutLane> lanes = new ArrayList<>(1);
        final AckEngine.Send snd = new AckEngine.Send();
        final Pacer.Slot pace = new Pacer.Slot();
        final QualityMonitor.PipeQ q = new QualityMonitor.PipeQ();
        private long records;
        private boolean failed;
        /** Own lanes of this direction the peer refused. */
        private long lanesRefused;
        /** Bytes sent and not acknowledged when the pipe failed. */
        private long bytesLost;
        /** The bytes sent over all lanes. */
        long sent() {
            long n = 0;
            for (OutLane l : lanes) n += l.tx.pos();
            return n;
        }

        long bytesLost() { return bytesLost; }

        /** The pipe failed: what was sent and not acknowledged is lost. */
        void lost() { long s = sent(); bytesLost = s - Math.min(s, snd.cum()); }

        void laneRefused() {
            lanesRefused++;
        }

        long lanesRefused() {
            return lanesRefused;
        }

        /** An outbound direction with its confirmed lane 0 on the carrier. */
        OutDir(CarrierInfo info) {
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
            cancelDeadlines();
        }

        void cancelDeadlines() {
            lo.cancel();
            pace.cancel();
        }
    }

    static final class OutLane {
        final long lane;
        private long nativeChannelId = Long.MIN_VALUE;

        long replaceNativeChannel(long id) {
            long previous = nativeChannelId;
            nativeChannelId = id;
            return previous;
        }
        final Shaping.Delay shapeDelay = new Shaping.Delay();
        final Transmitter.LaneTx tx = new Transmitter.LaneTx();
        final AckEngine.LaneAck ack = new AckEngine.LaneAck();
        final Lanes.LaneState st;
        private PipeTable.Writer writer;

        void writer(PipeTable.Writer writer) {
            if (this.writer != null) this.writer.invalidate();
            this.writer = writer;
        }

        void requestWrite() {
            if (writer != null) writer.changed();
        }

        boolean hasWriter() { return writer != null && writer.current(); }
        void grantRecord() { if (hasWriter()) writer.record(); }
        void grantChannel() { if (hasWriter()) writer.channel(); }
        /** Unacknowledged bytes in send order: (pos, end, so) triples. */
        final LongTriples segs = new LongTriples();
        /** End positions of records with activity frames. */
        final LongList activity = new LongList();
        final ArrayList<Stream> boundSt = new ArrayList<>(0);
        /** Multipath retention log, in sending order. */
        final FrameHistory log = new FrameHistory();
        // The earliest copy of an outstanding epoch answer suffices for cumulative lane ACKs.
        private long epochAnswer;
        private long epochAnswerPosition;
        private final LongList pingEnds = new LongList();
        private long pingTailEnd;
        private long pingTailCount;

        void rememberEpochAnswer(long epoch, long pos) {
            if (epochAnswer == epoch) return;
            epochAnswer = epoch;
            epochAnswerPosition = pos;
        }

        boolean acknowledgedEpochAnswer(long epoch, long pos) {
            return epochAnswer == epoch && pos >= epochAnswerPosition;
        }

        void rememberPing(long end) {
            if (pingEnds.size() < FrameHistory.SLOTS && pingTailCount == 0) {
                pingEnds.add(end);
            } else {
                // Keep a count past the exact sampling window. A covering ACK releases all of it.
                pingTailEnd = end;
                pingTailCount++;
            }
        }

        long drainAcknowledgedPings() {
            int count = 0;
            while (count < pingEnds.size() && ack.acked() >= pingEnds.get(count)) count++;
            pingEnds.removeFirst(count);
            if (pingTailCount == 0 || ack.acked() < pingTailEnd) return count;
            long answered = count + pingTailCount;
            pingTailCount = 0;
            return answered;
        }

        void retire() {
            writer(null);
            segs.release();
            activity.release();
            pingEnds.release();
            pingTailCount = 0;
            log.retire();
        }

        OutLane(long lane, CarrierInfo info, boolean confirmed) {
            this.lane = lane;
            st = new Lanes.LaneState(info, confirmed);
        }
    }

    /** backlog: the pipe's bytes unacknowledged ahead of it when sent, which a queue-proven range waited behind. */
    record Sample(long end, long sent, long delivered, long deliveredAt, long firstSentAt, long epoch,
                  boolean bubble, long backlog, long queue, boolean probe) {}

    /** Whether the pipe's carrier reports its own failure, as its first outbound lane says (CarrierInfo.ownFailure). */
    boolean ownFailure() {
        return out() != null && !out().lanes.isEmpty() && out().lanes.getFirst().st.info().ownFailure();
    }
}
