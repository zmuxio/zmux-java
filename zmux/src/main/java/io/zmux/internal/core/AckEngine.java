package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.util.ArrayList;
import java.util.List;

/**
 * Pipe acknowledgement of a session with acknowledgement (MULTIPATH.md Sections 3.5 and 6):
 * when PIPE_ACK is due and what it carries, send-order bookkeeping of produced batches, and
 * RTT and delivery-rate samples from acknowledged advances. Owned by the session's loop;
 * effects on other components go through {@link Port}.
 */
final class AckEngine {
    /** What AckEngine asks of the session. */
    interface Port {
        /** An acknowledgement covered a record that restarts the idle timer. */
        void activity();

        /** An RTT sample was taken; min is the pipe's minimum RTT. */
        void rttSampled(long min);

        /** A lane's acknowledged position advanced; cum is whether the pipe's cumulative point moved. */
        void advanced(Pipe p, Pipe.OutLane ol, boolean cum);

        /** Whether the multipath session is quiet (MULTIPATH.md 7.1): its RTT samples stay out of min_rtt. */
        boolean quiet();
    }

    private final Port port;
    private final Pacer pacer;
    private final List<Pipe> pipes;
    private final long threshold;
    private final long delay;
    private final long peerDelay;
    private boolean immediate;
    private boolean woken;
    private final ArrayList<long[]> entries = new ArrayList<>(2);

    AckEngine(TransportParams local, TransportParams peer, List<Pipe> pipes, Pacer pacer, Port port) {
        this.threshold = local.has(TransportParams.ACK_THRESHOLD) ? local.ackThreshold : 65536;
        this.delay = local.has(TransportParams.ACK_DELAY) ? Time.millis(local.maxAckDelayMillis) : 25 * Time.MILLISECOND;
        this.peerDelay = peerDelay(peer);
        this.pipes = pipes;
        this.pacer = pacer;
        this.port = port;
    }

    /** The peer's maximum acknowledgement delay. */
    static long peerDelay(TransportParams peer) {
        return peer.has(TransportParams.ACK_DELAY) ? Time.millis(peer.maxAckDelayMillis) : 25 * Time.MILLISECOND;
    }

    long peerDelay() {
        return peerDelay;
    }

    /** A PIPE_ACK must go out at the next write (PIPE_PING, a pipe ending). */
    void ackNow() {
        immediate = true;
    }

    /**
     * Accounts an inbound record of n position bytes; true when a PIPE_ACK is now due. elicit counts the record
     * toward the acknowledgement delay of the primary pipe; atOnce makes that acknowledgement due at once (a PIPE_PING
     * answered there). A quiet session's PIPE_PING alone does neither: its answer goes on the probed pipe pair
     * only (MULTIPATH.md 6.5).
     */
    boolean onRecord(Pipe.InLane il, long n, boolean elicit, boolean atOnce, long now) {
        if (elicit) {
            if (il.ack.elicit == 0) il.ack.elicitAt = now;
            il.ack.elicit += n;
        }
        if (atOnce) immediate = true;
        return due(now);
    }

    /** Whether a PIPE_ACK must be sent now. */
    boolean due(long now) {
        if (immediate) return true;
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            long sum = 0, first = Time.NEVER;
            for (Pipe.InLane il : p.in().lanes) {
                if (il.ack.elicit > 0) {
                    sum += il.ack.elicit;
                    first = Math.min(first, il.ack.elicitAt);
                }
                if (il.rx.ended() && il.rx.pos() > il.ack.reported) return true;
            }
            if (sum >= threshold || sum > 0 && now >= Time.add(first, delay)) return true;
        }
        return false;
    }

    /** The earliest acknowledgement delay deadline. */
    long deadline() {
        if (woken) return Time.NEVER;
        long t = Time.NEVER;
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            for (Pipe.InLane il : p.in().lanes) if (il.ack.elicit > 0) t = Math.min(t, Time.add(il.ack.elicitAt, delay));
        }
        return t;
    }

    /** Runs the delay timer; true when writers must wake to send a due PIPE_ACK. */
    boolean onTimeout(long now) {
        if (!due(now)) return false;
        woken = true;
        return true;
    }

    /** Writes a PIPE_ACK snapshot when one is due. */
    void write(RecordWriter w, long now) {
        if (due(now)) snapshot(w, now, true);
    }

    /** Writes a snapshot that answers what is due on the primary pipe. */
    boolean snapshot(RecordWriter w, long now) {
        return snapshot(w, now, true);
    }

    /**
     * Writes the positions of every inbound pipe and lane above 0, split over frames when needed (MULTIPATH.md 6.2).
     * settles: the snapshot answers what is due (the primary pipe's acknowledgement); a copy on another pipe (the
     * first record of a pipe, the answer to a PIPE_PING on its pipe pair) leaves that obligation in place. Returns
     * false when the record had no room for all of it.
     */
    boolean snapshot(RecordWriter w, long now, boolean settles) {
        ArrayList<long[]> es = entries;
        es.clear();
        for (Pipe p : pipes) {
            if (p.in() == null || p.mp() != null && p.mp().health.retired()) continue;
            for (Pipe.InLane il : p.in().lanes) {
                if (il.rx.pos() == 0) continue;
                long d = Math.max(Time.sub(now, il.rx.posAt()) / Time.MICROSECOND, 0);
                es.add(new long[] {p.id, il.lane, il.rx.pos(), d});
            }
        }
        if (es.isEmpty()) {
            if (settles) immediate = false;
            return true;
        }
        es.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
        int i = 0;
        while (i < es.size()) {
            int k = es.size() - i;
            int n;
            while (true) {
                n = VarInt.length(FrameType.PIPE_ACK) + VarInt.length(k);
                for (int j = i; j < i + k; j++) {
                    long[] e = es.get(j);
                    n += FrameEncoder.ackEntryLength(e[0], e[1], e[2], e[3]);
                }
                if (w.fits(n) || k == 1) break;
                k = (k + 1) / 2;
            }
            int from = i, cnt = k;
            if (!w.frame(n, false, bb -> {
                FrameEncoder.pipeAckHeader(bb, cnt);
                for (int j = from; j < from + cnt; j++) {
                    long[] e = es.get(j);
                    FrameEncoder.ackEntry(bb, e[0], e[1], e[2], e[3]);
                }
            })) {
                es.clear();
                return false;
            }
            i += k;
        }
        es.clear();
        if (!settles) return true;
        immediate = false;
        woken = false;
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            for (Pipe.InLane il : p.in().lanes) {
                il.ack.elicit = 0;
                il.ack.reported = il.rx.pos();
            }
        }
        return true;
    }

    /** Records a produced batch for RTT samples and idle accounting; ahead is queue_p at its hand-over (unsent_ahead over R_p, both then). */
    void onSent(Pipe p, Pipe.OutLane ol, long startPos, RecordWriter w, long now, long queue) {
        if (w.activity()) ol.activity.add(ol.tx.pos());
        if (ol.lane != 0 && !ol.st.confirmed()) return;
        AckEngine.Send ps = p.out().snd;
        long so = ps.so;
        long n = ol.tx.pos() - startPos;
        int k = ol.segs.size();
        if (k > 0 && ol.segs.b(k - 1) == startPos && ol.segs.c(k - 1) + (startPos - ol.segs.a(k - 1)) == so) {
            ol.segs.setB(k - 1, ol.tx.pos());
        } else {
            ol.segs.add(startPos, ol.tx.pos(), so);
        }
        ps.so += n;
        if (w.anyElicit()) {
            long end = w.framing() == Framing.RECORDS ? ps.so : so + w.elicitEnd();
            if (ps.samples.isEmpty()) {
                ps.deliveredAt = now;
                ps.firstSentAt = now;
            }
            ps.elicitEnd = Math.max(ps.elicitEnd, end);
            boolean bubble = ps.re.bubbleInFlight(ps.cum);
            ps.samples.add(new Pipe.Sample(end, now, ps.delivered, ps.deliveredAt, ps.firstSentAt, ps.re.blockEpoch(), bubble, queue));
        }
    }

    /** Trims a lane's send-order map to its acknowledged position; returns the pipe's cumulative point. */
    private static long cumPoint(Pipe.OutDir d, Pipe.OutLane ol) {
        int i = 0;
        while (i < ol.segs.size() && ol.segs.b(i) <= ol.ack.acked) i++;
        ol.segs.removeFirst(i);
        long cum = d.snd.so;
        for (Pipe.OutLane l : d.lanes) {
            if (l.segs.size() == 0) continue;
            long c = l.segs.c(0);
            if (l.ack.acked > l.segs.a(0)) c += l.ack.acked - l.segs.a(0);
            cum = Math.min(cum, c);
        }
        return cum;
    }

    /** The PIPE_ACK entries of a single-pipe session (MULTIPATH.md Section 3.5); p0 is pipe 0 or null. */
    void onSinglePipeAck(Frame.AckEntries it, Pipe p0, long now) throws WireException {
        while (it.next()) {
            if (it.pipeId != 0) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_ACK for a pipe other than 0", "pipe", it.pipeId);
            }
            if (it.position == 0) continue;
            Pipe.OutLane ol = p0 != null && p0.out() != null ? p0.out().lane(it.lane) : null;
            if (ol == null) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_ACK for an unopened lane", "pipe", 0, "value", it.lane);
            }
            if (ol.st.refused()) continue;
            if (it.position > ol.tx.pos()) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_ACK position beyond the bytes sent",
                        "pipe", 0, "pos", it.position);
            }
            if (it.position <= ol.ack.acked) continue;
            ol.ack.acked = it.position;
            advanced(p0, ol, it.position, it.delay, now);
        }
    }

    /** RTT samples and idle-timer evidence from a lane's advance; estimates move with the cumulative point only. */
    void advanced(Pipe p, Pipe.OutLane ol, long position, long delayMicros, long now) {
        int j = 0;
        while (j < ol.activity.size() && ol.activity.get(j) <= position) j++;
        if (j > 0) {
            ol.activity.removeFirst(j);
            port.activity();
        }
        AckEngine.Send ps = p.out().snd;
        long cum = cumPoint(p.out(), ol);
        if (cum <= ps.cum) {
            port.advanced(p, ol, false);
            return;
        }
        long newly = cum - ps.cum;
        ps.cum = cum;
        ps.delivered += newly;
        boolean roundEnd = ps.re.advance(cum, ps.so);
        long sampleRtt = 0, sampleSentAt = 0;
        double sampleRate = 0;
        Pipe.Sample last = null;
        int i = 0;
        while (i < ps.samples.size() && ps.samples.get(i).end() <= cum) last = ps.samples.get(i++);
        if (last != null) {
            long rtt = Time.sub(now, last.sent());
            long ad = Time.mul(delayMicros, Time.MICROSECOND);
            boolean late = ad > peerDelay && (ps.rttAt == 0 || Time.sub(now, ps.rttAt) < Time.SECOND);
            RttEstimator r = p.mp() != null ? p.mp().rtt : ps.rtt;
            boolean sampleOk = p.mp() == null || p.mp().sampleOk(last.sent());
            if (!late && sampleOk) {
                sampleRtt = r.update(rtt, Math.min(ad, peerDelay), peerDelay, now, p.mp() != null && port.quiet());
                sampleSentAt = last.sent();
                ps.rttAt = now;
                port.rttSampled(r.min());
            }
            long iv = Math.max(Time.sub(last.sent(), last.firstSentAt()), Time.sub(now, last.deliveredAt()));
            if (iv > 0 && sampleOk) {
                sampleRate = ps.re.sample((ps.delivered - last.delivered()) * 1e9 / iv, last.bubble(), last.epoch(), now);
            }
            ps.firstSentAt = last.sent();
        }
        ps.samples.subList(0, i).clear();
        ps.deliveredAt = now;
        pacer.onAck(p, sampleRtt, sampleSentAt, sampleRate, roundEnd, now);
        port.advanced(p, ol, true);
    }

    /** The peer acknowledged the lane up to pos (a multipath PIPE_ACK entry). */
    void acked(Pipe.OutLane ol, long pos) {
        ol.ack.acked = pos;
    }

    /** The acknowledged position of one outbound lane. */
    static final class LaneAck {
        private long acked;

        LaneAck() {
        }

        long acked() {
            return acked;
        }
    }

    /** The acknowledgement state of one inbound lane. */
    static final class InAck {
        private long elicit;
        private long elicitAt;
        private long reported;

        InAck() {
        }

        long elicit() {
            return elicit;
        }

        long elicitAt() {
            return elicitAt;
        }

        long reported() {
            return reported;
        }
    }

    /** The sender's view of an outbound pipe, counted in send order across its lanes. */
    static final class Send {
        private long so;
        private long cum;
        private final ArrayList<Pipe.Sample> samples = new ArrayList<>(0);
        private long delivered;
        private long deliveredAt;
        private long firstSentAt;
        private final RttEstimator rtt = new RttEstimator();
        private final RateEstimator re = new RateEstimator();
        private long rttAt;
        private long elicitEnd;

        Send() {
        }

        long so() {
            return so;
        }

        long cum() {
            return cum;
        }

        ArrayList<Pipe.Sample> samples() {
            return samples;
        }

        long delivered() {
            return delivered;
        }

        long deliveredAt() {
            return deliveredAt;
        }

        long firstSentAt() {
            return firstSentAt;
        }

        RttEstimator rtt() {
            return rtt;
        }

        RateEstimator re() {
            return re;
        }

        long rttAt() {
            return rttAt;
        }

        long elicitEnd() {
            return elicitEnd;
        }
    }
}
