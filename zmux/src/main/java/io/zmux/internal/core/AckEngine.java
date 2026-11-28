package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.util.ArrayList;
import java.util.List;

/**
 * Pipe acknowledgement of a session with acknowledgement:
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
        void rttSampled(Pipe pipe, long min);

        /** A lane's acknowledged position advanced; cum is whether the pipe's cumulative point moved. */
        void advanced(Pipe p, Pipe.OutLane ol, boolean cum);

        /** A fresh raw interval is available for classification by the session owner. */
        default void deliverySampled(Pipe pipe, long start, long end, boolean constrained) {}

        /** Whether the multipath session is quiet: its RTT samples stay out of min_rtt. */
        boolean quiet();
    }

    private final Port port;
    private final Pacer pacer;
    private final List<Pipe> pipes;
    private final long threshold;
    private final long delay;
    private final long peerDelay;
    private boolean immediate;
    // An ack-eliciting record arrived on an inbound pipe without a PIPE_ACK during the preceding
    // zmux_max_ack_delay: acknowledged at once, so that a sparse record is not held back by the delay
    // and the peer's RTT samples stay free of it.
    private boolean prompt;
    private boolean woken;
    private final TimerHeap pending = new TimerHeap();
    private int thresholdPipes;
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

    /** zmux_max_ack_delay of this endpoint. */
    long delay() {
        return delay;
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
     * only.
     */
    boolean onRecord(Pipe p, Pipe.InLane il, long n, boolean elicit, boolean atOnce, long now) {
        if (elicit) {
            if (il.ack.elicit == 0) pending.schedule(il.ack, Time.add(now, delay));
            il.ack.elicit += n;
            InPipeAck a = p.in().ack;
            if (a.pending < threshold) {
                a.pending = Math.min(threshold, a.pending + n);
                if (a.pending == threshold) thresholdPipes++;
            }
            if (!a.acked || Time.sub(now, a.ackedAt) >= delay) prompt = true;
        }
        if (atOnce) immediate = true;
        return due(now);
    }

    /** Whether a PIPE_ACK must be sent now. */
    boolean due(long now) {
        long first = pending.earliest();
        return immediate || prompt || thresholdPipes != 0 || (first != Time.NEVER && now >= first);
    }

    /** The earliest acknowledgement delay deadline. */
    long deadline() {
        return woken ? Time.NEVER : release();
    }

    /**
     * The latest time the pending acknowledgement may be sent: zmux_max_ack_delay after the first ack-eliciting byte it
     * covers (MULTIPATH.md Section 6.3), the one bound a shaping delay of the record carrying it never passes;
     * {@link Time#NEVER} when none is pending.
     */
    long release() {
        return pending.earliest();
    }

    boolean laneEnded(Pipe.InLane lane) {
        if (lane.rx.pos() <= lane.ack.reported) return false;
        ackNow();
        return true;
    }

    void pipeRetired(Pipe pipe) {
        if (pipe.in() == null) return;
        if (pipe.in().ack.pending == threshold) thresholdPipes--;
        pipe.in().ack.pending = 0;
        for (Pipe.InLane lane : pipe.in().lanes) pending.remove(lane.ack);
    }

    void clearTimers() {
        pending.clear();
    }

    /** Runs the delay timer; true when writers must wake to send a due PIPE_ACK. */
    boolean onTimeout(long now) {
        if (!due(now)) return false;
        woken = true;
        return true;
    }

    /**
     * Writes a PIPE_ACK snapshot when one is due, or when unreported positions can ride along with the credit frames
     * of the same write (credit: MAX_DATA or MAX_STREAM_DATA is pending).
     */
    void write(RecordWriter w, long now, boolean credit) {
        if (due(now) || (credit && unreported())) snapshot(w, now, true);
    }

    /** Whether an inbound lane holds a position not yet reported. */
    boolean unreported() {
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            for (Pipe.InLane il : p.in().lanes) if (il.rx.pos() > il.ack.reported) return true;
        }
        return false;
    }

    /** Writes a snapshot that answers what is due on the primary pipe. */
    boolean snapshot(RecordWriter w, long now) {
        return snapshot(w, now, true);
    }

    /**
     * Writes the positions of every inbound pipe and lane above 0, split over frames when needed.
     * settles: the snapshot answers what is due (the primary pipe's acknowledgement); a copy on another pipe (the
     * first record of a pipe, the answer to a PIPE_PING on its pipe pair) leaves that obligation in place. Returns
     * false when the record had no room for all of it.
     */
    boolean snapshot(RecordWriter w, long now, boolean settles) {
        ArrayList<long[]> es = entries;
        es.clear();
        for (Pipe p : pipes) {
            if (p.in() == null || (p.mp() != null && p.mp().health.retired())) continue;
            for (Pipe.InLane il : p.in().lanes) {
                if (il.rx.pos() == 0) continue;
                long d = Math.max(Time.sub(now, il.rx.posAt()) / Time.MICROSECOND, 0);
                es.add(new long[] {p.id, il.lane, il.rx.pos(), d});
            }
        }
        if (es.isEmpty()) {
            if (settles) {
                immediate = false;
                prompt = false;
                woken = false;
            }
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
            if (!w.controlFrame(n, false, true, bb -> {
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
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            p.in().ack.ackedAt = now;
            p.in().ack.acked = true;
        }
        if (!settles) return true;
        immediate = false;
        prompt = false;
        woken = false;
        pending.clear();
        thresholdPipes = 0;
        for (Pipe p : pipes) {
            if (p.in() == null) continue;
            p.in().ack.pending = 0;
            for (Pipe.InLane il : p.in().lanes) {
                il.ack.elicit = 0;
                il.ack.reported = il.rx.pos();
            }
        }
        return true;
    }

    /** Stops normal writes before the ACK history is full, leaving one progress record on lane zero. */
    boolean historyHeld(Pipe p) {
        if (p.out().snd.samples.size() >= FrameHistory.SLOTS - 1) return true;
        for (Pipe.OutLane lane : p.out().lanes) {
            if (lane.segs.size() >= FrameHistory.SLOTS - 1 || lane.activity.size() >= FrameHistory.SLOTS - 1) return true;
        }
        return false;
    }

    boolean canMarkActivity(Pipe.OutLane lane) { return lane.activity.size() < FrameHistory.SLOTS; }

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
        if (p.mp() != null) {
            for (int i = 0; i < w.deliveryEnds.size(); i++) {
                ps.deliveryEnds.addBounded(so + w.deliveryEnds.get(i), FrameHistory.SLOTS);
            }
        }
        if (p.mp() != null && (w.deliveryConstrained() || Pacer.paced(p) || ps.re.bound() > 0
                || (p.shape() != null && p.shape().active()))) ps.constrain(so, ps.so);
        if (w.anyElicit()) {
            long end = w.framing() == Framing.RECORDS ? ps.so : so + w.elicitEnd();
            if (ps.samples.isEmpty()) {
                ps.deliveredAt = now;
                ps.firstSentAt = now;
            }
            ps.elicitEnd = Math.max(ps.elicitEnd, end);
            boolean bubble = ps.re.bubbleInFlight(ps.cum);
            var sample = new Pipe.Sample(end, now, ps.delivered, ps.deliveredAt, ps.firstSentAt,
                    ps.re.blockEpoch(), bubble, Math.max(0, so - ps.cum), queue, w.suspPing());
            // Reserve the oldest unacknowledged samples for stall timing. Emergency
            // control may replace the last estimator sample while data stays held.
            if (ps.samples.size() == FrameHistory.SLOTS) ps.samples.set(FrameHistory.SLOTS - 1, sample);
            else ps.samples.add(sample);
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

    /** The PIPE_ACK entries of a single-pipe session; p0 is pipe 0 or null. */
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
        int covered = 0;
        while (covered < ps.deliveryEnds.size() && ps.deliveryEnds.get(covered) <= cum) covered++;
        if (covered > 0) {
            ps.ackedElicitEnd = ps.deliveryEnds.get(covered - 1);
            ps.deliveryEnds.removeFirst(covered);
        }
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
            boolean late = ad > peerDelay && ps.rttAt != 0 && Time.sub(now, ps.rttAt) < Time.SECOND;
            RttEstimator r = ps.rtt;
            // Queue-proven: the range's adjusted RTT exceeded min_rtt by at least 4 x min_rtt, so the
            // path held it back and was busy throughout (a deep carrier or path buffer that never
            // refuses bytes). Its rate sample is a bottleneck sample, also for bytes sent before
            // the pipe became SUSPECT; its RTT sample is skipped there all the same. It must also have
            // been sent behind a backlog of the pipe that takes at least 4 x min_rtt at R_p: a smaller
            // one (none, or a few control bytes) does not explain the delay, which a rise of the
            // propagation delay alone gives as well, and its sample of a few bytes over an idle
            // interval would replace R_p with a few hundred bytes a second.
            long minRtt = r.min();
            boolean proven = minRtt > 0 && Time.sub(rtt, Math.min(ad, peerDelay)) - minRtt >= Time.mul(minRtt, 4)
                    && last.backlog() >= ps.re.value() * Time.mul(minRtt, 4) / 1e9;
            boolean sampleOk = p.mp() == null || p.mp().sampleOk(last.sent());
            boolean rateOk = sampleOk || (proven && p.mp().health.out() != Attach.OUT_FAILED);
            if (!late && (sampleOk || (last.probe() && ps.paced(i, last, now)))) {
                sampleRtt = r.update(rtt, Math.min(ad, peerDelay), peerDelay, now, p.mp() != null && port.quiet());
                sampleSentAt = last.sent();
                ps.rttAt = now;
                ps.rttSampleEnd = last.end();
                port.rttSampled(p, r.min());
            }
            long iv = Math.max(Time.sub(last.sent(), last.firstSentAt()), Time.sub(now, last.deliveredAt()));
            if (iv > 0 && rateOk) {
                if (last.deliveredAt() < now) {
                    ps.deliveryStart = last.deliveredAt();
                    ps.deliveryEnd = now;
                    ps.deliveryBytes = ps.delivered - last.delivered();
                    ps.deliveryLoad = last.bubble() && !proven ? PathEvidence.Load.LIMITED
                            : PathEvidence.Load.UNKNOWN;
                    boolean constrained = ps.constrained(last.delivered(), cum);
                    port.deliverySampled(p, ps.deliveryStart, now, constrained);
                }
                sampleRate = ps.re.sample((ps.delivered - last.delivered()) * 1e9 / iv, last.bubble(), last.epoch(), proven, now);
            }
            ps.firstSentAt = last.sent();
            ps.prevSent = last.sent();
            ps.prevAckAt = now;
            ps.prevOk = true;
        }
        ps.samples.subList(0, i).clear();
        long oldest = ps.samples.isEmpty() ? ps.delivered : ps.samples.getFirst().delivered();
        while (!ps.constrained.isEmpty() && ps.constrained.getFirst().end() <= oldest) ps.constrained.removeFirst();
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
    static final class InAck extends TimerHeap.Entry {
        private long elicit;
        private long reported;

        InAck() {
        }

        long elicit() {
            return elicit;
        }

        long reported() {
            return reported;
        }
    }

    /**
     * The last PIPE_ACK sent while an inbound pipe existed (acked: one was), for the immediate acknowledgement of an
     * ack-eliciting record after zmux_max_ack_delay without one.
     */
    static final class InPipeAck {
        private long pending;
        private long ackedAt;
        private boolean acked;

        InPipeAck() {
        }
    }

    /** The sender's view of an outbound pipe, counted in send order across its lanes. */
    static final class Send {
        private long so;
        private long cum;
        private ArrayList<Pipe.Sample> samples = new ArrayList<>(0);
        private long delivered;
        private record Constrained(long start, long end) {}
        private final java.util.ArrayDeque<Constrained> constrained = new java.util.ArrayDeque<>();
        final QualityEvidence qualityEvidence = new QualityEvidence();
        private Session.DeliveryBacklog deliveryBacklog;

        Session.DeliveryBacklog deliveryBacklog() { return deliveryBacklog; }
        void deliveryBacklog(Session.DeliveryBacklog next) { deliveryBacklog = next; }
        private void constrain(long start, long end) {
            var tail = constrained.peekLast();
            if (tail != null && tail.end() == start) {
                constrained.removeLast();
                start = tail.start();
            }
            constrained.addLast(new Constrained(start, end));
        }

        private boolean constrained(long start, long end) {
            for (var range : constrained) {
                if (range.start() >= end) break;
                if (range.end() > start) return true;
            }
            return false;
        }

        private long deliveryStart, deliveryEnd, deliveryBytes;
        private PathEvidence.Load deliveryLoad = PathEvidence.Load.UNKNOWN;

        void backlogged() { deliveryLoad = PathEvidence.Load.BACKLOGGED; }
        private long deliveredAt;
        private long firstSentAt;
        private RttEstimator rtt = new RttEstimator();
        private final RateEstimator re = new RateEstimator();
        private final LongList deliveryEnds = new LongList();
        private long ackedElicitEnd, rttSampleEnd;
        long ackedElicitEnd() { return ackedElicitEnd; }
        long rttSampleEnd() { return rttSampleEnd; }

        private long rttAt;
        private long elicitEnd;
        /** The send time of the last batch an earlier advance acknowledged, and when that advance arrived (paced). */
        private long prevSent, prevAckAt;
        private boolean prevOk;

        /**
         * Whether the acknowledgement arriving now of last, the i-th and last sample it covers and a suspicion's
         * PIPE_PING, came apart from that of the bytes acknowledged before it by at least half the time between their
         * sending: the path delivered them at their own pace, as on a path whose round trip exceeds T_suspect, and the
         * sample measures that path. Released together after an outage (in one advance, or a gap that shrank to
         * nothing), the sample would measure the outage, which no sample does.
         */
        boolean paced(int i, Pipe.Sample last, long now) {
            if (i >= 2 || !prevOk) return false; // acknowledged together with earlier bytes, or nothing to compare
            return Time.sub(now, prevAckAt) >= Time.sub(last.sent(), prevSent) / 2;
        }

        Send() {
        }

        long so() {
            return so;
        }

        long cum() {
            return cum;
        }

        void clearHistory() {
            samples = new ArrayList<>(0);
            deliveryEnds.release();
        }

        ArrayList<Pipe.Sample> samples() {
            return samples;
        }

        long delivered() {
            return delivered;
        }

        PathEvidence quality(io.zmux.DirectionHealth health, long now) {
            var since = health.stableSince();
            if (since.isEmpty()) return new PathEvidence(health.evidenceGeneration(),
                    java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty());
            long stableSince = since.getAsLong();
            var current = timing(rtt.sampledAt(), rtt.smoothed(), 30 * Time.SECOND, stableSince, now);
            var baseline = timing(rtt.minAt(), rtt.min(), 300 * Time.SECOND, stableSince, now);
            long expires = Time.add(deliveryEnd, RateEstimator.WINDOW);
            var delivery = deliveryStart >= stableSince && deliveryStart < deliveryEnd && now < expires
                    ? java.util.Optional.of(new PathEvidence.Delivery(deliveryStart, deliveryEnd, deliveryBytes,
                            deliveryBytes * 1e9 / (deliveryEnd - deliveryStart), expires,
                            deliveryLoad))
                    : java.util.Optional.<PathEvidence.Delivery>empty();
            return new PathEvidence(health.evidenceGeneration(), current, baseline, delivery);
        }

        private java.util.Optional<PathEvidence.Timing> timing(long at, long value, long validity, long stableSince, long now) {
            long expires = Time.add(at, validity);
            return rtt.has() && at >= stableSince && now < expires
                    ? java.util.Optional.of(new PathEvidence.Timing(at, expires, java.time.Duration.ofNanos(value)))
                    : java.util.Optional.empty();
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

        /** Takes r as the direction's estimator (the pipe's attachment's): one estimator per pipe. */
        void useRtt(RttEstimator r) {
            rtt = r;
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
