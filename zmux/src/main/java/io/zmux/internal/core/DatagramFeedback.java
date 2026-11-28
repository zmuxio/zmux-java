package io.zmux.internal.core;

import io.zmux.internal.core.Session.DatagramLossReport;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.VarInt;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.LongUnaryOperator;

/** Feedback owned by the session loop. Keys name the forward pipe and carriage. */
final class DatagramFeedback {
    private static final long AGE = 60 * Time.SECOND;
    private static final long EVIDENCE_AGE = 10 * Time.SECOND;
    private static final long ADAPTIVE_LATCH = 100 * Time.MILLISECOND;
    private static final long UNKNOWN = Long.MIN_VALUE;
    private static final long SPACING = 25 * Time.MILLISECOND;
    private static final int HISTORY = CoreConfig.DEFAULT_DATAGRAM_WINDOW;
    private static final int REPORT_RANGES = 32;

    record Path(long pipe, int channel) {
        Path(long pipe, boolean nativePath) { this(pipe, nativePath ? 1 : 0); }
    }
    private record Latch(Path alternative, Impairments impairments) {}
    record AdaptivePair(Path original, Path alternative, long originalRevision, long alternativeRevision,
                        boolean urgent) {
        boolean nativePath() { return alternative.channel == 1; }
    }
    private record Impairments(long loss, long timing) {
        boolean active() { return loss != UNKNOWN || timing != UNKNOWN; }
    }
    private record Loss(long at, long finalized, long lost, long since, long alternativeSince) {
        boolean fresh(long now) { return finalized >= 32 && recent(at, now); }
        boolean qualifiesAsAlternative(long now) { return fresh(now) && compare(2) <= 0; }

        int compare(int percent) {
            int high = Long.compare(Math.multiplyHigh(lost, 100), Math.multiplyHigh(finalized, percent));
            return high != 0 ? high : Long.compareUnsigned(lost * 100, finalized * percent);
        }
    }

    private record Range(long first, long last) {}
    private static final class Inbound {
        final TreeMap<Long, Long> arrivals = new TreeMap<>();
        private long latest = -1;
        private long reportedAt = -1;
        private boolean pending;
    }
    private static final class Dispatch {
        final long at;
        private boolean pending;
        private long acknowledged = -1;
        private long ackDelay;
        Dispatch(long at, boolean pending) { this.at = at; this.pending = pending; }
    }

    private static final class Outbound {
        final long revision;
        final TreeMap<Long, Dispatch> sent = new TreeMap<>();
        private long acked;
        private long sampledAt = UNKNOWN;
        private long secondAt = UNKNOWN;
        private long thirdAt = UNKNOWN;
        private long timingSince;
        private long roundTrip;
        private long variation;
        // Zero means no provider. A registered generation with no loss sample is expired.
        private long lossGeneration;
        private Loss loss;
        private int samples;
        private long aged;
        private long forgotten;

        Outbound(long revision) { this.revision = revision; }

        boolean timingKnown(long now) { return recent(thirdAt, now); }
        long estimate() { return roundTrip + 4 * variation; }
    }

    private final int paths;
    private final LinkedHashMap<Path, Inbound> inbound = new LinkedHashMap<>();
    private final LinkedHashMap<Path, Outbound> outbound = new LinkedHashMap<>();
    private final LinkedHashMap<Path, Latch> impairments = new LinkedHashMap<>();
    private final ArrayDeque<Long> reports = new ArrayDeque<>();
    private int pendingReports;
    private long revision;
    private long rejectedPaths;

    DatagramFeedback(int attachmentLimit) {
        paths = (int) Math.min(Integer.MAX_VALUE, 2L * (attachmentLimit + 1L) * 2);
    }

    private Outbound outgoing(Path path) {
        Outbound out = outbound.get(path);
        if (out != null) return out;
        // retire() reclaims departed paths before admission can hit this limit.
        if (outbound.size() >= paths) {
            rejectedPaths++;
            return null;
        }
        out = new Outbound(++revision);
        outbound.put(path, out);
        return out;
    }

    long rejectedPaths() { return rejectedPaths; }

    void retire(long pipe) {
        var record = new Path(pipe, false);
        var nativePath = new Path(pipe, true);
        outbound.remove(record);
        outbound.remove(nativePath);
        clearPairings(record);
        clearPairings(nativePath);
    }

    private void clearPairings(Path path) {
        impairments.entrySet().removeIf(pair -> pair.getKey().equals(path) || pair.getValue().alternative.equals(path));
    }

    void clearPairings(long first, long second) {
        impairments.entrySet().removeIf(pair -> {
            long original = pair.getKey().pipe, alternative = pair.getValue().alternative.pipe;
            return (original == first && alternative == second) || (original == second && alternative == first);
        });
    }

    void resetOutbound(Path path) {
        clearPairings(path);
        Outbound out = new Outbound(++revision);
        Outbound old = outbound.replace(path, out);
        if (old != null) out.lossGeneration = old.lossGeneration;
    }

    AdaptivePair bindPair(Path original, Path alternative, boolean urgent) {
        Outbound a = outgoing(original);
        if (a == null) return null;
        Outbound b = outgoing(alternative);
        return b == null ? null : new AdaptivePair(original, alternative, a.revision, b.revision, urgent);
    }

    boolean current(AdaptivePair pair, long now) {
        Outbound a = outbound.get(pair.original), b = outbound.get(pair.alternative);
        return a != null && b != null && a.revision == pair.originalRevision && b.revision == pair.alternativeRevision
                && (pair.urgent || evaluate(pair.original, pair.alternative, now).active());
    }

    DatagramLossReport reportLoss(Path path, long observed, long generation, long finalized, long lost, long now) {
        if (generation <= 0) return DatagramLossReport.IGNORED;
        Outbound out = outbound.get(path);
        long previous = out != null ? out.lossGeneration : 0;
        Loss prior = out != null ? out.loss : null;
        if (generation == previous && prior == null) return DatagramLossReport.SOURCE_EXPIRED;
        if (generation < previous || !recent(observed, now)
                || (generation == previous && observed <= prior.at)) return DatagramLossReport.IGNORED;
        if (previous != 0 && generation > previous) {
            resetOutbound(path);
            out = outbound.get(path);
            prior = null;
        } else if (out == null) {
            out = outgoing(path);
            if (out == null) return DatagramLossReport.IGNORED;
        }
        out.lossGeneration = generation;
        out.loss = new Loss(observed, finalized, lost, prior != null && prior.fresh(observed) ? prior.since : observed,
                prior != null && prior.qualifiesAsAlternative(observed) ? prior.alternativeSince : observed);
        return DatagramLossReport.ACCEPTED;
    }

    boolean impairment(Path original, Path alternative, long now) {
        var result = evaluate(original, alternative, now);
        if (result.active()) {
            impairments.put(original, new Latch(alternative, result));
        } else {
            var prior = impairments.get(original);
            if (prior != null && prior.alternative.equals(alternative)) clearImpairment(original);
        }
        return result.active();
    }

    void clearImpairment(Path original) { impairments.remove(original); }

    private Impairments evaluate(Path original, Path alternative, long now) {
        Outbound a = outbound.get(original), b = outbound.get(alternative);
        long loss = UNKNOWN, timing = UNKNOWN;
        if (a != null && b != null) {
            var latch = impairments.get(original);
            var prior = latch != null && latch.alternative.equals(alternative) ? latch.impairments : null;
            if (a.loss != null && b.loss != null && a.loss.fresh(now) && b.loss.qualifiesAsAlternative(now)) {
                loss = latch(prior == null ? UNKNOWN : prior.loss, now, Math.max(a.loss.since, b.loss.alternativeSince),
                        a.loss.compare(5) >= 0, a.loss.compare(2) > 0);
            }
            if (a.timingKnown(now) && b.timingKnown(now)) {
                timing = latch(prior == null ? UNKNOWN : prior.timing, now, Math.max(a.timingSince, b.timingSince),
                        2 * a.estimate() > 3 * b.estimate(), 4 * a.estimate() > 5 * b.estimate());
            }
        }
        return new Impairments(loss, timing);
    }

    private static long latch(long entry, long now, long since, boolean threshold, boolean retained) {
        if (entry != UNKNOWN && entry >= since && (now - entry < ADAPTIVE_LATCH || retained)) return entry;
        return threshold ? now : UNKNOWN;
    }

    private static boolean recent(long at, long now) {
        return at != UNKNOWN && at <= now && Time.sub(now, at) < EVIDENCE_AGE;
    }

    void arrived(long pipe, boolean channel, long sequence, long at) {
        Path key = new Path(pipe, channel);
        Inbound in = inbound.get(key);
        if (in == null) {
            if (inbound.size() >= paths && inbound.pollFirstEntry().getValue().pending) pendingReports--;
            in = new Inbound();
            inbound.put(key, in);
        }
        if (in.arrivals.putIfAbsent(sequence, at) != null) return;
        if (in.arrivals.size() > HISTORY) in.arrivals.pollFirstEntry();
        in.latest = at;
        if (!in.pending) {
            in.pending = true;
            pendingReports++;
        }
    }

    void dispatched(long pipe, boolean channel, long sequence, long at) {
        Outbound out = outgoing(new Path(pipe, channel));
        if (out == null) return;
        out.sent.putIfAbsent(sequence, new Dispatch(at, channel));
        if (out.sent.size() > HISTORY) {
            long droppedAt = out.sent.pollFirstEntry().getValue().at;
            if (at - droppedAt >= AGE) out.aged++;
            else out.forgotten++;
        }
    }

    void completed(long pipe, long sequence, boolean accepted) {
        Outbound out = outbound.get(new Path(pipe, 1));
        if (out == null) return;
        Dispatch dispatch = out.sent.get(sequence);
        if (dispatch == null || !dispatch.pending) return;
        dispatch.pending = false;
        if (!accepted || dispatch.acknowledged >= 0) out.sent.remove(sequence);
        if (accepted && dispatch.acknowledged >= 0) observe(out, dispatch, dispatch.acknowledged, dispatch.ackDelay);
    }

    void acknowledge(Frame frame, long now) {
        Outbound out = outbound.get(new Path(frame.pipeId, frame.datagramChannel));
        if (out == null || out.sent.isEmpty()) return;
        long high = frame.value, low = high - frame.firstDatagramRange;
        int i = frame.entriesPos;
        for (;;) {
            NavigableMap<Long, Dispatch> covered = out.sent.subMap(low, true, high, true);
            for (var entries = covered.entrySet().iterator(); entries.hasNext(); ) {
                var copy = entries.next();
                Dispatch dispatch = copy.getValue();
                long delay = copy.getKey() == frame.value ? frame.datagramAckDelay : -1;
                if (dispatch.pending) {
                    if (dispatch.acknowledged < 0) {
                        dispatch.acknowledged = now;
                        dispatch.ackDelay = delay;
                    }
                } else {
                    observe(out, dispatch, now, delay);
                    entries.remove();
                }
            }
            if (i == frame.entriesEnd || out.sent.isEmpty()) return;
            long gap = frame.varint(i, frame.entriesEnd);
            i += VarInt.lengthOf(frame.get(i));
            long len = frame.varint(i, frame.entriesEnd);
            i += VarInt.lengthOf(frame.get(i));
            high = low - gap - 2;
            low = high - len;
        }
    }

    private static void observe(Outbound out, Dispatch dispatch, long now, long delay) {
        long age = now - dispatch.at;
        if (age < 0 || age >= AGE) {
            out.aged++;
            return;
        }
        out.acked++;
        long elapsed = age / Time.MICROSECOND;
        if (age >= EVIDENCE_AGE || delay < 0 || delay >= elapsed || now < out.sampledAt) return;
        long sample = elapsed - delay;
        if (!out.timingKnown(now)) out.timingSince = now;
        if (recent(out.sampledAt, now)) {
            out.variation += (Math.abs(out.roundTrip - sample) - out.variation) / 4;
            out.roundTrip += (sample - out.roundTrip) / 8;
        } else {
            out.roundTrip = sample;
            out.variation = sample / 2;
            out.samples = 0;
            out.sampledAt = out.secondAt = UNKNOWN;
        }
        out.thirdAt = out.secondAt;
        out.secondAt = out.sampledAt;
        out.sampledAt = now;
        if (out.samples < Integer.MAX_VALUE) out.samples++;
    }

    Datagrams.FeedbackSnapshot snapshot(long pipe, boolean nativePath, long now) {
        Outbound out = outbound.get(new Path(pipe, nativePath));
        if (out == null) return null;
        boolean recent = recent(out.sampledAt, now);
        int pending = 0;
        for (Dispatch dispatch : out.sent.values()) if (now - dispatch.at < AGE) pending++;
        return new Datagrams.FeedbackSnapshot(out.acked, pending, out.aged + out.sent.size() - pending,
                out.forgotten, recent ? out.samples : 0, recent ? out.roundTrip * Time.MICROSECOND : 0,
                recent ? now - out.sampledAt : 0);
    }

    boolean reportPending() { return pendingReports != 0; }

    private long nextAllowed(long now, int liveInbound) {
        while (!reports.isEmpty() && reports.peekFirst() <= now - Time.SECOND) reports.removeFirst();
        if (liveInbound <= 0) return Time.NEVER;
        return reports.size() < 2L * liveInbound ? now : Time.add(reports.peekFirst(), Time.SECOND);
    }

    private static long ready(Path key, Inbound in, LongUnaryOperator rtt) {
        return in.reportedAt < 0 ? 0 : Time.add(in.reportedAt, Math.max(SPACING, rtt.applyAsLong(key.pipe) / 4));
    }

    long next(long now, int liveInbound, LongUnaryOperator rtt) {
        if (!reportPending()) return Time.NEVER;
        long available = nextAllowed(now, liveInbound);
        long next = Time.NEVER;
        for (var slot : inbound.entrySet()) {
            Inbound in = slot.getValue();
            if (!in.pending) continue;
            if (now - in.latest >= AGE) {
                done(in);
            } else {
                long expires = Time.add(in.latest, AGE);
                // Expiry still wakes the owner when feedback cannot be sent before it expires.
                long sendAt = Math.max(available, ready(slot.getKey(), in, rtt));
                next = Math.min(next, Math.min(expires, sendAt));
            }
        }
        return next;
    }

    void write(RecordWriter writer, long now, int liveInbound, LongUnaryOperator rtt) {
        if (next(now, liveInbound, rtt) > now) return;
        for (var slot : inbound.entrySet()) {
            Path key = slot.getKey();
            Inbound in = slot.getValue();
            if (!in.pending || ready(key, in, rtt) > now) continue;
            in.arrivals.values().removeIf(at -> now - at >= AGE);
            if (in.arrivals.isEmpty()) { done(in); continue; }
            if (!writeOne(writer, key, in, now)) return;
            in.reportedAt = now;
            done(in);
            reports.addLast(now);
            if (nextAllowed(now, liveInbound) > now) return;
        }
    }

    private void done(Inbound in) {
        in.pending = false;
        pendingReports--;
    }

    private static boolean writeOne(RecordWriter writer, Path key, Inbound in, long now) {
        var descending = in.arrivals.descendingKeySet().iterator();
        long largest = descending.next(), high = largest, low = largest;
        var ranges = new ArrayDeque<Range>();
        while (descending.hasNext()) {
            long next = descending.next();
            if (low - next == 1) {
                low = next;
            } else {
                ranges.addLast(new Range(low, high));
                if (ranges.size() == REPORT_RANGES) break;
                high = low = next;
            }
        }
        if (ranges.size() < REPORT_RANGES) ranges.addLast(new Range(low, high));
        long delay = (now - in.arrivals.get(largest)) / 1000;
        ByteBuffer frame = ByteBuffer.allocate(4 + 1 + 5 * 8 + REPORT_RANGES * 16);
        Range first = ranges.removeFirst(), previous = first;
        FrameEncoder.datagramAckHeader(frame, key.pipe, key.channel, largest, delay, ranges.size(), first.last - first.first);
        for (Range range : ranges) {
            VarInt.put(frame, previous.first - range.last - 2);
            VarInt.put(frame, range.last - range.first);
            previous = range;
        }
        frame.flip();
        return writer.frame(frame.remaining(), false, dst -> dst.put(frame));
    }

    void clear() {
        inbound.clear();
        outbound.clear();
        impairments.clear();
        reports.clear();
        pendingReports = 0;
    }
}
