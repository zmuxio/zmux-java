package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.shaping.RuntimeReason;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Builds records into a {@link Batch}. A STREAM or DATAGRAM frame stays pending
 * until the next frame or the end of its record, so the LEN field of the last
 * frame of a record is omitted.
 */
final class RecordWriter {
    /** Coalesces small outgoing segments with the encoded headers for vectored writes. */
    private static final int INLINE_PAYLOAD_LIMIT = 4096;

    private Batch b;
    Batch b() { return b; }
    private Framing framing;
    Framing framing() { return framing; }
    private int recMax;
    int recMax() { return recMax; }
    /** The shaped pipe's state; null: not shaped. */
    private Shaping.PipeShape shape;
    private Shaping.PipeShape stopShape;
    void stopAfterCommit(Shaping.PipeShape ps) { stopShape = ps; }
    void committed() { if (stopShape != null) stopShape.stop(true); }
    Shaping.PipeShape shape() { return shape; }
    /** Backs PADDING frames (0x00 bytes); never written to. */
    private static final ByteBuffer ZERO_PAD = ByteBuffer.allocateDirect(65535).asReadOnlyBuffer();
    private int budget;
    private int recordWorkspace;
    private int codecEntries;
    int budget() { return budget; }
    private boolean open;
    private int sizeSeg;
    private int recBytes;
    private int own;
    private boolean data, control, prefix;
    private int added, coverStart, coverLength;
    private int bareFrames, lengthStart, lengthBytes, paddingStart, paddingBytes;
    private boolean mergeCover;
    private CoverResult coverResult;

    record CoverResult(Shaping.PipeShape shape, long id, int bytes, int cost, boolean standalone, RuntimeReason rejection) {}

    int added() { return added; }
    CoverResult coverResult() { return coverResult; }
    boolean canCover() { return !closed && !held && !prefix && !noHold; }
    private int recs;
    int recs() { return recs; }
    private boolean elicit;
    private boolean anyElicit;
    boolean anyElicit() { return anyElicit; }
    private long deliveryPosition;
    final LongList deliveryEnds = new LongList();
    private int elicitEnd;
    int elicitEnd() { return elicitEnd; }
    private boolean activity;
    boolean activity() { return activity; }
    /** The batch carries a traffic frame: STREAM, RESET_STREAM(_AT), STOP_SENDING or DATAGRAM. */
    private boolean traffic;
    boolean traffic() { return traffic; }
    private boolean onlyPing;
    boolean onlyPing() { return onlyPing; }
    private boolean closed;
    boolean closed() { return closed; }
    /** The planned record waits for its delay: no frame goes into it. */
    private boolean held;
    /** The batch carries frames that no delay holds. */
    private boolean noHold;
    private boolean laneOnly;
    boolean laneOnly() { return laneOnly; }
    final ArrayList<Stream> touched = new ArrayList<>(0);
    private boolean deliveryConstrained;
    void constrainDelivery() { deliveryConstrained = true; }
    boolean deliveryConstrained() { return deliveryConstrained; }
    private boolean newData;
    private long dataSpent;
    long dataSpent() { return dataSpent; }
    /** Stream data the limiter lets through; negative: no limit. */
    private long dataRoom;
    long dataRoom() { return dataRoom; }
    /** Multipath: log the frames for retention. */
    private boolean retain;
    private int historySlots;
    final ArrayList<FrameDesc> descs = new ArrayList<>(0);
    final ArrayList<Integer> descRecs = new ArrayList<>(0);
    private Pipe pipe;
    Pipe pipe() { return pipe; }
    final ArrayList<Pipe> abandonsSent = new ArrayList<>(0);
    private boolean revalPing;
    boolean revalPing() { return revalPing; }
    /** The batch holds a PIPE_PING. */
    private boolean pingSent;
    boolean pingSent() { return pingSent; }
    /** The batch's PIPE_PING is that of a suspicion (MULTIPATH.md Section 7.2 step 3). */
    private boolean suspPing;
    boolean suspPing() { return suspPing; }
    /** The epoch of a SESSION_EPOCH answer in the batch, 0 for none. */
    private long epochAnswer;
    long epochAnswer() { return epochAnswer; }
    void epochAnswer(long n) { epochAnswer = n; }

    // The pending STREAM or DATAGRAM frame.
    private boolean pendOn;
    private boolean pendDatagram;
    private long pendId;
    private long pendOff;
    private boolean pendFin, pendFinOnly;
    private int pendN;
    private final ArrayList<ByteBuffer> pendData = new ArrayList<>(2);
    private final ArrayList<Pin> pendPins = new ArrayList<>(2);

    void init(Batch b, Framing framing, int recMax, int budget) {
        this.b = b;
        codecEntries = b.codecEntries();
        this.framing = framing;
        this.recMax = recMax;
        this.budget = budget;
        recordWorkspace = 0;
        open = false;
        sizeSeg = 0;
        recBytes = 0;
        recs = 0;
        elicit = anyElicit = activity = traffic = closed = laneOnly = newData = held = noHold = false;
        elicitEnd = 0;
        deliveryPosition = 0;
        deliveryEnds.clear();
        onlyPing = true;
        touched.clear();
        deliveryConstrained = false;
        dataSpent = 0;
        added = coverLength = 0;
        mergeCover = false;
        coverResult = null;
        dataRoom = -1;
        shape = stopShape = null;
        retain = false;
        historySlots = Integer.MAX_VALUE;
        descs.clear();
        descRecs.clear();
        pipe = null;
        abandonsSent.clear();
        revalPing = false;
        pingSent = false;
        suspPing = false;
        epochAnswer = 0;
        pingRequests = 0;
        pendOn = pendFinOnly = false;
        pendData.clear();
        pendPins.clear();
    }

    private int sizeCost() {
        return framing == Framing.RECORDS ? 0 : VarInt.length(recMax);
    }

    void chargeRecordWorkspace(int bytes) { recordWorkspace = bytes; }

    /** Batch room spent once per new record, independent of its wire size. */
    private int startCost() { return sizeCost() + recordWorkspace; }

    private int pendLen(boolean withLen) {
        if (!pendOn) return 0;
        if (pendDatagram) return 1 + pendN + (withLen ? VarInt.length(pendN) : 0);
        return FrameEncoder.streamHeaderLength(pendId, pendOff, pendN, withLen) + pendN;
    }

    /** Frame bytes that still fit in the open record (or a new one) within the batch budget. */
    int room() {
        if (closed || codecEntries == 0) return 0;
        if (shape != null && !open) prep();
        if (held) return 0;
        if (open) {
            int r = recMax - recBytes - pendLen(true);
            return Math.min(r, budget - pendLen(true));
        }
        return Math.min(recMax, budget - startCost());
    }

    private void prep() {
        held = false;
        if (!shape.prep(this)) shape = null;
    }

    private void begin() {
        if (open) return;
        if (shape != null) prep();
        open = true;
        budget -= recordWorkspace;
        recBytes = own = 0;
        bareFrames = -1;
        lengthBytes = paddingBytes = 0;
        data = control = prefix = false;
        elicit = false;
        sizeSeg = b.segmentCount();
        if (framing == Framing.BYTES) b.reserveSeg(); // filled at end()
    }

    /** Whether a frame of n bytes fits, header included. */
    boolean fits(int n) {
        if (closed || codecEntries == 0) return false;
        if (shape != null && !open) prep();
        if (held) return false;
        if (open) {
            int len = recBytes + pendLen(true) + n;
            return len <= recMax && pendLen(true) + n <= budget;
        }
        return n <= recMax && startCost() + n <= budget;
    }

    private boolean newRecordFits(int n) {
        return !closed && codecEntries != 0 && n <= recMax && startCost() + n <= budget - (open ? pendLen(false) : 0);
    }

    /** Appends a frame of n bytes written by f; false when it does not fit. */
    boolean frame(int n, boolean elicits, Consumer<ByteBuffer> f) {
        return controlFrame(n, elicits, false, f);
    }

    boolean copiedFrame(int n, boolean elicits, Consumer<ByteBuffer> f) {
        return controlFrame(n, elicits, true, f);
    }

    boolean controlFrame(int n, boolean elicits, boolean repeated, Consumer<ByteBuffer> f) {
        return frame(n, elicits, f, false, !repeated);
    }

    /** A frame that needs room in the retransmission history if multipath is in use. */
    boolean trackedFrame(int bytes, boolean repeated, Consumer<ByteBuffer> encode, FrameDesc desc) {
        if (!historyRoom() || !controlFrame(bytes, true, repeated, encode)) return false;
        logFrame(desc);
        return true;
    }

    boolean historyRoom() { return historySlots != 0; }

    boolean setupFrame(int n, boolean elicits, Consumer<ByteBuffer> f) {
        return frame(n, elicits, f, true, false);
    }

    private boolean frame(int n, boolean elicits, Consumer<ByteBuffer> f, boolean setup, boolean original) {
        // The record floor keeps every unsplittable frame within a record: one beyond it would wait forever.
        if (n > recMax) throw new IllegalStateException("a " + n + "-byte frame exceeds the " + recMax + "-byte record limit");
        if (!fits(n)) {
            if (!open || !newRecordFits(n)) return false;
            end();
            if (!fits(n)) return false;
        }
        codecEntries--;
        begin();
        flushPend(true);
        ByteBuffer a = b.arena(n);
        int p0 = a.position();
        f.accept(a);
        int l = a.position() - p0;
        b.arenaSeg(-1, p0);
        recBytes += l;
        budget -= l;
        if (elicits) elicit = true;
        prefix |= setup;
        if (original) {
            own += l;
            control |= shape != null && shape.wantsOutbound();
        }
        return true;
    }

    /** Adds STREAM data; a contiguous continuation of the pending frame joins it. */
    boolean stream(Stream stream, long off, SendQueue.Peek fragment, boolean fin, boolean original) {
        ByteBuffer data = fragment == null ? null : fragment.data();
        Pin pin = fragment == null ? Pin.NONE : fragment.pin();
        if (!stream(stream.tab.id(), off, data, pin, fin, original)) return false;
        if (fragment != null) fragment.attachReceipt(b);
        return true;
    }

    boolean stream(long id, long off, ByteBuffer data, Pin pn, boolean fin, boolean original) {
        if (!historyRoom() || codecEntries == 0) return false;
        int len = data == null ? 0 : data.remaining();
        if (open && pendOn && !pendDatagram && !pendFin && pendId == id && pendOff + pendN == off && len > 0) {
            int grow = FrameEncoder.streamHeaderLength(id, pendOff, pendN + len, true) + len
                    - FrameEncoder.streamHeaderLength(id, pendOff, pendN, true);
            int grown = recBytes + pendLen(true) + grow;
            if (grown <= recMax && pendLen(true) + grow <= budget) {
                codecEntries--;
                pendData.add(data);
                pendPins.add(pn);
                pendN += len;
                pendFin = fin;
                if (original) ownsStream(len, fin);
                if (!pn.none()) b.pin(pn);
                return true;
            }
        }
        int n = FrameEncoder.streamHeaderLength(id, off, len, true) + len;
        if (!fits(n)) {
            if (!open || !newRecordFits(n)) return false;
            end();
            if (!fits(n)) return false;
        }
        codecEntries--;
        begin();
        flushPend(true);
        pendOn = true;
        pendDatagram = false;
        pendId = id;
        pendOff = off;
        pendFin = fin;
        pendN = len;
        if (original) ownsStream(len, fin);
        pendData.add(data == null ? ByteBuffer.allocate(0) : data);
        pendPins.add(pn);
        if (!pn.none()) b.pin(pn);
        elicit = true;
        activity = traffic = true;
        onlyPing = false;
        return true;
    }

    /** The largest Size of this lane's records. */
    int recordLimit() {
        return recMax;
    }

    /** Adds a DATAGRAM frame (LEN omitted when it ends its record). */
    boolean datagram(byte[] data, int ownPayload, Datagrams.NativeWork item, Datagrams.SendFlow flow,
                     java.util.function.Predicate<Datagrams.NativeWork> eligible) {
        int n = 1 + VarInt.length(data.length) + data.length;
        if (item != null && !open && shape != null && newRecordFits(n)) shape.recordDatagram(this, item, flow, eligible);
        if (!fits(n)) {
            if (!open || !newRecordFits(n)) return false;
            end();
            if (item != null && shape != null) shape.recordDatagram(this, item, flow, eligible);
            if (!fits(n)) return false;
        }
        if (item != null && shape != null) shape.recordDatagramSent(item);
        codecEntries--;
        begin();
        flushPend(true);
        pendOn = true;
        pendDatagram = true;
        if (ownPayload >= 0) {
            own += ownPayload;
            this.data = true;
        }
        pendN = data.length;
        pendData.add(ByteBuffer.wrap(data));
        pendPins.add(Pin.NONE);
        elicit = true;
        activity = traffic = true;
        onlyPing = false;
        return true;
    }

    void chargeDatagram(CarrierCharge charge, int payloadLength) {
        b.charge(charge, 0, payloadLength);
    }

    private void flushPend(boolean withLen) {
        if (!pendOn) return;
        pendOn = false;
        int hl = pendDatagram ? 1 + (withLen ? VarInt.length(pendN) : 0)
                : FrameEncoder.streamHeaderLength(pendId, pendOff, pendN, withLen);
        if (pendFinOnly) own += hl - (withLen && bareFrames >= 0 ? lengthBytes : 0);
        pendFinOnly = false;
        ByteBuffer a = b.arena(hl);
        int p0 = a.position();
        if (pendDatagram) FrameEncoder.datagramHeader(a, pendN, withLen);
        else FrameEncoder.streamHeader(a, pendId, pendOff, pendN, pendFin, withLen);
        int lastArenaSeg = b.arenaSeg(-1, p0);
        for (int i = 0; i < pendData.size(); i++) {
            ByteBuffer d = pendData.get(i);
            Pin pn = pendPins.get(i);
            if (d.remaining() < INLINE_PAYLOAD_LIMIT || pn.none()) {
                // Small or owned pieces are copied: they extend the arena segment.
                a = b.arena(d.remaining());
                int q0 = a.position();
                a.put(d.duplicate());
                lastArenaSeg = b.arenaSeg(lastArenaSeg, q0);
                continue;
            }
            b.refSeg(d);
        }
        pendData.clear();
        pendPins.clear();
        int l = hl + pendN;
        recBytes += l;
        budget -= l;
    }

    /** Closes the open record and writes its Size. */
    void end() {
        if (!open) return;
        Shaping.PipeShape ps = shape;
        if (ps != null && ps.planned() && recBytes + pendLen(false) > 0) endShaped(ps);
        else flushPend(false);
        open = false;
        if (recBytes == 0) {
            budget += recordWorkspace;
            if (framing == Framing.BYTES) b.dropSegs(sizeSeg);
            return;
        }
        int head = sizeBytes(recBytes);
        int start = b.bytes() - recBytes;
        boolean standalone = coverLength > 0 && coverResult.standalone();
        if (standalone) b.standalone(recs);
        int baselineHead = standalone ? 0 : sizeBytes(bareFrames < 0 ? recBytes : bareFrames);
        b.extraSpan(start + baselineHead, head - baselineHead);
        b.extraSpan(head + lengthStart, lengthBytes);
        b.extraSpan(head + paddingStart, paddingBytes);
        b.extraSpan(head + coverStart, coverLength);
        int first = sizeSeg;
        if (framing == Framing.BYTES) {
            ByteBuffer a = b.arena(8);
            int p0 = a.position();
            VarInt.put(a, recBytes);
            b.fillSeg(sizeSeg, p0);
            budget -= a.position() - p0;
        }
        if (coverLength > 0) {
            b.cover(coverStart + sizeBytes(recBytes), coverLength, coverResult.shape(), coverResult.id());
            coverLength = 0;
        }
        b.addRecord(first, b.segmentCount(), recBytes);
        b.recordReceipt(own, data && !prefix, control && !prefix);
        recs++;
        deliveryPosition += Batch.positionBytes(recBytes);
        if (elicit) {
            if (pipe != null && pipe.mp() != null) deliveryEnds.addBounded(deliveryPosition, FrameHistory.SLOTS);
            anyElicit = true;
            elicitEnd = b.bytes();
        }
    }

    private void endShaped(Shaping.PipeShape ps) {
        int tail = pendLen(false);
        int bare = recBytes + tail;
        int full = recBytes + pendLen(true);
        bareFrames = bare;
        int target = ps.target(bare);
        long room = extraRoom(ps);
        int available = Math.min(maxFrames(), bare + (int) Math.min(recMax, room));
        if (ps.owner().noPadding()) available = bare;
        // Keep the existing record intact if the remaining budget has shrunk below its size.
        //noinspection MathClampMigration
        int padded = Math.max(bare, Math.min(target, available));
        while (padded > bare && cost(bare, padded) > room) padded--;
        if (padded < target) {
            var reason = ps.owner().noPadding() ? RuntimeReason.DISABLED
                    : cost(bare, Math.min(maxFrames(), target)) > room ? RuntimeReason.OVERHEAD_LIMIT : RuntimeReason.CLIPPED_ROOM;
            ps.owner().clip(reason);
        }
        added += cost(bare, padded);

        if (padded > full) {
            lengthBytes = full - bare;
            if (lengthBytes > 0) lengthStart = b.bytes() + pendLen(true) - pendN - lengthBytes;
            flushPend(true);
            paddingStart = b.bytes();
            paddingBytes = padded - full;
            pad(paddingBytes);
            if (mergeCover) merge(ps, padded);
        } else {
            paddingStart = b.bytes();
            paddingBytes = padded - bare;
            pad(paddingBytes);
            if (mergeCover) merge(ps, padded);
            flushPend(false);
        }
        ps.ended(this);
    }

    private int sizeBytes(int frames) { return framing == Framing.BYTES ? VarInt.length(frames) : 0; }

    private int cost(int before, int after) { return after + sizeBytes(after) - before - sizeBytes(before); }

    private long extraRoom(Shaping.PipeShape ps) {
        return Math.max(0, ps.owner().profileRoom() - added);
    }

    /** Exact record capacity with the remaining batch budget, including a length-width transition. */
    private int maxFrames() {
        int total = (open ? recBytes : -recordWorkspace) + budget;
        int fit = Math.min(recMax, total);
        while (fit > 0 && fit + sizeBytes(fit) > total) fit--;
        return fit;
    }

    private void pad(int n) {
        if (n <= 0) return;
        b.refSeg(ZERO_PAD.slice(0, n));
        recBytes += n;
        budget -= n;
    }

    boolean cover() {
        Shaping.PipeShape ps = shape;
        if (coverResult != null) return true;
        if (open && recBytes + pendLen(false) > 0 && ps.planned()) {
            mergeCover = true;
            end();
            mergeCover = false;
            if (coverResult != null || !ps.active()) return true;
        }
        end();
        if (closed || held || codecEntries == 0) return false;
        int frames = Math.min(ps.injectionLength(), maxFrames());
        if (frames < Math.min(ps.injectionLength(), recMax) && b.n() != 0) return false;
        int cost = frames + sizeBytes(frames) + ps.overHere(this);
        if (!acceptCover(ps, frames, cost, true)) return true;
        shape = null;
        codecEntries--;
        begin();
        coverPad(frames);
        end();
        shape = ps;
        return true;
    }

    private void merge(Shaping.PipeShape ps, int frames) {
        int padding = Math.clamp(maxFrames() - frames, 0, ps.injectionLength());
        if (padding == 0 && ps.owner().coverRejection(1, 0, Long.MAX_VALUE, false, 0) == null) return;
        if (acceptCover(ps, padding, cost(frames, frames + padding), false)) coverPad(padding);
    }

    private boolean acceptCover(Shaping.PipeShape ps, int frames, int cost, boolean standalone) {
        if (frames < ps.injectionLength()) ps.owner().clip(RuntimeReason.CLIPPED_ROOM);
        var reason = ps.owner().coverRejection(frames, cost, extraRoom(ps),
                standalone && !ps.owner().peerZmux(), pingRequests);
        long id = ps.injectionId();
        ps.clearInjection();
        if (frames < 0) throw new IllegalStateException("negative cover layout");
        coverResult = new CoverResult(ps, id, reason == null ? frames : 0, reason == null ? cost : 0, standalone, reason);
        if (reason != null) return false;
        added += cost;
        return true;
    }

    private void coverPad(int n) {
        coverStart = b.bytes();
        coverLength = n;
        pad(n);
    }

    /** The shaped pipe of this batch. */
    void shape(Shaping.PipeShape ps) { shape = ps; }

    /** The planned record waits for its delay. */
    void holdRecord() { held = !noHold; }

    /** The batch carries frames that no planned delay holds. */
    void neverHold() {
        noHold = true;
        held = false;
    }

    /** Bounds the data bytes of a STREAM frame, room, by the shaping gate. */
    int dataFit(int room) {
        return shape == null ? room : shape.dataFit(this, room);
    }

    /** Records a frame of the record being built (multipath retention only). */
    void logFrame(FrameDesc d) {
        if (!retain) return;
        if (!historyRoom()) throw new IllegalStateException("missing history slot");
        historySlots--;
        descs.add(d);
        descRecs.add(recs);
    }

    /** A frame that counts as activity and is not a PING request was written. */
    void wroteActivity() {
        activity = true;
        onlyPing = false;
    }

    /** A RESET_STREAM, RESET_STREAM_AT or STOP_SENDING frame was written. */
    void wroteTraffic() { traffic = true; }

    /** A PING request was written (activity; the batch may still be only PINGs). */
    void wrotePingRequest() {
        activity = true;
        pingRequests++;
    }

    /** QX_PING requests written in this batch. */
    private int pingRequests;
    int pingRequests() { return pingRequests; }

    /** CONNECTION_CLOSE was written: nothing more goes into this batch. */
    void close() {
        closed = true;
    }

    /** PIPE_LANE was written as the lane's only record. */
    void closeLaneOnly() {
        closed = true;
        laneOnly = true;
    }

    /** Whether this is the batch's first new stream data (and marks it written). */
    boolean firstNewData() {
        if (newData) return false;
        newData = true;
        return true;
    }

    /** Multipath: log the frames of a batch for pipe p for retention. */
    void retainFor(Pipe p, int slots) {
        retain = true;
        historySlots = slots;
        pipe = p;
    }

    /** A PIPE_PING was written. */
    void pingWritten(boolean suspicion) {
        pingSent = true;
        suspPing |= suspicion;
    }

    /** A revalidation PIPE_PING was written. */
    void revalPingWritten() {
        revalPing = true;
    }

    /** Limits the stream data of this batch to the pacer's room (0: none). */
    void limitData(long room) {
        dataRoom = room;
    }

    /** The just-written STREAM frame contains original bytes or an original FIN. */
    private void ownsStream(int n, boolean fin) {
        own += n;
        data |= n > 0;
        pendFinOnly = n == 0 && fin;
        control |= pendFinOnly;
    }

    /** Charges n bytes of stream data to the limiter's room. */
    void spendData(long n) {
        dataSpent += n;
        if (dataRoom > 0) dataRoom = Math.max(dataRoom - n, 0);
        if (shape != null) shape.spent(n);
    }
}
