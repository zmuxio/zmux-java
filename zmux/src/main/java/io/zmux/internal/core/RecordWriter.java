package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Builds records into a {@link Batch}. A STREAM or DATAGRAM frame stays pending
 * until the next frame or the end of its record, so the LEN field of the last
 * frame of a record is omitted (IMPLEMENTATION.md Section 4).
 */
final class RecordWriter {
    private Batch b;
    Batch b() { return b; }
    private Framing framing;
    Framing framing() { return framing; }
    private int recMax;
    int recMax() { return recMax; }
    /** The shaped pipe's state; null: not shaped. */
    private Shaping.PipeShape shape;
    Shaping.PipeShape shape() { return shape; }
    /** The plan's largest frames length of the open record; 0: recMax. */
    private int cut;
    /** Stream data the shaping gate lets through; negative: no limit. */
    private long shapeRoom;
    /** Backs PADDING frames (0x00 bytes); never written to. */
    private static final ByteBuffer ZERO_PAD = ByteBuffer.allocateDirect(65535).asReadOnlyBuffer();
    private int budget;
    /** The budget the batch started with. */
    private int budget0;
    int budget() { return budget; }
    private boolean open;
    private int sizeSeg;
    private int recBytes;
    private int recs;
    int recs() { return recs; }
    /** The next record starts a carrier write: every record of a message carrier, the first of a batch otherwise. */
    boolean writeStart() { return recs == 0 || framing == Framing.RECORDS; }
    private boolean elicit;
    private boolean anyElicit;
    boolean anyElicit() { return anyElicit; }
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
    private boolean newData;
    private long dataSpent;
    long dataSpent() { return dataSpent; }
    /** Stream data the limiter lets through; negative: no limit. */
    private long dataRoom;
    long dataRoom() { return dataRoom; }
    /** Multipath: log the frames for retention. */
    private boolean retain;
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

    // The pending STREAM or DATAGRAM frame.
    private boolean pendOn;
    private boolean pendDatagram;
    private long pendId;
    private long pendOff;
    private boolean pendFin;
    private int pendN;
    private final ArrayList<ByteBuffer> pendData = new ArrayList<>(2);
    private final ArrayList<Pin> pendPins = new ArrayList<>(2);

    void init(Batch b, Framing framing, int recMax, int budget) {
        this.b = b;
        this.framing = framing;
        this.recMax = recMax;
        this.budget = budget0 = budget;
        open = false;
        sizeSeg = 0;
        recBytes = 0;
        recs = 0;
        elicit = anyElicit = activity = traffic = closed = laneOnly = newData = held = noHold = false;
        elicitEnd = 0;
        onlyPing = true;
        touched.clear();
        dataSpent = 0;
        dataRoom = -1;
        shape = null;
        cut = 0;
        shapeRoom = -1;
        retain = false;
        descs.clear();
        descRecs.clear();
        pipe = null;
        abandonsSent.clear();
        revalPing = false;
        pingSent = false;
        pingRequests = 0;
        pendOn = false;
        pendData.clear();
        pendPins.clear();
    }

    private int sizeCost() {
        return framing == Framing.RECORDS ? 0 : VarInt.length(recMax);
    }

    private int pendLen(boolean withLen) {
        if (!pendOn) return 0;
        if (pendDatagram) return 1 + pendN + (withLen ? VarInt.length(pendN) : 0);
        return FrameEncoder.streamHeaderLength(pendId, pendOff, pendN, withLen) + pendN;
    }

    /** The largest frames bytes of the open record. */
    private int max() {
        return cut > 0 ? cut : recMax;
    }

    /** Frame bytes that still fit in the open record (or a new one) within the batch budget. */
    int room() {
        if (closed) return 0;
        if (shape != null && !open) prep();
        if (held) return 0;
        if (open) {
            int r = Math.min(max(), framesBound(true)) - recBytes - pendLen(true);
            return Math.min(r, budget - pendLen(true));
        }
        return Math.min(Math.min(max(), framesBound(false)), budget - sizeCost());
    }

    private void prep() {
        held = false;
        if (!shape.prep(this)) shape = null;
    }

    private void begin() {
        if (open) return;
        if (shape != null) prep();
        open = true;
        recBytes = 0;
        elicit = false;
        sizeSeg = b.segmentCount();
        if (framing == Framing.BYTES) b.reserveSeg(); // filled at end()
    }

    /** Whether a frame of n bytes fits, header included. */
    boolean fits(int n) {
        if (closed) return false;
        if (shape != null && !open) prep();
        if (held) return false;
        if (open) {
            int len = recBytes + pendLen(true) + n;
            return len <= max() && pendLen(true) + n <= budget && len <= framesBound(true);
        }
        return n <= max() && sizeCost() + n <= budget && n <= framesBound(false);
    }

    /**
     * The most frames bytes the open record (open) or the next one may hold so that its plan's
     * padded target fits the batch (design 18.9): 0 when the next record's target does not fit, so
     * that it opens in the next batch. Unbounded unshaped, and for the batch's first record (a batch
     * smaller than the target could never send it), which also takes what the batch holds once open.
     */
    private int framesBound(boolean open) {
        Shaping.PipeShape ps = shape;
        if (ps == null || !ps.planned()) return Integer.MAX_VALUE;
        if (!open && recs == 0) return Integer.MAX_VALUE;
        // The room clampPad gives the record's PADDING.
        int total = Math.min(recMax, (open ? recBytes : 0) + budget - VarInt.length(recMax));
        int bound = ps.framesBound(total);
        return bound >= 0 ? bound : open ? Integer.MAX_VALUE : 0;
    }

    private boolean newRecordFits(int n) {
        return !closed && n <= max() && sizeCost() + n <= budget - (open ? pendLen(false) : 0);
    }

    /** Appends a frame of n bytes written by f; false when it does not fit. */
    boolean frame(int n, boolean elicits, Consumer<ByteBuffer> f) {
        if (!fits(n)) {
            if (!open || !newRecordFits(n)) return false;
            end();
            if (!fits(n)) return false;
        }
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
        return true;
    }

    /** Adds STREAM data; a contiguous continuation of the pending frame joins it. */
    boolean stream(long id, long off, ByteBuffer data, Pin pn, boolean fin) {
        int len = data == null ? 0 : data.remaining();
        if (open && pendOn && !pendDatagram && !pendFin && pendId == id && pendOff + pendN == off && len > 0) {
            int grow = FrameEncoder.streamHeaderLength(id, pendOff, pendN + len, true) + len
                    - FrameEncoder.streamHeaderLength(id, pendOff, pendN, true);
            int grown = recBytes + pendLen(true) + grow;
            if (grown <= max() && pendLen(true) + grow <= budget && grown <= framesBound(true)) {
                pendData.add(data);
                pendPins.add(pn);
                pendN += len;
                pendFin = fin;
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
        begin();
        flushPend(true);
        pendOn = true;
        pendDatagram = false;
        pendId = id;
        pendOff = off;
        pendFin = fin;
        pendN = len;
        pendData.add(data == null ? ByteBuffer.allocate(0) : data);
        pendPins.add(pn);
        if (!pn.none()) b.pin(pn);
        elicit = true;
        activity = traffic = true;
        onlyPing = false;
        return true;
    }

    /** Adds a DATAGRAM frame (LEN omitted when it ends its record). */
    boolean datagram(byte[] data) {
        int n = 1 + VarInt.length(data.length) + data.length;
        if (!fits(n)) {
            if (!open || !newRecordFits(n)) return false;
            end();
            if (!fits(n)) return false;
        }
        begin();
        flushPend(true);
        pendOn = true;
        pendDatagram = true;
        pendN = data.length;
        pendData.add(ByteBuffer.wrap(data));
        pendPins.add(Pin.NONE);
        elicit = true;
        activity = traffic = true;
        onlyPing = false;
        return true;
    }

    private void flushPend(boolean withLen) {
        if (!pendOn) return;
        pendOn = false;
        int hl = pendDatagram ? 1 + (withLen ? VarInt.length(pendN) : 0)
                : FrameEncoder.streamHeaderLength(pendId, pendOff, pendN, withLen);
        ByteBuffer a = b.arena(hl);
        int p0 = a.position();
        if (pendDatagram) FrameEncoder.datagramHeader(a, pendN, withLen);
        else FrameEncoder.streamHeader(a, pendId, pendOff, pendN, pendFin, withLen);
        int lastArenaSeg = b.arenaSeg(-1, p0);
        for (int i = 0; i < pendData.size(); i++) {
            ByteBuffer d = pendData.get(i);
            Pin pn = pendPins.get(i);
            if (d.remaining() < RecvBuffer.REF_THRESHOLD || pn.none()) {
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
            if (framing == Framing.BYTES) b.dropSegs(sizeSeg);
            return;
        }
        int first = sizeSeg;
        if (framing == Framing.BYTES) {
            ByteBuffer a = b.arena(8);
            int p0 = a.position();
            VarInt.put(a, recBytes);
            b.fillSeg(sizeSeg, p0);
            budget -= a.position() - p0;
        }
        b.addRecord(first, b.segmentCount(), recBytes);
        recs++;
        if (elicit) {
            anyElicit = true;
            elicitEnd = b.bytes();
        }
    }

    /**
     * Applies the plan to the record being closed; the pending frame is still to be flushed. A
     * size target (pad_to, pad_multiple) is met exactly: the PADDING follows the last frame, which
     * then keeps its Length field, unless the target falls within that field, where the PADDING
     * goes before the last frame, which ends the record without it. Planned extra PADDING (pad,
     * injections) follows the last frame in full, on top of the target of the frames.
     */
    private void endShaped(Shaping.PipeShape ps) {
        int bare = recBytes + pendLen(false), full = recBytes + pendLen(true);
        int extra = ps.extraPad();
        if (extra > 0) {
            flushPend(true);
            pad(clampPad(ps, ps.target(full) - full + extra, 0));
        } else {
            int t = ps.target(bare);
            if (t == bare) {
                flushPend(false);
            } else if (t >= full) {
                flushPend(true);
                pad(clampPad(ps, t - full, 0));
            } else {
                pad(clampPad(ps, t - bare, pendLen(false)));
                flushPend(false);
            }
        }
        ps.ended(this);
    }

    /** Bounds n bytes of PADDING by the record and batch room left besides rest bytes still to follow, then by the limits. */
    private int clampPad(Shaping.PipeShape ps, int n, int rest) {
        int avail = Math.min(recMax - recBytes - rest, budget - rest - VarInt.length(recMax));
        if (n > avail) {
            n = Math.max(avail, 0);
            ps.owner().clamp();
        }
        return ps.owner().allowPad(n);
    }

    /** Appends n bytes of PADDING to the open record. */
    private void pad(int n) {
        if (n <= 0) return;
        b.refSeg(ZERO_PAD.slice(0, n));
        recBytes += n;
        budget -= n;
    }

    /** Frame bytes of the open record, the pending frame included; 0 when none is open. */
    int openBytes() {
        return open ? recBytes + pendLen(false) : 0;
    }

    /** Nothing was written to the batch yet. */
    boolean batchEmpty() { return budget == budget0; }

    /** The largest PADDING-only record that fits the batch. */
    int padRoom() {
        return Math.min(recMax, budget - VarInt.length(recMax));
    }

    /** Writes a record of n bytes of PADDING only, outside the plan. */
    void padRecord(int n) {
        Shaping.PipeShape ps = shape;
        shape = null;
        begin();
        pad(n);
        end();
        shape = ps;
    }

    /** The shaped pipe of this batch. */
    void shape(Shaping.PipeShape ps) { shape = ps; }

    /** The plan's largest frames length; 0: none. */
    void cut(int n) { cut = n; }

    /** The planned record waits for its delay. */
    void holdRecord() { held = !noHold; }

    /** The batch carries frames that no planned delay holds. */
    void neverHold() {
        noHold = true;
        held = false;
    }

    /** The gate's share of stream data in this batch. */
    void shapeRoom(long n) { shapeRoom = n; }

    /** Bounds the data bytes of a STREAM frame, room, by the shaping gate. */
    int dataFit(int room) {
        return shapeRoom >= 0 && room > shapeRoom ? (int) shapeRoom : room;
    }

    /** The record ends its carrier write: nothing more goes into this batch. */
    void flushBatch() { budget = 0; }

    /** Records a frame of the record being built (multipath retention only). */
    void logFrame(FrameDesc d) {
        if (!retain) return;
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
    void retainFor(Pipe p) {
        retain = true;
        pipe = p;
    }

    /** A PIPE_PING was written. */
    void pingWritten() {
        pingSent = true;
    }

    /** A revalidation PIPE_PING was written. */
    void revalPingWritten() {
        revalPing = true;
    }

    /** Limits the stream data of this batch to the pacer's room (0: none). */
    void limitData(long room) {
        dataRoom = room;
    }

    /** Charges n bytes of stream data to the limiter's room. */
    void spendData(long n) {
        dataSpent += n;
        if (dataRoom > 0) dataRoom = Math.max(dataRoom - n, 0);
        if (shapeRoom > 0) shapeRoom = Math.max(shapeRoom - n, 0);
    }
}
