package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;

import io.zmux.internal.buffer.WriteWorkspace;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.RuntimeReason;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The output of one transmit poll: records for one outbound lane, as header bytes
 * in an arena and referenced payloads. A transport writes its segments and then reports
 * the write. The encoder arena and the referenced blocks remain live until the write
 * receipt and all provider handles have returned.
 */
public final class Batch implements io.zmux.carrier.OutboundBatch {
    private ByteBuffer arena = ByteBuffer.allocate(512);
    /** A read-only view of the arena's array, for segment views (null until needed). */
    private ByteBuffer arenaView;
    /** The active write and its provider handles share this encoder workspace. */
    private WriteWorkspace workspace;
    private boolean encoded;
    private boolean ownerReturned;
    private boolean bounded;
    private final Runnable returnPins = this::finishEncoding;
    private static final int MAX_ARENA = Session.DEFAULT_BATCH + Long.BYTES;

    /** Codec entries include frame/fragment staging, receipts and gather views. */
    static final int MAX_CODEC_ENTRIES = Session.DEFAULT_BATCH / 256;
    /** Includes both arenas during growth, metadata arrays, and their growth overlap. */
    public static final long ENCODING_WORKSPACE = 2L * MAX_ARENA + MAX_CODEC_ENTRIES * 512L + 4096;

    int codecEntries() { return bounded ? MAX_CODEC_ENTRIES : Integer.MAX_VALUE; }

    public void workspace(WriteWorkspace workspace) {
        if (this.workspace != null) throw new IllegalStateException("batch already has a workspace");
        this.workspace = workspace;
        bounded = true;
    }

    public boolean ready() { return workspace == null || workspace.available(); }
    // Segments: either ext (a referenced view) or arena[a0, a1).
    private ByteBuffer[] ext = new ByteBuffer[16];
    private int[] a0 = new int[16];
    private int[] a1 = new int[16];
    private int segs;
    // Records: segment range and frame bytes.
    private int[] recEnd = new int[8];
    private int[] recSize = new int[8];
    private int[] wireEnd = new int[8];
    private int recs;
    private int[] receipt = new int[8];
    private record Charge(int record, CarrierCharge reservation, long start, long end) {}
    private ArrayList<Charge> charges;
    private record Span(int offset, int length) {}
    private ArrayList<Span> extras;
    private int standaloneRecord = -1;
    private OverheadLedger.Reservation overhead;
    private HandshakePlans.Padding handshakePadding = HandshakePlans.Padding.NONE;
    private OverheadLedger.HandshakeReservation handshake;

    public void handshakePadding(HandshakePlans.Padding span) { handshakePadding = span; }

    void submitHandshake(Shaping owner, int fixed, boolean atomic) {
        handshake = owner.submitHandshake(bytes(), fixed, handshakePadding, atomic);
    }

    /** Applies the Join write's carrier prefix to its committed receipt. */
    public void handshakeProgress(int confirmed, boolean terminal) {
        if (handshake != null) {
            handshake.report(confirmed, terminal, unknownProgress);
            if (terminal) handshake = null;
        }
    }

    private long measuredBase, measuredExtra;
    private int deliveredRecords, measuredCover, progressPrefix;
    private boolean unknownProgress;

    public void markProgressUnknown() { unknownProgress = true; }
    public boolean progressUnknown() { return unknownProgress; }

    private int sealedPayloadPrefix = -1, sealedWirePrefix, partialFraming;

    void overhead(OverheadLedger.Reservation reservation) { overhead = reservation; }

    void standalone(int record) { standaloneRecord = record; }

    void extraSpan(int offset, int length) {
        if (length == 0) return;
        if (extras == null) extras = new ArrayList<>();
        extras.add(new Span(offset, length));
    }

    /** Map a byte-stream ciphertext prefix through fixed record headers and tags for shaping only. */
    public void sealedProgress(int confirmed, int header, int tag) {
        if (confirmed < sealedWirePrefix || overhead == null) return;
        sealedWirePrefix = confirmed;
        partialFraming = 0;
        int payload = 0;
        for (int i = 0; i < recs; i++) {
            int length = recSize[i];
            if (confirmed < header + length + tag) {
                int body = Math.clamp(confirmed - header, 0, length);
                payload += body;
                partialFraming = confirmed - body;
                break;
            }
            payload += length;
            confirmed -= header + length + tag;
        }
        sealedPayloadPrefix = payload;
    }

    private void settleOverhead(int proved, int records, boolean terminal) {
        if (overhead == null) return;
        long extra = 0;
        if (extras != null) {
            for (Span s : extras) extra += Math.clamp(proved - s.offset(), 0, s.length());
        }
        var info = owner.st.info();
        long total = proved + (long) partialFraming;
        for (int i = 0; i < records; i++) {
            int fixed = info.overhead(i == 0);
            total += fixed;
            if (i == standaloneRecord) extra += fixed;
        }
        if (records == standaloneRecord) extra += partialFraming;
        long base = total - extra;
        if (terminal) {
            overhead.settle(base - measuredBase, extra - measuredExtra, unknownProgress);
            overhead = null;
        } else {
            overhead.progress(base - measuredBase, extra - measuredExtra);
        }
        measuredBase = base;
        measuredExtra = extra;
    }

    private Batch unsealed;
    private int sealOverhead;
    private int coverStart, coverLength;
    private Shaping.PipeShape coverOwner;
    private long coverId;
    private boolean pending;
    private Shaping.PipeShape shape;
    private Pipe.OutLane owner;
    private Framing framing;

    void committed(Pipe.OutLane lane, Framing format, Shaping.PipeShape observer) {
        owner = lane;
        framing = format;
        shape = observer;
        pending = true;
    }

    Pipe.OutLane owner() { return owner; }

    void cover(int start, int length, Shaping.PipeShape origin, long request) {
        coverOwner = origin;
        coverId = request;
        coverStart = start;
        coverLength = length;
    }

    void recordReceipt(int own, boolean data, boolean control) {
        receipt[recs - 1] = own << 2 | (data ? 1 : 0) | (control ? 2 : 0);
    }

    /** Associates bytes in the record being built with a splice reservation. */
    void charge(CarrierCharge reservation, long start, long end) {
        if (reservation == null) return;
        reservation.retain();
        if (charges == null) charges = new ArrayList<>();
        charges.add(new Charge(recs, reservation, start, end));
    }

    private void completeCharges(int records) {
        if (charges == null) return;
        for (Charge charge : charges) {
            if (charge.record() < records) charge.reservation().accepted(charge.start(), charge.end());
            charge.reservation().release();
        }
        charges.clear();
    }

    private int confirmedRecordCount(int confirmed) {
        int records = 0;
        while (records < recs && wireEnd[records] <= confirmed) records++;
        return records;
    }

    /** Keeps the original receipt reachable if the protected write outlives the session owner. */
    public void carrySealedReceipt(Batch source, int overhead) {
        unsealed = source;
        sealOverhead = overhead;
    }

    /** Releases a carrier write that has no protocol completion to run. */
    public void releaseAfterWrite(int confirmed) {
        try {
            handshakeProgress(confirmed, true);
            if (unsealed != null) {
                unsealed.releaseAfterWrite(unsealed.confirmedRecords(confirmed, sealOverhead));
                unsealed = null;
            }
            if (charges != null && !charges.isEmpty()) completeCharges(confirmedRecordCount(confirmed));
        } finally {
            release();
        }
    }

    void complete(Transmitter tx, Shaping shaping, int confirmed) {
        progress(tx, shaping, confirmed, true);
    }

    void progress(Transmitter tx, Shaping shaping, int confirmed, boolean terminal) {
        if (!pending) return;
        if (confirmed < 0 || confirmed > n) markProgressUnknown();
        if (confirmed >= 0 && confirmed <= n) progressPrefix = Math.max(progressPrefix, confirmed);
        int complete = confirmedRecordCount(progressPrefix);
        int proved = sealedPayloadPrefix >= 0 ? sealedPayloadPrefix : framing == Framing.BYTES ? progressPrefix
                : complete == 0 ? 0 : wireEnd[complete - 1];
        if (terminal && framing == Framing.RECORDS && sealedPayloadPrefix < 0 && proved < progressPrefix) markProgressUnknown();
        settleOverhead(proved, complete, terminal);
        if (terminal && unknownProgress && shaping != null) shaping.unknownProgress();
        if (coverOwner != null) {
            int covered = Math.clamp(proved - coverStart, 0, coverLength);
            int delta = covered - measuredCover;
            assert shaping != null : "cover records require shaping";
            shaping.coverSent(delta);
            measuredCover = covered;
            if (delta != 0 || terminal) {
                RuntimeReason result = !terminal || covered == coverLength ? null
                        : proved == 0 && !unknownProgress ? RuntimeReason.CARRIER_REJECTED : RuntimeReason.PARTIAL_PROGRESS;
                shaping.completeCover(coverOwner, lane, coverId, delta, result);
            }
        }
        while (deliveredRecords < complete) {
            int i = deliveredRecords++;
            int entry = receipt[i];
            tx.earn(entry >>> 2);
            if (shape != null) {
                assert shaping != null : "shaped records require shaping";
                shaping.outbound(shape, lane, owner.st.info().overhead(i == 0) + wireEnd[i] - (i == 0 ? 0 : wireEnd[i - 1]),
                        io.zmux.shaping.Event.Unit.RECORD, (entry & 1) != 0, (entry & 2) != 0);
            }
        }
        if (terminal) {
            pending = false;
            completeCharges(complete);
            shape = coverOwner = null;
        }
    }

    private final ArrayList<Pin> pins = new ArrayList<>(0);
    /** Bytes in the segments. */
    private int n;
    int n() { return n; }

    private long pipe;
    private long lane;
    /** End the write side gracefully after writing the batch (after CONNECTION_CLOSE). */
    private boolean shutdown;

    public long pipe() { return pipe; }
    public long lane() { return lane; }
    public boolean shutdown() { return shutdown; }

    @Override public int bytes() { return n; }

    @Override public int messageCount() { return recs; }

    @Override public int messageEnd(int m) {
        if (m < 0 || m >= recs) throw new IndexOutOfBoundsException(m);
        return recEnd[m];
    }

    boolean beginEncoding(long pipe, long lane) {
        if (workspace == null) workspace = new WriteWorkspace();
        if (!workspace.tryTake()) return false;
        encoded = true;
        ownerReturned = false;
        reset(pipe, lane);
        return true;
    }

    /** Empties the batch after its owner holds the workspace. */
    void reset(long pipe, long lane) {
        arena.clear();
        Arrays.fill(ext, 0, segs, null);
        segs = 0;
        recs = 0;
        overhead = null;
        handshake = null;
        handshakePadding = HandshakePlans.Padding.NONE;
        if (extras != null) extras.clear();
        standaloneRecord = sealedPayloadPrefix = -1;
        measuredBase = measuredExtra = 0;
        deliveredRecords = measuredCover = progressPrefix = 0;
        unknownProgress = false;
        sealedWirePrefix = partialFraming = 0;
        coverLength = 0;
        coverOwner = null;
        pending = false;
        owner = null;
        shape = null;
        unsealed = null;
        n = 0;
        shutdown = false;
        this.pipe = pipe;
        this.lane = lane;
    }

    /**
     * Fills the batch with the segments and records of src, for a carrier written outside a
     * session; src stays valid until this batch's write is reported.
     */
    public void wrap(io.zmux.carrier.OutboundBatch src) {
        if (!ready()) throw new IllegalStateException("batch workspace is still retained");
        reset(0, 0);
        for (int m = 0, from = 0; m < src.messageCount(); m++) {
            int to = src.messageEnd(m), first = segs, size = 0;
            for (int i = from; i < to; i++) {
                ByteBuffer v = src.segment(i);
                size += v.remaining();
                refSeg(v);
            }
            addRecord(first, segs, size);
            from = to;
        }
    }

    void appendUnit(byte[] unit) {
        refSeg(ByteBuffer.wrap(unit));
        addRecord(0, segs, unit.length);
    }

    /** The write side ends gracefully after this batch is written. */
    void shutdownAfter() {
        shutdown = true;
    }

    /** Plaintext prefix covered by fully accepted authenticated records. */
    public int confirmedRecords(int confirmed, int overhead) {
        int plain = 0;
        for (int i = 0; i < recs; i++) {
            int size = recSize[i];
            confirmed -= size + overhead;
            if (confirmed < 0) break;
            plain += size;
        }
        return plain;
    }

    /** Complete protocol records and the known incomplete prefix of the next record. */
    public record Tally(int bytes, int records, int partial) {}

    public Tally tally(int confirmed) {
        int limit = Math.clamp(confirmed, 0, n);
        if (limit == n) return new Tally(n, recs, 0);
        int records = 0, bytes = 0;
        for (int i = 0; i < recs; i++) {
            int end = bytes + (framing == Framing.BYTES ? positionBytes(recSize[i]) : recSize[i]);
            if (end > limit) break;
            bytes = end;
            records++;
        }
        return new Tally(bytes, records, limit - bytes);
    }

    /** The frame bytes of record i. */
    int recordSize(int i) {
        return recSize[i];
    }

    @Override
    public int segmentCount() {
        return segs;
    }

    /** The arena with room for {@code more} bytes at its position; bytes put there become a segment with {@link #arenaSeg}. */
    ByteBuffer arena(int more) {
        if (arena.remaining() < more) {
            long needed = (long) arena.position() + more;
            int limit = bounded ? MAX_ARENA : Integer.MAX_VALUE;
            if (needed > limit) throw new IllegalStateException("encoded batch exceeds its arena");
            ByteBuffer a = ByteBuffer.allocate(Math.clamp(arena.capacity() * 2L, (int) needed, limit));
            arena.flip();
            a.put(arena);
            arena = a;
            arenaView = null;
        }
        return arena;
    }

    /**
     * Makes arena bytes from {@code from} to the arena position a segment; they extend segment {@code last}
     * when it is the final segment and ends at {@code from}. Returns the segment.
     */
    int arenaSeg(int last, int from) {
        int to = arena.position();
        if (last >= 0 && last == segs - 1 && ext[last] == null && a1[last] == from) {
            a1[last] = to;
            n += to - from;
            return last;
        }
        return addSeg(null, from, to);
    }

    /** Adds a referenced payload segment. */
    void refSeg(ByteBuffer e) {
        addSeg(e, 0, 0);
    }

    /** Reserves an empty arena segment (a record's Size, filled by {@link #fillSeg}). */
    int reserveSeg() {
        return addSeg(null, 0, 0);
    }

    /** Fills a reserved segment with the arena bytes from {@code from} to the arena position. */
    void fillSeg(int i, int from) {
        a0[i] = from;
        a1[i] = arena.position();
        n += a1[i] - from;
    }

    /** Drops the segments from i on (an empty record's reserved Size). */
    void dropSegs(int i) {
        for (int k = i; k < segs; k++) {
            n -= ext[k] != null ? ext[k].remaining() : a1[k] - a0[k];
            ext[k] = null;
        }
        segs = i;
    }

    /** Adopts one reference to storage owned by the transport until the last retained view ends. */
    public void keepBacking(io.zmux.internal.buffer.ReadBuf backing) {
        pins.add(new Pin(backing));
    }

    /** Keeps a payload's block referenced until the batch is released. */
    void pin(Pin p) {
        p.ref();
        pins.add(p);
    }

    private int addSeg(ByteBuffer e, int from, int to) {
        if (segs == ext.length) {
            int c = segs * 2;
            ext = Arrays.copyOf(ext, c);
            a0 = Arrays.copyOf(a0, c);
            a1 = Arrays.copyOf(a1, c);
        }
        ext[segs] = e;
        a0[segs] = from;
        a1[segs] = to;
        n += e != null ? e.remaining() : to - from;
        return segs++;
    }

    void addRecord(int first, int end, int size) {
        assert first == (recs == 0 ? 0 : recEnd[recs - 1]) : "records partition the segments";
        if (recs == recEnd.length) {
            int c = recs * 2;
            recEnd = Arrays.copyOf(recEnd, c);
            recSize = Arrays.copyOf(recSize, c);
            wireEnd = Arrays.copyOf(wireEnd, c);
            receipt = Arrays.copyOf(receipt, c);
        }
        recEnd[recs] = end;
        recSize[recs] = size;
        wireEnd[recs] = n;
        recs++;
    }

    @Override
    public ByteBuffer segment(int i) {
        if (i < 0 || i >= segs) throw new IndexOutOfBoundsException(i);
        if (ext[i] != null) return ext[i].asReadOnlyBuffer();
        ByteBuffer v = arenaView;
        if (v == null) arenaView = v = ByteBuffer.wrap(arena.array()).asReadOnlyBuffer();
        return v.slice(a0[i], a1[i] - a0[i]);
    }

    @Override
    public ByteBuffer[] segments(ByteBuffer[] reuse) {
        ByteBuffer[] out = reuse != null && reuse.length >= segs ? reuse : new ByteBuffer[Math.max(segs, 16)];
        for (int i = 0; i < segs; i++) out[i] = segment(i);
        return out;
    }

    /**
     * Keeps every view of this batch valid until the handle closes. The encoder
     * and the retained payload roots become reusable after the last reference returns.
     */
    @Override
    public Retained retain() {
        if (encoded) {
            Runnable release = workspace.retain(returnPins);
            return release::run;
        }
        Pin[] held = pins.toArray(new Pin[0]);
        for (Pin p : held) p.ref();
        var open = new java.util.concurrent.atomic.AtomicBoolean(true);
        return () -> {
            if (!open.compareAndSet(true, false)) return;
            for (int i = 0; i < held.length; i++) {
                Pin pin = held[i];
                held[i] = null;
                pin.unref();
            }
        };
    }

    /** Releases the references of a batch whose write is never reported to its session. */
    public void discard() {
        unsealed = null;
        completeCharges(0);
        release();
    }

    /** Drops the block references of a written (or discarded) batch. */
    void release() {
        if (encoded) {
            if (!ownerReturned) {
                ownerReturned = true;
                workspace.giveBack(returnPins, false);
            }
        } else freePins();
    }

    private void finishEncoding() {
        try { freePins(); }
        finally { encoded = false; }
    }

    private void freePins() {
        for (Pin p : pins) p.unref();
        pins.clear();
    }

    /** Bytes a record of {@code size} frame bytes counts in positions. */
    static int positionBytes(int size) {
        return VarInt.length(size) + size;
    }
}
