package io.zmux.internal.core;

import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The output of one transmit poll: records for one outbound lane, as header bytes
 * in an arena and referenced payloads. A transport writes its segments and then reports
 * the write, which releases the referenced blocks. Reused across polls; a writer that
 * {@link #retain() retains} it takes the arena with it and the next poll builds in a new one.
 */
public final class Batch implements io.zmux.carrier.OutboundBatch {
    private ByteBuffer arena = ByteBuffer.allocate(512);
    /** A read-only view of the arena's array, for segment views (null until needed). */
    private ByteBuffer arenaView;
    /** The arena's array belongs to retained views: the next poll builds in a new one. */
    private boolean arenaRetained;
    // Segments: either ext (a referenced view) or arena[a0, a1).
    private ByteBuffer[] ext = new ByteBuffer[16];
    private int[] a0 = new int[16];
    private int[] a1 = new int[16];
    private int segs;
    // Records: segment range and frame bytes.
    private int[] recFirst = new int[8];
    private int[] recEnd = new int[8];
    private int[] recSize = new int[8];
    private int recs;
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

    /** Empties the batch for a poll of an outbound lane. */
    void reset(long pipe, long lane) {
        if (arenaRetained) {
            arena = ByteBuffer.allocate(arena.capacity());
            arenaView = null;
            arenaRetained = false;
        } else {
            arena.clear();
        }
        Arrays.fill(ext, 0, segs, null);
        segs = 0;
        recs = 0;
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

    /** The write side ends gracefully after this batch is written. */
    void shutdownAfter() {
        shutdown = true;
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
            ByteBuffer a = ByteBuffer.allocate(Math.max(arena.capacity() * 2, arena.position() + more));
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
        if (recs == recFirst.length) {
            int c = recs * 2;
            recFirst = Arrays.copyOf(recFirst, c);
            recEnd = Arrays.copyOf(recEnd, c);
            recSize = Arrays.copyOf(recSize, c);
        }
        recFirst[recs] = first;
        recEnd[recs] = end;
        recSize[recs] = size;
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
     * Keeps every view of this batch valid until the handle closes: the payload blocks get a
     * reference each, and the arena (the record headers) goes with the views, so the next
     * poll builds in a new one. Nothing is copied.
     */
    @Override
    public Retained retain() {
        arenaRetained = true;
        Pin[] held = pins.toArray(new Pin[0]);
        for (Pin p : held) p.ref();
        var open = new java.util.concurrent.atomic.AtomicBoolean(true);
        return () -> { if (open.compareAndSet(true, false)) for (Pin p : held) p.unref(); };
    }

    /** Releases the references of a batch whose write is never reported to its session. */
    public void discard() {
        release();
    }

    /** Drops the block references of a written (or discarded) batch. */
    void release() {
        for (Pin p : pins) p.unref();
        pins.clear();
    }

    /** Bytes a record of {@code size} frame bytes counts in positions (MULTIPATH.md Section 6.1). */
    static int positionBytes(int size) {
        return VarInt.length(size) + size;
    }
}
