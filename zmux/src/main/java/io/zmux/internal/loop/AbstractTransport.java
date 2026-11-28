package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Batch;
import io.zmux.internal.buffer.ReadBuf;
import java.nio.ByteBuffer;

/**
 * What every transport shares: the binding to an inbound, the write in flight, the
 * directions in use, the read buffer that carries a partial record, and the replay of
 * input read before a move. Host thread only. A transport of another module (one living on
 * the thread of its own event loop) extends it through the protected members.
 */
public abstract non-sealed class AbstractTransport extends PhysicalTransport {
    final SessionHost loop;
    private final CarrierSetup.CloseRight right = new CarrierSetup.CloseRight();
    @Override final CarrierSetup.Bound closeRight() { return right.original(this); }
    @Override public final CarrierSetup setup() { return right.owner; }

    private Inbound in;
    private boolean started;
    private int directionMask = Action.DIR_BIDI;
    /** The batch of the write in flight, or null. */
    private Batch inFlight;
    private int proved;
    /** The output half is shut (or being shut after the write in flight). */
    protected boolean outputShut;
    /** Direction bits of {@link #directions()}. */
    protected static final int IN = Action.DIR_IN, OUT = Action.DIR_OUT;
    /** Unconsumed input: a partial record, or what a paused gate has not taken yet. */
    ChannelReader reader;

    protected AbstractTransport(SessionHost loop) {
        this.loop = loop;
    }

    /** Adapter modules implement these protected hooks. The owner supplies the capability on both paths. */
    public abstract static class Native extends AbstractTransport {
        protected Native(SessionHost loop) { super(loop); }

        @Override final void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
            closeNative(permit, cause);
        }
        @Override final Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost target) {
            return moveNative(permit, target);
        }

        @Override protected final void inputFailed(java.io.IOException cause) {
            try { nativeInputFailed(); } finally { CarrierSetup.connectionLost(this, cause); }
        }
        protected abstract void nativeInputFailed();

        protected abstract void closeNative(CarrierSetup.Bound.Permit permit, Throwable cause);
        protected abstract Transport moveNative(CarrierSetup.Bound.Permit permit, SessionHost target);
    }

    @Override public final SessionHost loop() { return loop; }

    @Override
    public final void start(Inbound in) {
        this.in = in;
        if (started) return;
        started = true;
        begin();
    }

    @Override public final void rebind(Inbound in) { this.in = in; }

    @Override public final Inbound inbound() { return in; }

    protected final boolean started() { return started; }

    /** Starts reading; once, on the host, with an inbound bound. */
    protected abstract void begin();

    /** The directions in use (all halves until {@link #restrict}). */
    protected final int mask() { return directionMask; }

    @Override public int directions() { return directionMask; }

    @Override
    public final void restrict(int required) {
        if ((directions() & required) != required) throw new IllegalArgumentException("carrier lacks a required half");
        if ((required & Action.DIR_IN) == 0) shutdownInput();
        if ((required & Action.DIR_OUT) == 0) shutdownOutput();
        directionMask = required;
    }

    @Override public boolean writable() { return inFlight == null && !outputShut; }

    protected final boolean writing() { return inFlight != null; }

    @Override
    public final void write(Batch b) {
        if (inFlight != null) throw new IllegalStateException("a write is in flight");
        inFlight = b;
        proved = 0;
        send(b);
    }

    /** Writes the batch in flight; reports through {@link #writeDone} (possibly at once). */
    protected abstract void send(OutboundBatch b);

    protected final void writeProgress(int prefix) {
        Batch b = inFlight;
        if (b == null || prefix <= proved || prefix > b.bytes()) return;
        proved = prefix;
        in.progress(this, b, prefix);
    }

    /** Reports the write in flight to the inbound. */
    protected final void writeDone(Throwable cause) {
        Batch b = inFlight;
        if (b != null) writeDone(cause == null ? b.bytes() : 0, cause);
    }

    /** Reports the confirmed prefix at the carrier interface. */
    protected final void writeDone(int confirmed, Throwable cause) {
        Batch b = inFlight;
        if (b == null) return;
        inFlight = null;
        if (confirmed < 0 || confirmed > b.bytes()) {
            b.markProgressUnknown();
            confirmed = proved;
        }
        in.written(this, b, Math.max(confirmed, proved), cause);
    }

    /** Reports to the inbound that the write side is shut (on the loop). */
    protected final void outputShutDone() {
        if (in != null) in.outputShut(this);
    }

    /** Releases the write in flight with its carrier receipt once the session's loop has stopped. */
    protected final void dropWrite(int confirmed) {
        Batch b = inFlight;
        inFlight = null;
        if (b != null) b.releaseAfterWrite(confirmed);
    }

    @Override public void pauseInput() {}
    @Override public void resumeInput() {}
    @Override public void redeliver() {}

    @Override
    Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost to) {
        java.util.Objects.requireNonNull(permit);
        return null;
    }

    /**
     * Hands this transport, bound to a gate on its own loop, to a session on a host of
     * another kind: it stays where it is, exposed to that host as a byte-stream pipe input
     * read and written by an {@link ExternalTransport} there.
     */
    final Transport bridge(SessionHost to) {
        return bridgeWithInput(to, asPipe());
    }

    /** On the loop, not started: this carrier as a pipe input, its exporter and declaration kept. */
    final io.zmux.carrier.PipeIo asPipe() {
        Forwarded f = new Forwarded(this, null, false, false);
        boolean exporter = exporterPresent();
        var tls = tlsSession();
        var security = io.zmux.Admission.OuterSecurity.tls(tls == null ? null : tls.getProtocol());
        return f.pipe(exporter ? this::export : null, info().applicationProtocol(), "", exporter && info().carrier().tls(), security, null);
    }

    // ---- input ----

    /** Whether input may be delivered now (started, not paused, ended or closed). */
    protected abstract boolean accepting();

    /** The input failed in a way the transport detected (a record larger than the read buffer). */
    protected abstract void inputFailed(java.io.IOException cause);

    /** Feeds input to the inbound: the count consumed, or {@link Inbound#GONE}. */
    final int deliver(ReadBuf buf, ByteBuffer data, int pos, int lim) {
        return in.bytes(this, buf, data, pos, lim);
    }

    final void inputEnded(boolean graceful, boolean partial) {
        in.ended(this, graceful, partial);
    }

    private Throwable inputError;
    private int partialInputBytes;

    @Override public final int partialInputBytes() { return partialInputBytes; }

    final boolean dropPartialInput() {
        partialInputBytes = reader == null ? 0 : reader.unreadBytes();
        if (reader != null) reader.release();
        return partialInputBytes > 0;
    }

    @Override public final Throwable inputError() { return inputError; }

    /** The input half failed with cause, which the session keeps as its pipe's error. */
    final void inputEnded(boolean partial, Throwable cause) {
        inputError = cause;
        in.ended(this, false, partial);
    }

    /** The input half ended; partial when unconsumed bytes remain (they are dropped). */
    protected final void inputEnded(boolean graceful) {
        in.ended(this, graceful, dropPartialInput());
    }

    protected final void writableChanged(boolean on) {
        in.writable(this, on);
    }

    /** An unexpected failure of a handler: true when the inbound took it as its own. */
    protected final boolean internalFailure(Throwable t) {
        return !(t instanceof java.io.IOException) && in != null && in.failed(this, t);
    }

    /** Takes the read buffer for input (on the host). */
    protected final void openReader() {
        if (reader == null) reader = new ChannelReader(loop.pool);
    }

    /** Parses one owned provider view and releases the transport's reference afterwards. */
    protected final int input(io.zmux.Chunk chunk) {
        var owned = ChunkAccess.take(chunk);
        ByteBuffer data = owned.data();
        int from = data.position();
        try {
            input(data, owned.buf());
            return data.position() - from;
        } finally {
            owned.buf().unref();
        }
    }

    /** Byte-stream input, with an owner when retained is present. */
    final void input(ByteBuffer data, ReadBuf retained) {
        if (retained == null || !in.session()) {
            feed(data);
            return;
        }
        Inbound session = in;
        while (data.hasRemaining() && accepting()) {
            if (reader.hasPartial()) {
                if (session.segmentedInput() && !reader.compacted()) {
                    parkOwnedInput(data, retained);
                    redeliver(reader);
                    return;
                }
                if (!complete(data)) return;
                continue;
            }
            int pos = data.position();
            int consumed = session.bytes(this, retained, data, pos, data.limit());
            if (consumed == Inbound.GONE) return;
            loop.inPlaceInput(consumed);
            data.position(pos + consumed);
            if (data.hasRemaining()) {
                if (session.segmentedInput()) {
                    parkOwnedInput(data, retained);
                    compactSparseCarry(reader);
                    return;
                }
                if (!carry(data)) return;
            }
        }
    }

    private void parkOwnedInput(ByteBuffer data, ReadBuf retained) {
        reader.hold(retained, data, data.position(), data.limit());
        loop.inPlaceInput(data.remaining());
        data.position(data.limit());
    }

    /** Copies data through the read buffer into the inbound; false once the transport is gone or paused. */
    final boolean feed(ByteBuffer data) {
        while (data.hasRemaining() && accepting()) {
            ByteBuffer room = inputRoom(false, 1);
            int n = Math.min(room.remaining(), data.remaining());
            if (n == 0) return oversized();
            copyInput(data, room, n);
            if (!redeliver(reader)) return false;
        }
        return !data.hasRemaining();
    }

    /**
     * Copies from data what completes the record the read buffer holds the start of, and
     * delivers it; false once the transport is gone.
     */
    private boolean complete(ByteBuffer data) {
        ByteBuffer room = reader.room();
        int missing = in.missingRecordBytes(room, reader.pos, reader.lim);
        int n = Math.min(Math.min(missing, room.remaining()), data.remaining());
        if (n == 0) return oversized();
        copyInput(data, room, n);
        return redeliver(reader);
    }

    /** Copies the start of a record that data ends with into the read buffer; false once the transport is gone. */
    private boolean carry(ByteBuffer data) {
        ByteBuffer room = reader.room();
        if (room.remaining() < data.remaining()) return oversized();
        copyInput(data, room, data.remaining());
        return true;
    }

    private void copyInput(ByteBuffer data, ByteBuffer room, int n) {
        room.put(room.position(), data, data.position(), n);
        data.position(data.position() + n);
        reader.lim += n;
        loop.copiedInput(n);
    }

    final ByteBuffer inputRoom(boolean idle, int minimum) {
        return reader.room(in.segmentedInput(), idle, minimum);
    }

    private boolean oversized() {
        inputFailed(new java.io.IOException("record larger than the read buffer"));
        return false;
    }

    /** Delivers the unconsumed input of the read buffer again; false when the transport left its owner. */
    protected final boolean redeliverBuffered() {
        return redeliver(reader);
    }

    /** The unread input, for the transport that continues elsewhere (null when none); the read buffer is released. */
    protected final byte[] takeUnread() {
        if (reader == null) return null;
        byte[] b = null;
        if (reader.hasPartial()) b = reader.copyUnread();
        reader.release();
        return b;
    }

    /** Delivers the unconsumed bytes of r; false when the transport left its owner. */
    final boolean redeliver(ChannelReader r) {
        if (r == null || !r.hasPartial()) return true;
        int c = r.hasRanges() ? in.bytes(this, r.ranges()) : deliver(r.buf, r.buf.data(), r.pos, r.lim);
        if (c == Inbound.GONE) return false;
        boolean segmented = in.segmentedInput();
        r.consumed(c, segmented);
        if (segmented) compactSparseCarry(r);
        return true;
    }

    private void compactSparseCarry(ChannelReader r) {
        int copied = r.compactSparseCarry();
        if (copied != 0) loop.copiedInput(copied);
    }

    /** Moves the unread range and its ownership to the next transport. */
    protected final boolean moveInputTo(AbstractTransport next) {
        if (reader == null) return false;
        next.openReader();
        reader.moveTo(next.reader);
        return true;
    }

    /** Uses a carrier's unstarted halves on another host, preserving any input already read. */
    protected final Transport bridgeWithInput(SessionHost target, io.zmux.carrier.PipeIo halves) {
        var next = new ExternalTransport(target, halves);
        moveInputTo(next);
        return next;
    }

    /** Releases the unread input without replaying it. */
    protected final void discardBufferedInput() {
        if (reader != null) reader.release();
    }

    /**
     * The selection key a transport moving from one loop to another keeps: on the same loop
     * the key is re-attached to the new transport with no interest until it starts;
     * otherwise it is cancelled and the new transport registers on its loop.
     */
    static java.nio.channels.SelectionKey handOver(java.nio.channels.SelectionKey key, EventLoop from, EventLoop to,
                                                   Object n) {
        if (key == null) return null;
        if (from == to && key.isValid()) {
            key.interestOps(0);
            key.attach(n);
            return key;
        }
        key.cancel();
        return null;
    }
}
