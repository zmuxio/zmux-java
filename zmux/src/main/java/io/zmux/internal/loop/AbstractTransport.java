package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;

/**
 * What every transport shares: the binding to an inbound, the write in flight, the
 * directions in use, the read buffer that carries a partial record, and the replay of
 * input read before a move. Host thread only. A transport of another module (one living on
 * the thread of its own event loop) extends it through the protected members.
 */
public abstract class AbstractTransport implements Transport {
    final SessionHost loop;
    private Inbound in;
    private boolean started;
    private int directionMask = Action.DIR_BIDI;
    /** The batch of the write in flight, or null. */
    private Batch inFlight;
    /** The output half is shut (or being shut after the write in flight). */
    protected boolean outputShut;
    /** Direction bits of {@link #directions()}. */
    protected static final int IN = Action.DIR_IN, OUT = Action.DIR_OUT;
    /** Unconsumed input: a partial record, or what a paused gate has not taken yet. */
    ChannelReader reader;
    /** Input read before the transport moved to this loop, replayed first. */
    private byte[] prefix;

    protected AbstractTransport(SessionHost loop) {
        this.loop = loop;
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
        send(b);
    }

    /** Writes the batch in flight; reports through {@link #writeDone} (possibly at once). */
    protected abstract void send(OutboundBatch b);

    /** Reports the write in flight to the inbound. */
    protected final void writeDone(Throwable cause) {
        Batch b = inFlight;
        if (b == null) return;
        inFlight = null;
        in.written(this, b, cause);
    }

    /** Releases the write in flight without reporting it (the transport closed). */
    protected final void dropWrite() {
        Batch b = inFlight;
        inFlight = null;
        if (b != null) b.discard();
    }

    @Override public void pauseInput() {}
    @Override public void resumeInput() {}
    @Override public void redeliver() {}

    @Override
    public Transport moveTo(SessionHost to) {
        close(null);
        return null;
    }

    /**
     * Hands this transport, bound to a gate on its own loop, to a session on a host of
     * another kind: it stays where it is, exposed to that host as a byte-stream pipe input
     * read and written by an {@link ExternalTransport} there. Input is copied once on the way.
     */
    final Transport bridge(SessionHost to) {
        Forwarded f = new Forwarded(this, null, false, false);
        io.zmux.carrier.PipeIo io = f.pipe(info().applicationProtocol());
        if (info().carrier().tls()) io = io.withExporter(this::export);
        return new ExternalTransport(to, io);
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

    @Override public final Throwable inputError() { return inputError; }

    /** The input half failed with cause, which the session keeps as its pipe's error. */
    final void inputEnded(boolean partial, Throwable cause) {
        inputError = cause;
        in.ended(this, false, partial);
    }

    /** The input half ended; partial when unconsumed bytes remain (they are dropped). */
    protected final void inputEnded(boolean graceful) {
        boolean partial = reader != null && reader.hasPartial();
        if (reader != null) reader.release();
        in.ended(this, graceful, partial);
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

    /**
     * One input batch of a byte stream. Owned when free is not null: a session parses it in
     * place and may keep references to it; free runs once nothing does. Lent otherwise:
     * what is kept is copied before this returns. Stops at a pause, the end of input or a
     * move: data's position tells what was taken.
     */
    protected final void input(ByteBuffer data, Runnable free) {
        if (free == null) {
            feed(data);
            return;
        }
        OwnedInput owned = new OwnedInput(loop, data, free);
        try {
            input(data, owned.buf);
        } finally {
            owned.release();
        }
    }

    /** Byte-stream input; owned when retained is not null (see {@link #input(ByteBuffer, Runnable)}). */
    final void input(ByteBuffer data, ReadBuf retained) {
        if (retained == null || !in.session()) {
            feed(data);
            return;
        }
        Inbound session = in;
        while (data.hasRemaining() && accepting()) {
            if (reader.hasPartial()) {
                if (!complete(data)) return;
                continue;
            }
            int pos = data.position();
            int consumed = session.bytes(this, retained, data, pos, data.limit());
            if (consumed == Inbound.GONE) return;
            loop.inPlaceInput(consumed);
            data.position(pos + consumed);
            if (data.hasRemaining() && !carry(data)) return;
        }
    }

    /** Copies data through the read buffer into the inbound; false once the transport is gone or paused. */
    final boolean feed(ByteBuffer data) {
        while (data.hasRemaining() && accepting()) {
            ByteBuffer room = reader.room();
            int n = Math.min(room.remaining(), data.remaining());
            if (n == 0) return oversized();
            room.put(room.position(), data, data.position(), n);
            data.position(data.position() + n);
            reader.lim += n;
            loop.copiedInput(n);
            int consumed = deliver(reader.buf, reader.buf.data(), reader.pos, reader.lim);
            if (consumed == Inbound.GONE) return false;
            reader.consumed(consumed);
        }
        return !data.hasRemaining();
    }

    /**
     * Copies from data what completes the record the read buffer holds the start of, and
     * delivers it; false once the transport is gone.
     */
    private boolean complete(ByteBuffer data) {
        ByteBuffer room = reader.room();
        int n = Math.min(Math.min(missing(), room.remaining()), data.remaining());
        if (n == 0) return oversized();
        room.put(room.position(), data, data.position(), n);
        data.position(data.position() + n);
        reader.lim += n;
        loop.copiedInput(n);
        int consumed = deliver(reader.buf, reader.buf.data(), reader.pos, reader.lim);
        if (consumed == Inbound.GONE) return false;
        reader.consumed(consumed);
        return true;
    }

    /** The bytes the first record in the read buffer still lacks (its Size first). */
    private int missing() {
        ByteBuffer b = reader.buf.data();
        int have = reader.lim - reader.pos;
        int sizeLength = VarInt.lengthOf(b.get(reader.pos));
        if (have < sizeLength) return sizeLength - have;
        long total = sizeLength + VarInt.get(b, reader.pos, reader.lim);
        return Math.clamp(total - have, 1, Integer.MAX_VALUE);
    }

    /** Copies the start of a record that data ends with into the read buffer; false once the transport is gone. */
    private boolean carry(ByteBuffer data) {
        ByteBuffer room = reader.room();
        if (room.remaining() < data.remaining()) return oversized();
        int n = data.remaining();
        room.put(room.position(), data, data.position(), n);
        data.position(data.limit());
        reader.lim += n;
        loop.copiedInput(n);
        return true;
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
        byte[] b = unread(reader);
        if (reader != null) reader.release();
        return b;
    }

    /** Copies the unread bytes [pos, lim) of r for the transport that continues on another loop. */
    static byte[] unread(ChannelReader r) {
        if (r == null || r.buf == null || r.pos >= r.lim) return null;
        byte[] b = new byte[r.lim - r.pos];
        r.buf.data().get(r.pos, b);
        return b;
    }

    /** Delivers the unconsumed bytes of r; false when the transport left its owner. */
    final boolean redeliver(ChannelReader r) {
        if (r == null || !r.hasPartial()) return true;
        int c = deliver(r.buf, r.buf.data(), r.pos, r.lim);
        if (c == Inbound.GONE) return false;
        r.consumed(c);
        return true;
    }

    /** Input read before this transport took over, delivered first when it starts (before {@link #begin}). */
    protected final void prefix(byte[] b) {
        prefix = b;
    }

    /** Replays the input read before a move; false when the transport left again. */
    protected final boolean replay() {
        return replay(reader);
    }

    final boolean replay(ChannelReader r) {
        byte[] p = prefix;
        if (p == null) return true;
        prefix = null;
        ByteBuffer room = r.room();
        room.put(p);
        r.lim += p.length;
        int c = deliver(r.buf, r.buf.data(), r.pos, r.lim);
        if (c == Inbound.GONE) return false;
        r.consumed(c);
        return true;
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
