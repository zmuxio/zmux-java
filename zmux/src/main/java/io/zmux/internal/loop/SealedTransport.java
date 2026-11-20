package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import io.zmux.internal.core.ReadBuf;
import io.zmux.seal.SecureCarrierException;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.RecordProtection;
import io.zmux.internal.join.Suite;
import java.nio.ByteBuffer;

/**
 * A carrier whose records the handshake's keys protect: upward a message
 * carrier, one record per message, downward one write per batch. One implementation for Join and
 * Establish. A failed authentication is permanent and closes the carrier abortively; a cut record is a
 * carrier failure.
 */
public final class SealedTransport implements Transport, Inbound {
    /** The largest plaintext of a sealed record. */
    public static final int MAX_RECORD = 65519;
    private static final int LEN = 2;

    private final Transport lower;
    private final RecordProtection records;
    private final io.zmux.carrier.PipeIo.Exporter exporter;
    private byte[] firstBytes;
    private final boolean stream;
    private final TransportInfo info;
    private Inbound upper;
    private Batch upperBatch;
    private byte[] scratch;
    /** The record that failed to open, which ended the input; null while none has. */
    private Throwable openFailure;
    private boolean inputEnded;

    /** A Join's carrier: its keys and exporter are the handshake's. */
    SealedTransport(Transport lower, Result result) {
        this(lower, result, (label, context, length) -> {
            try {
                return result.export(label, context, length);
            } catch (SecureCarrierException invalid) {
                return null;
            }
        });
    }

    /** One record per message of the carrier's records; exporter is what this carrier exports. */
    public SealedTransport(Transport lower, RecordProtection records, io.zmux.carrier.PipeIo.Exporter exporter) {
        this.lower = lower;
        this.records = records;
        this.exporter = exporter;
        firstBytes = records.firstBytes();
        stream = lower.info().carrier().framing() != Framing.RECORDS;
        TransportInfo l = lower.info();
        info = new TransportInfo(sealedInfo(l.carrier()), l.applicationProtocol(), l.connection(), l.receiveMemory(), l.fateGroup());
    }

    /** What the core sees of a carrier once it is sealed. */
    public static CarrierInfo sealedInfo(CarrierInfo c) {
        if (c.maxRecord() > 0 && c.maxRecord() - Suite.TAG_LEN - (c.framing() != Framing.RECORDS ? LEN : 0) < CarrierInfo.RECORD_FLOOR)
            throw new IllegalArgumentException("the carrier's record limit " + c.maxRecord() + " leaves less than "
                    + CarrierInfo.RECORD_FLOOR + " bytes once sealed");
        int seal = Suite.TAG_LEN + (c.framing() != Framing.RECORDS ? LEN : 0), overhead = c.writeOverhead() + seal;
        // The carrier's own record limit holds for the sealed record it carries.
        return new CarrierInfo(Framing.RECORDS, false, c.minRtt(), false, c.queueBound(), null, c.kind(), c.label(),
                Math.min(overhead, io.zmux.carrier.PipeIo.MAX_WRITE_OVERHEAD), c.variant(),
                c.maxRecord() > 0 ? Math.min(c.maxRecord() - seal, MAX_RECORD) : MAX_RECORD, c.ownFailure());
    }

    @Override public TransportInfo info() { return info; }
    @Override public SessionHost loop() { return lower.loop(); }
    @Override public void start(Inbound in) {
        upper = in;
        byte[] first = firstBytes;
        firstBytes = null;
        // Before any later byte of the carrier.
        if (first != null && !deliver(ByteBuffer.wrap(first), first.length)) return;
        lower.start(this);
    }
    @Override public void rebind(Inbound in) { upper = in; lower.rebind(this); }
    @Override public Inbound inbound() { return upper; }
    @Override public Throwable inputError() { return openFailure != null ? openFailure : lower.inputError(); }
    @Override public int directions() { return lower.directions(); }
    @Override public void restrict(int required) { lower.restrict(required); }
    @Override public boolean writable() { return upperBatch == null && lower.writable(); }
    @Override public void pauseInput() { lower.pauseInput(); }
    @Override public void resumeInput() { lower.resumeInput(); }
    @Override public void redeliver() { lower.redeliver(); }
    @Override public void shutdownOutput() { lower.shutdownOutput(); }
    @Override public void shutdownInput() { lower.shutdownInput(); }
    @Override public void close(Throwable cause) { lower.close(cause); }
    @Override public boolean abortable() { return lower.abortable(); }
    @Override public void abort(Throwable cause) { lower.abort(cause); }
    @Override public boolean isClosed() { return lower.isClosed(); }
    @Override public Transport moveTo(SessionHost to) { lower.close(null); return null; }
    @Override public boolean exporterPresent() { return true; }
    @Override public boolean isSecure() { return lower.isSecure(); }
    @Override public io.zmux.RecordMode firstPipeMode() { return lower.firstPipeMode(); }
    @Override public Object carrierPeer() { return lower.carrierPeer(); }
    @Override public long handshakePadding() { return lower.handshakePadding(); }
    @Override public boolean ownsHalf(Object half) { return lower.ownsHalf(half); }
    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return lower.whenClosed(); }
    @Override public void taken() { lower.taken(); }
    @Override public java.util.concurrent.CompletionStage<Boolean> whenTaken() { return lower.whenTaken(); }

    @Override
    public javax.net.ssl.SSLSession tlsSession() { return lower.tlsSession(); }

    @Override
    public byte[] export(String label, byte[] context, int length) {
        return exporter.export(label, context, length);
    }

    @Override
    public void write(Batch b) {
        upperBatch = b;
        Batch out = new Batch();
        try {
            int recs = b.messageCount();
            if (stream) {
                ByteBuffer all = ByteBuffer.allocate(b.bytes() + recs * (Suite.TAG_LEN + LEN));
                for (int m = 0, from = 0; m < recs; m++) from = sealOne(b, m, from, all);
                all.flip();
                out.wrap(OutboundBatch.of(all));
            } else {
                ByteBuffer[] msgs = new ByteBuffer[recs];
                for (int m = 0, from = 0; m < recs; m++) {
                    int to = b.messageEnd(m);
                    int plain = 0;
                    for (int i = from; i < to; i++) plain += b.segment(i).remaining();
                    ByteBuffer one = ByteBuffer.allocate(plain + Suite.TAG_LEN);
                    from = sealOne(b, m, from, one);
                    one.flip();
                    msgs[m] = one;
                }
                out.wrap(OutboundBatch.of(msgs));
            }
        } catch (SecureCarrierException e) {
            lower.close(e);
            upperBatch = null;
            upper.written(this, b, e);
            return;
        }
        lower.write(out);
    }

    /** Seals record m into out, with its length on a byte stream; the segment after it. */
    private int sealOne(Batch b, int m, int from, ByteBuffer out) throws SecureCarrierException {
        int to = b.messageEnd(m);
        int plain = 0;
        for (int i = from; i < to; i++) plain += b.segment(i).remaining();
        ByteBuffer p;
        if (to - from == 1) {
            p = b.segment(from);
        } else {
            if (scratch == null || scratch.length < plain) scratch = new byte[Math.max(plain, 4096)];
            p = ByteBuffer.wrap(scratch, 0, plain);
            for (int i = from; i < to; i++) p.put(b.segment(i));
            p.flip();
        }
        if (stream) out.putShort((short) (plain + Suite.TAG_LEN));
        ByteBuffer dst = out.slice();
        out.position(out.position() + records.seal(p, dst));
        return to;
    }

    @Override public boolean session() { return true; }

    @Override
    public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        int at = pos;
        while (lim - at >= LEN) {
            int n = (data.get(at) & 0xff) << 8 | (data.get(at + 1) & 0xff);
            if (lim - at - LEN < n) break;
            if (!open(data.slice(at + LEN, n))) return GONE;
            at += LEN + n;
            if (lower.isClosed()) return GONE;
        }
        return at - pos;
    }

    @Override
    public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        open(data.slice(pos, lim - pos));
    }

    /** Opens one record for the session, into a buffer the session may keep. */
    private boolean open(ByteBuffer ct) {
        if (ct.remaining() < Suite.TAG_LEN) {
            openFailed(new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
            return false;
        }
        ByteBuffer plain = ByteBuffer.allocate(ct.remaining() - Suite.TAG_LEN);
        int n;
        try {
            n = records.open(ct, plain);
        } catch (SecureCarrierException e) {
            openFailed(e);
            return false;
        }
        plain.position(0).limit(n);
        return deliver(plain, n);
    }

    /**
     * A record failed to open: the carrier closes abortively, and the input ends with that cause,
     * since closing the lower carrier reports no end of its own.
     */
    private void openFailed(SecureCarrierException e) {
        openFailure = e;
        lower.close(e);
        ended(this, false, false);
    }

    private boolean deliver(ByteBuffer plain, int n) {
        if (upper.session()) {
            upper.message(this, new ReadBuf(plain, null), plain, 0, n);
            return true;
        }
        // Before admission a record is classified with its Size, as on any message carrier.
        ByteBuffer framed = ByteBuffer.allocate(n + 8);
        io.zmux.internal.wire.VarInt.put(framed, n);
        framed.put(plain).flip();
        return upper.bytes(this, new ReadBuf(framed, null), framed, 0, framed.limit()) != GONE;
    }

    @Override public void ended(Transport t, boolean graceful, boolean partial) {
        if (inputEnded) return;
        inputEnded = true;
        upper.ended(this, graceful, partial);
    }
    @Override public void writable(Transport t, boolean on) { upper.writable(this, on); }
    @Override public void outputShut(Transport t) { upper.outputShut(this); }

    @Override
    public void written(Transport t, Batch b, Throwable cause) {
        b.discard();
        Batch up = upperBatch;
        upperBatch = null;
        if (up != null) upper.written(this, up, cause);
    }

    @Override public boolean failed(Transport t, Throwable error) { return upper.failed(this, error); }
}
