package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.WriteWorkspace;
import io.zmux.internal.wire.VarInt;
import io.zmux.seal.SecureCarrierException;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.RecordProtection;
import io.zmux.internal.join.Suite;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * A carrier whose records the handshake's keys protect: upward a message
 * carrier, one record per message, downward one write per batch. One implementation for Join and
 * Establish. A failed authentication is permanent and closes the carrier abortively; a cut record is a
 * carrier failure.
 */
public final class SealedTransport extends PhysicalTransport implements Inbound {
    /** The largest plaintext of a sealed record. */
    public static final int MAX_RECORD = 65519;
    private static final int LEN = 2;
    /** Ciphertext expansion and the record's shares of the view and batch descriptor arrays. */
    private static final int RECORD_WORKSPACE = Suite.TAG_LEN + LEN + 512;
    private static final long OUTPUT_WORKSPACE = Session.DEFAULT_BATCH + MAX_RECORD + 4096L;

    static long workspace(int directions) {
        return ((directions & Action.DIR_OUT) != 0 ? OUTPUT_WORKSPACE : 0)
                + ((directions & Action.DIR_IN) != 0 ? io.zmux.internal.buffer.ReceiveSizes.MAX_BLOCK : 0);
    }

    private final PhysicalTransport lower;
    private final RecordProtection records;
    private final io.zmux.carrier.PipeIo.Exporter exporter;
    private byte[] firstBytes;
    private final boolean stream;
    private final TransportInfo info;
    private Inbound upper;
    private Batch upperBatch;
    private int lowerConfirmed;
    private final WriteWorkspace output;
    private byte[] scratch;
    private final CompletionStage<Void> closed;
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
        this.lower = (PhysicalTransport) lower;
        this.records = records;
        output = this.lower.nextSeal();
        closed = output.closeAfter(this.lower.whenClosed());
        output.onWritable(this::outputWritableFromRelease);
        closed.whenComplete((v, error) -> scratch = null);
        this.exporter = exporter;
        firstBytes = records.firstBytes();
        stream = lower.info().carrier().framing() != Framing.RECORDS;
        TransportInfo l = lower.info();
        info = new TransportInfo(sealedInfo(l.carrier()), l.applicationProtocol(), l.connection(), l.receiveMemory(), l.fateGroup());
    }

    @Override public java.util.List<Object> physicalConnections() { return lower.physicalConnections(); }

    /** What the core sees of a carrier once it is sealed. */
    public static CarrierInfo sealedInfo(CarrierInfo c) {
        if (c.maxRecord() > 0 && c.maxRecord() - Suite.TAG_LEN - (c.framing() != Framing.RECORDS ? LEN : 0) < CarrierInfo.RECORD_FLOOR)
            throw new IllegalArgumentException("the carrier's record limit " + c.maxRecord() + " leaves less than "
                    + CarrierInfo.RECORD_FLOOR + " bytes once sealed");
        int seal = Suite.TAG_LEN + (c.framing() != Framing.RECORDS ? LEN : 0);
        // The carrier's own record limit holds for the sealed record it carries.
        return new CarrierInfo(Framing.RECORDS, false, c.minRtt(), false, c.queueBound(), null, c.kind(), c.label(),
                c.writeOverhead(), c.variant(), c.maxRecord() > 0 ? Math.min(c.maxRecord() - seal, MAX_RECORD) : MAX_RECORD,
                c.ownFailure(), c.recordOverhead() + seal, c.framing() == Framing.BYTES || c.coalescesRecords());
    }

    @Override WriteWorkspace encoder() { return lower.encoder(); }
    @Override WriteWorkspace history() { return lower.history(); }
    @Override public TransportInfo info() { return info; }
    @Override public long inputQueueCapacity() { return lower.inputQueueCapacity(); }
    @Override java.util.List<TlsTransport.RecordWorkspace> tlsWorkspaces() { return lower.tlsWorkspaces(); }
    @Override public long privateWorkspace() { return lower.privateWorkspace() + workspace(directions()); }
    @Override public int recordWorkspace() { return RECORD_WORKSPACE; }
    @Override public io.zmux.carrier.CarrierHalfSource receiveSource() { return lower.receiveSource(); }
    @Override public io.zmux.carrier.CarrierHalfSource sendSource() { return lower.sendSource(); }
    @Override public int partialInputBytes() { return lower.partialInputBytes(); }
    @Override public Object carrierIdentity() { return lower.carrierIdentity(); }
    @Override public Runnable watchCarrier(Runnable changed) {
        return lower.watchCarrier(changed);
    }

    @Override public Query carrierQuery(Object identity) {
        return lower.carrierQuery(identity);
    }
    @Override public SessionHost loop() { return lower.loop(); }
    @Override public void start(Inbound in) {
        upper = in;
        byte[] first = firstBytes;
        firstBytes = null;
        // Before any later byte of the carrier.
        if (first != null && !deliverFirst(first)) return;
        lower.start(this);
    }
    @Override public void rebind(Inbound in) { upper = in; lower.rebind(this); }
    @Override public Inbound inbound() { return upper; }
    @Override public Throwable inputError() { return openFailure != null ? openFailure : lower.inputError(); }
    @Override public boolean resumesAfterPeekTimeout() { return lower.resumesAfterPeekTimeout(); }
    @Override public int directions() { return lower.directions(); }
    @Override public CarrierSetup setup() { return lower.setup(); }
    @Override CarrierSetup.Bound closeRight() { return lower.closeRight().as(this); }
    @Override public void restrict(int required) { lower.restrict(required); }
    @Override public boolean writable() { return upperBatch == null && output.available() && lower.writable(); }
    @Override public void pauseInput() { lower.pauseInput(); }
    @Override public void resumeInput() { lower.resumeInput(); }
    @Override public void redeliver() { lower.redeliver(); }
    @Override public void shutdownOutput() { lower.shutdownOutput(); }
    @Override public void shutdownInput() { lower.shutdownInput(); }
    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
        lower.closePhysical(permit, cause);
    }
    @Override protected java.util.function.Consumer<Throwable> closeEscalation() { return lower.closeEscalation(); }
    @Override public boolean isClosed() { return lower.isClosed(); }
    @Override Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost to) {
        java.util.Objects.requireNonNull(permit);
        return null;
    }
    @Override public boolean exporterPresent() { return true; }
    @Override public boolean isSecure() { return lower.isSecure(); }
    @Override public io.zmux.RecordMode firstPipeMode() { return lower.firstPipeMode(); }
    @Override public Object carrierPeer() { return lower.carrierPeer(); }
    @Override public io.zmux.carrier.HandshakeCost handshakeCost() { return lower.handshakeCost(); }
    @Override public boolean ownsHalf(Object half) { return lower.ownsHalf(half); }
    @Override public CompletionStage<Void> whenClosed() { return closed; }
    @Override public java.util.concurrent.CompletionStage<Void> writesReleased() { return lower.writesReleased(); }
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
        if (upperBatch != null) throw new IllegalStateException("sealed carrier write already in flight");
        int recs = b.messageCount();
        if (b.bytes() + (long) recs * RECORD_WORKSPACE > Session.DEFAULT_BATCH) {
            throw new IllegalStateException("sealed batch exceeds its reserved workspace");
        }
        var out = new Batch();
        if (!output.tryTake()) {
            upper.written(this, b, 0, new java.io.IOException("sealed carrier closed during write"));
            return;
        }
        ReadBuf backing = null;
        boolean owned = false;
        try {
            ByteBuffer all = ByteBuffer.allocate(b.bytes() + recs * recordOverhead());
            backing = new ReadBuf(all, ignored -> outputReturned());
            if (stream) {
                for (int m = 0, from = 0; m < recs; m++) from = sealOne(b, m, from, all);
                all.flip();
                out.wrap(OutboundBatch.of(all));
            } else {
                ByteBuffer[] msgs = new ByteBuffer[recs];
                for (int m = 0, from = 0; m < recs; m++) {
                    int start = all.position();
                    from = sealOne(b, m, from, all);
                    msgs[m] = all.slice(start, all.position() - start);
                }
                out.wrap(OutboundBatch.of(msgs));
            }
            out.keepBacking(backing);
            owned = true;
        } catch (SecureCarrierException e) {
            CarrierSetup.connectionLost(this, e);
            upper.written(this, b, 0, e);
            return;
        } finally {
            if (!owned) {
                if (backing != null) backing.unref();
                else outputReturned();
            }
        }
        upperBatch = b;
        out.carrySealedReceipt(b, recordOverhead());
        lower.write(out);
    }

    private void outputReturned() {
        output.giveBack(null, true);
    }

    private void outputWritableFromRelease() {
        SessionHost host = loop();
        if (host.inLoop()) outputWritable();
        else host.tryExecute(this::outputWritable);
    }

    private void outputWritable() {
        if (upper != null && !isClosed() && writable()) upper.writable(this, true);
    }

    private int recordOverhead() { return Suite.TAG_LEN + (stream ? LEN : 0); }

    /** Seals record m into out, with its length on a byte stream; the segment after it. */
    private int sealOne(Batch b, int m, int from, ByteBuffer out) throws SecureCarrierException {
        int to = b.messageEnd(m);
        int plain = 0;
        for (int i = from; i < to; i++) plain += b.segment(i).remaining();
        if (plain > info.carrier().maxRecord()) {
            throw new IllegalStateException("sealed plaintext exceeds the record limit");
        }
        ByteBuffer p;
        if (to - from == 1) {
            p = b.segment(from);
        } else {
            if (scratch == null) scratch = new byte[MAX_RECORD];
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
    public int missingRecordBytes(ByteBuffer data, int pos, int lim) {
        int have = lim - pos;
        return have < LEN ? LEN - have : Math.max(1, LEN + ciphertextLength(data, pos) - have);
    }

    private static int ciphertextLength(ByteBuffer data, int pos) {
        return (data.get(pos) & 0xff) << 8 | (data.get(pos + 1) & 0xff);
    }

    @Override
    public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        int at = pos;
        while (lim - at >= LEN) {
            int n = ciphertextLength(data, at);
            if (!checkLength(n)) return GONE;
            if (lim - at - LEN < n) break;
            if (!open(data.slice(at + LEN, n))) return GONE;
            at += LEN + n;
            if (lower.isClosed()) return GONE;
        }
        return at - pos;
    }

    @Override
    public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        if (checkLength(lim - pos)) open(data.slice(pos, lim - pos));
    }

    private boolean checkLength(int length) {
        if (inputEnded) return false;
        if (length >= Suite.TAG_LEN && length - Suite.TAG_LEN <= info.carrier().maxRecord()) return true;
        openFailed(new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
        return false;
    }

    /** Authenticates into the final backing. No plaintext is published until open succeeds. */
    private boolean open(ByteBuffer ct) {
        int length = ct.remaining() - Suite.TAG_LEN;
        int prefix = upper.session() ? 0 : VarInt.length(length);
        ReadBuf owned = loop().pool.takeRecord(prefix + length);
        ByteBuffer data = owned.data();
        try {
            if (prefix != 0) VarInt.put(data, length);
            data.limit(prefix + length);
            int n = records.open(ct, data);
            if (n != length || data.position() != prefix + n) {
                openFailed(new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
                return false;
            }
            return deliver(owned, data, prefix, n);
        } catch (SecureCarrierException e) {
            openFailed(e);
            return false;
        } finally {
            owned.unref();
        }
    }

    /** Authenticated bytes carried inside the handshake already have independent storage. */
    private boolean deliverFirst(byte[] first) {
        int prefix = upper.session() ? 0 : VarInt.length(first.length);
        ByteBuffer data = prefix == 0 ? ByteBuffer.wrap(first) : ByteBuffer.allocate(prefix + first.length);
        if (prefix != 0) {
            VarInt.put(data, first.length);
            data.put(first).flip();
        }
        ReadBuf owned = new ReadBuf(data, null);
        try {
            return deliver(owned, data, prefix, first.length);
        } finally {
            owned.unref();
        }
    }

    /**
     * A record failed to open: the carrier closes abortively, and the input ends with that cause,
     * since closing the lower carrier reports no end of its own.
     */
    private void openFailed(SecureCarrierException e) {
        openFailure = e;
        CarrierSetup.connectionLost(this, e);
        ended(this, false, false);
    }

    private boolean deliver(ReadBuf owned, ByteBuffer data, int prefix, int length) {
        int end = prefix + length;
        data.position(0).limit(end);
        if (prefix == 0) {
            upper.message(this, owned, data, 0, end);
            return true;
        }
        return upper.bytes(this, owned, data, 0, end) != GONE;
    }

    @Override public void ended(Transport t, boolean graceful, boolean partial) {
        if (inputEnded) return;
        inputEnded = true;
        upper.ended(this, graceful, partial);
    }
    @Override public void writable(Transport t, boolean on) { upper.writable(this, on && writable()); }
    @Override public void outputShut(Transport t) { upper.outputShut(this); }

    @Override public Batch.Tally accountedWrite(Batch batch, int confirmed) {
        var complete = super.accountedWrite(batch, confirmed);
        return new Batch.Tally(complete.bytes(), complete.records(), Math.max(0, lowerConfirmed - complete.bytes()));
    }

    @Override public void progress(Transport t, Batch b, int confirmed) {
        Batch up = upperBatch;
        if (up == null) return;
        if (stream) up.sealedProgress(confirmed, LEN, Suite.TAG_LEN);
        upper.progress(this, up, up.confirmedRecords(confirmed, recordOverhead()));
    }

    @Override
    public void written(Transport t, Batch b, int confirmed, Throwable cause) {
        b.discard();
        Batch up = upperBatch;
        upperBatch = null;
        if (up != null) {
            int plain = cause == null && confirmed == b.bytes() ? up.bytes()
                    : up.confirmedRecords(confirmed, recordOverhead());
            if (stream) up.sealedProgress(confirmed, LEN, Suite.TAG_LEN);
            if (confirmed < 0 || b.progressUnknown()) up.markProgressUnknown();
            lowerConfirmed = Math.max(0, confirmed);
            if (!output.available() && !isClosed()) upper.writable(this, false);
            try { upper.written(this, up, plain, cause); }
            finally { lowerConfirmed = 0; }
        }
    }

    @Override public boolean failed(Transport t, Throwable error) { return upper.failed(this, error); }
}
