package io.zmux.carrier;

import io.zmux.CarrierKind;
import io.zmux.RecordMode;
import io.zmux.internal.join.RecordProtection;
import io.zmux.internal.loop.PipeIoAccess;
import io.zmux.internal.wire.VarInt;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A pipe input: at most one receiving and one sending half (at least one), owned by zmux and closed under the {@link PipeWriter} linger contract, with
 * optional properties of the carrier. Immutable: each {@code with} method returns a new input.
 *
 * <p>{@link #framing()} says how records travel on the carrier. {@link #label()} is a name for statistics and logs,
 * or null. {@link #carrierConn()} is the identity of the multi-stream connection the pipe is a stream of (compared
 * by identity), or null; an adapter that wraps another pipe input keeps it. {@link #carrier()} is the kind of
 * carrier the pipe runs over, or null (raw TCP or TLS, or unknown); the pipe's events, statistics and
 * {@link io.zmux.shaping.ShapedPipe} report it. {@link #variant()} is a short label of the host for the transform the
 * carrier runs, "" for none, set by a {@link io.zmux.Candidate}, a {@link io.zmux.Admission.Decision} or the host;
 * zmux only reports it, in statistics, {@link io.zmux.shaping.ShapedPipe} and {@link io.zmux.CarrierCache} keys.
 */
public final class PipeIo {
    /** Application error used when a carrier fails without a graceful end. */
    public static final int CARRIER_FAILED = 0x5a4d0003;

    /** The smallest nonzero {@link Capabilities#maxRecord()}: the carrier record floor, so every unsplittable frame fits one record. */
    public static final int MIN_RECORD = 8192;

    /** The largest {@link Capabilities#writeOverhead()}. */
    public static final int MAX_WRITE_OVERHEAD = 65535;

    static {
        PipeIoAccess.set(new PipeIoAccess.Access() {
            @Override
            public PipeIo withEstablished(PipeIo io, RecordMode mode, Exporter exporter, RecordProtection records, Object peer) {
                Objects.requireNonNull(mode, "mode");
                Capabilities c = io.capabilities.toBuilder().exporter(Objects.requireNonNull(exporter, "exporter")).build();
                return new PipeIo(io.reader, io.writer, io.framing, io.label, io.carrierConn, c, io.carrier, io.variant, mode, peer,
                        mode == RecordMode.SEALED ? Objects.requireNonNull(records, "records") : null);
            }

            @Override
            public PipeIo withCarrierPeer(PipeIo io, Object peer) {
                return new PipeIo(io.reader, io.writer, io.framing, io.label, io.carrierConn, io.capabilities, io.carrier, io.variant,
                        io.recordMode, peer, io.records);
            }

            @Override public RecordMode recordMode(PipeIo io) { return io.recordMode; }

            @Override public Object carrierPeer(PipeIo io) { return io.peer; }

            @Override public Object sendingConnection(PipeIo io) {
                return io.carrierConn instanceof SplitConnection pair ? pair.outbound : io.carrierConn;
            }

            @Override public RecordProtection records(PipeIo io) { return io.records; }
        });
    }

    private final PipeReader reader;
    private final PipeWriter writer;
    private final Framing framing;
    private final String label;
    private final Object carrierConn;
    private final Capabilities capabilities;
    private final CarrierKind carrier;
    private final String variant;
    // Set by an Establish that ran on the carrier: the first pipe's record mode, the session's host value, and the
    // record protection of a Sealed carrier.
    private final RecordMode recordMode;
    private final Object peer;
    private final RecordProtection records;

    /**
     * A pipe input of no declared kind, without optional capabilities.
     *
     * @param reader the receiving half, or null
     * @param writer the sending half, or null; one of the two is required
     * @param framing how records travel on the carrier
     * @param label a name for statistics and logs, or null
     * @param carrierConn the identity of the carrier's multi-stream connection, or null
     */
    public PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn) {
        this(reader, writer, framing, label, carrierConn, Capabilities.NONE, null, "", null, null, null);
    }

    private PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn, Capabilities capabilities,
                   CarrierKind carrier, String variant, RecordMode recordMode, Object peer, RecordProtection records) {
        if (reader == null && writer == null) throw new IllegalArgumentException("a pipe input needs a half");
        this.reader = reader;
        this.writer = writer;
        this.framing = Objects.requireNonNull(framing, "framing");
        this.label = label;
        this.carrierConn = carrierConn;
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.carrier = carrier;
        this.variant = variant == null ? "" : variant;
        this.recordMode = recordMode;
        this.peer = peer;
        this.records = records;
    }

    /** A byte-stream pipe input of two halves. */
    public static PipeIo of(PipeReader reader, PipeWriter writer) {
        return new PipeIo(reader, writer, Framing.BYTE_STREAM, null, null);
    }

    public PipeReader reader() { return reader; }

    public PipeWriter writer() { return writer; }

    public Framing framing() { return framing; }

    public String label() { return label; }

    public Object carrierConn() { return carrierConn; }

    public Capabilities capabilities() { return capabilities; }

    public CarrierKind carrier() { return carrier; }

    public String variant() { return variant; }

    /** Whether this carrier declares the Bound contract ({@link #withCarrierSecure}). */
    public boolean isCarrierSecure() { return capabilities.isCarrierSecure(); }

    /** Whether this process terminates this carrier's completed TLS or QUIC handshake ({@link #withTerminatedTls}). */
    public boolean isTerminatedTls() { return capabilities.isTerminatedTls(); }

    private PipeIo with(Framing f, String l, Object conn, Capabilities c, CarrierKind k, String v) {
        return new PipeIo(reader, writer, f, l, conn, c, k, v, recordMode, peer, records);
    }

    /**
     * This pipe input with other halves (one of them non-null) and all else kept: an adapter's way to wrap the halves
     * of another input.
     */
    public PipeIo withHalves(PipeReader reader, PipeWriter writer) {
        return new PipeIo(reader, writer, framing, label, carrierConn, capabilities, carrier, variant, recordMode, peer, records);
    }

    public PipeIo withFraming(Framing f) { return with(f, label, carrierConn, capabilities, carrier, variant); }

    public PipeIo withLabel(String l) { return with(framing, l, carrierConn, capabilities, carrier, variant); }

    public PipeIo withCarrierConn(Object c) { return with(framing, label, c, capabilities, carrier, variant); }

    /** This pipe input with carrier kind k (null: undeclared). */
    public PipeIo withCarrier(CarrierKind k) { return with(framing, label, carrierConn, capabilities, k, variant); }

    /** This pipe input with the host's label of the transform its carrier runs ("" or null: none). */
    public PipeIo withVariant(String v) { return with(framing, label, carrierConn, capabilities, carrier, v); }

    /** This pipe input with capabilities c. */
    public PipeIo withCapabilities(Capabilities c) { return with(framing, label, carrierConn, c, carrier, variant); }

    public PipeIo withLaneOpener(LaneOpener opener) { return withCapabilities(capabilities.toBuilder().laneOpener(opener).build()); }

    public PipeIo withExporter(Exporter exporter) { return withCapabilities(capabilities.toBuilder().exporter(exporter).build()); }

    public PipeIo withReceiveMemory(long bytes) { return withCapabilities(capabilities.toBuilder().receiveMemory(bytes).build()); }

    /** The authenticated application protocol selected by the carrier handshake. */
    public PipeIo withProtocol(String protocol) { return withCapabilities(capabilities.toBuilder().applicationProtocol(protocol).build()); }

    /** The server name (SNI) the client sent in the carrier's TLS or QUIC handshake. */
    public PipeIo withServerName(String name) { return withCapabilities(capabilities.toBuilder().serverName(name).build()); }

    /** A pooled connection's default wire fate group, used when join options leave it unset. */
    public PipeIo withFateGroup(long group) { return withCapabilities(capabilities.toBuilder().fateGroup(group).build()); }

    /**
     * The fixed number of bytes each write on the carrier adds below it (a wrapping layer's record header and tag,
     * say), 0 to {@link #MAX_WRITE_OVERHEAD}; handed to shaping as {@link io.zmux.shaping.ShapedPipe#writeOverhead()}.
     *
     * @throws IllegalArgumentException outside that range
     */
    public PipeIo withWriteOverhead(int bytes) { return withCapabilities(capabilities.toBuilder().writeOverhead(bytes).build()); }

    /**
     * The largest record the carrier takes, at least {@link #MIN_RECORD} bytes of Size (0: the carrier's default): zmux
     * builds this pipe's records, on each of its lanes, and sizes datagrams within it, beside the peer's
     * {@code max_record_size}, a TLS carrier's record limit and zmux's own (the smallest applies). Under a transform
     * with record overhead (Sealed) the limit less the overhead must still reach {@link #MIN_RECORD}, or adding the
     * pipe fails. For a carrier with a small datagram or message size, say.
     *
     * @throws IllegalArgumentException neither 0 nor at least {@link #MIN_RECORD}
     */
    public PipeIo withMaxRecord(int bytes) { return withCapabilities(capabilities.toBuilder().maxRecord(bytes).build()); }

    /**
     * Whether this process terminates the carrier's TLS or QUIC handshake and it has completed (a carrier whose
     * handshake has not completed counts as without TLS). It is no proof of an end-to-end handshake: an intermediary
     * may have opened this one. The exporter is {@link Capabilities#exporter()} alone.
     */
    public PipeIo withTerminatedTls(boolean terminated) { return withCapabilities(capabilities.toBuilder().terminatedTls(terminated).build()); }

    /**
     * Whether this carrier meets the Bound contract, record confidentiality and integrity included; an exporter alone
     * is no such declaration.
     */
    public PipeIo withCarrierSecure() { return withCapabilities(capabilities.toBuilder().carrierSecure(true).build()); }

    /**
     * Declares that this carrier reports its own failure: when the path under it fails, its reads or writes fail (an
     * error, a reset, an unexpected end), as a stream of an outer zmux session does when that session's liveness ends
     * it ({@link io.zmux.ZmuxSession#openPipeStream}). Stall detection then never fails the pipe: a carrier queued behind
     * a rate limit stalls without being dead, and its own end is the failure signal. The pipe still turns suspect and is
     * avoided and probed while it stalls; the pipe's own {@link io.zmux.PipeLiveness.Builder#deadTimeout} overrides it.
     * A transform may keep it when it fails whenever the carrier below it fails; {@link #split} keeps it only when both
     * halves report it.
     */
    public PipeIo withReportsFailure() { return withCapabilities(capabilities.toBuilder().reportsFailure(true).build()); }

    /** The padding bytes a handshake run on the carrier before this input added; the session meters them. */
    public PipeIo withHandshakePadding(long bytes) { return withCapabilities(capabilities.toBuilder().handshakePadding(bytes).build()); }

    @Override
    public String toString() {
        return "PipeIo[" + framing + (label == null ? "" : ", " + label) + (carrier == null ? "" : ", " + carrier)
                + (variant.isEmpty() ? "" : ", " + variant) + (reader == null ? ", send only" : writer == null ? ", receive only" : "") + "]";
    }

    /** Synthetic identity for charging and nesting a split across connections, with its sender identified separately. */
    // Identity matters: two splits over the same sender are distinct connections, so this cannot be a record.
    @SuppressWarnings("ClassCanBeRecord")
    private static final class SplitConnection {
        final Object outbound;

        SplitConnection(Object outbound) { this.outbound = outbound; }
    }

    /**
     * Whether uplink and downlink are opposite one-way halves (one only reads, the other only writes) with the same
     * framing and application protocol.
     */
    private static boolean complementary(PipeIo uplink, PipeIo downlink) {
        boolean upReads = uplink.reader != null && uplink.writer == null;
        boolean upWrites = uplink.reader == null && uplink.writer != null;
        boolean downReads = downlink.reader != null && downlink.writer == null;
        boolean downWrites = downlink.reader == null && downlink.writer != null;
        return (upReads || upWrites) && upReads == downWrites && upWrites == downReads
                && uplink.framing == downlink.framing
                && uplink.capabilities.applicationProtocol().equals(downlink.capabilities.applicationProtocol());
    }

    /**
     * Combines complementary confidential halves; arguments are in initiator uplink/downlink order. What describes the
     * writing goes with the half this end writes on; the fate group, label and variant are the uplink's.
     */
    public static PipeIo split(PipeIo uplink, PipeIo downlink) {
        Objects.requireNonNull(uplink);
        Objects.requireNonNull(downlink);
        if (!complementary(uplink, downlink)) {
            throw new IllegalArgumentException("split bootstrap needs complementary halves with matching framing and protocol");
        }
        boolean upReads = uplink.reader != null, upWrites = uplink.writer != null; // exactly one holds
        Capabilities uc = uplink.capabilities, dc = downlink.capabilities;
        // One connection has one value; two connections give the uplink's value followed by the downlink's.
        Exporter up = uc.exporter(), down = dc.exporter();
        Exporter exporter = up == null && down == null ? null : (label, context, length) -> {
            byte[] u = up == null ? null : up.export(label, context, length);
            if (up != null && u == null) return null;
            byte[] d = down == null ? null : down.export(label, context, length);
            if (down != null && d == null) return null;
            if (u == null) return d;
            if (d == null || Arrays.equals(u, d)) return u;
            byte[] joined = Arrays.copyOf(u, u.length + d.length);
            System.arraycopy(d, 0, joined, u.length, d.length);
            return joined;
        };
        boolean same = uplink.carrierConn == downlink.carrierConn;
        long memory = same ? Math.max(uc.receiveMemory(), dc.receiveMemory()) : Math.addExact(uc.receiveMemory(), dc.receiveMemory());
        Capabilities caps = Capabilities.builder().exporter(exporter).receiveMemory(memory).applicationProtocol(uc.applicationProtocol())
                .serverName(uc.serverName()).fateGroup(uc.fateGroup()).writeOverhead((upWrites ? uc : dc).writeOverhead()).maxRecord((upWrites ? uc : dc).maxRecord())
                .terminatedTls(uc.isTerminatedTls() && dc.isTerminatedTls())
                .carrierSecure(uc.isCarrierSecure() && dc.isCarrierSecure() && up != null && down != null)
                // Both halves must report it: a plain half (a raw connection) can stall silently beside one that does.
                .reportsFailure(uc.reportsFailure() && dc.reportsFailure()).build();
        return new PipeIo(upReads ? uplink.reader : downlink.reader, upWrites ? uplink.writer : downlink.writer,
                uplink.framing, uplink.label, same ? uplink.carrierConn : new SplitConnection((upWrites ? uplink : downlink).carrierConn), caps, CarrierKind.SPLIT, uplink.variant,
                null, null, null);
    }

    /**
     * Opens an independent, send-only lane on the same carrier connection. A lane is a sending half, so the opener
     * returns its writer; the lane shares the pipe's framing and connection.
     */
    @FunctionalInterface
    public interface LaneOpener {
        CompletionStage<PipeWriter> open();
    }

    /**
     * The exporter of the carrier's innermost end-to-end handshake (RFC 8446 Section 7.5): the one source of its
     * keying material, read when it is needed (a session reads it when the peer's parameters arrive, a join on its
     * own carrier). It returns null while that handshake is incomplete, may be called from any thread, and must not
     * block.
     */
    @FunctionalInterface
    public interface Exporter {
        byte[] export(String label, byte[] context, int length);
    }

    /**
     * Optional capabilities of a carrier; {@link #builder()} starts from none. The receive-memory estimate is shared
     * by connection identity, not per stream. Immutable.
     */
    public static final class Capabilities {
        static final Capabilities NONE = new Builder().build();

        private final LaneOpener laneOpener;
        private final Exporter exporter;
        private final long receiveMemory;
        private final String applicationProtocol;
        private final String serverName;
        private final long fateGroup;
        private final int writeOverhead;
        private final int maxRecord;
        private final boolean terminatedTls;
        private final boolean secure;
        private final boolean reportsFailure;
        private final long handshakePadding;

        private Capabilities(Builder b) {
            laneOpener = b.laneOpener;
            exporter = b.exporter;
            receiveMemory = b.receiveMemory;
            applicationProtocol = b.applicationProtocol;
            serverName = b.serverName;
            fateGroup = b.fateGroup;
            writeOverhead = b.writeOverhead;
            maxRecord = b.maxRecord;
            terminatedTls = b.terminatedTls;
            secure = b.secure;
            reportsFailure = b.reportsFailure;
            handshakePadding = b.handshakePadding;
        }

        public static Builder builder() { return new Builder(); }

        /** A builder holding these capabilities. */
        public Builder toBuilder() {
            Builder b = new Builder();
            b.laneOpener = laneOpener;
            b.exporter = exporter;
            b.receiveMemory = receiveMemory;
            b.applicationProtocol = applicationProtocol;
            b.serverName = serverName;
            b.fateGroup = fateGroup;
            b.writeOverhead = writeOverhead;
            b.maxRecord = maxRecord;
            b.terminatedTls = terminatedTls;
            b.secure = secure;
            b.reportsFailure = reportsFailure;
            b.handshakePadding = handshakePadding;
            return b;
        }

        /** Opens further lanes on the carrier's connection; null: none. */
        public LaneOpener laneOpener() { return laneOpener; }

        /** The exporter of the carrier's innermost end-to-end handshake; null: none. */
        public Exporter exporter() { return exporter; }

        /** The receive memory the reader declares, in bytes: what it may deliver before earlier batches are released. */
        public long receiveMemory() { return receiveMemory; }

        /** The application protocol the carrier's TLS or QUIC handshake negotiated; "" without one. */
        public String applicationProtocol() { return applicationProtocol; }

        /** The server name (SNI) the client sent in the carrier's TLS or QUIC handshake; "" without one. */
        public String serverName() { return serverName; }

        /** The default wire fate group of a pooled connection; 0: none. */
        public long fateGroup() { return fateGroup; }

        /**
         * The fixed number of bytes each write on the carrier adds below it, 0 to {@link #MAX_WRITE_OVERHEAD}: a hint
         * zmux hands to shaping ({@link io.zmux.shaping.ShapedPipe#writeOverhead()}) and does not check.
         */
        public int writeOverhead() { return writeOverhead; }

        /** The largest record Size the carrier takes, 0 or at least {@link #MIN_RECORD}; 0: the carrier's default. */
        public int maxRecord() { return maxRecord; }

        /** See {@link PipeIo#withTerminatedTls}. */
        public boolean isTerminatedTls() { return terminatedTls; }

        /** See {@link PipeIo#withCarrierSecure}. */
        public boolean isCarrierSecure() { return secure; }

        /** See {@link PipeIo#withReportsFailure}. */
        public boolean reportsFailure() { return reportsFailure; }

        /** The padding bytes a handshake run on the carrier before this input added, which the session meters. */
        public long handshakePadding() { return handshakePadding; }

        /** Builds {@link Capabilities}; every setting starts at none. */
        public static final class Builder {
            private LaneOpener laneOpener;
            private Exporter exporter;
            private long receiveMemory;
            private String applicationProtocol = "";
            private String serverName = "";
            private long fateGroup;
            private int writeOverhead;
            private int maxRecord;
            private boolean terminatedTls;
            private boolean secure;
            private boolean reportsFailure;
            private long handshakePadding;

            private Builder() {}

            public Builder laneOpener(LaneOpener o) { laneOpener = o; return this; }

            public Builder exporter(Exporter e) { exporter = e; return this; }

            /**
             * The carrier's receive memory in bytes.
             *
             * @throws IllegalArgumentException when negative
             */
            public Builder receiveMemory(long bytes) {
                if (bytes < 0) throw new IllegalArgumentException("negative receiveMemory");
                receiveMemory = bytes;
                return this;
            }

            public Builder applicationProtocol(String p) { applicationProtocol = Objects.requireNonNull(p, "applicationProtocol"); return this; }

            public Builder serverName(String n) { serverName = Objects.requireNonNull(n, "serverName"); return this; }

            /**
             * The carrier's fate group.
             *
             * @throws IllegalArgumentException outside a varint62
             */
            public Builder fateGroup(long g) {
                if (g < 0 || g > VarInt.MAX) throw new IllegalArgumentException("fateGroup exceeds varint62");
                fateGroup = g;
                return this;
            }

            /**
             * The bytes the carrier adds to each write.
             *
             * @throws IllegalArgumentException outside [0, {@link #MAX_WRITE_OVERHEAD}]
             */
            public Builder writeOverhead(int bytes) {
                if (bytes < 0 || bytes > MAX_WRITE_OVERHEAD)
                    throw new IllegalArgumentException("writeOverhead " + bytes + " outside [0, " + MAX_WRITE_OVERHEAD + "]");
                writeOverhead = bytes;
                return this;
            }

            /**
             * The largest record Size the carrier takes; 0: the carrier's default.
             *
             * @throws IllegalArgumentException neither 0 nor at least {@link #MIN_RECORD}
             */
            public Builder maxRecord(int bytes) {
                if (bytes != 0 && bytes < MIN_RECORD)
                    throw new IllegalArgumentException("maxRecord " + bytes + " is neither 0 nor at least " + MIN_RECORD);
                maxRecord = bytes;
                return this;
            }

            public Builder terminatedTls(boolean on) { terminatedTls = on; return this; }

            public Builder carrierSecure(boolean on) { secure = on; return this; }

            public Builder reportsFailure(boolean on) { reportsFailure = on; return this; }

            /**
             * The padding bytes a handshake run on the carrier before this input added.
             *
             * @throws IllegalArgumentException when negative
             */
            public Builder handshakePadding(long bytes) {
                if (bytes < 0) throw new IllegalArgumentException("negative handshakePadding");
                handshakePadding = bytes;
                return this;
            }

            public Capabilities build() { return new Capabilities(this); }
        }
    }
}
