package io.zmux.carrier;

import java.util.Objects;

/**
 * A pipe input: at most one receiving and one sending half (at least one), each closed by
 * zmux exactly once, with optional properties of the carrier.
 *
 * @param framing how records travel on the carrier
 * @param label a name for statistics and logs, or null
 * @param carrierConn the identity of the multi-stream connection the pipe is a stream of
 *     (compared by identity), or null; an adapter that wraps another pipe input keeps it
 * @param carrier the kind of carrier the pipe runs over, or null (raw TCP or TLS, or unknown); the
 *     pipe's events, statistics and {@link io.zmux.shaping.ShapedPipe} report it
 * @param variant a short label of the host for the transform the carrier runs, "" for none, set by a
 *     {@link io.zmux.Candidate}, a {@link io.zmux.Admission.Decision} or the host; zmux only
 *     reports it, in statistics, {@link io.zmux.shaping.ShapedPipe} and {@link io.zmux.CarrierCache} keys
 */
public record PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn, Capabilities capabilities,
                     io.zmux.CarrierKind carrier, String variant) {

    /** Application error used when a carrier fails without a graceful end. */
    public static final int CARRIER_FAILED = 0x5a4d0003;

    public PipeIo {
        if (reader == null && writer == null) throw new IllegalArgumentException("a pipe input needs a half");
        Objects.requireNonNull(framing, "framing");
        Objects.requireNonNull(capabilities, "capabilities");
        if (variant == null) variant = "";
    }

    /** Opens an independent, send-only lane on the same carrier connection. */
    @FunctionalInterface
    public interface LaneOpener { java.util.concurrent.CompletionStage<PipeIo> open(); }

    /**
     * The exporter of the carrier's innermost end-to-end handshake (RFC 8446 Section 7.5): the
     * one source of its keying material, read when it is needed (a session reads it when the
     * peer's parameters arrive, a join on its own carrier). It returns null while that handshake
     * is incomplete, may be called from any thread, and must not block.
     */
    @FunctionalInterface
    public interface Exporter {
        byte[] export(String label, byte[] context, int length);
    }

    /**
     * Optional capabilities. The receive-memory estimate is shared by connection identity, not
     * per stream. applicationProtocol and serverName are those of the carrier's TLS or QUIC
     * handshake ("" without one): the negotiated protocol and the server name (SNI) the
     * client sent. writeOverhead is the fixed number of bytes each write on the carrier adds
     * below it, 0 to {@link #MAX_WRITE_OVERHEAD}: a hint zmux hands to shaping
     * ({@link io.zmux.shaping.ShapedPipe#writeOverhead()}) and does not check. terminatedTls: see
     * {@link PipeIo#withTerminatedTls}. secure: see {@link PipeIo#secure()}. recordMode, peer and seal
     * come from a carrier transform that ran an Establish: see {@link PipeIo#withEstablished}. handshakePadding is the
     * padding bytes a handshake run on the carrier before this input added, which the session meters: see
     * {@link PipeIo#withHandshakePadding}.
     */
    public record Capabilities(LaneOpener laneOpener, Exporter exporter, long receiveMemory, String applicationProtocol,
                               String serverName, long fateGroup, int writeOverhead, boolean terminatedTls, boolean secure,
                               io.zmux.RecordMode recordMode, Object peer, RecordProtection seal, long handshakePadding) {
        public static final Capabilities NONE = new Capabilities(null, null, 0, "", "", 0, 0, false, false, null, null, null, 0);
        public Capabilities {
            Objects.requireNonNull(applicationProtocol); Objects.requireNonNull(serverName);
            if (handshakePadding < 0) throw new IllegalArgumentException("negative handshakePadding");
            if (receiveMemory < 0) throw new IllegalArgumentException("negative receiveMemory");
            if (fateGroup < 0 || fateGroup > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("fateGroup exceeds varint62");
            if (writeOverhead < 0 || writeOverhead > MAX_WRITE_OVERHEAD)
                throw new IllegalArgumentException("writeOverhead " + writeOverhead + " outside [0, " + MAX_WRITE_OVERHEAD + "]");
        }

    }

    /** The largest {@link Capabilities#writeOverhead()}. */
    public static final int MAX_WRITE_OVERHEAD = 65535;

    /** A carrier of no declared kind. */
    public PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn, Capabilities capabilities) {
        this(reader, writer, framing, label, carrierConn, capabilities, null, "");
    }

    /** A carrier without a variant. */
    public PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn, Capabilities capabilities,
                  io.zmux.CarrierKind carrier) {
        this(reader, writer, framing, label, carrierConn, capabilities, carrier, "");
    }

    /** A carrier of no declared kind without optional capabilities. */
    public PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn) {
        this(reader, writer, framing, label, carrierConn, Capabilities.NONE, null, "");
    }

    /** This pipe input with carrier kind k (null: undeclared). */
    public PipeIo withCarrier(io.zmux.CarrierKind k) {
        return new PipeIo(reader, writer, framing, label, carrierConn, capabilities, k, variant);
    }

    public PipeIo withLaneOpener(LaneOpener opener) {
        var c = capabilities;
        return withCapabilities(new Capabilities(opener, c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    public PipeIo withExporter(Exporter exporter) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), exporter, c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    public PipeIo withReceiveMemory(long bytes) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), bytes, c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /** The authenticated application protocol selected by the carrier handshake. */
    public PipeIo withProtocol(String protocol) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), protocol, c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /** The server name (SNI) the client sent in the carrier's TLS or QUIC handshake. */
    public PipeIo withServerName(String name) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), name, c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /** A pooled connection's default wire fate group, used when join options leave it unset. */
    public PipeIo withFateGroup(long group) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), group, c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /**
     * The fixed number of bytes each write on the carrier adds below it (a wrapping layer's
     * record header and tag, say), 0 to {@link #MAX_WRITE_OVERHEAD}; handed to shaping as
     * {@link io.zmux.shaping.ShapedPipe#writeOverhead()}.
     *
     * @throws IllegalArgumentException outside that range
     */
    public PipeIo withWriteOverhead(int bytes) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), bytes, c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /**
     * Whether this process terminates the carrier's TLS or QUIC handshake and it has completed
     * (a carrier whose handshake has not completed counts as without TLS). It is no proof of an
     * end-to-end handshake: an intermediary may have opened this one. The exporter is
     * {@link Capabilities#exporter()} alone.
     */
    public PipeIo withTerminatedTls(boolean terminated) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), terminated, c.secure(), c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /**
     * Declares that this carrier meets the Bound contract, record confidentiality
     * and integrity included; an exporter alone is no such declaration.
     */
    public PipeIo secure() {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), true, c.recordMode(), c.peer(), c.seal(), c.handshakePadding()));
    }

    /** Whether this carrier declares the Bound contract ({@link #secure()}). */
    public boolean isSecure() { return capabilities.secure(); }

    /**
     * This carrier after an Establish ran on it: a secure first pipe with the
     * handshake's record mode. exporter is the handshake's, records protects the records when the mode is
     * Sealed (ignored otherwise), and peer (or null) is the session's host value.
     */
    public PipeIo withEstablished(io.zmux.RecordMode mode, Exporter exporter, RecordProtection records, Object peer) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), Objects.requireNonNull(exporter, "exporter"), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), Objects.requireNonNull(mode, "mode"), peer,
                mode == io.zmux.RecordMode.SEALED ? Objects.requireNonNull(records, "records") : null, c.handshakePadding()));
    }

    /** The padding bytes a handshake run on the carrier before this input added; the session meters them. */
    public PipeIo withHandshakePadding(long bytes) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead(), c.terminatedTls(), c.secure(), c.recordMode(), c.peer(), c.seal(), bytes));
    }

    /** Whether this process terminates this carrier's completed TLS or QUIC handshake ({@link #withTerminatedTls}). */
    public boolean terminatedTls() { return capabilities.terminatedTls(); }

    /** This pipe input with the host's label of the transform its carrier runs ("" or null: none). */
    public PipeIo withVariant(String v) {
        return new PipeIo(reader, writer, framing, label, carrierConn, capabilities, carrier, v);
    }

    /** This pipe input with capabilities c. */
    public PipeIo withCapabilities(Capabilities c) {
        return new PipeIo(reader, writer, framing, label, carrierConn, c, carrier, variant);
    }

    /**
     * Whether uplink and downlink are opposite one-way halves (one only reads, the other only
     * writes) with the same framing and application protocol.
     */
    private static boolean complementary(PipeIo uplink, PipeIo downlink) {
        boolean upReads = uplink.reader() != null && uplink.writer() == null;
        boolean upWrites = uplink.reader() == null && uplink.writer() != null;
        boolean downReads = downlink.reader() != null && downlink.writer() == null;
        boolean downWrites = downlink.reader() == null && downlink.writer() != null;
        return (upReads || upWrites) && upReads == downWrites && upWrites == downReads
                && uplink.framing() == downlink.framing()
                && uplink.capabilities().applicationProtocol().equals(downlink.capabilities().applicationProtocol());
    }

    /**
     * Combines complementary confidential halves; arguments are in initiator uplink/downlink order. What describes the
     * writing goes with the half this end writes on; the fate group, label and variant are the uplink's.
     */
    public static PipeIo split(PipeIo uplink, PipeIo downlink) {
        Objects.requireNonNull(uplink); Objects.requireNonNull(downlink);
        if (!complementary(uplink, downlink)) {
            throw new IllegalArgumentException("split bootstrap needs complementary halves with matching framing and protocol");
        }
        boolean upReads = uplink.reader() != null, upWrites = uplink.writer() != null; // exactly one holds
        // One connection has one value; two connections give the uplink's value followed by the downlink's.
        Exporter up = uplink.capabilities().exporter(), down = downlink.capabilities().exporter();
        Exporter exporter = up == null && down == null ? null : (label, context, length) -> {
            byte[] u = up == null ? null : up.export(label, context, length);
            if (up != null && u == null) return null;
            byte[] d = down == null ? null : down.export(label, context, length);
            if (down != null && d == null) return null;
            if (u == null) return d;
            if (d == null || java.util.Arrays.equals(u, d)) return u;
            byte[] joined = java.util.Arrays.copyOf(u, u.length + d.length);
            System.arraycopy(d, 0, joined, u.length, d.length);
            return joined;
        };
        boolean same = uplink.carrierConn() == downlink.carrierConn();
        long memory = same ? Math.max(uplink.capabilities().receiveMemory(), downlink.capabilities().receiveMemory())
                : Math.addExact(uplink.capabilities().receiveMemory(), downlink.capabilities().receiveMemory());
        return new PipeIo(upReads ? uplink.reader() : downlink.reader(), upWrites ? uplink.writer() : downlink.writer(),
                uplink.framing(), uplink.label(), same ? uplink.carrierConn() : new Object(),
                new Capabilities(null, exporter, memory, uplink.capabilities().applicationProtocol(), uplink.capabilities().serverName(), uplink.capabilities().fateGroup(),
                        (upWrites ? uplink : downlink).capabilities().writeOverhead(),
                        uplink.terminatedTls() && downlink.terminatedTls(), uplink.isSecure() && downlink.isSecure() && up != null && down != null, null, null, null, 0),
                io.zmux.CarrierKind.SPLIT, uplink.variant());
    }

    /** Transfers a bidirectional logical stream to a carrier, without blocking forwarding threads. */
    public static PipeIo ofStream(io.zmux.ZmuxStream stream) { return Objects.requireNonNull(stream).asPipe(); }

    /** A byte-stream pipe input of two halves. */
    public static PipeIo of(PipeReader reader, PipeWriter writer) {
        return new PipeIo(reader, writer, Framing.BYTE_STREAM, null, null);
    }

    public PipeIo withFraming(Framing f) {
        return new PipeIo(reader, writer, f, label, carrierConn, capabilities, carrier, variant);
    }

    public PipeIo withLabel(String l) {
        return new PipeIo(reader, writer, framing, l, carrierConn, capabilities, carrier, variant);
    }

    public PipeIo withCarrierConn(Object c) {
        return new PipeIo(reader, writer, framing, label, c, capabilities, carrier, variant);
    }
}
