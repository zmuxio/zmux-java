package io.zmux.carrier;

import java.util.Objects;

/**
 * A pipe input: at most one receiving and one sending half (at least one), each closed by
 * zmux exactly once, with optional properties of the carrier (API.md Section 4.1).
 *
 * @param framing how records travel on the carrier
 * @param label a name for statistics and logs, or null
 * @param carrierConn the identity of the multi-stream connection the pipe is a stream of
 *     (compared by identity), or null; an adapter that wraps another pipe input keeps it
 * @param carrier the kind of carrier the pipe runs over, or null (raw TCP or TLS, or unknown); the
 *     pipe's events, statistics and {@link io.zmux.shaping.ShapedPipe} report it
 */
public record PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn, Capabilities capabilities,
                     io.zmux.CarrierKind carrier) {

    /** Application error used when a carrier fails without a graceful end. */
    public static final int CARRIER_FAILED = 0x5a4d0003;

    public PipeIo {
        if (reader == null && writer == null) throw new IllegalArgumentException("a pipe input needs a half");
        Objects.requireNonNull(framing, "framing");
        Objects.requireNonNull(capabilities, "capabilities");
    }

    /** Opens an independent, send-only lane on the same carrier connection. */
    @FunctionalInterface
    public interface LaneOpener { java.util.concurrent.CompletionStage<PipeIo> open(); }

    /** Derives carrier-bound keys after authentication; null means unavailable. */
    @FunctionalInterface
    public interface Exporter {
        byte[] export(String label, byte[] context, int length);
        default int sessionLength() { return 32; }
    }

    /**
     * Optional capabilities. The receive-memory estimate is shared by connection identity, not
     * per stream. applicationProtocol and serverName are those of the carrier's TLS or QUIC
     * handshake ("" without one): the negotiated protocol and the server name (SNI) the
     * client sent. writeOverhead is the fixed number of bytes each write on the carrier adds
     * below it, 0 to {@link #MAX_WRITE_OVERHEAD}: a hint zmux hands to shaping
     * ({@link io.zmux.shaping.ShapedPipe#writeOverhead()}) and does not check.
     */
    public record Capabilities(LaneOpener laneOpener, Exporter exporter, long receiveMemory, String applicationProtocol,
                               String serverName, long fateGroup, int writeOverhead) {
        public static final Capabilities NONE = new Capabilities(null, null, 0, "", "", 0, 0);
        public Capabilities {
            Objects.requireNonNull(applicationProtocol); Objects.requireNonNull(serverName);
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
        this(reader, writer, framing, label, carrierConn, capabilities, null);
    }

    /** A carrier of no declared kind without optional capabilities. */
    public PipeIo(PipeReader reader, PipeWriter writer, Framing framing, String label, Object carrierConn) {
        this(reader, writer, framing, label, carrierConn, Capabilities.NONE, null);
    }

    /** This pipe input with carrier kind k (null: undeclared). */
    public PipeIo withCarrier(io.zmux.CarrierKind k) {
        return new PipeIo(reader, writer, framing, label, carrierConn, capabilities, k);
    }

    public PipeIo withLaneOpener(LaneOpener opener) {
        var c = capabilities;
        return withCapabilities(new Capabilities(opener, c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead()));
    }

    public PipeIo withExporter(Exporter exporter) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), exporter, c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead()));
    }

    public PipeIo withReceiveMemory(long bytes) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), bytes, c.applicationProtocol(), c.serverName(), c.fateGroup(), c.writeOverhead()));
    }

    /** The authenticated application protocol selected by the carrier handshake. */
    public PipeIo withProtocol(String protocol) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), protocol, c.serverName(), c.fateGroup(), c.writeOverhead()));
    }

    /** The server name (SNI) the client sent in the carrier's TLS or QUIC handshake. */
    public PipeIo withServerName(String name) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), name, c.fateGroup(), c.writeOverhead()));
    }

    /** A pooled connection's default wire fate group, used when join options leave it unset. */
    public PipeIo withFateGroup(long group) {
        var c = capabilities;
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), group, c.writeOverhead()));
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
        return withCapabilities(new Capabilities(c.laneOpener(), c.exporter(), c.receiveMemory(), c.applicationProtocol(), c.serverName(), c.fateGroup(), bytes));
    }

    /** This pipe input with capabilities c. */
    public PipeIo withCapabilities(Capabilities c) {
        return new PipeIo(reader, writer, framing, label, carrierConn, c, carrier);
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

    /** Combines complementary confidential halves; arguments are in initiator uplink/downlink order. */
    public static PipeIo split(PipeIo uplink, PipeIo downlink) {
        Objects.requireNonNull(uplink); Objects.requireNonNull(downlink);
        if (!complementary(uplink, downlink)) {
            throw new IllegalArgumentException("split bootstrap needs complementary halves with matching framing and protocol");
        }
        boolean upReads = uplink.reader() != null, upWrites = uplink.writer() != null; // exactly one holds
        Exporter exporter = new Exporter() {
            @Override public int sessionLength() { return 64; }
            @Override public byte[] export(String label, byte[] context, int length) {
                if (length != 64 || !io.zmux.internal.wire.JoinCrypto.EXPORTER_SESSION.equals(label)
                        || uplink.capabilities().exporter() == null || downlink.capabilities().exporter() == null) return null;
                byte[] up = uplink.capabilities().exporter().export(label, context, 32);
                byte[] down = downlink.capabilities().exporter().export(label, context, 32);
                if (up == null || up.length != 32 || down == null || down.length != 32) return null;
                byte[] joined = new byte[64]; System.arraycopy(up, 0, joined, 0, 32); System.arraycopy(down, 0, joined, 32, 32);
                return joined;
            }
        };
        boolean same = uplink.carrierConn() == downlink.carrierConn();
        long memory = same ? Math.max(uplink.capabilities().receiveMemory(), downlink.capabilities().receiveMemory())
                : Math.addExact(uplink.capabilities().receiveMemory(), downlink.capabilities().receiveMemory());
        return new PipeIo(upReads ? uplink.reader() : downlink.reader(), upWrites ? uplink.writer() : downlink.writer(),
                uplink.framing(), uplink.label(), same ? uplink.carrierConn() : new Object(),
                new Capabilities(null, exporter, memory, uplink.capabilities().applicationProtocol(), uplink.capabilities().serverName(), same && uplink.capabilities().fateGroup() == downlink.capabilities().fateGroup() ? uplink.capabilities().fateGroup() : 0,
                        (upWrites ? uplink : downlink).capabilities().writeOverhead()),
                uplink.carrier() != null ? uplink.carrier() : downlink.carrier());
    }

    /** Transfers a bidirectional logical stream to a carrier, without blocking forwarding threads. */
    public static PipeIo ofStream(io.zmux.ZmuxStream stream) { return Objects.requireNonNull(stream).asPipe(); }

    /** A byte-stream pipe input of two halves. */
    public static PipeIo of(PipeReader reader, PipeWriter writer) {
        return new PipeIo(reader, writer, Framing.BYTE_STREAM, null, null);
    }

    public PipeIo withFraming(Framing f) {
        return new PipeIo(reader, writer, f, label, carrierConn, capabilities, carrier);
    }

    public PipeIo withLabel(String l) {
        return new PipeIo(reader, writer, framing, l, carrierConn, capabilities, carrier);
    }

    public PipeIo withCarrierConn(Object c) {
        return new PipeIo(reader, writer, framing, label, c, capabilities, carrier);
    }
}
