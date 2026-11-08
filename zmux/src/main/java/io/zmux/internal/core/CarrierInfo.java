package io.zmux.internal.core;

/**
 * A carrier half as the core sees it.
 *
 * @param tls build records of at most 16382 bytes of frames
 * @param minRtt the carrier provides a minimum RTT; without it the first record carries
 *     a QX_PING request 
 * @param lanes the carrier offers lanes
 * @param queueBound the carrier bounds its own queue (low-water mark or unsent count);
 *     otherwise the carrier queue limiter paces it
 * @param channel the carrier's datagram channel, or null
 * @param kind the carrier kind its adapter declared, or null
 * @param label the label the application gave the carrier, or null
 * @param writeOverhead the fixed bytes each carrier write adds below the pipe, for the shaper factory
 * @param variant the host's label of the transform the carrier runs, "" when none
 */
public record CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound,
                          DatagramChannel channel, io.zmux.CarrierKind kind, String label, int writeOverhead, String variant, int maxRecord) {
    public CarrierInfo {
        if (variant == null) variant = "";
    }

    /** A carrier without a write overhead. */
    public CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound,
                       DatagramChannel channel, io.zmux.CarrierKind kind, String label) {
        this(framing, tls, minRtt, lanes, queueBound, channel, kind, label, 0, "", 0);
    }

    /** A carrier without a datagram channel or a declared kind. */
    public CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound) {
        this(framing, tls, minRtt, lanes, queueBound, null, null, null);
    }

    /** This carrier of the given kind. */
    public CarrierInfo withKind(io.zmux.CarrierKind k) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, k, label, writeOverhead, variant, maxRecord);
    }

    /** This carrier with datagram channel ch. */
    public CarrierInfo withChannel(DatagramChannel ch) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, ch, kind, label, writeOverhead, variant, maxRecord);
    }

    /** This carrier with the application's label l. */
    public CarrierInfo withLabel(String l) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, l, writeOverhead, variant, maxRecord);
    }

    /** This carrier with write overhead n. */
    public CarrierInfo withWriteOverhead(int n) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, n, variant, maxRecord);
    }

    /** This carrier with variant v. */
    public CarrierInfo withVariant(String v) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, writeOverhead, v, maxRecord);
    }
}
