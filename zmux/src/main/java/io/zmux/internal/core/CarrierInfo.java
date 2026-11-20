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
 * @param maxRecord the largest record Size the carrier takes, applied beside the peer's and TLS's; 0 when none
 * @param ownFailure the carrier reports its own failure (its reads or writes fail when its path fails), so a stall
 *     never fails the pipe unless the pipe's own dead timeout says so
 */
public record CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound,
                          DatagramChannel channel, io.zmux.CarrierKind kind, String label, int writeOverhead, String variant, int maxRecord,
                          boolean ownFailure) {
    /** The carrier record floor: every carrier takes records of this Size; a record limit is never below it. */
    public static final int RECORD_FLOOR = 8192;

    public CarrierInfo {
        if (variant == null) variant = "";
    }

    /** The smaller of two record limits, 0 being none. */
    public static int minRecord(int a, int b) {
        return a == 0 ? b : b == 0 ? a : Math.min(a, b);
    }

    /** A carrier without a write overhead. */
    public CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound,
                       DatagramChannel channel, io.zmux.CarrierKind kind, String label) {
        this(framing, tls, minRtt, lanes, queueBound, channel, kind, label, 0, "", 0, false);
    }

    /** A carrier without a datagram channel or a declared kind. */
    public CarrierInfo(Framing framing, boolean tls, boolean minRtt, boolean lanes, boolean queueBound) {
        this(framing, tls, minRtt, lanes, queueBound, null, null, null);
    }

    /** This carrier of the given kind. */
    public CarrierInfo withKind(io.zmux.CarrierKind k) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, k, label, writeOverhead, variant, maxRecord, ownFailure);
    }

    /** This carrier with datagram channel ch. */
    public CarrierInfo withChannel(DatagramChannel ch) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, ch, kind, label, writeOverhead, variant, maxRecord, ownFailure);
    }

    /** This carrier with the application's label l. */
    public CarrierInfo withLabel(String l) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, l, writeOverhead, variant, maxRecord, ownFailure);
    }

    /** This carrier with write overhead n. */
    public CarrierInfo withWriteOverhead(int n) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, n, variant, maxRecord, ownFailure);
    }

    /** This carrier with variant v. */
    public CarrierInfo withVariant(String v) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, writeOverhead, v, maxRecord, ownFailure);
    }

    /** This carrier with record cap n (0: none). */
    public CarrierInfo withMaxRecord(int n) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, writeOverhead, variant, n, ownFailure);
    }

    /** This carrier, reporting its own failure or not. */
    public CarrierInfo withOwnFailure(boolean on) {
        return new CarrierInfo(framing, tls, minRtt, lanes, queueBound, channel, kind, label, writeOverhead, variant, maxRecord, on);
    }
}
