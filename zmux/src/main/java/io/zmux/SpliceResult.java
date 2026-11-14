package io.zmux;

/**
 * How a splice ended: per direction the stream bytes and datagram bytes forwarded (together what
 * was charged to the direction's limiter), the datagrams dropped, and the event that ended it. A queued datagram is
 * not an acknowledgement of peer delivery. Cancelling the stage is the cancellation of the splice: it then has no
 * result; a cancellation through {@link SpliceControl#cancel(long)} has one.
 */
public record SpliceResult(Direction aToB, Direction bToA) {
    /** How one direction ended. */
    public sealed interface End {
        /** FIN read upstream, and the downstream stream finished after the data before it. */
        record Finished() implements End {}

        /** The upstream stream was reset with code; the downstream stream was reset with it. */
        record Reset(long code) implements End {}

        /** The downstream stream was stopped with code; the upstream stream was stopped with it. */
        record Stopped(long code) implements End {}

        /** The upstream stream's session closed or was lost. */
        record UpstreamLost() implements End {}

        /** The downstream stream's session closed or was lost. */
        record DownstreamLost() implements End {}

        /** The splice was cancelled through its {@link SpliceControl}: both streams were reset and stopped with code. */
        record Cancelled(long code) implements End {}
    }

    /**
     * One direction's counters.
     *
     * @param streamBytes stream bytes forwarded
     * @param datagramBytes datagram payload bytes forwarded (by a packet relay also those it wrote as capsules)
     * @param datagramsDropped every datagram received and not forwarded
     * @param datagramsTooLarge those of datagramsDropped above the next hop's {@link ZmuxStream#maxDatagramPayload(long)}
     *     (0 without datagrams there); a packet relay writes them as capsules instead and counts here only those it could not
     * @param datagramsLimited those of datagramsDropped the direction's or the next hop's datagram limiter refused
     */
    public record Direction(long streamBytes, long datagramBytes, long datagramsDropped, long datagramsTooLarge,
                            long datagramsLimited, End end) {}
}
