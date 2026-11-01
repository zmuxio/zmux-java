package io.zmux;

/**
 * How a splice ended (API.md Section A.2): per direction the stream bytes and datagram bytes forwarded (together what
 * was charged to the direction's limiter), the datagrams dropped, and the event that ended it. A queued datagram is
 * not an acknowledgement of peer delivery. Cancelling the stage is the cancellation of the splice: it then has no
 * result.
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
    }

    public record Direction(long streamBytes, long datagramBytes, long datagramsDropped, End end) {}
}
