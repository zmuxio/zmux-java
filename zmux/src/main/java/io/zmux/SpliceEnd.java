package io.zmux;

/** How one direction of a splice ended ({@link SpliceDirection#end()}). */
public sealed interface SpliceEnd {
    /** FIN read upstream, and the downstream stream finished after the data before it. */
    record Finished() implements SpliceEnd {}

    /** The upstream stream was reset with code; the downstream stream was reset with it. */
    record Reset(long code) implements SpliceEnd {}

    /** The downstream stream was stopped with code; the upstream stream was stopped with it. */
    record Stopped(long code) implements SpliceEnd {}

    /** The upstream stream's session closed or was lost. */
    record UpstreamLost() implements SpliceEnd {}

    /** The downstream stream's session closed or was lost. */
    record DownstreamLost() implements SpliceEnd {}

    /** The splice was aborted through a {@link SpliceAbort}: both streams were reset and stopped with code. */
    record Cancelled(long code) implements SpliceEnd {}

    /**
     * The direction's limiter failed in {@code acquire} (an exception, a failed stage, or a grant outside its
     * range): the downstream stream was reset and the upstream stream stopped with ZMUX_CANCELLED (0x5a4d0001);
     * the other direction went on.
     */
    record LimiterFailed() implements SpliceEnd {}
}
