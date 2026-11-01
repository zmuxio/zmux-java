package io.zmux;

/**
 * Events of a session, delivered by {@link ZmuxSession#nextEvent}. Unread events are
 * coalesced, never dropped: a pipe direction keeps each state once, at its latest
 * occurrence (a transient SUSPECT and the terminal state both stay visible); other kinds keep
 * their latest value; a retired pipe's unread events leave the queue and are counted in
 * {@link PipesRetired}.
 */
public sealed interface SessionEvent {
    /** The peer sent GOAWAY and lowered a watermark: no new streams; streams at or above it fail. */
    record GoAwayReceived(long maxBidi, long maxUni, long code) implements SessionEvent {}

    /** Received datagrams were dropped (queue full or no stream); total is the session's count. */
    record DatagramsDropped(long total) implements SessionEvent {}

    /**
     * A pipe's outbound direction changed state (STATE_MACHINE.md 3.1). In the pipe events,
     * carrier is the kind of the pipe's carrier as its pipe input declared it, null when undeclared.
     */
    record OutboundPipeStateChanged(long pipe, CarrierKind carrier, PipeState.Outbound state) implements SessionEvent {}

    /** A pipe's inbound direction changed state (STATE_MACHINE.md 3.2). */
    record InboundPipeStateChanged(long pipe, CarrierKind carrier, PipeState.Inbound state) implements SessionEvent {}

    /** A tier's saturation state changed. Lower numbered tiers are preferred. */
    record TierSaturation(long priority, boolean saturated) implements SessionEvent {}

    /** The effective local outbound settings changed. */
    record PipePriorityChanged(long pipe, CarrierKind carrier, long priority, boolean standby, long weight) implements SessionEvent {}

    /** A quality estimate left the configured band; rate is bytes/s. */
    record PipeQualityChanged(long pipe, CarrierKind carrier, java.time.Duration smoothedRtt, double rate) implements SessionEvent {}

    /** The sum of measured usable outbound rate estimates fell below the floor, in bytes/s. */
    record CapacityBelow(double rate) implements SessionEvent {}

    /** The sum of measured usable rate estimates recovered past the configured hysteresis. */
    record CapacityRestored(double rate) implements SessionEvent {}

    /**
     * A candidate of {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} was tried (API.md
     * Section 4.8); an unread one is replaced by the next try of the same kind.
     */
    record CarrierCandidate(CandidateResult result) implements SessionEvent {}

    /** The carrier kind of the session was chosen, at the dial and again on every switch. */
    record CarrierChosen(CarrierKind kind) implements SessionEvent {}

    /**
     * A peer join's token did not verify (MULTIPATH.md Section 5.4 step 5): reason is
     * {@code mac} or {@code mac_carrier_bound}. Also counted in
     * {@link SessionStats#joinAuthFailures()}; an unread one is replaced by the next of the same reason.
     */
    record JoinAuthFailed(String reason) implements SessionEvent {}

    /** Every pipe was lost; the session waits for a join to resume (RESUME.md). */
    record Suspended() implements SessionEvent {}

    /** A join ended the suspension. */
    record Resumed() implements SessionEvent {}

    /**
     * The only report of pipe retirement (MULTIPATH.md 8.4): the pipes retired since this
     * event was last read; of those, the ones whose failure ({@code failed}) or end without
     * failure ({@code closed}) was not yet read, judged by the unread terminal state of each
     * direction (an unread failure in either direction counts as failed); the events a queue
     * limit discarded ({@code dropped}; unread non-terminal events of a retired pipe leave the
     * queue uncounted, and the limit never discards this summary or a terminal event); and the IDs of the last retired pipes, at
     * most four, oldest first. It enters the stream at the first retirement after it was last
     * read and accumulates until read. An application that needs every retired ID compares
     * {@link Paths#list()} with the handles it holds.
     */
    record PipesRetired(long retired, long failed, long closed, long dropped, java.util.List<Long> last) implements SessionEvent {}
}
