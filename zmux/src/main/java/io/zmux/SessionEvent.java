package io.zmux;

/**
 * Events of a session, delivered by {@link ZmuxSession#nextEvent}; every zmux implementation has
 * these events under these names. Unread events are
 * coalesced, never dropped: a pipe direction keeps each pipe event once, at its latest
 * occurrence (a transient {@link PipeSuspect} and the terminal one both stay visible); other kinds keep
 * their latest value; a retired pipe's unread events leave the queue and are counted in
 * {@link PipesRetired}.
 */
public sealed interface SessionEvent {
    /** The peer sent GOAWAY and lowered a watermark: no new streams; streams at or above it fail. */
    record GoAwayReceived(long maxBidi, long maxUni, long code) implements SessionEvent {}

    /*
     * The pipe events. In each, carrier is the kind of the pipe's carrier as its pipe input
     * declared it, null when undeclared, and direction the pipe direction the event is about,
     * from the local endpoint: SEND_ONLY for the outbound direction, RECEIVE_ONLY for the inbound.
     */

    /** A local outbound pipe direction appeared (joining, or already active). */
    record PipeAdded(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /** An outbound pipe direction became active. */
    record PipeJoined(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /** An outbound pipe direction stopped making progress. */
    record PipeSuspect(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /** A suspect outbound pipe direction is active again. */
    record PipeRecovered(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /** An outbound pipe direction failed. */
    record PipeFailed(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /**
     * A pipe direction ended: the outbound direction CLOSED, the inbound one ENDED. Retirement
     * is reported by {@link PipesRetired}.
     */
    record PipeClosed(PipeId pipe, CarrierKind carrier, PipeOptions.Direction direction) implements SessionEvent {}

    /** A tier's saturation state changed. Lower numbered tiers are preferred. */
    record TierSaturation(long priority, boolean saturated) implements SessionEvent {}

    /**
     * The effective outbound settings of a pipe (priority, standby or weight) changed; source
     * is {@link PipeStats.SettingSource#LOCAL} for a local setting and
     * {@link PipeStats.SettingSource#PEER} for the peer's request.
     */
    record PipePriorityChanged(PipeId pipe, CarrierKind carrier, long priority, boolean standby, long weight,
                               PipeStats.SettingSource source) implements SessionEvent {}

    /** A quality estimate left the configured band; deliveryRate is bytes/s. */
    record PipeQualityChanged(PipeId pipe, CarrierKind carrier, java.time.Duration smoothedRtt, long deliveryRate) implements SessionEvent {}

    /** The sum of measured usable outbound rate estimates fell below the floor, in bytes/s. */
    record CapacityBelow(long rate) implements SessionEvent {}

    /** The sum of measured usable rate estimates recovered past the configured hysteresis, in bytes/s. */
    record CapacityRestored(long rate) implements SessionEvent {}

    /**
     * A candidate of {@link Zmux#dial(CarrierChoice, ZmuxConfig, CarrierChoicePolicy)} was tried; an unread one is replaced by the next try of the same kind.
     */
    record CarrierCandidate(CandidateResult result) implements SessionEvent {}

    /** The carrier kind of the session was chosen, at the dial and again on every switch. */
    record CarrierChosen(CarrierKind kind, String variant) implements SessionEvent {
        /** A chosen kind of no variant. */
        public CarrierChosen(CarrierKind kind) { this(kind, ""); }
    }

    /**
     * A peer's Hello did not verify, for example because the two ends have different join
     * secrets: reason is {@code auth}. Also counted in
     * {@link SessionStats#joinAuthFailures()}; an unread one is replaced by the next of the same reason.
     */
    record JoinAuthFailed(String reason) implements SessionEvent {}

    /**
     * Every pipe of a direction was lost in a session with resume: it waits, until its grace
     * period, for a join to resume it. Writes buffer up to the limits and then block; reads wait.
     */
    record SessionSuspended() implements SessionEvent {}

    /**
     * Follows {@link SessionSuspended} when the session can rejoin but nothing in the library
     * will dial for it: it has no {@link ZmuxConfig.Builder#redial(Redial) redial hook} and no running
     * pool ({@link ZmuxSession#maintainPipes}). It ends at the grace period unless the
     * application adds a pipe; one that resumes by its own {@link ZmuxSession#addPipe} calls
     * can ignore this event. Also logged as a warning ({@link ZmuxConfig.Builder#logger(System.Logger)}).
     */
    record ResumeUnattended() implements SessionEvent {}

    /** A join ended the suspension. */
    record SessionResumed() implements SessionEvent {}

    /** The session committed a session epoch: its secret is derived, and joins of it are accepted. */
    record EpochCommitted(long epoch) implements SessionEvent {}

    /** A session epoch, its secret, its join keys and its Locators were deleted. */
    record EpochDeleted(long epoch) implements SessionEvent {}

    /**
     * The only report of pipe retirement: the pipes retired since this
     * event was last read; of those, the ones whose failure ({@code failed}) or end without
     * failure ({@code closed}) was not yet read, judged by the unread terminal state of each
     * direction (an unread failure in either direction counts as failed); the events a queue
     * limit discarded ({@code dropped}; unread non-terminal events of a retired pipe leave the
     * queue uncounted, and the limit never discards this summary or a terminal event); and the IDs of the last retired pipes, at
     * most four, oldest first. It enters the stream at the first retirement after it was last
     * read and accumulates until read. An application that needs every retired ID compares
     * {@link ZmuxSession#pipes()} with the handles it holds.
     */
    record PipesRetired(long retired, long failed, long closed, long dropped, java.util.List<PipeId> last) implements SessionEvent {}
}
