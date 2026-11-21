package io.zmux;

/**
 * Events of a session, delivered by {@link ZmuxSession#nextEvent}; every zmux implementation has
 * these events under these names. The stream is a keyed queue that never drops an event: it holds
 * at most one unread event per key. When an event occurs whose key has an unread one, that one is
 * updated as the key's rule says and moves to the tail, so events are read in the order of their
 * keys' latest occurrences and the last event read of a key is its latest state. The keys:
 *
 * <ul>
 * <li>kept as they are (each occurs once per key): {@link PipeAdded} and {@link PipeJoined} by pipe,
 *     {@link PipeFailed} and {@link PipeClosed} by pipe and direction;
 * <li>replaced by the latest: {@link PipeSuspect} and {@link PipeRecovered} by pipe and direction,
 *     {@link PipePriorityChanged} and {@link PipeQualityChanged} by pipe, {@link TierSaturation} by
 *     priority, {@link CarrierCandidate} by carrier kind and variant, and by kind alone
 *     {@link CapacityBelow}, {@link CapacityRestored}, {@link EpochCommitted}, {@link EpochDeleted},
 *     {@link CarrierChosen}, {@link DatagramPathChanged}, {@link SessionSuspended}, {@link SessionResumed}, {@link ResumeUnattended}
 *     (a {@link SessionResumed} removes an unread one) and {@link GoAwayReceived};
 * <li>counted: {@link JoinAuthFailed} by reason;
 * <li>accumulated: {@link PipesRetired}; a retired pipe's unread events leave the queue and are
 *     counted in it.
 * </ul>
 *
 * Once the session has ended nothing more enters the queue; what is queued is still read, then
 * {@code nextEvent} throws the session's error.
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
     * A candidate of {@link Zmux#dial(CarrierChoice, ZmuxConfig, CarrierChoicePolicy)} was tried; an unread one is
     * replaced by the next try of the same carrier kind and variant.
     */
    record CarrierCandidate(CandidateResult result) implements SessionEvent {}

    /** The carrier kind of the session was chosen, at the dial and again on every switch. */
    record CarrierChosen(CarrierKind kind, String variant) implements SessionEvent {
        /** A chosen kind of no variant. */
        public CarrierChosen(CarrierKind kind) { this(kind, ""); }
    }

    /**
     * A peer's Hello did not verify, for example because the two ends have different join
     * secrets: reason is {@code auth}. Also counted in {@link SessionStats#joinAuthFailures()}.
     * One unread event stands for each reason: {@code count} is the number of failures with that
     * reason since the previous one was read, at least 1 (a {@link Tracer} gets one event, with
     * count 1, per failure).
     */
    record JoinAuthFailed(String reason, long count) implements SessionEvent {}

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

    /**
     * A new {@link ConnectionInfo#pathDatagramPayload()} (datagramPayload): the carrier's datagram size changed (path
     * MTU discovery), another pipe carries the datagrams, or they moved between the carrier's datagrams and records.
     * It is noticed when the session sends or its pipes change.
     */
    record DatagramPathChanged(int datagramPayload) implements SessionEvent {}

    /** The session committed a session epoch: its secret is derived, and joins of it are accepted. */
    record EpochCommitted(long epoch) implements SessionEvent {}

    /** A session epoch, its secret, its join keys and its Locators were deleted. */
    record EpochDeleted(long epoch) implements SessionEvent {}

    /**
     * The only report of pipe retirement: the pipes retired since this
     * event was last read; of those, the ones whose failure ({@code failed}) or end without
     * failure ({@code closed}) was not yet read, judged by the unread terminal state of each
     * direction (an unread failure in either direction counts as failed); and the IDs of the
     * last retired pipes, at most four, oldest first. A retired pipe's other unread events leave
     * the queue uncounted. It enters the stream at the first retirement after it was last read,
     * accumulates until read and moves to the tail at each retirement. A {@link Tracer} gets one
     * per retired pipe, with {@code retired} 1, {@code last} that pipe, and {@code failed} 1 if
     * either direction failed or else {@code closed} 1. An application that needs every retired
     * ID compares {@link ZmuxSession#pipes()} with the handles it holds.
     */
    record PipesRetired(long retired, long failed, long closed, java.util.List<PipeId> last) implements SessionEvent {}
}
