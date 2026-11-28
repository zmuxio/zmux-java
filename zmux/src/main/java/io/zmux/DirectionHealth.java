package io.zmux;

import java.util.OptionalLong;

/**
 * The health of one local direction. Times use the session's monotonic clock.
 *
 * @param since when the current observation was committed
 * @param stableSince start of the continuous service interval, when known
 * @param evidenceGeneration generation of that direction's evidence
 * @param proofKind evidence committed in the current generation
 */
public record DirectionHealth(State state, Reason reason, long since, boolean canCarry, boolean settled,
                              OptionalLong stableSince, long evidenceGeneration, ProofKind proofKind) {
    /** The observation that proved service in the current evidence generation. */
    public enum ProofKind { NONE, PROBE, VERIFIED_RECORD }

    /** The current state of the direction. */
    public enum State { ABSENT, JOINING, UNKNOWN, USABLE, UNAVAILABLE, DRAINING, ENDED }
    /** The condition that put the direction in its current state. */
    public enum Reason {
        NONE, AWAIT_ATTACHMENT, OBSERVE_JOIN, IDLE, PROBATION, REVALIDATE, ACK_STALL, WRITE_STALL,
        INBOUND_SILENCE, CARRIER_ERROR, UNEXPECTED_EOF, JOIN_TIMEOUT, PROGRESS, PLANNED_DRAIN, SESSION_CLOSING, CANCELLED, TOPOLOGY_EXCLUDED
    }

    /** Whether a member still has this direction. */
    public boolean alive() { return state != State.ENDED; }

    /** Whether the direction counts as confirmed service without immediately replacing it. */
    public boolean serves() { return state == State.UNKNOWN || state == State.USABLE; }
}
