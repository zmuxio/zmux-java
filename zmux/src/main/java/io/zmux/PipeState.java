package io.zmux;

/**
 * The states of a pipe's two directions, each null for a direction the
 * attachment does not have. A retired attachment has no state.
 */
public record PipeState(Outbound outbound, Inbound inbound) {
    /** The outbound direction, where the local endpoint writes. */
    public enum Outbound { JOINING, ACTIVE, SUSPECT, DRAINING, FAILED, CLOSED }

    /** The inbound direction, where the local endpoint reads. */
    public enum Inbound { JOINING, OPEN, ENDED }
}
