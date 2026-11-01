package io.zmux;

/** The role a session takes on its bootstrap carrier. */
public enum Role {
    /** The initiator: the dialling side of the carrier. */
    CLIENT,
    /** The responder: the accepting side of the carrier. */
    SERVER,
    /** Negotiated over a symmetric carrier (zmux_role auto, EXTENSIONS.md Section 2). */
    PEER
}
