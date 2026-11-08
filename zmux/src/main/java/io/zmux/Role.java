package io.zmux;

/** The role a session takes on its bootstrap carrier. */
public enum Role {
    /** The initiator: the dialling side of the carrier. */
    CLIENT,
    /** The responder: the accepting side of the carrier. */
    SERVER,
    /** Negotiated over a symmetric carrier (the zmux_role parameter, auto, with a random nonce). */
    PEER
}
