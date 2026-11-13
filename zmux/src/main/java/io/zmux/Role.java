package io.zmux;

/**
 * The role of an endpoint. {@link #CLIENT} is the initiator: the transport client of the bootstrap
 * carrier (the TCP, TLS, QUIC or HTTP client), which uses the client's stream IDs and the even pipe
 * IDs; {@link #SERVER} is the responder; {@link #AUTO} negotiates the two over a carrier without
 * transport roles. The secure carrier's {@link io.zmux.seal.VerifyInput#role()} uses the same values.
 */
public enum Role {
    /** The initiator: the transport client of the bootstrap carrier. */
    CLIENT,
    /** The responder: the transport server of the bootstrap carrier. */
    SERVER,
    /** Negotiated over a carrier without transport roles (the zmux_role parameter, with a random nonce). */
    AUTO
}
