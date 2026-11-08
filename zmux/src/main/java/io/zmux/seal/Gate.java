package io.zmux.seal;

import java.net.SocketAddress;

/**
 * The server's host: it decides who may establish. {@code src} may be null. An exception from any method
 * refuses the carrier with the reason {@code gate_error}, the exception kept as the {@link io.zmux.RejectError}'s
 * cause.
 */
public interface Gate {
    /** The pre-shared key of a Handle, null when the Handle is unknown. Runs before any authentication and has no side effect. */
    byte[] resolve(byte[] handle, SocketAddress src) throws Exception;

    /**
     * The node credential for message 2, null for none. {@code handle} is empty on a Profile without a key;
     * {@code profile} is the handshake's.
     */
    Credential credential(byte[] handle, Profile profile, SocketAddress src) throws Exception;

    /**
     * Accepts a client after message 3 verified, and after the {@link ServerConfig#anchor()} judged it when there is
     * one; {@code a} holds the Handle and the client's whole {@link Peer}. Any server state that should exist only
     * after a successful handshake is committed here; a one-time token is consumed idempotently for one client key.
     * The result is the session's host value ({@link Peer#value()}); an exception closes the carrier.
     */
    Object commit(Admission a, SocketAddress src) throws Exception;
}
