package io.zmux;

/** {@link ZmuxSession#setJoinSecret} on a session whose deferred PSK was supplied already: it can be set once. */
public final class JoinSecretSetException extends ZmuxException {
    JoinSecretSetException() {
        super("join secret already set");
    }
}
