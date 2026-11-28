package io.zmux;

/** An inbound Join lost its authorization before it could commit. */
public final class JoinAuthorizationChangedException extends PipeException {
    public JoinAuthorizationChangedException() {
        super("authorization_changed");
    }
}
