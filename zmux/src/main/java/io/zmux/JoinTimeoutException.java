package io.zmux;

/** The join confirmation timed out. */
public final class JoinTimeoutException extends java.net.SocketTimeoutException {
    public JoinTimeoutException() { super("join confirmation timed out"); }
}
