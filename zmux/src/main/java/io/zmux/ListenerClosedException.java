package io.zmux;

/**
 * The listener was closed by this endpoint ({@link ZmuxListener#close()}, or a
 * {@link ZmuxListener#shutdown(long, String) shutdown}): it accepts no session and dials none.
 */
public final class ListenerClosedException extends ZmuxException {
    public ListenerClosedException() {
        super("listener closed");
    }
}
