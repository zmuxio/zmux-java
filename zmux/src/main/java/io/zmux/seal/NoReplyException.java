package io.zmux.seal;

import io.zmux.ZmuxException;
import java.io.Serial;

/**
 * The carrier ended, or the handshake timeout passed, before the server's reply (message 2) arrived. A server
 * says nothing about a handshake it refuses, so this is all a client learns; the cause is the end of stream
 * or the timeout.
 *
 * <p>A server that stalls a refused carrier rather than closing it leaves the client waiting until the
 * client's own handshake timeout ({@link ClientConfig#timeout()}); give a secure carrier handshake a time
 * limit. With no timeout, a stalled client waits until the server ends the stall.
 */
public final class NoReplyException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    public NoReplyException(Throwable cause) {
        super("no reply: the server may not accept the Profile, may not know the Handle, may hold another"
                + " pre-shared key, or may not allow the record mode", cause);
    }
}
