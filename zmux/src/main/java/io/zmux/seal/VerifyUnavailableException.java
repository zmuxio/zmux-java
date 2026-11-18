package io.zmux.seal;

import io.zmux.ZmuxException;
import java.io.Serial;

/**
 * An {@link Anchor} could not decide now, for a reason of its own (a store or a service it
 * relies on failed): the carrier closes as for a rejection, but a server does not count it as
 * a refused carrier.
 */
public final class VerifyUnavailableException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    public VerifyUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
