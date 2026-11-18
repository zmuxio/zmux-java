package io.zmux.seal;

import io.zmux.ZmuxException;
import java.io.Serial;

/**
 * The verifier ({@link Anchor}) rejects the peer: thrown by {@link Anchor#verify}. It is the verifier's answer, not
 * the handshake's result; the end that judged then fails with {@link CredentialsFailedException}, and the reason
 * stays local.
 */
public final class VerifyRejectedException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    public VerifyRejectedException(String reason) {
        super("verify rejected: " + reason, null);
    }

    public VerifyRejectedException(String reason, Throwable cause) {
        super("verify rejected: " + reason, cause);
    }
}
