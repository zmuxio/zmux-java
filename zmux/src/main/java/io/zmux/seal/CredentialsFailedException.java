package io.zmux.seal;

import io.zmux.ZmuxException;
import java.io.Serial;

/**
 * The verifier rejected the peer, or a proof or the PQ authentication the end requires failed
 * ({@code credentials_failed}). The reason stays local.
 */
public final class CredentialsFailedException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    public CredentialsFailedException(String reason) {
        super("credentials failed: " + reason, null);
    }

    public CredentialsFailedException(String reason, Throwable cause) {
        super("credentials failed: " + reason, cause);
    }
}
