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

    /** The local reason within the credentials_failed class. */
    public enum Detail { VERIFIER, VERIFIER_UNAVAILABLE, PROOF, PQ_AUTH_REQUIRED }

    private final Detail detail;

    public CredentialsFailedException(String reason) {
        this(reason, null);
    }

    public CredentialsFailedException(String reason, Throwable cause) {
        this(Detail.VERIFIER, reason, cause);
    }

    CredentialsFailedException(Detail detail, String reason, Throwable cause) {
        super("credentials failed: " + reason, cause);
        this.detail = detail;
    }

    /** Returns the local reason category. */
    public Detail detail() {
        return detail;
    }
}
