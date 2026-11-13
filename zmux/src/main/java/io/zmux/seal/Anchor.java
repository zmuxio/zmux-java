package io.zmux.seal;

/**
 * The host's verifier of a peer: asked once for each Establish by
 * the end that authenticates its peer. It returns a verdict, or rejects the peer with
 * {@link CredentialsFailedException}, or cannot decide now with {@link VerifierUnavailableException}:
 * both close the carrier and fail the handshake as {@code credentials_failed}, and only a
 * rejection counts as a refused carrier on a server. Any other exception or error of the verifier is a
 * rejection.
 */
@FunctionalInterface
public interface Anchor {
    /** The reserved credential type of "no credential", which a sender does not send. */
    long CRED_TYPE_NONE = 0;

    VerifyVerdict verify(VerifyInput in) throws CredentialsFailedException, VerifierUnavailableException;
}
