package io.zmux.seal;

/**
 * The host's verifier of a peer: asked once for each Establish by
 * the end that authenticates its peer. It returns a verdict, or rejects the peer with
 * {@link VerifyRejectedException}, or cannot decide now with {@link VerifyUnavailableException}:
 * both close the carrier, and a client's establishment fails with {@link CredentialsFailedException}; a server
 * raises nothing to the application, and only a rejection counts as a refused carrier. Any other exception or error
 * of the verifier is a rejection.
 */
@FunctionalInterface
public interface Anchor {
    /** The reserved credential type of "no credential", which a sender does not send. */
    long CRED_TYPE_NONE = 0;

    VerifyVerdict verify(VerifyInput in) throws VerifyRejectedException, VerifyUnavailableException;
}
