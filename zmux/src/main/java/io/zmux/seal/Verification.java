package io.zmux.seal;

/**
 * The one place an end judges its peer: the verifier once,
 * then the proof its verdict requires, then the facts.
 */
final class Verification {
    /** A judged peer: the verdict and whether the peer is PQ authenticated. */
    record Judged(VerifyVerdict verdict, boolean pqAuthenticated) {}

    private Verification() {}

    /**
     * Asks anchor once. A rejection, any other exception or error of the verifier and a missing verdict
     * are {@link CredentialsFailedException} (the handshake's result); {@link VerifyUnavailableException} passes through.
     * Then the proof (transcript signature) the verdict requires is checked: proof is the one the
     * message carries, or null. PQ authentication holds when a proof key of a PQ KeyAlg was named and its
     * proof verified.
     */
    static Judged judge(Anchor anchor, VerifyInput in, Proof proof, boolean requirePqAuth) throws CredentialsFailedException, VerifyUnavailableException {
        VerifyVerdict v;
        try {
            v = anchor.verify(in);
        } catch (VerifyRejectedException e) {
            throw new CredentialsFailedException("the verifier rejected the peer", e);
        } catch (VirtualMachineError e) {
            throw e; // the JVM itself, not the verifier's judgement
        } catch (RuntimeException | Error e) {
            // Any other failure of the verifier rejects the peer.
            throw new CredentialsFailedException("the verifier failed", e);
        }
        if (v == null) throw new CredentialsFailedException("the verifier returned no verdict");
        boolean pq = proofIsPq(v, proof);
        if (requirePqAuth && !pq) throw new CredentialsFailedException(CredentialsFailedException.Detail.PQ_AUTH_REQUIRED, "PQ authentication required", null);
        return new Judged(v, pq);
    }

    /** Checks the proof the verdict's proof key requires; true when that key is PQ. No proof key: false. */
    private static boolean proofIsPq(VerifyVerdict v, Proof proof) throws CredentialsFailedException {
        byte[] proofKey = v.proofKeyId();
        if (proofKey == null) return false;
        if (proof == null) throw new CredentialsFailedException(CredentialsFailedException.Detail.PROOF, "no proof by the verdict's proof key", null);
        if (!proof.verifies(proofKey)) throw new CredentialsFailedException(CredentialsFailedException.Detail.PROOF, "the proof does not verify with the verdict's proof key", null);
        // A proof that verifies names a known KeyAlg (Proof.verifies).
        return java.util.Objects.requireNonNull(KeyAlg.of(proof.alg()), "a verified proof has a known KeyAlg").pq();
    }

}
