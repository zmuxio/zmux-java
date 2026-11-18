package io.zmux.internal.random;

import static io.zmux.seal.SecureCarrierException.Kind.RANDOM_FAILED;

import io.zmux.seal.SecureCarrierException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * The operating system's entropy source, the one source of handshake randomness: Establish's
 * X25519 keys, ML-KEM key generation (d, z) and encapsulation (m), and Join's R.
 *
 * <p>Linux and macOS use NativePRNGNonBlocking (/dev/urandom), Windows Windows-PRNG (the system
 * generator). Every call opens a new instance that reads the system's source (a provider may mix
 * its own state in, never in its place), so copies of a process restored from one snapshot draw
 * different values once the restored systems' sources differ, which they do after the kernel
 * reseeds. A source that fails is random_failed, with no fallback.
 */
public final class OsRandom {
    /** The platform's algorithm name. */
    public static final String SOURCE =
            System.getProperty("os.name", "").startsWith("Windows") ? "Windows-PRNG" : "NativePRNGNonBlocking";

    private OsRandom() {}

    /** A new instance of the system's source, for one draw or one key operation. */
    public static SecureRandom get() throws SecureCarrierException {
        try {
            return SecureRandom.getInstance(SOURCE);
        } catch (GeneralSecurityException | RuntimeException e) {
            throw new SecureCarrierException(RANDOM_FAILED, "random source", e); // no fallback
        }
    }

    /** Fills {@code out} from the system's source. */
    public static void fill(byte[] out) throws SecureCarrierException {
        SecureRandom r = get();
        try {
            r.nextBytes(out);
        } catch (RuntimeException e) {
            throw new SecureCarrierException(RANDOM_FAILED, "random source", e);
        }
    }
}
