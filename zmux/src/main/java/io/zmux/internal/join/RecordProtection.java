package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import java.nio.ByteBuffer;

/**
 * The keys of a Sealed carrier, one record at a time. A failure is permanent.
 * A carrier's two directions each use one thread at a time.
 */
public interface RecordProtection {
    /** Seals plain's remaining bytes into out; the bytes written. */
    int seal(ByteBuffer plain, ByteBuffer out) throws SecureCarrierException;

    /** Opens ct's remaining bytes into out; the plaintext's length. */
    int open(ByteBuffer ct, ByteBuffer out) throws SecureCarrierException;

    /**
     * Plaintext the handshake carried as the start of the peer's zmux stream, to be taken as its first record
     *; null when none.
     */
    default byte[] firstBytes() { return null; }
}
