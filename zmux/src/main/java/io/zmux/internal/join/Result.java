package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import java.nio.ByteBuffer;

/**
 * What a finished handshake leaves, from the handshakes that finish here, Join and Establish.
 * {@link #sealer} and {@link #opener} are null unless the Mode is Sealed.
 */
public record Result(Mode mode, Suite suite, Sealer sealer, Opener opener, byte[] exporterSecret)
        implements RecordProtection {

    /** A copy: the secret stays with this result. */
    @Override public byte[] exporterSecret() {
        return exporterSecret.clone();
    }

    @Override public int seal(ByteBuffer plain, ByteBuffer out) throws SecureCarrierException { return sealer.seal(plain, out); }

    @Override public int open(ByteBuffer ct, ByteBuffer out) throws SecureCarrierException { return opener.open(ct, out); }

    /** Derives keying material from this handshake, in every Mode. */
    public byte[] export(String label, byte[] context, int n) throws SecureCarrierException {
        return Kdf.export(suite, exporterSecret, label, context, n);
    }

    /** Names the Mode and Suite only: the result holds keys. */
    @Override public String toString() {
        return "Result[mode=" + mode + ", suite=" + suite + "]";
    }
}
