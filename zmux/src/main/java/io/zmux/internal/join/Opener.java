package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.carrier.SecureCarrierException.Code.AUTH;

import java.nio.ByteBuffer;

/** Verifies the records of one direction. Any failure is permanent. It has one owner at a time. */
public final class Opener {
    private final CipherState cs;
    private SecureCarrierException failed;

    private Opener(CipherState cs) {
        this.cs = cs;
    }

    /** An Opener with {@code key} at counter {@code n}, an unsigned value. */
    public static Opener create(Suite suite, byte[] key, long n) {
        return new Opener(new CipherState(suite, key, n));
    }

    CipherState state() {
        return cs;
    }

    public int overhead() {
        return Suite.TAG_LEN;
    }

    /** Opens one record, given without a length. */
    public byte[] open(byte[] ct, int off, int len) throws SecureCarrierException {
        check(len);
        cs.stepRekey();
        try {
            return cs.decrypt(null, ct, off, len);
        } catch (SecureCarrierException e) {
            failed = e;
            throw e;
        }
    }

    /**
     * Opens the remaining bytes of {@code ct} into {@code out} and returns the bytes written.
     * {@code out} must have room for the plaintext.
     */
    public int open(ByteBuffer ct, ByteBuffer out) throws SecureCarrierException {
        check(ct.remaining());
        if (out.remaining() < ct.remaining() - Suite.TAG_LEN) {
            throw new IllegalArgumentException("output buffer too small");
        }
        cs.stepRekey();
        try {
            return cs.decrypt(ct, out);
        } catch (SecureCarrierException e) {
            failed = e;
            throw e;
        }
    }

    private void check(int len) throws SecureCarrierException {
        if (failed != null) throw failed;
        if (len < Suite.TAG_LEN || len > Sealer.MAX_MESSAGE) {
            failed = new SecureCarrierException(AUTH);
            throw failed;
        }
    }
}
