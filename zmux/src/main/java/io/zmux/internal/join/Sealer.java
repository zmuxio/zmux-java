package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.carrier.SecureCarrierException.Code.TOO_LARGE;

import java.nio.ByteBuffer;

/** Protects the records of one direction, without framing. It has one owner at a time. */
public final class Sealer {
    /** The largest message: what a length prefix allows. */
    public static final int MAX_MESSAGE = 65535;

    private final CipherState cs;
    private SecureCarrierException failed;

    private Sealer(CipherState cs) {
        this.cs = cs;
    }

    /** A Sealer with {@code key} at counter {@code n}, an unsigned value. */
    public static Sealer create(Suite suite, byte[] key, long n) {
        return new Sealer(new CipherState(suite, key, n));
    }

    CipherState state() {
        return cs;
    }

    /** What the cipher adds to a record, before any framing. */
    public int overhead() {
        return Suite.TAG_LEN;
    }

    /** The largest record plaintext. */
    public int maxPlaintext() {
        return MAX_MESSAGE - Suite.TAG_LEN;
    }

    /** Seals one record and returns the ciphertext, without a length. An oversized plaintext consumes no nonce. */
    public byte[] seal(byte[] plain, int off, int len) throws SecureCarrierException {
        check(len);
        cs.stepRekey();
        try {
            return cs.encrypt(null, plain, off, len);
        } catch (SecureCarrierException e) {
            failed = e;
            throw e;
        }
    }

    /**
     * Seals the remaining bytes of {@code plain} into {@code out}, which may overlap it exactly
     * in place, and returns the bytes written. {@code out} must have room for them and the overhead.
     */
    public int seal(ByteBuffer plain, ByteBuffer out) throws SecureCarrierException {
        check(plain.remaining());
        if (out.remaining() < plain.remaining() + Suite.TAG_LEN) {
            throw new IllegalArgumentException("output buffer too small");
        }
        cs.stepRekey();
        try {
            return cs.encrypt(plain, out);
        } catch (SecureCarrierException e) {
            failed = e;
            throw e;
        }
    }

    private void check(int len) throws SecureCarrierException {
        if (failed != null) throw failed;
        if (len > maxPlaintext()) throw new SecureCarrierException(TOO_LARGE);
    }
}
