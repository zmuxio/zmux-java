package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Code.EXHAUSTED;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** The CipherState of Noise revision 34 over a Suite: a key, or none yet, and the counter n. */
public final class CipherState {
    /** The reserved nonce 2^64-1, as an unsigned value in a long. */
    static final long RESERVED_NONCE = -1L;

    private static final long REKEY_MASK = (1L << 20) - 1;

    private final Suite suite;
    private final Aead aead;
    private byte[] key;
    private long n;

    /** A state without a key. */
    public CipherState(Suite suite) {
        this.suite = suite;
        this.aead = new Aead(suite);
    }

    /** A state with a key at counter {@code n}. */
    public CipherState(Suite suite, byte[] k, long n) {
        this(suite);
        init(k);
        this.n = n;
    }

    /** Sets the key and resets n. */
    public void init(byte[] k) {
        key = k.clone();
        aead.setKey(key);
        n = 0;
    }

    public boolean hasKey() {
        return key != null;
    }

    public Suite suite() {
        return suite;
    }

    /** The counter, an unsigned value. */
    public long n() {
        return n;
    }

    /** Sets the counter, an unsigned value. */
    public void setN(long n) {
        this.n = n;
    }

    byte[] key() {
        return key;
    }

    /** Seals {@code plain}. */
    public byte[] encrypt(byte[] ad, byte[] plain, int off, int len) throws SecureCarrierException {
        checkNonce();
        byte[] out = aead.encrypt(n, ad, plain, off, len);
        n++;
        return out;
    }

    /** Opens {@code ct}. */
    public byte[] decrypt(byte[] ad, byte[] ct, int off, int len) throws SecureCarrierException {
        checkNonce();
        byte[] out = aead.decrypt(n, ad, ct, off, len);
        n++;
        return out;
    }

    int encrypt(ByteBuffer plain, ByteBuffer out) throws SecureCarrierException {
        checkNonce();
        int w = aead.encrypt(n, plain, out);
        n++;
        return w;
    }

    int decrypt(ByteBuffer ct, ByteBuffer out) throws SecureCarrierException {
        checkNonce();
        int w = aead.decrypt(n, ct, out);
        n++;
        return w;
    }

    private void checkNonce() throws SecureCarrierException {
        if (n == RESERVED_NONCE) throw new SecureCarrierException(EXHAUSTED);
    }

    /** Replaces the key on a positive multiple of the rekey interval; n is unchanged. */
    void stepRekey() {
        if (n != 0 && (n & REKEY_MASK) == 0) {
            byte[] out = aead.encrypt(RESERVED_NONCE, null, new byte[key.length], 0, key.length);
            key = Arrays.copyOf(out, key.length);
            aead.setKey(key);
        }
    }
}
