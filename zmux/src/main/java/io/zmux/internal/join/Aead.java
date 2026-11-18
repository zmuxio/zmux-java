package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Kind.AUTH;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** One key of a Suite's AEAD; the JDK object is kept across messages. */
final class Aead {
    private static final int NONCE_LEN = 12;

    private final Suite suite;
    private final Cipher cipher;
    private final byte[] nonce = new byte[NONCE_LEN];
    private SecretKeySpec key;

    Aead(Suite suite) {
        this.suite = suite;
        try {
            // The transformation is one of the Suite constants (AES/GCM or ChaCha20-Poly1305), chosen by the suite.
            @SuppressWarnings("InsecureCryptoUsage") Cipher c = Cipher.getInstance(suite.transformation);
            this.cipher = c;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("the JDK lacks " + suite.transformation, e);
        }
    }

    void setKey(byte[] k) {
        key = new SecretKeySpec(k, suite.keyAlgorithm);
    }

    private void start(int mode, long n, byte[] ad) throws GeneralSecurityException {
        // Four zero bytes, then n: big-endian for AES-GCM, little-endian for ChaCha20-Poly1305.
        for (int i = 0; i < Long.BYTES; i++) {
            int shift = suite.isAes() ? (Long.BYTES - 1 - i) * Byte.SIZE : i * Byte.SIZE;
            nonce[NONCE_LEN - Long.BYTES + i] = (byte) (n >>> shift);
        }
        AlgorithmParameterSpec spec =
                suite.isAes() ? new GCMParameterSpec(Suite.TAG_LEN * Byte.SIZE, nonce) : new IvParameterSpec(nonce);
        cipher.init(mode, key, spec);
        if (ad != null && ad.length > 0) cipher.updateAAD(ad);
    }

    byte[] encrypt(long n, byte[] ad, byte[] in, int off, int len) {
        try {
            start(Cipher.ENCRYPT_MODE, n, ad);
            return cipher.doFinal(in, off, len);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e); // lengths and key are the Suite's
        }
    }

    int encrypt(long n, ByteBuffer in, ByteBuffer out) {
        try {
            start(Cipher.ENCRYPT_MODE, n, null);
            return cipher.doFinal(in, out);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e); // the caller checked the room
        }
    }

    byte[] decrypt(long n, byte[] ad, byte[] in, int off, int len) throws SecureCarrierException {
        try {
            start(Cipher.DECRYPT_MODE, n, ad);
            return cipher.doFinal(in, off, len);
        } catch (AEADBadTagException e) {
            throw new SecureCarrierException(AUTH);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    int decrypt(long n, ByteBuffer in, ByteBuffer out) throws SecureCarrierException {
        try {
            start(Cipher.DECRYPT_MODE, n, null);
            return cipher.doFinal(in, out);
        } catch (AEADBadTagException e) {
            throw new SecureCarrierException(AUTH);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
