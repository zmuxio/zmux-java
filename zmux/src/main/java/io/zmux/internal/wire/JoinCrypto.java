package io.zmux.internal.wire;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import javax.crypto.KDF;
import javax.crypto.Mac;
import javax.crypto.spec.HKDFParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** Session secret, join keys and join tokens (MULTIPATH.md Section 10). */
public final class JoinCrypto {
    /** TLS exporter labels (MULTIPATH.md Sections 10.2 and 10.5). */
    public static final String EXPORTER_SESSION = "EXPORTER-zmux-session";
    public static final String EXPORTER_PIPE = "EXPORTER-zmux-pipe";

    private JoinCrypto() {}

    /** The session secret: HKDF-Extract(salt = "zmux session" || nonces, IKM = exporter || psk). */
    public static byte[] sessionSecret(byte[] initiatorNonce, byte[] responderNonce, byte[] exporter, byte[] psk) {
        byte[] label = "zmux session".getBytes(StandardCharsets.US_ASCII);
        byte[] salt = concat(label, initiatorNonce, responderNonce);
        byte[] ikm = concat(exporter, psk);
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");
            return hkdf.deriveData(HKDFParameterSpec.ofExtract().addIKM(ikm).addSalt(salt).extractOnly());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HKDF-SHA256 unavailable", e);
        }
    }

    /** join_key_i and join_key_r (MULTIPATH.md Section 10.4). */
    public static byte[][] joinKeys(byte[] secret) {
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");
            SecretKeySpec prk = new SecretKeySpec(secret, "HKDF-PRK");
            byte[] ki = hkdf.deriveData(HKDFParameterSpec.expandOnly(prk, "zmux join i".getBytes(StandardCharsets.US_ASCII), 32));
            byte[] kr = hkdf.deriveData(HKDFParameterSpec.expandOnly(prk, "zmux join r".getBytes(StandardCharsets.US_ASCII), 32));
            return new byte[][] {ki, kr};
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HKDF-SHA256 unavailable", e);
        }
    }

    /** The 16-byte join token: HMAC-SHA256(key, macInput || binding) truncated. */
    public static byte[] token(byte[] key, byte[] macInput, byte[] binding) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            m.update(macInput);
            m.update(binding);
            return Arrays.copyOf(m.doFinal(), 16);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    /** Verifies a join token in constant time. */
    public static boolean verify(byte[] key, byte[] macInput, byte[] binding, byte[] token) {
        return MessageDigest.isEqual(token(key, macInput, binding), token);
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int i = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, i, p.length);
            i += p.length;
        }
        return out;
    }
}
