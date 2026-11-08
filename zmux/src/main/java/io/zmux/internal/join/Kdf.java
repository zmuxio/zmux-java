package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.carrier.SecureCarrierException.Code.INVALID_CONFIG;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** The derivations of the secure carrier, all over SHA-256. */
public final class Kdf {
    /** Carrier exporter label of a Bound Join. */
    public static final String EXPORTER_JOIN = "EXPORTER-zmux-join";
    /** Carrier exporter label of a Bound Establish. */
    public static final String EXPORTER_SEAL_BOUND = "EXPORTER-zmux-seal-bound";
    /** Exporter label of the session secret. */
    public static final String EXPORTER_SESSION = "EXPORTER-zmux-session";

    /** The length of a carrier exporter and a Binding. */
    public static final int BINDING_LEN = 32;

    private static final int HASH_LEN = 32;
    private static final int MAX_EXPORT = 255 * HASH_LEN;

    private Kdf() {}

    /** H of the concatenated parts. */
    public static byte[] hash(byte[]... parts) {
        MessageDigest d = sha256();
        for (byte[] p : parts) {
            if (p != null) d.update(p);
        }
        return d.digest();
    }

    /** HMAC of the concatenated parts. */
    public static byte[] hmac(byte[] key, byte[]... parts) {
        Mac m = mac(key);
        for (byte[] p : parts) {
            if (p != null) m.update(p);
        }
        return m.doFinal();
    }

    /** HKDF-Extract; an empty salt is {@code HashLen} zero bytes. */
    public static byte[] extract(byte[] salt, byte[] ikm) {
        return hmac(salt == null || salt.length == 0 ? new byte[HASH_LEN] : salt, ikm);
    }

    /** HKDF-Expand with any key of at least the hash length, and {@code n} at most 255 outputs. */
    public static byte[] expand(byte[] prk, byte[] info, int n) {
        Mac m = mac(prk);
        byte[] out = new byte[n];
        byte[] t = new byte[0];
        int done = 0;
        for (int i = 1; done < n; i++) {
            m.update(t);
            m.update(info);
            m.update((byte) i);
            t = m.doFinal();
            int take = Math.min(t.length, n - done);
            System.arraycopy(t, 0, out, done, take);
            done += take;
        }
        return out;
    }

    /** HKDF-Expand with the info {@code label ‖ th}. */
    public static byte[] expandLabel(byte[] prk, String label, byte[] th, int n) {
        byte[] l = label.getBytes(US_ASCII);
        byte[] info = Arrays.copyOf(l, l.length + th.length);
        System.arraycopy(th, 0, info, l.length, th.length);
        return expand(prk, info, n);
    }

    /**
     * {@code join_psk[e]_x}: SHA-256 HKDF-Expand of the epoch's secret to the Suite's hash
     * length, with {@code "zmux join " ‖ x ‖ SHA-256(session_crypto) ‖ varint(Suite)} as info.
     */
    public static byte[] joinPsk(Suite s, byte[] epochSecret, boolean joinerInitiator, byte[] sessionCrypto)
            throws SecureCarrierException {
        return joinPsk(s.id(), epochSecret, joinerInitiator, sessionCrypto, s.hashLen());
    }

    /** {@link #joinPsk(Suite, byte[], boolean, byte[])} for a Suite value and an output length. */
    public static byte[] joinPsk(long suite, byte[] epochSecret, boolean joinerInitiator, byte[] sessionCrypto, int hashLen)
            throws SecureCarrierException {
        if (epochSecret == null || epochSecret.length == 0 || sessionCrypto == null || sessionCrypto.length == 0) {
            throw new SecureCarrierException(INVALID_CONFIG, "epoch secret or session crypto");
        }
        ByteArrayOutputStream info = new ByteArrayOutputStream();
        info.writeBytes((joinerInitiator ? "zmux join i" : "zmux join r").getBytes(US_ASCII));
        info.writeBytes(hash(sessionCrypto));
        Tlv.putVarint(info, suite);
        return expand(epochSecret, info.toByteArray(), hashLen);
    }

    /**
     * {@code Combine(salt, ss_1, ..., ss_n)}: each shared secret in turn the input keying
     * material of HKDF-Extract, with the previous output as salt.
     */
    public static byte[] combine(byte[] salt, byte[]... ss) {
        byte[] out = salt;
        for (byte[] x : ss) out = extract(out, x);
        return out;
    }

    /**
     * {@code epoch_secret[e+1]} of the next session epoch: {@code Combine(prk[e+1], ss...)}, with
     * {@code prk[e+1] = HKDF-Expand(epoch_secret[e], "zmux epoch" ‖ varint(e+1) ‖
     * SHA-256(session_crypto) ‖ SHA-256(offer_body ‖ answer_body), 32)}.
     */
    public static byte[] nextEpochSecret(byte[] secret, long next, byte[] sessionCrypto, byte[] offerBody, byte[] answerBody, byte[]... ss) {
        ByteArrayOutputStream info = new ByteArrayOutputStream();
        info.writeBytes("zmux epoch".getBytes(US_ASCII));
        Tlv.putVarint(info, next);
        info.writeBytes(hash(sessionCrypto));
        info.writeBytes(hash(offerBody, answerBody));
        byte[] prk = expand(secret, info.toByteArray(), HASH_LEN);
        byte[] out = combine(prk, ss);
        Arrays.fill(prk, (byte) 0);
        return out;
    }

    /** The session binding: Extract(salt = "zmux session binding", ikm = nonces). */
    public static byte[] sessionBinding(byte[] initNonce, byte[] respNonce) {
        return extract("zmux session binding".getBytes(US_ASCII), concat(initNonce, respNonce));
    }

    /** The session secret: Extract(salt = "zmux session" ‖ nonces, ikm = exporter ‖ psk). */
    public static byte[] sessionSecret(byte[] initNonce, byte[] respNonce, byte[] exporter, byte[] psk) {
        Mac m = mac(concat("zmux session".getBytes(US_ASCII), initNonce, respNonce));
        if (exporter != null) m.update(exporter);
        if (psk != null) m.update(psk);
        return m.doFinal();
    }

    /** A Binding from the exporters of the uplink and downlink halves; a half without one is invalid. */
    public static byte[] binding(byte[] up, byte[] down) throws SecureCarrierException {
        if (up == null || down == null || up.length != BINDING_LEN || down.length != BINDING_LEN) {
            throw new SecureCarrierException(INVALID_CONFIG, "a carrier half has no exporter");
        }
        if (Arrays.equals(up, down)) return up.clone();
        return expand(extract(null, concat(up, down)), "zmux-seal bound".getBytes(US_ASCII), BINDING_LEN);
    }

    /** {@code Export(label, context, n)} from an exporter secret. */
    public static byte[] export(Suite s, byte[] exporterSecret, String label, byte[] context, int n) throws SecureCarrierException {
        byte[] l = label.getBytes(UTF_8);
        int ctx = context == null ? 0 : context.length;
        if (exporterSecret == null || exporterSecret.length < s.hashLen() || l.length > 255 || ctx > 65535 || n < 1 || n > MAX_EXPORT) {
            throw new SecureCarrierException(INVALID_CONFIG, "exporter parameters");
        }
        ByteArrayOutputStream info = new ByteArrayOutputStream(l.length + ctx + 5);
        info.write(l.length);
        info.writeBytes(l);
        info.write(ctx >>> 8);
        info.write(ctx);
        if (context != null) info.writeBytes(context);
        info.write(n >>> 8);
        info.write(n);
        return expand(exporterSecret, info.toByteArray(), n);
    }

    /** The concatenation of the arrays. */
    public static byte[] concat(byte[]... parts) {
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

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Mac mac(byte[] key) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
