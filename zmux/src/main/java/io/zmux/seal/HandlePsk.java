package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;
import static java.nio.charset.StandardCharsets.US_ASCII;

import io.zmux.internal.join.Kdf;

/**
 * A Handle and pre-shared key derived from one uniformly random secret. Not for passwords: a Handle lets anyone test
 * guesses. Both are copied in and out and compared by content; {@link #toString()} shows neither.
 */
public record HandlePsk(byte[] handle, byte[] psk) {
    public HandlePsk {
        handle = java.util.Objects.requireNonNull(handle, "handle").clone();
        psk = java.util.Objects.requireNonNull(psk, "psk").clone();
    }

    @Override public byte[] handle() { return handle.clone(); }

    @Override public byte[] psk() { return psk.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof HandlePsk h && java.util.Arrays.equals(handle, h.handle) && java.security.MessageDigest.isEqual(psk, h.psk);
    }

    @Override
    public int hashCode() { return java.util.Arrays.hashCode(handle); }

    @Override
    public String toString() { return "HandlePsk[handle of " + handle.length + " bytes, psk of " + psk.length + " bytes]"; }

    /** The shortest secret. */
    public static final int MIN_SECRET = 16;

    private static final int PSK_LEN = 32;

    /** {@code salt} is an optional per-deployment value and may be null. */
    public static HandlePsk derive(byte[] secret, byte[] domain, byte[] salt) throws SecureCarrierException {
        if (secret == null || secret.length < MIN_SECRET || domain.length > 0xffff) {
            throw new SecureCarrierException(INVALID_CONFIG, "token secret or domain");
        }
        byte[] label = "zmux-seal token".getBytes(US_ASCII);
        byte[] prk = Kdf.extract(salt == null ? label : Kdf.concat(label, salt), secret);
        return new HandlePsk(
                Kdf.expand(prk, info("handle", domain), Handshake.HANDLE_LEN), Kdf.expand(prk, info("psk", domain), PSK_LEN));
    }

    private static byte[] info(String label, byte[] domain) {
        byte[] l = {(byte) (domain.length >>> 8), (byte) domain.length};
        return Kdf.concat(label.getBytes(US_ASCII), l, domain);
    }
}
