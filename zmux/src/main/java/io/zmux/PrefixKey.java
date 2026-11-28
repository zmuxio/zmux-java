package io.zmux;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.SecretKey;

/** A key copy owned by a presenter or an installed ticket or generation. */
final class PrefixKey {
    private byte[] bytes;

    PrefixKey(byte[] bytes) {
        if (bytes.length != 32) throw new IllegalArgumentException("expected a 32-byte prefix key");
        this.bytes = bytes.clone();
    }

    synchronized byte[] copy() {
        if (bytes == null) throw new IllegalStateException("prefix key retired");
        return bytes.clone();
    }

    synchronized PrefixKey duplicate() {
        if (bytes == null) throw new IllegalStateException("prefix key retired");
        return new PrefixKey(bytes);
    }

    synchronized void clear() {
        if (bytes == null) return;
        Arrays.fill(bytes, (byte) 0);
        bytes = null;
    }

    Mac mac() { return init(copy()); }

    static Mac mac(byte[] key) { return init(key.clone()); }

    // The encoded array is the same temporary buffer that init clears.
    private record MacInput(byte[] bytes) implements SecretKey {
        @Override public String getAlgorithm() { return "HmacSHA256"; }
        @Override public String getFormat() { return "RAW"; }
        @Override public byte[] getEncoded() { return bytes; }
    }

    /** The provider initializes its own MAC state before the temporary input is cleared. */
    private static Mac init(byte[] temporary) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new MacInput(temporary));
            return mac;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        } finally {
            Arrays.fill(temporary, (byte) 0);
        }
    }
}
