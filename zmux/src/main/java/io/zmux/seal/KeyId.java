package io.zmux.seal;

import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Tlv;
import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * The name of a key, the one a pin and a verdict's proof key use:
 * {@code SHA-256("zmux-seal key" ‖ varint(KeyAlg) ‖ key)}, 32 bytes. Pins, configured roots and
 * every key reference on the wire use it.
 */
public final class KeyId {
    /** The length of a KeyId. */
    public static final int LEN = 32;

    private static final byte[] LABEL = "zmux-seal key".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private KeyId() {}

    /** The KeyId of key, the canonical encoding of alg; a key of another length is refused. */
    public static byte[] of(KeyAlg alg, byte[] key) {
        Objects.requireNonNull(alg);
        if (key.length != alg.keyLength()) throw new IllegalArgumentException(alg + " key of " + key.length + " bytes");
        ByteArrayOutputStream b = new ByteArrayOutputStream(LABEL.length + 1 + key.length);
        b.writeBytes(LABEL);
        Tlv.putVarint(b, alg.id());
        b.writeBytes(key);
        return Kdf.hash(b.toByteArray());
    }

    /** The KeyId of an X25519 static public key ({@link StaticKey#publicKey()}), the one a {@link PinAnchor} pins. */
    public static byte[] staticKeyId(byte[] publicKey) {
        return of(KeyAlg.X25519, publicKey);
    }
}
