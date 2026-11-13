package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.TLV_TRUNCATED;
import static java.nio.charset.StandardCharsets.US_ASCII;

import io.zmux.internal.wire.VarInt;
import io.zmux.internal.join.Tlv;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * The value of a proof item, the transcript signature of a handshake message: {@code KeyAlg(varint) ‖ PubLen(varint) ‖ Pub ‖ Sig}.
 * {@code alg} is the wire value, which may name no known algorithm. A proof read from a message
 * carries the {@code message} M it signs; one to be written has none.
 */
record Proof(long alg, byte[] pub, byte[] sig, byte[] message) {
    private static final byte[] LABEL_R = "zmux-seal proof r".getBytes(US_ASCII);
    private static final byte[] LABEL_I = "zmux-seal proof i".getBytes(US_ASCII);

    /** M = label ‖ 0x00 ‖ h_p of message idx: 1 for message 2, 2 for message 3. */
    static byte[] message(int idx, byte[] hp) {
        byte[] label = idx == 1 ? LABEL_R : LABEL_I;
        byte[] m = Arrays.copyOf(label, label.length + 1 + hp.length);
        System.arraycopy(hp, 0, m, label.length + 1, hp.length);
        return m;
    }

    /** The proof of signer with a signature of zeros: the length of the item before it is signed. */
    static Proof placeholder(Signer signer) {
        KeyAlg a = signer.alg();
        return new Proof(a.id(), signer.publicKey(), new byte[a.signatureLength()], null);
    }

    Proof withSig(byte[] s) { return new Proof(alg, pub, s, message); }

    Proof withMessage(byte[] m) { return new Proof(alg, pub, sig, m); }

    byte[] encode() {
        ByteArrayOutputStream v = new ByteArrayOutputStream(VarInt.length(alg) + VarInt.length(pub.length) + pub.length + sig.length);
        Tlv.putVarint(v, alg);
        Tlv.putVarint(v, pub.length);
        v.writeBytes(pub);
        v.writeBytes(sig);
        return v.toByteArray();
    }

    /** The structure only: shortest varints, and Pub within the value. */
    static Proof parse(byte[] v) throws SecureCarrierException {
        long alg = Tlv.minimalVarint(v, 0, v.length);
        int at = VarInt.length(alg);
        long len = Tlv.minimalVarint(v, at, v.length);
        at += VarInt.length(len);
        if (len > v.length - at) throw new SecureCarrierException(TLV_TRUNCATED);
        int end = at + (int) len;
        return new Proof(alg, Arrays.copyOfRange(v, at, end), Arrays.copyOfRange(v, end, v.length), null);
    }

    /**
     * Whether this read proof is the one a verdict requires: its key has the KeyId keyId and the
     * KeyAlg of a proof key, and the signature verifies over M.
     */
    boolean verifies(byte[] keyId) {
        KeyAlg a = KeyAlg.of(alg);
        if (a == null || a.use() != KeyAlg.Use.PROOF_KEY || pub.length != a.keyLength() || message == null) return false;
        return MessageDigest.isEqual(KeyId.of(a, pub), keyId) && a.verify(pub, message, sig);
    }
}
