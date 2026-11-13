package io.zmux.seal;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.zmux.internal.join.CipherState;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Suite;
import java.util.Arrays;

/** The SymmetricState of Noise revision 34 over a Suite. */
final class SymmetricState {
    final CipherState cs;
    byte[] ck;
    byte[] h;

    private final int keyLen;

    SymmetricState(String protocolName, Suite suite) {
        cs = new CipherState(suite);
        keyLen = suite.keyLen();
        byte[] name = protocolName.getBytes(US_ASCII);
        h = name.length <= suite.hashLen() ? Arrays.copyOf(name, suite.hashLen()) : Kdf.hash(name);
        ck = h.clone();
    }

    void mixHash(byte[] data) {
        h = Kdf.hash(h, data);
    }

    /** HKDF of Noise Section 4.3 with three outputs. */
    private byte[][] hkdf3(byte[] ikm) {
        byte[] okm = Kdf.expand(Kdf.hmac(ck, ikm), new byte[0], 3 * h.length);
        return new byte[][] {
            Arrays.copyOfRange(okm, 0, h.length),
            Arrays.copyOfRange(okm, h.length, 2 * h.length),
            Arrays.copyOfRange(okm, 2 * h.length, 3 * h.length)
        };
    }

    void mixKey(byte[] ikm) {
        byte[][] o = hkdf3(ikm);
        ck = o[0];
        cs.init(Arrays.copyOf(o[1], keyLen));
    }

    void mixKeyAndHash(byte[] ikm) {
        byte[][] o = hkdf3(ikm);
        ck = o[0];
        mixHash(o[1]);
        cs.init(Arrays.copyOf(o[2], keyLen));
    }

    byte[] encryptAndHash(byte[] plain) throws SecureCarrierException {
        byte[] out = cs.hasKey() ? cs.encrypt(h, plain, 0, plain.length) : plain.clone();
        mixHash(out);
        return out;
    }

    byte[] decryptAndHash(byte[] ct) throws SecureCarrierException {
        byte[] out = cs.hasKey() ? cs.decrypt(h, ct, 0, ct.length) : ct.clone();
        mixHash(ct);
        return out;
    }

    /** The two keys of Split. */
    byte[][] split() {
        byte[][] o = hkdf3(new byte[0]);
        return new byte[][] {Arrays.copyOf(o[0], keyLen), Arrays.copyOf(o[1], keyLen)};
    }
}
