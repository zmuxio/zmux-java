package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import io.zmux.internal.kex.MlKem768;
import io.zmux.internal.kex.X25519;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * A key share of a Group and both sides of its exchange. The
 * initiator keeps its private values from its share until it has the responder's; the
 * responder computes its share and the shared secrets at once and keeps nothing.
 */
public record KeyShare(Group group, byte[] share) {
    /** The initiator's private values and its share. Not thread-safe; used once. */
    public static final class Initiator {
        private final Group group;
        private PrivateKey x;
        private MlKem768.DecapsulationKey dk;
        private final byte[] share;

        private Initiator(Group group, PrivateKey x, MlKem768.DecapsulationKey dk, byte[] share) {
            this.group = group;
            this.x = x;
            this.dk = dk;
            this.share = share;
        }

        public Group group() { return group; }

        public KeyShare share() { return new KeyShare(group, share.clone()); }

        /**
         * The shared secrets, in the Group's order, from the responder's share of the right
         * length; the private values are dropped. A refused share is KEX.
         */
        public byte[][] finish(byte[] responder) throws SecureCarrierException {
            if (x == null) throw new IllegalStateException("key share used");
            try {
                byte[] dh = X25519.dh(x, Arrays.copyOf(responder, X25519.LEN));
                if (group == Group.X25519) return new byte[][] {dh};
                byte[] ct = Arrays.copyOfRange(responder, X25519.LEN, responder.length);
                return new byte[][] {dh, MlKem768.decapsulate(dk.key(), ct)};
            } finally {
                x = null;
                dk = null;
            }
        }
    }

    /**
     * New private values of group and their share; {@code x} is the X25519 scalar, null for a
     * random one, and {@code kem} the source of the ML-KEM seed, null for the operating system's.
     */
    public static Initiator initiate(Group group, byte[] x, SecureRandom kem) throws SecureCarrierException {
        PrivateKey priv = X25519.privateKey(x != null ? x : X25519.generate(null));
        byte[] e = X25519.publicKey(priv);
        if (group == Group.X25519) return new Initiator(group, priv, null, e);
        MlKem768.DecapsulationKey dk = MlKem768.generate(kem);
        return new Initiator(group, priv, dk, Kdf.concat(e, dk.encapsulationKey()));
    }

    /** The responder's share and the shared secrets, in the Group's order. */
    public record Response(KeyShare share, byte[][] secrets) {}

    /**
     * Answers an initiator's share of the right length: {@code x} is the responder's X25519
     * scalar (null for a random one) and {@code kem} the source of m (null for the operating
     * system's). A refused {@code ek} or an all-zero X25519 output is KEX.
     */
    public static Response respond(KeyShare initiator, byte[] x, SecureRandom kem) throws SecureCarrierException {
        Group g = initiator.group();
        byte[] in = initiator.share();
        PrivateKey priv = X25519.privateKey(x != null ? x : X25519.generate(null));
        byte[] e = X25519.publicKey(priv);
        if (g == Group.X25519_MLKEM768) MlKem768.check(Arrays.copyOfRange(in, X25519.LEN, in.length));
        byte[] dh = X25519.dh(priv, Arrays.copyOf(in, X25519.LEN));
        if (g == Group.X25519) return new Response(new KeyShare(g, e), new byte[][] {dh});
        MlKem768.Encapsulation enc = MlKem768.encapsulate(Arrays.copyOfRange(in, X25519.LEN, in.length), kem);
        return new Response(new KeyShare(g, Kdf.concat(e, enc.ciphertext())), new byte[][] {dh, enc.secret()});
    }

    /** The value of the key share item: {@code Group(varint) ‖ Share}. */
    byte[] encode() {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Tlv.putVarint(out, group.id());
        out.writeBytes(share);
        return out.toByteArray();
    }
}
