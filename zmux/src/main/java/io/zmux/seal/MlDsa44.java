package io.zmux.seal;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.NamedParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

/** Pure ML-DSA-44 of FIPS 204 through the JDK, with an empty context, and the key's raw pkEncode form. */
final class MlDsa44 {
    static final String NAME = "ML-DSA-44";
    static final int PUBLIC_KEY_LEN = 1312;
    static final int SIGNATURE_LEN = 2420;
    static final int SEED_LEN = 32;

    /** The SubjectPublicKeyInfo before the raw key: id-ml-dsa-44 (2.16.840.1.101.3.4.3.17), no parameters. */
    private static final byte[] SPKI_PREFIX = java.util.HexFormat.of().parseHex("30820532300b06096086480165030403110382052100");

    private MlDsa44() {}

    static boolean verify(byte[] key, byte[] message, byte[] sig) {
        try {
            Signature v = Signature.getInstance(NAME);
            v.initVerify(publicKey(key));
            v.update(message);
            return v.verify(sig);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return false;
        }
    }

    private static PublicKey publicKey(byte[] raw) throws GeneralSecurityException {
        byte[] spki = Arrays.copyOf(SPKI_PREFIX, SPKI_PREFIX.length + raw.length);
        System.arraycopy(raw, 0, spki, SPKI_PREFIX.length, raw.length);
        return KeyFactory.getInstance(NAME).generatePublic(new X509EncodedKeySpec(spki));
    }

    /** The raw pkEncode of a JDK key. */
    static byte[] raw(PublicKey k) {
        byte[] spki = k.getEncoded();
        if (spki.length != SPKI_PREFIX.length + PUBLIC_KEY_LEN || !Arrays.equals(spki, 0, SPKI_PREFIX.length, SPKI_PREFIX, 0, SPKI_PREFIX.length)) {
            throw new IllegalArgumentException("not an ML-DSA-44 public key");
        }
        return Arrays.copyOfRange(spki, SPKI_PREFIX.length, spki.length);
    }

    /** A key pair; KeyGen draws its seed ξ, 32 bytes, from random. */
    static KeyPair generate(SecureRandom random) throws GeneralSecurityException {
        KeyPairGenerator g = KeyPairGenerator.getInstance(NAME);
        g.initialize(NamedParameterSpec.ML_DSA_44, random);
        return g.generateKeyPair();
    }

    /** Signs message; the signature's rnd, 32 bytes, comes from random: hedged, or zeros for the deterministic form. */
    static byte[] sign(PrivateKey key, byte[] message, SecureRandom random) throws GeneralSecurityException {
        Signature s = Signature.getInstance(NAME);
        s.initSign(key, random);
        s.update(message);
        return s.sign();
    }
}
