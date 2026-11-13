package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;
import static io.zmux.seal.SecureCarrierException.Code.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Code.TLV_CREDENTIALS;
import static io.zmux.seal.SecureCarrierException.Code.TOO_LARGE;

import io.zmux.internal.join.Tlv;
import java.io.ByteArrayOutputStream;

/**
 * The content of a handshake message. Absent items are null; application data that is empty is
 * not sent.
 */
record EstablishPayload(Credential credential, byte[] appData, byte[] firstBytes, Proof proof, int padding, boolean randomPadding) {
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    EstablishPayload(Credential credential, byte[] appData, byte[] firstBytes, int padding) {
        this(credential, appData, firstBytes, null, padding, false);
    }

    static final int KNOWN_CREDENTIAL = 1;
    static final int KNOWN_FIRST_BYTES = 2;
    static final int KNOWN_APP_DATA = 4;
    static final int KNOWN_PROOF = 8;

    /** The default bound on the application data of a received message. */

    private static final int T_PADDING = 0;
    private static final int T_CREDENTIAL = 1;
    private static final int T_APP_DATA = 2;
    private static final int T_FIRST_BYTES = 3;
    private static final int T_PROOF = 8;

    /** A payload of padding only, random for a message sent without a key. */
    static EstablishPayload padding(int n, boolean random) {
        return new EstablishPayload(null, null, null, null, n, random);
    }

    /** This payload with a padding field of n (see {@link Tlv#paddingField}), random for a message sent without a key. */
    EstablishPayload withPadding(int n, boolean random) {
        return new EstablishPayload(credential, appData, firstBytes, proof, n, random);
    }

    /** This payload with proof p. */
    EstablishPayload withProof(Proof p) {
        return new EstablishPayload(credential, appData, firstBytes, p, padding, randomPadding);
    }

    private boolean hasAppData() {
        return appData != null && appData.length > 0;
    }

    /** The encoding, refusing what a message with these known types may not carry. */
    byte[] encode(int known) throws SecureCarrierException {
        if ((credential != null && (known & KNOWN_CREDENTIAL) == 0) || (firstBytes != null && (known & KNOWN_FIRST_BYTES) == 0)
                || (hasAppData() && (known & KNOWN_APP_DATA) == 0) || (proof != null && (known & KNOWN_PROOF) == 0)) {
            throw new SecureCarrierException(INVALID_CONFIG, "payload type not allowed in this message");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (padding != 0) {
            byte[] fill = new byte[Math.max(padding, 0)];
            if (randomPadding) RANDOM.nextBytes(fill);
            Tlv.putItem(out, T_PADDING, fill);
        }
        if (credential != null) Tlv.putItem(out, T_CREDENTIAL, encode(credential));
        if (hasAppData()) Tlv.putItem(out, T_APP_DATA, appData);
        if (firstBytes != null) Tlv.putItem(out, T_FIRST_BYTES, firstBytes);
        if (proof != null) Tlv.putItem(out, T_PROOF, proof.encode());
        return out.toByteArray();
    }

    /** The value of a credential item: {@code CredType(varint) ‖ Credential}. */
    static byte[] encode(Credential c) throws SecureCarrierException {
        if (c.type() <= Anchor.CRED_TYPE_NONE || c.type() > io.zmux.internal.wire.VarInt.MAX) throw new SecureCarrierException(INVALID_CONFIG, "credential type");
        ByteArrayOutputStream v = new ByteArrayOutputStream();
        Tlv.putVarint(v, c.type());
        v.writeBytes(c.raw());
        return v.toByteArray();
    }

    /** The length of the encoding without padding: what the message's plaintext holds besides it. */
    int length() {
        int n = 0;
        if (credential != null) n += item(io.zmux.internal.wire.VarInt.length(credential.type()) + credential.raw().length);
        if (hasAppData()) n += item(appData.length);
        if (firstBytes != null) n += item(firstBytes.length);
        if (proof != null) n += item(proof.encode().length);
        return n;
    }

    private static int item(int v) {
        return 1 + io.zmux.internal.wire.VarInt.length(v) + v;
    }

    /**
     * Validates a whole payload against the types known in its message. Application data above maxAppData is
     * TOO_LARGE; with refuseAppData (message 1 without a key, which carries none) any is MALFORMED.
     */
    static EstablishPayload parse(byte[] b, int known, long maxAppData, boolean refuseAppData) throws SecureCarrierException {
        Credential[] cred = new Credential[1];
        byte[][] app = new byte[1][];
        byte[][] first = new byte[1][];
        Proof[] proof = new Proof[1];
        int[] padding = new int[1];
        Tlv.parse(b, (type, val) -> {
            if (type == T_PADDING) {
                padding[0] = val.length;
            } else if (type == T_CREDENTIAL && (known & KNOWN_CREDENTIAL) != 0) {
                if (val.length == 0) throw new SecureCarrierException(TLV_CREDENTIALS);
                long t = Tlv.minimalVarint(val, 0, val.length);
                if (t == Anchor.CRED_TYPE_NONE) throw new SecureCarrierException(TLV_CREDENTIALS, "CredType 0 is no credential");
                int n = io.zmux.internal.wire.VarInt.length(t);
                cred[0] = new Credential(t, java.util.Arrays.copyOfRange(val, n, val.length));
            } else if (type == T_APP_DATA && refuseAppData) {
                throw new SecureCarrierException(MALFORMED, "application data in an unencrypted message 1");
            } else if (type == T_APP_DATA && (known & KNOWN_APP_DATA) != 0) {
                if (val.length > maxAppData) {
                    throw new SecureCarrierException(TOO_LARGE, "application data of " + val.length + " bytes, above " + maxAppData);
                }
                app[0] = val;
            } else if (type == T_FIRST_BYTES && (known & KNOWN_FIRST_BYTES) != 0) {
                first[0] = val;
            } else if (type == T_PROOF && (known & KNOWN_PROOF) != 0) {
                proof[0] = Proof.parse(val);
            } else {
                return false;
            }
            return true;
        });
        return new EstablishPayload(cred[0], app[0], first[0], proof[0], padding[0], false);
    }
}
