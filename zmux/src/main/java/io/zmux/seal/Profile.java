package io.zmux.seal;

import io.zmux.internal.kex.MlKem768;

import io.zmux.internal.join.Suite;

/**
 * One complete handshake and cipher combination of Establish. The client chooses; there is no negotiation. Join,
 * Profile 2, is the core's for every further carrier of a session and is not one of these. Numbers, patterns and
 * Levels are assigned here only.
 */
public enum Profile {
    /** XXhfs+psk0, ML-KEM-768 beside X25519, ChaCha20-Poly1305: a client's default without AES hardware. */
    XXHFS_PSK0_CHACHA(1, true, Suite.CHACHA, "ChaChaPoly"),
    /** {@link #XXHFS_PSK0_CHACHA} with AES-256-GCM: a client's default with AES hardware. */
    XXHFS_PSK0_AES(3, true, Suite.AES, "AESGCM"),
    /** XXhfs without a pre-shared key: it sends the node's credentials to anonymous clients, so a server takes it only when listed. */
    XXHFS_CHACHA(4, false, Suite.CHACHA, "ChaChaPoly"),
    /** {@link #XXHFS_CHACHA} with AES-256-GCM. */
    XXHFS_AES(5, false, Suite.AES, "AESGCM");

    /** The handshake name a shaper is told for the messages of every Profile. */
    public static final String HANDSHAKE_ESTABLISH = "establish";

    /** The first wire byte that announces a varint extended Profile. */
    static final int ESCAPE = 8;

    private final long value;
    private final boolean psk;
    private final Suite suite;
    private final String cipherName;

    Profile(long value, boolean psk, Suite suite, String cipherName) {
        this.value = value;
        this.psk = psk;
        this.suite = suite;
        this.cipherName = cipherName;
    }

    public long value() {
        return value;
    }

    /** Whether the Profile takes a pre-shared key and a Handle. */
    public boolean hasPsk() {
        return psk;
    }

    /**
     * Whether the handshake and its records stay confidential against a passive quantum
     * adversary (the PQ confidentiality column): true for every Profile, which all combine
     * X25519 with ML-KEM-768.
     */
    public boolean pqConfidential() {
        return true;
    }

    Suite suite() {
        return suite;
    }

    /** The Noise protocol name. */
    public String noiseName() {
        return "Noise_" + (psk ? "XXhfs+psk0" : "XXhfs") + "_25519+" + MlKem768.NAME + "_" + cipherName + "_SHA256";
    }

    /** The Profile of a value, or null when it is unknown or reserved. */
    public static Profile of(long value) {
        for (Profile p : values()) {
            if (p.value == value) return p;
        }
        return null;
    }
}
