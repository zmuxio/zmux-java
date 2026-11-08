package io.zmux.internal.join;

import java.util.Locale;

/** A cipher and hash combination, shared by Join Suites and Establish Profiles. */
public final class Suite {
    /** AES-256-GCM with SHA-256. */
    public static final Suite AES = new Suite(1, "AES/GCM/NoPadding", "AES");
    /** ChaCha20-Poly1305 with SHA-256. */
    public static final Suite CHACHA = new Suite(2, "ChaCha20-Poly1305", "ChaCha20");

    public static final int TAG_LEN = 16;

    private static final long AES_ID = 1;
    private static final long CHACHA_ID = 2;

    private final int id;
    final String transformation;
    final String keyAlgorithm;

    private Suite(int id, String transformation, String keyAlgorithm) {
        this.id = id;
        this.transformation = transformation;
        this.keyAlgorithm = keyAlgorithm;
    }

    public int id() {
        return id;
    }

    public int keyLen() {
        return 32;
    }

    public int hashLen() {
        return 32;
    }

    /** N, the length of R. */
    public int nonceLen() {
        return 32;
    }

    public int tagLen() {
        return TAG_LEN;
    }

    boolean isAes() {
        return this == AES;
    }

    /** The Suite of an identifier, or null when unknown. */
    public static Suite lookup(long id) {
        if (id == AES_ID) return AES;
        if (id == CHACHA_ID) return CHACHA;
        return null;
    }

    /** The identifiers of this implementation, ascending. */
    public static long[] supported() {
        return new long[] {AES_ID, CHACHA_ID};
    }

    /**
     * The Suite to choose among {@code usable}: AES where the architecture has hardware for it,
     * otherwise ChaCha20-Poly1305 when usable; null when none of {@code usable} is known.
     */
    public static Suite preferred(long[] usable) {
        boolean aes = false;
        boolean chacha = false;
        for (long id : usable) {
            aes |= id == AES_ID;
            chacha |= id == CHACHA_ID;
        }
        if (aes && (!chacha || aesHardware())) return AES;
        return chacha ? CHACHA : null;
    }

    /** Whether AES is expected to run on hardware instructions here. */
    public static boolean aesHardware() {
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        return arch.equals("amd64") || arch.equals("x86_64") || arch.equals("aarch64");
    }
}
