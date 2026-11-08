package io.zmux.internal.random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Handshake randomness comes from the operating system's source, a new instance per use. */
class OsRandomTest {
    @Test void theSourceIsThePlatforms() throws Exception {
        boolean windows = System.getProperty("os.name", "").startsWith("Windows");
        assertEquals(windows ? "Windows-PRNG" : "NativePRNGNonBlocking", OsRandom.SOURCE);
        assertEquals(OsRandom.SOURCE, OsRandom.get().getAlgorithm());
        assertNotSame(OsRandom.get(), OsRandom.get(), "never cached");
        byte[] a = new byte[32], b = new byte[32];
        OsRandom.fill(a);
        OsRandom.fill(b);
        assertFalse(java.util.Arrays.equals(a, b));
    }
}
