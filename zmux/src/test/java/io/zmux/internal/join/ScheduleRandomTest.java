package io.zmux.internal.join;

import static org.junit.jupiter.api.Assertions.*;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

/** R's entropy comes from the operating system's source on every Join. */
class ScheduleRandomTest {
    @Test void theSourceIsThePlatformsAndEveryDrawIsFresh() throws Exception {
        boolean windows = System.getProperty("os.name", "").startsWith("Windows");
        assertEquals(windows ? "Windows-PRNG" : "NativePRNGNonBlocking", Schedule.SOURCE);
        assertEquals(io.zmux.internal.random.OsRandom.SOURCE, Schedule.SOURCE, "the one source");
        assertNotNull(SecureRandom.getInstance(Schedule.SOURCE), "available on this platform");
        Schedule.Entropy a = Schedule.fresh(Suite.AES), b = Schedule.fresh(Suite.AES);
        assertEquals(Suite.AES.nonceLen(), a.random().length);
        assertFalse(java.util.Arrays.equals(a.random(), b.random()));
        assertTrue(b.attempt() > a.attempt());
    }
}
