package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class LostRecordsTest {
    @Test void lostRecordsAreExactWhereTheProductOverflows() {
        assertEquals(3, Session.lostRecords(5, 5, 10));
        // The product overflows and the count is beyond a double's precision.
        assertEquals((1L << 53) + 1, Session.lostRecords((1L << 53) + 1, 1L << 20, 1L << 20));
        assertEquals(((1L << 53) + 1) / 2 + 1, Session.lostRecords((1L << 53) + 1, 1L << 20, 1L << 21));
        assertEquals(Long.MAX_VALUE, Session.lostRecords(Long.MAX_VALUE, Long.MAX_VALUE, 1));
    }
}
