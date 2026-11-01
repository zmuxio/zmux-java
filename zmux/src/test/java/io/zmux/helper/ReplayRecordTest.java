package io.zmux.helper;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ReplayRecordTest {
    @Test void aKeyIsAcceptedOncePerGroupUntilItsGroupExpires() {
        var r = new ReplayRecord(3);
        byte[] a = {1}, b = {2};
        assertTrue(r.add(10, a, 9));
        assertFalse(r.add(10, a, 9));
        assertTrue(r.add(11, a, 9), "another group");
        assertTrue(r.add(10, b, 10));
        assertFalse(r.add(12, new byte[] {3}, 10), "full");
        assertTrue(r.add(12, new byte[] {3}, 11), "group 10 expired");
        assertEquals(2, r.size());
        a[0] = 2;
        assertTrue(r.add(11, a, 11), "keys are copied");
    }
}
