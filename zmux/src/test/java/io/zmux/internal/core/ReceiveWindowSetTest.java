package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The argument rules of a receiving part's window setter, applied on the core's own state. */
@Timeout(30)
class ReceiveWindowSetTest {
    /** Credit already advertised is never withdrawn: a higher initial raises the window, a lower one leaves it. */
    @Test void aLowerInitialNeverWithdrawsCredit() throws Exception {
        Session s = HeldControlTest.session(CoreConfig.Role.INITIATOR);
        Stream st = s.streams().openStream(true);
        s.receiving().setReceiveWindow(st, 8 << 20, 0);
        assertEquals(8 << 20, st.rcv.window());
        s.receiving().setReceiveWindow(st, 1 << 20, 2 << 20);
        assertEquals(8 << 20, st.rcv.window(), "a lower initial leaves the window in force");
        s.receiving().setReceiveWindow(st, 4096, 0);
        assertEquals(8 << 20, st.rcv.window(), "so does the 4 KiB floor");
    }

    /** A max of 0 is the explicit automatic cap, not the session's and not the previous one. */
    @Test void aZeroMaxIsAnExplicitAutomaticCap() throws Exception {
        Session s = HeldControlTest.session(CoreConfig.Role.INITIATOR);
        Stream st = s.streams().openStream(true);
        assertEquals(0, st.rcv.maxWin(), "never set: the session's cap applies");
        s.receiving().setReceiveWindow(st, 1 << 20, 4 << 20);
        assertEquals(4 << 20, st.rcv.maxWin());
        s.receiving().setReceiveWindow(st, 1 << 20, 0);
        assertEquals(StreamRecv.AUTOMATIC, st.rcv.maxWin());
    }

    /** The argument check: a nonzero max below initial or a negative value is refused. */
    @Test void aMaxBelowInitialIsRefused() {
        assertDoesNotThrow(() -> StreamRecv.checkReceiveWindow(1 << 20, 0));
        assertDoesNotThrow(() -> StreamRecv.checkReceiveWindow(1 << 20, 1 << 20));
        assertDoesNotThrow(() -> StreamRecv.checkReceiveWindow(4096, 4096));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(0, 0));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(4095, 0));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(0, 1));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(2 << 20, 1 << 20));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> StreamRecv.checkReceiveWindow(0, -1));
    }
}
