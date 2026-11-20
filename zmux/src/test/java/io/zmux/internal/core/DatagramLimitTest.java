package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.TransportParams;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The advertised datagram maximum is what the record writer fits; a datagram that no longer fits is dropped, not left at the queue head. */
@Timeout(30)
class DatagramLimitTest {
    static Session session(CoreConfig.Role role, int maxRecord) {
        CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role).datagrams(true);
        TransportParams l = c.local();
        l.present |= TransportParams.DATAGRAM;
        l.maxDatagramFrameSize = 65535;
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true, null, null, null, 0, "", maxRecord, false);
        s.attachBootstrap(info, info, null);
        return s;
    }

    static void pump(Session a, Session b) {
        Batch batch = new Batch();
        for (int round = 0; round < 50; round++) {
            boolean moved = false;
            for (Session from : new Session[] {a, b}) {
                Session to = from == a ? b : a;
                while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0)) {
                    byte[] bytes = BatchFixture.flat(batch);
                    from.writeDone(batch, null, 0);
                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                    assertEquals(bytes.length, to.receive(0, 0, new ReadBuf(bb, null), bb, 0, bytes.length, 0));
                    moved = true;
                }
            }
            if (!moved) return;
        }
        fail("pump does not converge");
    }

    @Test void maxDataCountsTheFrameTypeAndLength() {
        assertEquals(-1, Datagrams.maxData(1));
        assertEquals(0, Datagrams.maxData(2));
        assertEquals(61, Datagrams.maxData(63));  // 1 + 1 + 61
        assertEquals(63, Datagrams.maxData(66));  // 63 has a 1-byte Length, 64 would need 2 bytes
        assertEquals(65530, Datagrams.maxData(65535)); // 1 + 4 + 65530
    }

    @Test void exactMaximumIsDeliveredAndATooLargeQueuedOneIsDropped() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, 1000), b = session(CoreConfig.Role.RESPONDER, 0);
        // Queued before the peer's limits are known: checked only when written.
        a.sendDatagram(null, 0, new byte[5000]);
        a.sendDatagram(null, 0, new byte[] {7});
        pump(a, b);
        assertArrayEquals(new byte[] {7}, b.datagrams().receive(), "a datagram too large for the records does not hold the queue");
        assertNull(b.datagrams().receive());
        assertTrue(a.statsSnapshot().datagramsDroppedSend() >= 1);

        int max = a.datagrams().maxPayload(0);
        assertEquals(Datagrams.maxData(1000), max);
        a.sendDatagram(null, 0, new byte[max]);
        pump(a, b);
        assertEquals(max, b.datagrams().receive().length);
        var e = assertThrows(CoreException.class, () -> a.sendDatagram(null, 0, new byte[max + 1]));
        assertEquals(CoreException.Reason.DATAGRAM_TOO_LARGE, e.reason());
    }
}
