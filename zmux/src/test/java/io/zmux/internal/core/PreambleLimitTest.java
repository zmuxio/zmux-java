package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The preamble bound (API.md Section 4.4): checked when set, and again when the stream takes its identifier. */
@Timeout(30)
class PreambleLimitTest {
    static Session session(CoreConfig.Role role, int maxPreamble) {
        CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role);
        c.local().maxPreambleSize = maxPreamble;
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
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

    static CoreException.Reason reason(ThrowingRunnable r) {
        try { r.run(); return null; } catch (CoreException e) { return e.reason(); }
    }

    interface ThrowingRunnable { void run() throws CoreException; }

    @Test void beforeThePeersParametersTheFloorBoundsAndAShrunkLimitFailsTheStreamAlone() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, 4096), b = session(CoreConfig.Role.RESPONDER, 300);
        Stream big = a.streams().openStream(true);
        assertEquals(CoreException.Reason.PREAMBLE_TOO_LARGE, reason(() -> a.streams().setOpenInfo(big, new byte[2000])),
                "above the 1024-byte floor before the peer's parameters");
        a.streams().setOpenInfo(big, new byte[600]); // within the floor, above what the peer will allow
        a.sending().write(big, ByteBuffer.wrap(new byte[] {1}), true);
        Stream small = a.streams().openStream(true);
        a.streams().setOpenInfo(small, new byte[10]);
        a.sending().write(small, ByteBuffer.wrap(new byte[] {2}), true);
        pump(a, b);
        assertNull(a.error(), "the session goes on");
        CoreException e = a.sending().writeError(big);
        assertNotNull(e);
        assertEquals(StreamError.Kind.PREAMBLE_TOO_LARGE, e.streamError().kind(), "the oversized stream failed locally");
        StreamError sent = a.sending().writeError(small).streamError();
        assertEquals(StreamError.Kind.CLOSED_LOCALLY, sent.kind(), "the next stream went out finished");
        assertEquals(0, sent.streamId(), "with the first identifier: nothing of the failed one was sent");
        // Established: the peer's bound applies at once, and a refused setting changes nothing.
        Stream later = a.streams().openStream(true);
        a.streams().setOpenInfo(later, new byte[200]);
        assertEquals(CoreException.Reason.PREAMBLE_TOO_LARGE, reason(() -> a.streams().addPreambleTlv(later, 0x1000, new byte[200])));
        a.streams().addPreambleTlv(later, 0x1000, new byte[20]);
    }
}
