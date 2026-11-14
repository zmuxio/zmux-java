package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A listener that refuses a new session by its policy, while draining or with its backlog full,
 * sends CONNECTION_REFUSED after its own parameters, as under memory pressure. TCP, not a
 * synchronous memory pipe: the refusal must reach a client still writing its first records.
 */
@Timeout(30)
class ListenerRefuseTest {
    private static final InetSocketAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 0);

    @Test void aDrainingListenerRefusesANewSession() throws Exception {
        try (var l = Zmux.listen(LOOPBACK, ListenerConfig.builder().backlog(1).build())) {
            l.hub.drain(() -> {});
            assertRefused(l);
        }
    }

    @Test void aFullBacklogRefusesANewSession() throws Exception {
        try (var l = Zmux.listen(LOOPBACK, ListenerConfig.builder().backlog(1).build());
             var _ = Zmux.connect(l.localAddress())) {
            // The one backlog slot is taken by a session nobody accepts.
            PipePoolTest.await(() -> l.queued.get() == 1);
            assertRefused(l);
            assertEquals(1, l.stats().sessions());
        }
    }

    private static void assertRefused(ZmuxListener l) throws Exception {
        SessionError e;
        try (var c = Zmux.connect(l.localAddress())) {
            e = c.awaitClosed();
        } catch (SessionClosedException closed) {
            e = closed.error();
        } catch (IOException other) {
            throw new AssertionError("the client ended without the peer's refusal", other);
        }
        var t = assertInstanceOf(SessionError.TransportClosed.class, e, String.valueOf(e));
        assertTrue(t.remote(), String.valueOf(e));
        assertEquals(0x02, t.code(), String.valueOf(e));
        PipePoolTest.await(() -> l.stats().refused() == 1);
    }
}
