package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Keep-alive: at most one keep-alive request per period of inactivity after each activity. */
class KeepAliveOnceTest {
    @Test void idleUsesTheLocalDeadlineAndDoesNotFireWhenItOverflows() throws Exception {
        long precise = Time.MILLISECOND + 1;
        for (long timeout : new long[]{precise, Time.NEVER}) {
            var config = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR)
                    .localIdleTimeout(timeout).keepAlive(CoreConfig.OFF).deadTimeout(CoreConfig.OFF);
            config.local().maxIdleTimeoutMillis = timeout == Time.NEVER
                    ? io.zmux.internal.wire.VarInt.MAX : 2;
            var peer = CoreConfig.builder().role(CoreConfig.Role.RESPONDER).transportRole(CoreConfig.Role.RESPONDER)
                    .keepAlive(CoreConfig.OFF).deadTimeout(CoreConfig.OFF);
            peer.local().maxIdleTimeoutMillis = 0;
            Session client = new Session(config.build(), 0);
            Session server = new Session(peer.build(), 0);
            var carrier = new CarrierInfo(Framing.BYTES, false, true, false, true);
            client.attachBootstrap(carrier, carrier, null);
            server.attachBootstrap(carrier, carrier, null);
            HeldControlTest.pump(client, server);
            client.handleTimeout(precise - 1);
            assertEquals(SessionState.ACTIVE, client.state(), () -> String.valueOf(client.error()));
            client.handleTimeout(timeout == Time.NEVER ? Time.NEVER - 1 : timeout);
            if (timeout == Time.NEVER) {
                assertNull(client.error(), () -> String.valueOf(client.error()));
            } else assertEquals(SessionError.Kind.IDLE, client.error().kind());
        }
    }

    @Test void aKeepAliveIsRequestedAtMostOnceAfterEachActivity() {
        int[] writes = {0};
        KeepAlive.Port port = new KeepAlive.Port() {
            @Override public void wantWrite() { writes[0]++; }
            @Override public void budgetExhausted() {}
            @Override public void pingMeasured(long rtt) {}
            @Override public void fenceReached() {}
        };
        long k = 10 * Time.SECOND;
        var ka = new KeepAlive(CoreConfig.builder().keepAlive(k).probeJitter(0).build(), port, null, 0);
        ka.start(0, 0);
        assertEquals(k, ka.deadline(true));
        ka.onTimeout(k, true);
        assertEquals(1, writes[0]);
        assertTrue(ka.wants());
        // The request expires unanswered and nothing else is active: no second keep-alive.
        for (long t = k; t < 100 * k; t += k) {
            ka.onTimeout(t, true);
            assertEquals(1, writes[0], "a second keep-alive without activity at " + t);
        }
        // Activity re-arms exactly one more.
        ka.activity(100 * k);
        assertEquals(101 * k, ka.deadline(true));
        ka.onTimeout(101 * k, true);
        ka.onTimeout(102 * k, true);
        assertEquals(2, writes[0]);
    }
}
