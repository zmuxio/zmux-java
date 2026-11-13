package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The negotiation results a session reports. */
@Timeout(30)
class NegotiationResultsTest {
    @Test void tlsSessionReportsTheCompleteList() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), lc)) {
                var sf = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connectTls(l.localAddress(), TlsOptions.client(TestTls.client(), "localhost"), cfg);
                ZmuxSession s = sf.get();
                var i = c.info();
                assertTrue(i.established());
                assertTrue(i.initiator());
                assertEquals("draft-02", i.qmuxVersion());
                assertEquals("qmux-02", i.applicationProtocol());
                assertTrue(i.tls().isPresent(), "this process ran the TLS handshake");
                assertEquals("TLSv1.3", i.tls().orElseThrow().getProtocol());
                assertTrue(i.peerIsZmux());
                assertTrue(i.localDatagrams() && i.peerDatagrams());
                assertTrue(i.localResetStreamAt() && i.peerResetStreamAt());
                assertFalse(i.multipath() || i.resume() || i.suspended() || i.joinPending());
                assertTrue(i.peerGoAway().isEmpty());
                assertTrue(i.maxDatagramPayload() > 0);
                assertFalse(s.info().initiator());
                assertTrue(s.info().tls().isPresent());
                try (ZmuxStream st = c.openStream()) {
                    int perStream = st.maxDatagramPayload(0);
                    assertTrue(perStream > 0 && perStream < i.maxDatagramPayload(), perStream + " vs " + i.maxDatagramPayload());
                    assertTrue(st.maxDatagramPayload(1L << 40) < perStream, "a longer context leaves less");
                }
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }

    @Test void receivedGoAwayIsReportedWithItsWatermarks() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor();
             Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            assertTrue(p.client().info().tls().isEmpty(), "a memory carrier has no TLS");
            ZmuxStream open = p.client().openStream();
            open.write(ByteBuffer.wrap(new byte[] {1}));
            ZmuxStream accepted = p.server().acceptStream();
            var shut = ex.submit(() -> { p.server().shutdown(Duration.ofSeconds(10)); return null; });
            SessionEvent e;
            do e = p.client().nextEvent(Duration.ofSeconds(10)); while (!(e instanceof SessionEvent.GoAwayReceived));
            var g = (SessionEvent.GoAwayReceived) e;
            var reported = p.client().info().peerGoAway().orElseThrow();
            // A draining peer may lower its watermarks with a later GOAWAY: the info is the
            // latest one, and each one has its event, in order.
            while (!new ConnectionInfo.GoAway(g.maxBidi(), g.maxUni(), g.code()).equals(reported)) {
                do e = p.client().nextEvent(Duration.ofSeconds(10)); while (!(e instanceof SessionEvent.GoAwayReceived));
                g = (SessionEvent.GoAwayReceived) e;
            }
            assertTrue(reported.maxBidi() >= 1, "the open stream stays below the watermark");
            open.finish(); accepted.finish();
            SessionTest.readAll(open);
            SessionTest.readAll(accepted);
            open.close(); accepted.close();
            shut.get();
        }
    }
}
