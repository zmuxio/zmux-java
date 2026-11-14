package io.zmux;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Multipath sessions over real carriers: listener joins, spreading, pipe loss, leak checks. */
@Timeout(120)
class MultipathTest {
    private static ZmuxConfig.Builder mp(EventLoops loops, PipeAuth auth) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(auth);
    }

    /** Opens n streams of size bytes from c; s echoes each one's digest. during runs mid-transfer. */
    private static void exchange(ExecutorService ex, ZmuxSession c, ZmuxSession s, int n, int size, Callable<?> during)
            throws Exception {
        List<Future<?>> server = new ArrayList<>();
        Future<?> acceptor = ex.submit(() -> {
            for (int i = 0; i < n; i++) {
                ZmuxStream st = s.acceptStream();
                server.add(ex.submit(() -> {
                    byte[] got = SessionTest.readAll(st);
                    st.write(MessageDigest.getInstance("SHA-256").digest(got));
                    st.finish();
                    st.close();
                    return null;
                }));
            }
            return null;
        });
        List<Future<?>> cs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int seed = i;
            cs.add(ex.submit(() -> {
                byte[] data = new byte[size];
                new Random(seed).nextBytes(data);
                ZmuxStream st = c.openStream();
                st.write(data);
                st.finish();
                assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(data), SessionTest.readAll(st));
                st.close();
                return null;
            }));
        }
        if (during != null) during.call();
        for (Future<?> f : cs) f.get();
        acceptor.get();
    }

    private static void closeBoth(ZmuxSession c, ZmuxSession s) throws Exception {
        c.abort(0, "done");
        s.awaitClosed();
        c.awaitClosed();
    }

    @Test
    void joinAndSpreadOverTcp() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            ZmuxConfig cfg = mp(loops, PipeAuth.psk(new byte[32]).withEndToEnd()).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).scheduler(Scheduler.SPREAD_ETA).build();
            var lo = ListenerConfig.builder().session(cfg).instanceId(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), lo);
                 var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get();
                assertTrue(c.info().multipath());
                ZmuxPipe p1 = c.addPipe(l.localAddress(), PipeOptions.defaults());
                ZmuxPipe p2 = c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertEquals(3, c.pipes().size());
                assertEquals(PipeState.Outbound.ACTIVE, p1.state().orElseThrow().outbound());
                exchange(ex, c, s, 8, 2 << 20, null);
                assertEquals(3, s.pipes().size());
                p2.close();
                exchange(ex, c, s, 2, 1 << 20, null);
                closeBoth(c, s);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @Test
    void transferSurvivesLosingThePipeItStartedOn() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = mp(loops, PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get();
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ZmuxPipe zero = c.pipes().stream().filter(p -> p.id().value() == 0).findFirst().orElseThrow();
                exchange(ex, c, s, 4, 4 << 20, () -> {
                    Thread.sleep(20);
                    zero.abort();
                    return null;
                });
                // A retired handle's reads report absence; its operations fail.
                assertTrue(zero.state().isEmpty());
                assertTrue(zero.stats().isEmpty());
                assertThrows(PipeGoneException.class, zero::revalidate);
                assertEquals(1, c.pipes().size());
                closeBoth(c, s);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @Test
    void boundJoinOverTls() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = mp(loops, PipeAuth.exporter().withEndToEnd()).allowRecordMode((carrier, mode, peer) -> mode == RecordMode.BOUND).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()).withSecure()).build());
                 var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connectTls(l.localAddress(), TlsOptions.client(TestTls.client(), "localhost"), cfg);
                ZmuxSession s = sf.get();
                ZmuxPipe p = c.addTlsPipe(l.localAddress(), TlsOptions.client(TestTls.client(), "localhost").withSecure(), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                assertTrue(p.id().value() > 0);
                exchange(ex, c, s, 4, 1 << 20, null);
                closeBoth(c, s);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @Test
    void addPipeWithoutMultipathFails() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = mp(loops, PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get();
                // Without multipath the session has no pipes to add.
                ZmuxConfig single = ZmuxConfig.builder().eventLoops(loops).build();
                Future<ZmuxSession> sf2 = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c2 = Zmux.connect(l.localAddress(), single);
                ZmuxSession s2 = sf2.get();
                assertThrows(NotNegotiatedException.class, () -> c2.addPipe(l.localAddress(), PipeOptions.defaults()));
                closeBoth(c2, s2);
                exchange(ex, c, s, 1, 1 << 16, null);
                closeBoth(c, s);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class, () -> PipeAuth.psk(new byte[8]).withEndToEnd());
        assertThrows(IllegalArgumentException.class, () -> new PipeAuth(new byte[16], false, true, false));
        org.junit.jupiter.api.Assertions.assertFalse(PipeAuth.none().isConfigured(), "no source is a value; a session that needs one fails to start");
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().suspectFloor(Duration.ofMillis(10)));
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().deadTimeout(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().maxPipes(0));
        assertThrows(IllegalArgumentException.class, () -> PipeLiveness.builder().suspectFloor(Duration.ofMillis(1)));
        assertTrue(ZmuxConfig.builder().resumeTimeout(Duration.ofSeconds(5)).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build().isMultipath());
    }
}
