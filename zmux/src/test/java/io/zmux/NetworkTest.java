package io.zmux;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Sessions over real TCP and TLS carriers, with leak checks after each. */
@Timeout(120)
class NetworkTest {
    static InetSocketAddress any() throws Exception {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    interface Dial {
        ZmuxSession dial(InetSocketAddress addr, ZmuxConfig cfg) throws Exception;
    }

    void exchange(ZmuxListener l, ZmuxConfig cfg, Dial dial, int streams, int size) throws Exception {
        try (var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
            ZmuxSession c = dial.dial(l.localAddress(), cfg);
            ZmuxSession s = sf.get();
            List<Future<?>> fs = new ArrayList<>();
            fs.add(ex.submit(() -> {
                for (int i = 0; i < streams; i++) {
                    ZmuxStream st = s.acceptStream();
                    fs.add(ex.submit(() -> {
                        byte[] got = SessionTest.readAll(st);
                        st.write(MessageDigest.getInstance("SHA-256").digest(got));
                        st.finish();
                        st.close();
                        return null;
                    }));
                }
                return null;
            }));
            List<Future<?>> cs = new ArrayList<>();
            for (int i = 0; i < streams; i++) {
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
            for (Future<?> f : cs) f.get();
            fs.getFirst().get();
            c.abort(0, "done");
            assertInstanceOf(SessionError.ApplicationClosed.class, s.awaitClosed());
            c.awaitClosed();
        }
    }

    @Test
    void tcp() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (ZmuxListener l = Zmux.listen(any(), ListenerConfig.of(cfg))) {
                exchange(l, cfg, Zmux::connect, 16, 4 << 20);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @Test
    void tls() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (ZmuxListener l = Zmux.listen(any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build())) {
                exchange(l, cfg, (a, c) -> {
                    ZmuxSession s = Zmux.connectTls(a, TlsOptions.client(TestTls.client(), "localhost"), c);
                    assertEquals("qmux-02", s.info().applicationProtocol());
                    return s;
                }, 8, 2 << 20);
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    /**
     * A record the TLS layer rejects, read together with the peer's CONNECTION_CLOSE, does not hide
     * it: the records unwrapped before the bad one reach the session first.
     */
    @Test
    void tlsCloseFollowedByABadRecordIsStillApplicationClosed() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (ZmuxListener l = Zmux.listen(any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build());
                 var relay = java.nio.channels.ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
                var spoil = new java.util.concurrent.atomic.AtomicBoolean();
                Thread.ofVirtual().start(() -> {
                    try (var in = relay.accept(); var out = java.nio.channels.SocketChannel.open(l.localAddress())) {
                        Thread.ofVirtual().start(() -> copy(out, in));
                        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(1 << 17);
                        while (in.read(b) >= 0) {
                            if (spoil.get()) {
                                // The CONNECTION_CLOSE's record, then one whose MAC cannot verify, in one write.
                                b.put(new byte[] {0x17, 0x03, 0x03, 0x00, 0x20}).put(new byte[32]);
                                b.flip();
                                while (b.hasRemaining()) out.write(b);
                                return;
                            }
                            b.flip();
                            while (b.hasRemaining()) out.write(b);
                            b.clear();
                        }
                    } catch (Exception ended) { /* The test is over. */ }
                });
                ZmuxSession c = Zmux.connectTls((InetSocketAddress) relay.getLocalAddress(), TlsOptions.client(TestTls.client(), "localhost"), cfg);
                try (ZmuxSession s = l.accept(java.time.Duration.ofSeconds(10))) {
                    c.openStream().write(new byte[] {1});
                    s.acceptStream().close();
                    Thread.sleep(100); // Nothing else is in flight.
                    spoil.set(true);
                    c.abort(7, "bye");
                    var why = s.closed().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    var app = assertInstanceOf(SessionError.ApplicationClosed.class, why, String.valueOf(why));
                    assertEquals(7, app.code());
                }
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    private static void copy(java.nio.channels.SocketChannel from, java.nio.channels.SocketChannel to) {
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(1 << 16);
        try {
            while (from.read(b) >= 0) { b.flip(); while (b.hasRemaining()) to.write(b); b.clear(); }
        } catch (java.io.IOException ended) { /* Either side closed. */ }
    }

    @Test
    void tlsWithoutCommonAlpnFails() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig server = ZmuxConfig.builder().eventLoops(loops).alpn(List.of("other-02")).build();
            ZmuxConfig client = ZmuxConfig.builder().eventLoops(loops).build();
            try (ZmuxListener l = Zmux.listen(any(), ListenerConfig.builder().session(server).tls(TlsOptions.server(TestTls.server())).build())) {
                assertThrows(Exception.class, () -> Zmux.connectTls(l.localAddress(), TlsOptions.client(TestTls.client(), "localhost"), client));
            }
        }
    }

    @Test
    void peerProcessExitIsPipesLost() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (ZmuxListener l = Zmux.listen(any(), ListenerConfig.of(cfg));
                 var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get();
                ZmuxStream st = c.openStream();
                st.write(new byte[10]);
                ZmuxStream peer = s.acceptStream();
                // Abrupt end of the server's carrier, as when its process exits.
                s.driver.post(() -> s.core.forceClose(new RuntimeException("test")));
                SessionError e = c.awaitClosed();
                assertTrue(e instanceof SessionError.TransportClosed || e instanceof SessionError.PipesLost, e.toString());
                peer.close();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
