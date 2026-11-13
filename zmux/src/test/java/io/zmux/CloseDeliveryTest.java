package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** N16: a coded close over TCP and TLS delivers the data written before it, then the code. */
@Timeout(60)
class CloseDeliveryTest {

    /**
     * The client writes 1 MiB on one stream and a few bytes on a second it leaves open, then
     * closes with code and reason and waits for the whole end: the server reads the 1 MiB in
     * order, then ends with ApplicationClosed(code, reason, remote).
     */
    static void closeDelivers(ZmuxSession client, ZmuxSession server, long code, String reason) throws Exception {
        byte[] data = new byte[1 << 20];
        new java.util.Random(code).nextBytes(data);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var st = client.openStream();
            st.write(data);
            var second = client.openStream();
            second.write(new byte[] {1, 2, 3});
            var in = server.acceptStream(Duration.ofSeconds(5));
            assertNotNull(in);
            Future<byte[]> read = workers.submit(() -> {
                var got = new java.io.ByteArrayOutputStream();
                byte[] b = new byte[1 << 16];
                try {
                    for (int n; (n = in.read(b, 0, b.length)) > 0; ) got.write(b, 0, n);
                } catch (IOException ended) { /* The session's end. */ }
                return got.toByteArray();
            });
            // With a linger of an hour, an end within the guard means the peer read the close and ended its side.
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofHours(1));
            SessionError.ApplicationClosed mine;
            try {
                long t0 = System.nanoTime();
                if (code == 0 && reason.isEmpty()) client.close(); else client.close(code, reason);
                assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(5), "close is bounded");
                mine = assertInstanceOf(SessionError.ApplicationClosed.class, client.closed().get(30, TimeUnit.SECONDS));
                assertEquals(new SessionError.ApplicationClosed(code, reason, true), server.closed().get(30, TimeUnit.SECONDS));
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
            assertEquals(code, mine.code());
            assertFalse(mine.remote());
            var e = assertThrows(SessionClosedException.class, () -> second.write(new byte[] {4}));
            assertEquals(mine, e.error());
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, server.closed().get(5, TimeUnit.SECONDS));
            assertEquals(new SessionError.ApplicationClosed(code, reason, true), why);
            assertTrue(Arrays.equals(data, read.get(5, TimeUnit.SECONDS)), "the peer read the whole 1 MiB in order");
        }
    }

    @Test void overTcp() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                for (long code : new long[] {0x2a, 0}) {
                    var c = Zmux.connect(l.localAddress(), cfg);
                    closeDelivers(c, l.accept(Duration.ofSeconds(5)), code, code == 0 ? "" : "bye");
                }
            }
        }
    }

    @Test void overTls() throws Exception {
        TestTls.init();
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var tls = TlsOptions.client(TestTls.client(), "localhost");
                for (long code : new long[] {0x2a, 0}) {
                    var c = Zmux.connectTls(l.localAddress(), tls, cfg);
                    closeDelivers(c, l.accept(Duration.ofSeconds(5)), code, code == 0 ? "" : "bye");
                }
            }
        }
    }
}
