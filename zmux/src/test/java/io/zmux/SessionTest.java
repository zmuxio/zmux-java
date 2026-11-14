package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class SessionTest {
    static EventLoops loops;
    static ZmuxConfig cfg;

    @BeforeAll
    static void setUp() {
        loops = EventLoops.create(2);
        cfg = ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
    }

    @AfterAll
    static void tearDown() {
        loops.close();
        assertEquals(0, loops.liveThreads(), "loop threads left");
    }

    static byte[] readAll(ZmuxReceiveStream s) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[65536];
        for (int n; (n = s.read(buf, 0, buf.length)) >= 0; ) out.write(buf, 0, n);
        return out.toByteArray();
    }

    @Test
    void echoWithPreamble() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream a = p.client().openStream(Preamble.openInfo("dst:1".getBytes(UTF_8)));
            a.write("hello".getBytes(UTF_8));
            a.finish();
            ZmuxStream b = p.server().acceptStream();
            assertArrayEquals("dst:1".getBytes(UTF_8), b.preamble().openInfo());
            assertArrayEquals("hello".getBytes(UTF_8), readAll(b));
            b.write("world".getBytes(UTF_8));
            b.finish();
            assertArrayEquals("world".getBytes(UTF_8), readAll(a));
            assertTrue(p.client().info().preamble());
        }
    }

    @Test
    void preamblePriorityReportsTheOpenersPriority() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream plain = p.client().openStream();
            assertTrue(plain.preamblePriority().isEmpty(), "a local stream has none");
            plain.write("a".getBytes(UTF_8));
            ZmuxStream ranked = p.client().openStream();
            ranked.setPeerPriority(new Priority(1, true));
            ranked.write("b".getBytes(UTF_8));
            ranked.finish();
            plain.finish();
            ZmuxStream first = p.server().acceptStream();
            ZmuxStream second = p.server().acceptStream();
            assertTrue(first.preamblePriority().isEmpty(), "the opener sent none");
            assertEquals(java.util.Optional.of(new Priority(1, true)), second.preamblePriority());
        }
    }

    @Test
    void bulkAndManyStreams() throws Exception {
        try (Pair p = Pair.memory(cfg); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            int streams = 32, size = 1 << 20;
            List<Future<?>> fs = new ArrayList<>();
            for (int i = 0; i < streams; i++) {
                int seed = i;
                fs.add(ex.submit(() -> {
                    byte[] data = new byte[size];
                    new Random(seed).nextBytes(data);
                    ZmuxStream s = p.client().openStream(Preamble.openInfo(new byte[] {(byte) seed}));
                    s.write(data);
                    s.finish();
                    assertArrayEquals(data, readAll(s));
                    s.close();
                    return null;
                }));
            }
            for (int i = 0; i < streams; i++) {
                ZmuxStream s = p.server().acceptStream();
                fs.add(ex.submit(() -> {
                    byte[] got = readAll(s);
                    byte[] want = new byte[size];
                    new Random(s.preamble().openInfo()[0]).nextBytes(want);
                    assertArrayEquals(want, got);
                    s.write(got);
                    s.finish();
                    s.close();
                    return null;
                }));
            }
            for (Future<?> f : fs) f.get();
        }
    }

    @Test
    void resetAndStopReachThePeer() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream a = p.client().openStream();
            a.write("x".getBytes(UTF_8));
            ZmuxStream b = p.server().acceptStream();
            a.reset(7);
            StreamResetException e = assertThrows(StreamResetException.class, () -> readAll(b));
            assertEquals(7, e.code());
            b.stop(9);
            ZmuxStream c = p.client().openStream();
            c.write("y".getBytes(UTF_8));
            ZmuxStream d = p.server().acceptStream();
            d.stop(5);
            StreamStoppedException s = assertThrows(StreamStoppedException.class, () -> {
                // Until the STOP_SENDING arrives: a fixed amount can fit in the buffers under load.
                long end = System.nanoTime() + 10_000_000_000L;
                while (System.nanoTime() < end) c.write(new byte[1024]);
            });
            assertEquals(5, s.code());
        }
    }

    @Test
    void concurrentStreamDatagramWaitersAreEachServedAndTimeoutsWithdraw() throws Exception {
        try (Pair p = Pair.memory(cfg); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxStream a = p.client().openStream();
            a.write(new byte[] {1});
            ZmuxStream b = p.server().acceptStream();
            assertThrows(java.net.SocketTimeoutException.class, () -> b.receiveDatagram(Duration.ofMillis(20)));
            Future<StreamDatagram> first = ex.submit(() -> b.receiveDatagram());
            Future<StreamDatagram> second = ex.submit(() -> b.receiveDatagram());
            Thread.sleep(50);
            a.sendDatagram(1, ByteBuffer.wrap(new byte[] {1}));
            a.sendDatagram(2, ByteBuffer.wrap(new byte[] {2}));
            long contexts = first.get(5, java.util.concurrent.TimeUnit.SECONDS).context()
                    + second.get(5, java.util.concurrent.TimeUnit.SECONDS).context();
            assertEquals(3, contexts);
            assertThrows(java.net.SocketTimeoutException.class, () -> p.server().nextEvent(Duration.ofMillis(20)));
            assertThrows(java.net.SocketTimeoutException.class, () -> p.server().datagrams().receive(Duration.ofMillis(20)));
            p.client().datagrams().send(ByteBuffer.wrap(new byte[] {7}));
            assertArrayEquals(new byte[] {7}, p.server().datagrams().receive(Duration.ofSeconds(5)));
        }
    }

    @Test
    void readTimeoutCountsTheWaitBehindAnotherReader() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream a = p.client().openStream();
            a.write(new byte[] {1});
            ZmuxStream b = p.server().acceptStream();
            assertEquals(1, b.read(new byte[1], 0, 1));
            var pending = b.readAsync(ByteBuffer.allocate(1));
            b.setReadTimeout(Duration.ofMillis(30));
            assertThrows(java.net.SocketTimeoutException.class, () -> b.read(new byte[1], 0, 1));
            a.write(new byte[] {2});
            assertEquals(1, pending.get(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test
    void closingTheInputViewKeepsTheSendingPart() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream a = p.client().openStream();
            a.write(new byte[] {1});
            ZmuxStream b = p.server().acceptStream();
            assertEquals(1, b.read(new byte[1], 0, 1));
            b.inputStream().close();
            try (var out = b.outputStream()) { out.write(new byte[] {4, 5}); }
            assertArrayEquals(new byte[] {4, 5}, readAll(a));
        }
    }

    @Test
    void datagramsAndPing() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            p.client().datagrams().send(ByteBuffer.wrap("dg".getBytes(UTF_8)));
            assertArrayEquals("dg".getBytes(UTF_8), p.server().datagrams().receive());
            Duration rtt = p.client().ping();
            assertTrue(rtt.compareTo(Duration.ofSeconds(5)) < 0);
            ZmuxStream a = p.client().openStream();
            a.sendDatagram(3, ByteBuffer.wrap("sd".getBytes(UTF_8)));
            ZmuxStream b = p.server().acceptStream();
            StreamDatagram d = b.receiveDatagram();
            assertEquals(3, d.context());
            assertArrayEquals("sd".getBytes(UTF_8), d.payload());
        }
    }

    @Test
    void closeReportsTheCodeToThePeer() throws Exception {
        Pair p = Pair.memory(cfg);
        ZmuxStream b0 = p.client().openStream();
        b0.write(new byte[1]);
        ZmuxStream pending = p.server().acceptStream();
        p.client().abort(42, "bye");
        SessionError e = p.server().awaitClosed();
        SessionError.ApplicationClosed ac = assertInstanceOf(SessionError.ApplicationClosed.class, e);
        assertEquals(42, ac.code());
        assertTrue(ac.remote());
        assertEquals("bye", ac.reason());
        SessionClosedException x = assertThrows(SessionClosedException.class, () -> readAll(pending));
        assertInstanceOf(SessionError.ApplicationClosed.class, x.error());
        p.client().awaitClosed();
        p.close();
    }

    @Test
    void readTimeoutAndShutdown() throws Exception {
        Pair p = Pair.memory(cfg);
        ZmuxStream a = p.client().openStream();
        a.write("q".getBytes(UTF_8));
        ZmuxStream b = p.server().acceptStream();
        assertEquals(1, b.read(new byte[8], 0, 8));
        b.setReadTimeout(Duration.ofMillis(50));
        assertThrows(java.net.SocketTimeoutException.class, () -> b.read(new byte[8], 0, 8));
        a.close();
        b.close();
        p.client().shutdown(0, "", Duration.ofSeconds(5));
        assertTrue(p.client().isClosed());
        p.server().awaitClosed();
        p.close();
    }

    /** A stream whose both parts ended no longer holds up a shutdown, though neither handle was closed. */
    @Test
    void shutdownDoesNotWaitForTheHandlesOfEndedStreams() throws Exception {
        Pair p = Pair.memory(cfg);
        ZmuxStream a = p.client().openStream();
        a.write("q".getBytes(UTF_8));
        a.finish();
        ZmuxStream b = p.server().acceptStream();
        assertEquals("q", new String(b.inputStream().readAllBytes(), StandardCharsets.UTF_8));
        b.write("r".getBytes(UTF_8));
        b.finish();
        assertEquals("r", new String(a.inputStream().readAllBytes(), StandardCharsets.UTF_8));
        long start = System.nanoTime();
        p.client().shutdown(0, "", Duration.ofSeconds(10));
        assertTrue(p.client().isClosed());
        assertTrue(System.nanoTime() - start < Duration.ofSeconds(5).toNanos(), "the shutdown waited for the open handles");
        // The session ended gracefully, not by the timeout's close.
        assertInstanceOf(SessionError.ApplicationClosed.class, p.server().awaitClosed());
        p.close();
    }

    @Test
    void streamViewsAndUtf8() throws Exception {
        try (Pair p = Pair.memory(cfg)) {
            ZmuxStream a = p.client().openStream();
            try (var out = a.outputStream()) {
                out.write("ünïcode".getBytes(StandardCharsets.UTF_8));
            }
            ZmuxStream b = p.server().acceptStream();
            assertEquals("ünïcode", new String(b.inputStream().readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
