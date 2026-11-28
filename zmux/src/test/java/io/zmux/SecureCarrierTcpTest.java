package io.zmux;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The secure carrier's record modes over real TCP: both directions of an added pipe, rejections, eviction and the stall. */
@org.junit.jupiter.api.Timeout(60)
class SecureCarrierTcpTest {
    private static final byte[] PSK = new byte[32];
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static ZmuxConfig.Builder multipath(ZmuxConfig.Builder b, PipeAuth auth) {
        return b.sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(auth).noProbation();
    }

    private static ZmuxConfig.Builder config(EventLoops loops) {
        return multipath(ZmuxConfig.builder().eventLoops(loops), PipeAuth.psk(PSK).withEndToEnd());
    }

    private static void bidi(ZmuxSession c, ZmuxSession s, int n) throws Exception {
        byte[] data = new byte[n];
        new Random(n).nextBytes(data);
        try (var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var echo = ex.submit(() -> {
                ZmuxStream st = s.acceptStream();
                byte[] got = st.inputStream().readAllBytes();
                st.write(got);
                st.finish();
                return null;
            });
            ZmuxStream st = c.openStream();
            st.write(data);
            st.finish();
            assertArrayEquals(data, st.inputStream().readAllBytes());
            echo.get(10, TimeUnit.SECONDS);
        }
    }

    /** The bootstrap pipe dies, so that the data can go only over the added pipe. */
    private static void addedPipeCarries(ZmuxSession c, ZmuxSession s, ZmuxPipe added, RecordMode mode) throws Exception {
        assertTrue(added.id().value() > 0);
        ListenerEntryTest.await(() -> s.pipes().size() == 2);
        assertEquals(mode, c.pipeStats().stream().filter(p -> p.pipe().equals(added.id())).findFirst().orElseThrow().recordMode());
        assertEquals(mode, s.pipeStats().stream().filter(p -> p.pipe().equals(added.id())).findFirst().orElseThrow().recordMode());
        c.pipes().stream().filter(p -> p.id().value() == 0).findFirst().orElseThrow().abort();
        ListenerEntryTest.await(() -> c.pipes().stream().noneMatch(p -> p.id().value() == 0));
        bidi(c, s, 1 << 18);
    }

    @Test void sealedByDefault() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                addedPipeCarries(c, s, c.addPipe(l.localAddress(), PipeOptions.defaults()), RecordMode.SEALED);
                assertEquals(RecordMode.UNMANAGED, c.weakestRecordMode());
            }
        }
    }

    @Test void trustedOnlyWithBothEndsExplicit() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var strict = config(loops).build();
            var trusting = config(loops).recordPolicy(x -> RecordMode.TRUSTED).allowRecordMode((x, m, p) -> m == RecordMode.TRUSTED).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(trusting)); var c = Zmux.connect(l.localAddress(), trusting); var s = l.accept(WAIT)) {
                addedPipeCarries(c, s, c.addPipe(l.localAddress(), PipeOptions.defaults()), RecordMode.TRUSTED);
            }
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(strict)); var c = Zmux.connect(l.localAddress(), strict); var s = l.accept(WAIT)) {
                var trusted = PipeOptions.builder().recordMode(RecordMode.TRUSTED).build();
                assertThrows(IOException.class, () -> c.addPipe(l.localAddress(), trusted));
                ListenerEntryTest.await(() -> l.stats().rejected() == 1);
                bidi(c, s, 1 << 10);
            }
        }
    }

    @Test void boundOverTlsAndNeverOverPlainTcp() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops).allowRecordMode((x, m, p) -> m == RecordMode.BOUND).build();
            var tls = TlsOptions.client(TestTls.client(), "localhost").withSecure();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()).withSecure()).build());
                 var c = Zmux.connectTls(l.localAddress(), tls, cfg); var s = l.accept(WAIT)) {
                var bound = PipeOptions.builder().recordMode(RecordMode.BOUND).build();
                addedPipeCarries(c, s, c.addTlsPipe(l.localAddress(), tls, bound), RecordMode.BOUND);
                assertThrows(Exception.class, () -> c.addPipe(l.localAddress(), bound), "no Secure declaration");
            }
        }
    }

    private static void writeAll(SocketChannel ch, byte[] b) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b);
        while (bb.hasRemaining()) ch.write(bb);
    }

    private static byte[] unknownHello() throws Exception {
        var j = io.zmux.internal.join.Joiner.create(io.zmux.internal.join.Suite.AES, io.zmux.internal.join.Mode.SEALED,
                io.zmux.internal.join.Framing.STREAM, new byte[16], new byte[32], new byte[0]);
        return j.writeHello(new io.zmux.internal.join.HelloPayload(new io.zmux.internal.join.JoinPipe(1, 2, 0, 0, false), null, 0));
    }

    /** Reads until the peer ends the carrier; the bytes read. */
    private static int readToEnd(SocketChannel ch) throws IOException {
        ch.socket().setSoTimeout(0);
        ByteBuffer b = ByteBuffer.allocate(1024);
        int total = 0;
        try {
            for (int n; (n = ch.read(b)) >= 0; b.clear()) total += n;
        } catch (IOException reset) { /* An abortive end is an end. */ }
        return total;
    }

    @Test void anUnknownLocatorClosesOrStallsWithoutAByte() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops).build();
            var infos = new CopyOnWriteArrayList<RejectInfo>();
            var close = ListenerConfig.builder().session(cfg).onReject(i -> infos.add(i)).build();
            try (var l = Zmux.listen(NetworkTest.any(), close); SocketChannel ch = SocketChannel.open(l.localAddress())) {
                writeAll(ch, unknownHello());
                assertEquals(0, readToEnd(ch));
                assertEquals(1, infos.size());
                assertEquals("unknown_locator", infos.getFirst().reason());
                assertEquals(RecordMode.SEALED, infos.getFirst().recordMode());
            }
            var stall = ListenerConfig.builder().session(cfg).rejection(RejectInfo.Action.STALL).build();
            try (var l = Zmux.listen(NetworkTest.any(), stall); SocketChannel ch = SocketChannel.open(l.localAddress())) {
                writeAll(ch, unknownHello());
                ListenerEntryTest.await(() -> l.stats().rejected() == 1);
                ch.configureBlocking(false);
                Thread.sleep(200);
                assertEquals(0, ch.read(ByteBuffer.allocate(16)), "a stalled carrier neither answers nor ends");
            }
        }
    }

    /** A joiner that stops after the Hello: its carrier stays Unconfirmed. */
    private static byte[] helloFor(ZmuxSession c, long id) throws Exception {
        var local = (io.zmux.internal.wire.TransportParams) c.run(op -> c.core.localParams());
        var peer = (io.zmux.internal.wire.TransportParams) c.run(op -> c.core.peerParams());
        byte[] secret = io.zmux.internal.join.Kdf.sessionSecret(local.sessionNonce, peer.sessionNonce, new byte[0], PSK);
        byte[] suites = io.zmux.internal.join.SessionCrypto.session(local.sessionCrypto, peer.sessionCrypto);
        byte[] psk = io.zmux.internal.join.Kdf.joinPsk(io.zmux.internal.join.Suite.AES, secret, true, suites);
        var j = io.zmux.internal.join.Joiner.create(io.zmux.internal.join.Suite.AES, io.zmux.internal.join.Mode.SEALED,
                io.zmux.internal.join.Framing.STREAM, peer.locator, psk, new byte[0]);
        return j.writeHello(new io.zmux.internal.join.HelloPayload(new io.zmux.internal.join.JoinPipe(id, 2, 0, 0, false), null, 0));
    }

    @Test void aHelloWithTheWrongKeyIsRejectedAsAuthAndTheSessionLives() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var infos = new CopyOnWriteArrayList<RejectInfo>();
            var cfg = config(loops).build();
            var lc = ListenerConfig.builder().session(cfg).onReject(i -> infos.add(i)).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT);
                 SocketChannel ch = SocketChannel.open(l.localAddress())) {
                var peer = (io.zmux.internal.wire.TransportParams) c.run(op -> c.core.peerParams());
                var j = io.zmux.internal.join.Joiner.create(io.zmux.internal.join.Suite.AES, io.zmux.internal.join.Mode.SEALED,
                        io.zmux.internal.join.Framing.STREAM, peer.locator, new byte[32], new byte[0]);
                writeAll(ch, j.writeHello(new io.zmux.internal.join.HelloPayload(new io.zmux.internal.join.JoinPipe(2, 2, 0, 0, false), null, 0)));
                assertEquals(0, readToEnd(ch));
                ListenerEntryTest.await(() -> infos.size() == 1);
                assertEquals("auth", infos.getFirst().reason());
                ListenerEntryTest.await(() -> s.stats().joinAuthFailures() == 1);
                bidi(c, s, 1 << 10);
            }
        }
    }

    @Test void anUnconfirmedCarrierIsEvictedByTheFifthAndHoldsNoPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                InetSocketAddress at = l.localAddress();
                SocketChannel[] chs = new SocketChannel[5];
                ByteBuffer one = ByteBuffer.allocate(1);
                for (int i = 0; i < 5; i++) {
                    chs[i] = SocketChannel.open(at);
                    writeAll(chs[i], helloFor(c, 2L * (i + 1)));
                    one.clear();
                    assertEquals(1, chs[i].read(one), "the Reply of Hello " + i);
                }
                assertEquals(1, s.pipes().size(), "Unconfirmed attachments hold no pipe");
                chs[0].socket();
                ByteBuffer rest = ByteBuffer.allocate(4096);
                int n;
                try { do { rest.clear(); n = chs[0].read(rest); } while (n >= 0); } catch (IOException reset) { n = -1; }
                assertEquals(-1, n, "the oldest was closed");
                chs[4].configureBlocking(false);
                rest.clear();
                assertTrue(chs[4].read(rest) >= 0, "the newest stays");
                for (var ch : chs) ch.close();
                bidi(c, s, 1 << 10);
            }
        }
    }

    @Test void pipeStatsListModesAndSessionCountsFailures() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                List<PipeStats> st = c.pipeStats();
                assertEquals(RecordMode.UNMANAGED, st.getFirst().recordMode());
                assertEquals(0, c.stats().joinFailures());
                assertEquals(0, s.stats().joinBatchDropped());
                assertEquals(0, s.stats().logLinesDropped(), "the logger is called directly: no queue to drop from");
            }
        }
    }
}
