package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.CarrierKind;
import io.zmux.internal.join.HelloPayload;
import io.zmux.internal.join.JoinPipe;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Suite;
import io.zmux.internal.wire.TransportParams;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Attacks on Join over real listeners, sessions and TCP where the carrier matters. */
@Timeout(120)
class JoinAttackTest {
    private static final byte[] PSK = new byte[32];
    private static final Duration WAIT = Duration.ofSeconds(10);

    private static ZmuxConfig.Builder config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops)
                .maxPipes(4).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO)
                .allowRecordMode((c, m, p) -> true);
    }

    private static byte[] framed(int... frame) {
        byte[] out = new byte[frame.length + 1];
        out[0] = (byte) frame.length;
        for (int i = 0; i < frame.length; i++) out[i + 1] = (byte) frame[i];
        return out;
    }

    private static byte[] bytes(int... v) {
        byte[] b = new byte[v.length];
        for (int i = 0; i < v.length; i++) b[i] = (byte) v[i];
        return b;
    }

    private static void alive(ZmuxSession c, ZmuxSession s) throws Exception { SealTest.echo(c, s); }

    private static PipeOptions mode(RecordMode m) { return PipeOptions.builder().mode(m).build(); }

    /** A1 to A5: a third party reads one Hello and nothing else, whatever it answers; nothing of it reaches the session. */
    @Test void aThirdPartyReadsOneHelloAndNothingElse() throws Exception {
        byte[] fakeReply = new byte[1 + 32 + 2 + 16];
        fakeReply[0] = 3;
        fakeReply[34] = 16;
        Map<String, byte[]> answers = new LinkedHashMap<>();
        answers.put("A1 silent", null);
        answers.put("A2 forged acknowledgement", framed(0x80, 0, 0, 0x2a, 1, 2, 0, 0x80, 1, 0, 0, 0));
        answers.put("A3 forged stream data", framed(0x0f, 0, 0, 6, 'F', 'O', 'R', 'G', 'E', 'D'));
        answers.put("A4 forged CONNECTION_CLOSE", framed(0x1c, 0x0a, 0, 0));
        answers.put("A5 forged Reply", fakeReply);
        answers.put("A5 beyond max_record_size", bytes(0x80, 1, 0, 0, 0xff));
        answers.put("A5 beyond any record", bytes(0xc0, 0, 1, 0, 0, 0, 0, 0));
        answers.put("A5 garbage", bytes(2, 0xff, 0xff));
        for (RecordMode m : List.of(RecordMode.SEALED, RecordMode.BOUND, RecordMode.TRUSTED)) {
            for (var a : answers.entrySet()) oneAnswer(m, a.getKey(), a.getValue());
        }
    }

    private void oneAnswer(RecordMode m, String name, byte[] answer) throws Exception {
        String what = m + " " + name;
        try (EventLoops loops = EventLoops.create(1); var party = ServerSocketChannel.open()) {
            party.bind(NetworkTest.any());
            ZmuxConfig cfg = config(loops).handshakeTimeout(Duration.ofSeconds(2)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                SocketChannel near = SocketChannel.open(party.getLocalAddress());
                PipeIo io = PipeChannel.pipe(near, near, Framing.BYTE_STREAM, 1 << 16);
                if (m == RecordMode.BOUND) io = io.withExporter((x, y, n) -> new byte[n]).secure();
                PipeIo carrier = io;
                Thread adder = Thread.ofVirtual().start(() -> {
                    try { c.addPipe(carrier, mode(m)); } catch (Exception expected) { /* It never joins. */ }
                });
                try (SocketChannel far = party.accept()) {
                    ByteBuffer hello = ByteBuffer.allocate(4096);
                    assertTrue(far.read(hello) > 0, what);
                    assertEquals(2, hello.get(0), what + ": a Hello first");
                    if (answer != null) far.write(ByteBuffer.wrap(answer));
                    Thread.sleep(30);
                    // Nothing more reached the carrier, and no pipe of it went ACTIVE.
                    far.configureBlocking(false);
                    int extra = far.read(ByteBuffer.allocate(16));
                    assertTrue(extra <= 0, what + ": bytes after the Hello");
                    for (var st : c.pipes()) {
                        if (st.id() != 0) {
                            var state = st.state();
                            if (state.isPresent()) assertNotEquals(PipeState.Outbound.ACTIVE, state.get().outbound(), what);
                        }
                    }
                    alive(c, s);
                    far.close();
                }
                adder.join(5000);
                assertEquals(1, s.pipes().size(), what + ": a pipe at the server");
            }
        }
    }

    /** A TCP relay that copies, keeps what the client sent, and answers nothing of its own. */
    private static final class Relay implements AutoCloseable {
        final ServerSocketChannel server = ServerSocketChannel.open();
        final ByteArrayOutputStream fromClient = new ByteArrayOutputStream();
        final ByteArrayOutputStream fromServer = new ByteArrayOutputStream();

        Relay(InetSocketAddress target) throws Exception {
            server.bind(NetworkTest.any());
            Thread.ofVirtual().start(() -> {
                try (SocketChannel a = server.accept(); SocketChannel b = SocketChannel.open(target)) {
                    Thread t = Thread.ofVirtual().start(() -> copy(a, b, fromClient));
                    copy(b, a, fromServer);
                    t.join();
                } catch (Exception ignored) { /* The relay's end. */ }
            });
        }

        private static void copy(SocketChannel from, SocketChannel to, ByteArrayOutputStream keep) {
            ByteBuffer b = ByteBuffer.allocate(1 << 16);
            try {
                while (from.read(b.clear()) >= 0) {
                    b.flip();
                    synchronized (keep) { keep.write(b.array(), 0, b.limit()); }
                    while (b.hasRemaining()) to.write(b);
                }
                to.shutdownOutput();
            } catch (Exception ended) {
                try { to.close(); } catch (Exception ignored) { /* Closing anyway. */ }
            }
        }

        InetSocketAddress address() throws Exception { return (InetSocketAddress) server.getLocalAddress(); }

        byte[] client() { synchronized (fromClient) { return fromClient.toByteArray(); } }
        byte[] backward() { synchronized (fromServer) { return fromServer.toByteArray(); } }
        @Override public void close() throws Exception { server.close(); }
    }

    private static boolean contains(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) if (hay[i + j] != needle[j]) continue outer;
            return true;
        }
        return false;
    }

    /**
     * Waits until the acceptor attached the joiner's pipe. The joiner's add returns once the Reply
     * verified, but the acceptor attaches only at Confirmed:
     * removing the first pipe before that leaves the acceptor without a live pipe (8.5).
     */
    static void attached(ZmuxSession acceptor, ZmuxPipe joined) throws Exception {
        joined.awaitPeerAttached(java.time.Duration.ofSeconds(5));
        assertTrue(acceptor.pipes().stream().anyMatch(q -> q.id() == joined.id()), "the acceptor did not attach the pipe");
    }

    @Test void aSealedJoinPassesARelayThatSeesNoPlaintext() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT); var relay = new Relay(l.localAddress())) {
                ZmuxPipe p = c.addPipe(relay.address(), mode(RecordMode.SEALED));
                attached(s, p);
                for (ZmuxPipe q : c.pipes()) if (q.id() == 0) q.abort();
                byte[] marker = "THE-PLAINTEXT-MARKER-0123456789".getBytes();
                ZmuxStream st = c.openStream();
                st.write(marker);
                st.finish();
                ZmuxStream in = s.acceptStream();
                assertArrayEquals(marker, in.inputStream().readAllBytes());
                in.write(marker);
                in.finish();
                assertArrayEquals(marker, st.inputStream().readAllBytes());
                assertTrue(p.id() > 0);
                assertFalse(contains(relay.client(), marker), "plaintext towards the server");
                assertFalse(contains(relay.backward(), marker), "plaintext towards the client");
            }
        }
    }

    /**
     * Carries a Sealed join from the joiner to the acceptor unit by unit; {@code drop} drops whole
     * records, and {@code fin} ends the carrier at a record boundary.
     */
    private static final class UnitRelay implements AutoCloseable {
        final ServerSocketChannel server = ServerSocketChannel.open();
        final java.util.concurrent.atomic.AtomicBoolean drop = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicBoolean fin = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicLong dropped = new java.util.concurrent.atomic.AtomicLong();

        UnitRelay(InetSocketAddress target) throws Exception {
            server.bind(NetworkTest.any());
            Thread.ofVirtual().start(() -> {
                try (SocketChannel a = server.accept(); SocketChannel b = SocketChannel.open(target)) {
                    Thread back = Thread.ofVirtual().start(() -> {
                        ByteBuffer x = ByteBuffer.allocate(1 << 16);
                        try {
                            while (b.read(x.clear()) >= 0) {
                                x.flip();
                                while (x.hasRemaining()) a.write(x);
                            }
                        } catch (Exception ended) { /* The relay's end. */ }
                    });
                    forward(a, b);
                    back.join();
                } catch (Exception ignored) { /* The relay's end. */ }
            });
        }

        private void forward(SocketChannel from, SocketChannel to) throws Exception {
            from.configureBlocking(false);
            ByteBuffer buf = ByteBuffer.allocate(1 << 20);
            boolean units = false;
            while (true) {
                if (fin.get()) {
                    to.shutdownOutput();
                    from.configureBlocking(true);
                    while (from.read(ByteBuffer.allocate(1 << 16)) >= 0) { }
                    return;
                }
                int n = from.read(buf);
                if (n < 0) return;
                if (n == 0) Thread.sleep(1);
                buf.flip();
                if (!units && buf.remaining() >= 4 && buf.remaining() >= 4 + (buf.get(3) & 0xff) + 32) {
                    ByteBuffer head = buf.slice(0, 4 + (buf.get(3) & 0xff) + 32);
                    buf.position(head.remaining());
                    while (head.hasRemaining()) to.write(head);
                    units = true;
                }
                while (units && buf.remaining() >= 2) {
                    int unit = 2 + (buf.getShort(buf.position()) & 0xffff);
                    if (buf.remaining() < unit) break;
                    if (drop.get()) {
                        dropped.addAndGet(unit);
                    } else {
                        ByteBuffer one = buf.slice(buf.position(), unit);
                        while (one.hasRemaining()) to.write(one);
                    }
                    buf.position(buf.position() + unit);
                }
                buf.compact();
            }
        }

        InetSocketAddress address() throws Exception { return (InetSocketAddress) server.getLocalAddress(); }
        @Override public void close() throws Exception { server.close(); }
    }

    private interface Check { boolean ok() throws Exception; }

    private static void until(String what, Check c) throws Exception {
        long end = System.nanoTime() + 30_000_000_000L;
        while (!c.ok()) {
            assertTrue(System.nanoTime() < end, what);
            Thread.sleep(2);
        }
    }

    /** A carrier that ends at a record boundary with no PIPE_ABANDON fails its pipe: the data it lost is sent again and no stream sees an end. */
    @Test void aSealedEndAtARecordBoundaryIsAFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT); var relay = new UnitRelay(l.localAddress())) {
                ZmuxPipe p = c.addPipe(relay.address(), mode(RecordMode.SEALED));
                long id = p.id();
                until("the acceptor's pipe", () -> s.pipes().size() >= 2);
                ZmuxStream st = c.openStream();
                st.write(new byte[] {7});
                ZmuxStream in = s.acceptStream();
                var read = new java.util.concurrent.CompletableFuture<byte[]>();
                Thread.ofVirtual().start(() -> {
                    try {
                        read.complete(in.inputStream().readAllBytes());
                    } catch (Exception e) {
                        read.completeExceptionally(e);
                    }
                });
                relay.drop.set(true);
                byte[] chunk = new byte[4096];
                Arrays.fill(chunk, (byte) 7);
                long total = 1, end = System.nanoTime() + 30_000_000_000L;
                while (relay.dropped.get() < 2048) {
                    assertTrue(System.nanoTime() < end, "nothing went over the relayed pipe");
                    st.write(chunk);
                    total += chunk.length;
                }
                relay.fin.set(true);
                until("the end to fail the pipe", () -> {
                    for (ZmuxPipe q : s.pipes()) {
                        if (q.id() == id) {
                            var state = q.state();
                            return state.isEmpty() || state.get().outbound() == PipeState.Outbound.FAILED;
                        }
                    }
                    return true;
                });
                until("the lost data to be sent again", () -> c.stats().bytesReinjected() > 0);
                assertFalse(read.isDone(), "the stream saw an end");
                st.write(chunk);
                total += chunk.length;
                st.finish();
                byte[] all = read.get(30, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(total, all.length);
                for (byte b : all) assertEquals(7, b);
                alive(c, s);
            }
        }
    }

    /** A Hello in Bound on a carrier with no Binding is refused as binding_unavailable and counted, whatever the joiner mixed. */
    @Test void aBoundHelloWithoutABindingIsBindingUnavailable() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            var reasons = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var lc = ListenerConfig.builder().session(cfg).onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT);
                 SocketChannel near = SocketChannel.open(l.localAddress())) {
                long auth = s.stats().joinAuthFailures();
                near.write(ByteBuffer.wrap(helloFor(c, 40, Mode.BOUND, new byte[32], null)));
                assertEquals(-1, near.read(ByteBuffer.allocate(64)), "nothing is answered");
                until("the refusal", () -> reasons.equals(List.of("binding_unavailable")));
                until("the count", () -> s.stats().joinAuthFailures() == auth + 1);
                alive(c, s);
            }
        }
    }

    /** A Bound join through a party that terminates the carrier: the two ends export different values. */
    @Test void aBoundJoinThroughARelayFailsAtTheAcceptor() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).handshakeTimeout(Duration.ofSeconds(2)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                long before = l.stats().rejected();
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1].withExporter((x, y, n) -> new byte[n]).secure(), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                PipeIo near = pair[0].withExporter((x, y, n) -> { byte[] o = new byte[n]; Arrays.fill(o, (byte) 1); return o; }).secure();
                assertThrows(Exception.class, () -> c.addPipe(near, mode(RecordMode.BOUND)));
                until("the rejection", () -> l.stats().rejected() > before);
                until("the acceptor's count", () -> s.stats().joinAuthFailures() > 0);
                alive(c, s);
            }
        }
    }

    /** A real TCP join, its Hello replayed on a new TCP carrier: counted by the session, refused by the listener. */
    @Test void aReplayIsCountedByTheSessionAndRefusedByTheListener() throws Exception {
        for (RecordMode m : List.of(RecordMode.SEALED, RecordMode.TRUSTED)) {
            try (EventLoops loops = EventLoops.create(2)) {
                ZmuxConfig cfg = config(loops).build();
                try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                     var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT); var relay = new Relay(l.localAddress())) {
                    ZmuxPipe p = c.addPipe(relay.address(), mode(m));
                    for (long end = System.nanoTime() + 5_000_000_000L; s.pipes().size() < 2 && System.nanoTime() < end; ) Thread.sleep(5);
                    assertEquals(2, s.pipes().size());
                    byte[] sent = relay.client();
                    var hello = io.zmux.internal.join.Hello.parse(sent, 0, sent.length, io.zmux.internal.join.Framing.STREAM);
                    int helloLen = hello.consumed();
                    long replays = s.stats().joinReplays(), rejected = l.stats().rejected(), added = s.stats().pipesAdded();
                    try (SocketChannel again = SocketChannel.open(l.localAddress())) {
                        again.write(ByteBuffer.wrap(sent, 0, helloLen));
                        ByteBuffer b = ByteBuffer.allocate(4096);
                        int got = 0;
                        try {
                            for (int n; (n = again.read(b)) >= 0; b.clear()) got += n;
                        } catch (java.io.IOException reset) { /* An abortive end is an end. */ }
                        assertEquals(0, got, m + ": a replay gets nothing");
                    }
                    for (long end = System.nanoTime() + 5_000_000_000L; s.stats().joinReplays() == replays && System.nanoTime() < end; ) Thread.sleep(5);
                    assertEquals(replays + 1, s.stats().joinReplays(), m + ": counted by the session");
                    assertTrue(l.stats().rejected() > rejected, m + ": refused by the listener");
                    assertEquals(added, s.stats().pipesAdded(), m.name());
                    assertEquals(2, s.pipes().size(), m.name());
                    assertTrue(p.id() > 0);
                    alive(c, s);
                }
            }
        }
    }

    /** A Bound Hello replayed on another path: its Binding differs, so it does not open there. */
    @Test void aBoundHelloDoesNotVerifyOnAnotherCarrier() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                byte[] hello = helloFor(c, 40, Mode.BOUND, new byte[32], null);
                long auth = s.stats().joinAuthFailures(), replays = s.stats().joinReplays();
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1].withExporter((x, y, n) -> { byte[] o = new byte[n]; Arrays.fill(o, (byte) 2); return o; }).secure(),
                        JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                PipeChannel near = PipeChannel.of(pair[0]);
                near.write(ByteBuffer.wrap(hello));
                assertEquals(-1, near.read(ByteBuffer.allocate(64)), "nothing is answered");
                until("the count", () -> s.stats().joinAuthFailures() == auth + 1);
                assertEquals(replays, s.stats().joinReplays());
                assertEquals(1, s.pipes().size());
                alive(c, s);
            }
        }
    }

    /** Closing the session or the listener ends an Unconfirmed carrier at once, and rejects nobody. */
    @Test void closingEndsAnUnconfirmedCarrier() throws Exception {
        for (String what : List.of("session", "listener")) {
            try (EventLoops loops = EventLoops.create(2)) {
                java.util.concurrent.atomic.AtomicInteger rejects = new java.util.concurrent.atomic.AtomicInteger();
                ZmuxConfig cfg = config(loops).build();
                var lc = ListenerConfig.builder().session(cfg).onReject(i -> { rejects.incrementAndGet(); return RejectInfo.Action.CLOSE; }).build();
                try (var l = Zmux.listen(NetworkTest.any(), lc);
                     var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT);
                     SocketChannel near = SocketChannel.open(l.localAddress())) {
                    near.write(ByteBuffer.wrap(helloFor(c, 20, Mode.SEALED, new byte[0], null)));
                    assertTrue(near.read(ByteBuffer.allocate(4096)) > 0, "a Reply");
                    long rejected = l.stats().rejected(), started = System.nanoTime();
                    if (what.equals("session")) s.close(); else l.close();
                    assertEquals(-1, near.read(ByteBuffer.allocate(64)), what);
                    assertTrue(System.nanoTime() - started < 5_000_000_000L, what);
                    ListenerResourcesTest.until(() -> l.stats().refused() > 0);
                    assertEquals(rejected, l.stats().rejected(), what + ": not a rejection");
                    assertEquals(0, rejects.get(), what);
                }
            }
        }
    }

    /** The wait for a Reply's plan ended by the session is no authentication failure. */
    @Test void aSessionEndDuringAReplyPlanRejectsNobody() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var asked = new java.util.concurrent.CountDownLatch(1);
            ZmuxConfig server = config(loops).handshakeShaper(v -> {
                asked.countDown();
                return new io.zmux.carrier.HandshakeShaper.UnitPlan(0, 0, 0, Duration.ofHours(1));
            }).build();
            ZmuxConfig client = config(loops).build();
            java.util.concurrent.atomic.AtomicInteger rejects = new java.util.concurrent.atomic.AtomicInteger();
            var lc = ListenerConfig.builder().session(server).onReject(i -> { rejects.incrementAndGet(); return RejectInfo.Action.CLOSE; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.connect(l.localAddress(), client); var s = l.accept(WAIT);
                 SocketChannel near = SocketChannel.open(l.localAddress())) {
                near.write(ByteBuffer.wrap(helloFor(c, 20, Mode.SEALED, new byte[0], null)));
                assertTrue(asked.await(5, java.util.concurrent.TimeUnit.SECONDS));
                Thread.sleep(50);
                long rejected = l.stats().rejected();
                s.close();
                assertEquals(-1, near.read(ByteBuffer.allocate(64)));
                ListenerResourcesTest.until(() -> l.stats().refused() > 0);
                assertEquals(rejected, l.stats().rejected());
                assertEquals(0, rejects.get());
            }
        }
    }

    /** The acceptor's deadline runs from the slot: the delay of its own Reply is inside it. */
    @Test void theAcceptorsDeadlineRunsFromItsSlot() throws Exception {
        long timeout = 1500, delay = 1000;
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig server = config(loops).joinTimeout(Duration.ofMillis(timeout))
                    .handshakeShaper(v -> v.message().equals("reply") ? new io.zmux.carrier.HandshakeShaper.UnitPlan(0, 0, 0, Duration.ofMillis(delay)) : null).build();
            ZmuxConfig client = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(server).build());
                 var c = Zmux.connect(l.localAddress(), client); var s = l.accept(WAIT);
                 SocketChannel near = SocketChannel.open(l.localAddress())) {
                long timedOut = l.stats().timedOut();
                near.write(ByteBuffer.wrap(helloFor(c, 20, Mode.SEALED, new byte[0], null)));
                long start = System.nanoTime();
                ByteBuffer in = ByteBuffer.allocate(4096);
                int n;
                while ((n = near.read(in.clear())) > 0) { /* The Reply, then the end. */ }
                assertEquals(-1, n);
                long took = (System.nanoTime() - start) / 1_000_000;
                assertTrue(took >= delay, "the Reply's delay ran: " + took);
                assertTrue(took < timeout + delay - 200, "from the slot, not from the Reply: " + took);
                ListenerResourcesTest.until(() -> l.stats().timedOut() > timedOut);
            }
        }
    }

    /** A delay beyond the deadline ends the carrier at the deadline, with no Reply. */
    @Test void aReplyDelayedBeyondTheDeadlineEndsAtIt() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig server = config(loops).joinTimeout(Duration.ofMillis(500))
                    .handshakeShaper(v -> v.message().equals("reply") ? new io.zmux.carrier.HandshakeShaper.UnitPlan(0, 0, 0, Duration.ofHours(1)) : null).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(server).build());
                 var c = Zmux.connect(l.localAddress(), config(loops).build()); var s = l.accept(WAIT);
                 SocketChannel near = SocketChannel.open(l.localAddress())) {
                near.write(ByteBuffer.wrap(helloFor(c, 20, Mode.SEALED, new byte[0], null)));
                long start = System.nanoTime();
                assertEquals(-1, near.read(ByteBuffer.allocate(64)));
                assertTrue(System.nanoTime() - start < 3_000_000_000L);
            }
        }
    }

    /** A writer whose nth write and close wait for a gate; counts its closing. */
    private static final class GatedWriter implements PipeWriter {
        final PipeWriter inner;
        final int nth;
        final java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1), blocked = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch closed = new java.util.concurrent.CountDownLatch(1);
        final boolean closeWaits;

        GatedWriter(PipeWriter inner, int nth, boolean closeWaits) { this.inner = inner; this.nth = nth; this.closeWaits = closeWaits; }

        @Override public void write(OutboundBatch data, Completion done) {
            if (writes.incrementAndGet() != nth) { inner.write(data, done); return; }
            blocked.countDown();
            Thread.ofVirtual().start(() -> {
                try { gate.await(); } catch (InterruptedException e) { return; }
                inner.write(data, done);
            });
        }

        private void closing(Runnable then) {
            if (closeWaits) {
                blocked.countDown();
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            closed.countDown();
            then.run();
        }

        @Override public void close() { closing(inner::close); }
        @Override public void abort(Throwable cause) { closing(() -> inner.abort(cause)); }
    }

    /** The end of the session or of the attachment while the Confirm is written starts nothing and closes the carrier. */
    @Test void anEndWhileTheConfirmIsWrittenStartsNoPipe() throws Exception {
        for (String what : List.of("close", "abort")) {
            try (EventLoops loops = EventLoops.create(2)) {
                ZmuxConfig cfg = config(loops).build();
                try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                     var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                    PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    l.handle(pair[1], JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                    GatedWriter w = new GatedWriter(pair[0].writer(), 2, false); // the Hello, then the Confirm
                    PipeIo carrier = new PipeIo(pair[0].reader(), w, Framing.BYTE_STREAM, "gated", null);
                    var added = new java.util.concurrent.CompletableFuture<ZmuxPipe>();
                    Thread.ofVirtual().start(() -> {
                        try { added.complete(c.addPipe(carrier, PipeOptions.defaults())); } catch (Throwable e) { added.completeExceptionally(e); }
                    });
                    assertTrue(w.blocked.await(5, java.util.concurrent.TimeUnit.SECONDS));
                    if (what.equals("close")) {
                        c.close();
                    } else {
                        for (ZmuxPipe q : c.pipes()) if (q.id() != 0) q.abort();
                    }
                    w.gate.countDown();
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> added.get(5, java.util.concurrent.TimeUnit.SECONDS), what);
                    assertTrue(w.closed.await(5, java.util.concurrent.TimeUnit.SECONDS), what + ": carrier closed");
                    if (what.equals("close")) c.closed().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                }
            }
        }
    }

    /** A carrier half that takes its time to close holds up nothing on the loop. */
    @Test void aSlowCarrierCloseDoesNotHoldTheLoop() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1], JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                GatedWriter w = new GatedWriter(pair[0].writer(), -1, true);
                try {
                    ZmuxPipe p = c.addPipe(new PipeIo(pair[0].reader(), w, Framing.BYTE_STREAM, "gated", null), PipeOptions.defaults());
                    assertEquals(1, w.gate.getCount());
                    p.abort();
                    assertTrue(w.blocked.await(5, java.util.concurrent.TimeUnit.SECONDS));
                    var other = new java.util.concurrent.CompletableFuture<Void>();
                    Thread.ofVirtual().start(() -> {
                        try { alive(c, s); other.complete(null); } catch (Throwable e) { other.completeExceptionally(e); }
                    });
                    other.get(5, java.util.concurrent.TimeUnit.SECONDS);
                } finally {
                    w.gate.countDown();
                }
            }
        }
    }

    /** Bound on a split carrier goes no further than the local check; a Hello of that Mode fails as any wrong Binding. */
    @Test void aSplitCarrierCannotUseBound() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                PipeIo[] a = MemoryPipe.pair(Framing.BYTE_STREAM), b = MemoryPipe.pair(Framing.BYTE_STREAM);
                var writes = new java.util.concurrent.atomic.AtomicInteger();
                PipeWriter counted = new PipeWriter() {
                    @Override public void write(OutboundBatch d, Completion done) { writes.incrementAndGet(); a[0].writer().write(d, done); }
                    @Override public void close() { a[0].writer().close(); }
                };
                PipeIo up = new PipeIo(null, counted, Framing.BYTE_STREAM, "up", null).withExporter((x, y, n) -> new byte[n]).secure();
                PipeIo down = new PipeIo(b[0].reader(), null, Framing.BYTE_STREAM, "down", null).withExporter((x, y, n) -> new byte[n]).secure();
                PipeIo split = PipeIo.split(up, down);
                assertTrue(split.isSecure());
                assertThrows(Exception.class, () -> c.addPipe(split, mode(RecordMode.BOUND)));
                assertEquals(0, writes.get(), "no byte is sent");
                alive(c, s);
            }
        }
    }

    @Test void aPairKeepsWhatDescribesItsHalves() {
        PipeIo[] a = MemoryPipe.pair(Framing.BYTE_STREAM);
        PipeIo up = new PipeIo(null, a[0].writer(), Framing.BYTE_STREAM, "up", null).withWriteOverhead(5).withFateGroup(3).withVariant("v");
        PipeIo down = new PipeIo(a[0].reader(), null, Framing.BYTE_STREAM, "down", null).withWriteOverhead(9).withFateGroup(4).withVariant("w");
        PipeIo p = PipeIo.split(up, down);
        assertEquals(5, p.capabilities().writeOverhead(), "this end writes on the uplink");
        assertEquals(3, p.capabilities().fateGroup());
        assertEquals("v", p.variant());
        assertEquals(CarrierKind.SPLIT, p.carrier());
        // The acceptor: reads the uplink, writes the downlink.
        PipeIo up2 = new PipeIo(a[1].reader(), null, Framing.BYTE_STREAM, "up", null).withWriteOverhead(5).withFateGroup(3).withVariant("v");
        PipeIo down2 = new PipeIo(null, a[1].writer(), Framing.BYTE_STREAM, "down", null).withWriteOverhead(9).withFateGroup(4).withVariant("w");
        PipeIo q = PipeIo.split(up2, down2);
        assertEquals(9, q.capabilities().writeOverhead(), "this end writes on the downlink");
        assertEquals(3, q.capabilities().fateGroup());
        assertEquals("v", q.variant());
    }

    @Test void aEmptyPlanIsAPlanAndNoneIsCounted() throws Exception {
        for (boolean planned : List.of(true, false)) {
            try (EventLoops loops = EventLoops.create(1)) {
                ZmuxConfig cfg = config(loops).handshakeShaper(v -> planned ? io.zmux.carrier.HandshakeShaper.UnitPlan.NONE : null).build();
                try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                     var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                    c.addPipe(l.localAddress(), PipeOptions.defaults());
                    ListenerResourcesTest.until(() -> c.stats().pipesAdded() >= 2);
                    var stats = c.driver.shapingStats();
                    long n = stats == null ? 0 : stats.handshakeUnplanned();
                    if (planned) assertEquals(0, n);
                    else assertTrue(n >= 2, "unplanned " + n);
                }
            }
        }
    }

    /** A first batch asked for beyond what a Hello's ciphertext holds does not fail the join. */
    @Test void aFirstBatchBeyondTheHellosCiphertextStillJoins() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).joinEarlyHello(70_000)
                    .maxPipes(2).resumeTimeout(Duration.ofSeconds(30)).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                ZmuxStream st = c.openStream();
                for (ZmuxPipe q : c.pipes()) q.abort();
                for (long end = System.nanoTime() + 5_000_000_000L; c.stats().suspensions() == 0 && System.nanoTime() < end; ) Thread.sleep(5);
                assertTrue(c.stats().suspensions() > 0, "suspended");
                Thread.ofVirtual().start(() -> {
                    try { st.write(new byte[128 << 10]); } catch (Exception ended) { /* The test is over. */ }
                });
                Thread.sleep(50);
                assertTrue(c.addPipe(l.localAddress(), PipeOptions.defaults()).id() > 0);
                assertEquals(128 << 10, s.acceptStream().inputStream().readNBytes(128 << 10).length);
            }
        }
    }

    /** A joiner stopped after the Hello: keys as the session's own, with an optional batch. */
    private static byte[] helloFor(ZmuxSession c, long id, Mode m, byte[] binding, byte[] batch) throws Exception {
        var local = (TransportParams) c.run(op -> c.core.localParams());
        var peer = (TransportParams) c.run(op -> c.core.peerParams());
        byte[] secret = Kdf.sessionSecret(local.sessionNonce, peer.sessionNonce, new byte[0], PSK);
        byte[] suites = io.zmux.internal.join.SessionCrypto.session(local.sessionCrypto, peer.sessionCrypto);
        byte[] psk = Kdf.joinPsk(Suite.AES, secret, true, suites);
        var j = Joiner.create(Suite.AES, m, io.zmux.internal.join.Framing.STREAM, peer.locator, psk, binding);
        return j.writeHello(new HelloPayload(new JoinPipe(id, 2, 0, 0, false), batch, 0));
    }

    /** Unconfirmed attachments hold no capacity: full of them, a real join still fits, and none ends a suspension. */
    @Test void unconfirmedCarriersDoNotCrowdOutAJoinOrEndASuspension() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).allowRecordMode((c, m, p) -> true)
                    .maxPipes(2).resumeTimeout(Duration.ofSeconds(30)).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                SocketChannel[] held = new SocketChannel[4];
                try {
                    for (int i = 0; i < held.length; i++) {
                        held[i] = SocketChannel.open(l.localAddress());
                        held[i].write(ByteBuffer.wrap(helloFor(c, 20 + 2L * i, Mode.SEALED, new byte[0], null)));
                        assertTrue(held[i].read(ByteBuffer.allocate(4096)) > 0, "an authenticated Hello gets its Reply");
                    }
                    assertEquals(1, s.pipes().size(), "no pipe before Confirmed");
                    ZmuxPipe real = c.addPipe(l.localAddress(), PipeOptions.defaults());
                    assertTrue(real.id() > 0);
                    for (long end = System.nanoTime() + 5_000_000_000L; s.pipes().size() < 2 && System.nanoTime() < end; ) Thread.sleep(5);
                    assertEquals(2, s.pipes().size());
                    alive(c, s);
                    // All real pipes gone: the session suspends, and an Unconfirmed carrier does not end that.
                    for (ZmuxPipe q : c.pipes()) q.abort();
                    for (long end = System.nanoTime() + 5_000_000_000L; s.stats().suspensions() == 0 && System.nanoTime() < end; ) Thread.sleep(5);
                    assertTrue(s.stats().suspensions() > 0, "suspended");
                    long added = s.stats().pipesAdded();
                    try (SocketChannel another = SocketChannel.open(l.localAddress())) {
                        another.write(ByteBuffer.wrap(helloFor(c, 30, Mode.SEALED, new byte[0], null)));
                        assertTrue(another.read(ByteBuffer.allocate(4096)) > 0);
                        assertEquals(added, s.stats().pipesAdded(), "no pipe");
                        assertEquals(0, activePipes(s), "still suspended");
                    }
                } finally {
                    for (SocketChannel ch : held) if (ch != null) ch.close();
                }
            }
        }
    }

    private static long activePipes(ZmuxSession s) throws Exception {
        long n = 0;
        for (ZmuxPipe q : s.pipes()) if (q.state().map(x -> x.outbound() == PipeState.Outbound.ACTIVE).orElse(false)) n++;
        return n;
    }

    @Test void aFirstBatchFrameInErrorIsDroppedAndCounted() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                long id = 20;
                for (byte[] batch : List.of(bytes(0xff), bytes(0x10, 0x80, 0, 0, 0))) {
                    long before = s.stats().joinBatchDropped();
                    try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                        ch.write(ByteBuffer.wrap(helloFor(c, id, Mode.SEALED, new byte[0], batch)));
                        assertTrue(ch.read(ByteBuffer.allocate(4096)) > 0, "a Reply: the batch failed alone");
                    }
                    id += 2;
                    until("the batch's count", () -> s.stats().joinBatchDropped() > before);
                    alive(c, s);
                }
            }
        }
    }

    /** First-write tampering with the secret flags of a session's parameters on the path. */
    private static PipeIo tampered(PipeIo p, IntUnaryOperator flags) {
        PipeWriter w = p.writer();
        return new PipeIo(p.reader(), new PipeWriter() {
            private boolean done;
            @Override public void write(OutboundBatch batch, Completion c) {
                if (done) { w.write(batch, c); return; }
                done = true;
                byte[] out = new byte[(int) batch.bytes()];
                ByteBuffer to = ByteBuffer.wrap(out);
                for (int i = 0; i < batch.segmentCount(); i++) to.put(batch.segment(i).duplicate());
                ByteBuffer id = ByteBuffer.allocate(8);
                io.zmux.carrier.VarInt.put(id, TransportParams.ID_MULTIPATH);
                byte[] idBytes = Arrays.copyOf(id.array(), id.position());
                outer:
                for (int i = 0; i + idBytes.length < out.length; i++) {
                    for (int j = 0; j < idBytes.length; j++) if (out[i + j] != idBytes[j]) continue outer;
                    int at = i + idBytes.length;
                    at += io.zmux.carrier.VarInt.lengthOf(out[at]); // The value's length.
                    out[at] = (byte) flags.applyAsInt(out[at] & 0xff);
                    break;
                }
                w.write(OutboundBatch.of(ByteBuffer.wrap(out)), c);
            }
            @Override public void close() { w.close(); }
            @Override public void abort(Throwable cause) { w.abort(cause); }
        }, p.framing(), p.label(), p.carrierConn(), p.capabilities(), p.carrier(), p.variant());
    }

    @Test void aClearedSecretFlagClosesTheSessionAndNeverTakesTheIntersection() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            PipeAuth both = PipeAuth.psk(PSK).withExporter().withEndToEnd();
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).handshakeTimeout(Duration.ofSeconds(2))
                    .maxPipes(4).pipeAuth(both).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build())) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                PipeIo.Exporter same = (x, y, n) -> new byte[n];
                l.handle(pair[1].withExporter(same), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                PipeIo near = tampered(pair[0].withExporter(same), f -> f & ~TransportParams.SECRET_PSK);
                ZmuxSession c = null;
                try {
                    c = Zmux.open(near, Role.CLIENT, cfg);
                    for (long end = System.nanoTime() + 5_000_000_000L; !c.isClosed() && System.nanoTime() < end; ) Thread.sleep(5);
                    assertTrue(c.isClosed(), "the client's end closes");
                } catch (java.io.IOException refused) {
                    // Closed before it was established: as good.
                } finally {
                    if (c != null) c.close();
                }
                try {
                    ZmuxSession s = l.accept(Duration.ofMillis(200));
                    if (s == null) return; // None accepted: as good.
                    for (long end = System.nanoTime() + 5_000_000_000L; !s.isClosed() && System.nanoTime() < end; ) Thread.sleep(5);
                    assertTrue(s.isClosed(), "a session with differing flags does not live");
                } catch (java.io.IOException none) { /* None accepted: as good. */ }
            }
        }
    }

    @Test void clearingEveryFlagLeavesNoJoinKeys() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = config(loops).handshakeTimeout(Duration.ofSeconds(2)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build())) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1], JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                ZmuxSession c = Zmux.open(tampered(pair[0], f -> 0), Role.CLIENT, cfg);
                try {
                    ZmuxSession s = l.accept(WAIT);
                    assertFalse(s.info().multipath() && s.joinReady(), "no join keys");
                    assertThrows(Exception.class, () -> s.addPipe(MemoryPipe.pair(Framing.BYTE_STREAM)[0], PipeOptions.defaults()));
                    assertThrows(Exception.class, () -> c.addPipe(l.localAddress(), PipeOptions.defaults()));
                } catch (java.io.IOException closedInstead) {
                    assertNotNull(closedInstead); // A session that closes never joins either.
                } finally {
                    c.close();
                }
            }
        }
    }

    /** With an unmanaged first pipe the mode stays Unmanaged for the session's life, its first pipe gone or not. */
    @Test void weakestRecordModeOfAnUnmanagedFirstPipeLastsTheSession() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                assertEquals(RecordMode.UNMANAGED, c.weakestRecordMode());
                ZmuxPipe p = c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertEquals(RecordMode.SEALED, p.stats().orElseThrow().recordMode());
                attached(s, p);
                for (ZmuxPipe q : c.pipes()) if (q.id() == 0) q.abort();
                alive(c, s);
                assertEquals(RecordMode.UNMANAGED, c.weakestRecordMode());
                assertEquals(RecordMode.UNMANAGED, s.weakestRecordMode());
            }
        }
    }

    /** On the secure carrier it is the weakest live pipe's. */
    @Test void weakestRecordModeOnTheSecureCarrierFollowsItsPipes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = SealTest.sessions(loops).allowRecordMode((c, m, p) -> true).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(io.zmux.seal.Seal.server(new io.zmux.seal.ServerConfig(SealTest.gate(null), SealTest.NODE))).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.open(SealTest.establish(SealTest.tcp(l.localAddress()), SealTest.client().build()), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                alive(c, s);
                assertEquals(RecordMode.SEALED, c.weakestRecordMode());
                ZmuxPipe t = c.addPipe(l.localAddress(), mode(RecordMode.TRUSTED));
                assertEquals(RecordMode.TRUSTED, c.weakestRecordMode());
                for (long end = System.nanoTime() + 5_000_000_000L; s.weakestRecordMode() != RecordMode.TRUSTED && System.nanoTime() < end; ) Thread.sleep(5);
                assertEquals(RecordMode.TRUSTED, s.weakestRecordMode());
                t.abort();
                for (long end = System.nanoTime() + 5_000_000_000L; c.weakestRecordMode() != RecordMode.SEALED && System.nanoTime() < end; ) Thread.sleep(5);
                assertEquals(RecordMode.SEALED, c.weakestRecordMode());
            }
        }
    }

    /** Milliseconds until the carrier ends, asserting that nothing is read before. */
    private static long silentFor(SocketChannel ch) throws Exception {
        long start = System.nanoTime();
        int got = 0;
        try {
            for (int n; (n = ch.read(ByteBuffer.allocate(4096))) >= 0; ) got += n;
        } catch (java.io.IOException reset) { /* An abortive end is an end. */ }
        assertEquals(0, got, "a refused Join gets nothing");
        return (System.nanoTime() - start) / 1_000_000;
    }

    /** A Hello for a known Locator that fails authentication. */
    private static byte[] forged(ZmuxSession c, long id) throws Exception {
        byte[] h = helloFor(c, id, Mode.SEALED, new byte[0], null);
        h[h.length - 1] ^= 1;
        return h;
    }

    /** A stalled Join ends one join timeout of the listener after the refusal, also for a session that configures another. */
    @Test void aStalledJoinLastsTheListenersJoinTimeout() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig listener = config(loops).joinTimeout(Duration.ofMillis(800)).build();
            ZmuxConfig own = config(loops).joinTimeout(Duration.ofSeconds(20)).build();
            List<String> reasons = new java.util.concurrent.CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(listener).sessionConfigs((info, admitted) -> own)
                    .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.STALL; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.connect(l.localAddress(), config(loops).build()); var s = l.accept(WAIT)) {
                for (boolean known : List.of(true, false)) {
                    try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                        byte[] h = forged(c, 20);
                        if (!known) h[8] ^= 1; // the Locator
                        ch.write(ByteBuffer.wrap(h));
                        long ms = silentFor(ch);
                        assertTrue(ms >= 600 && ms < 5000, known + ": stalled " + ms + " ms");
                    }
                }
                assertEquals(List.of("auth", "unknown_locator"), reasons);
                assertEquals(1, s.pipes().size());
            }
        }
    }

    /** A Hello whose Suite this implementation lacks does not parse: OnReject as malformed, the same stall. */
    @Test void aSuiteOfNoImplementationIsMalformed() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).joinTimeout(Duration.ofMillis(800)).build();
            List<String> reasons = new java.util.concurrent.CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(cfg)
                    .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.STALL; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.connect(l.localAddress(), config(loops).build()); var s = l.accept(WAIT);
                 SocketChannel ch = SocketChannel.open(l.localAddress())) {
                byte[] h = helloFor(c, 20, Mode.SEALED, new byte[0], null);
                h[1] = 3; // an unassigned Suite
                ch.write(ByteBuffer.wrap(h));
                long ms = silentFor(ch);
                assertTrue(ms >= 600 && ms < 5000, "stalled " + ms + " ms");
                assertEquals(List.of("malformed"), reasons);
            }
        }
    }

    /** A source over its bound of carriers waiting to authenticate goes to OnReject as pending_limit, and is closed. */
    @Test void aSourceOverItsPendingLimitIsARejection() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            List<RejectInfo> infos = new java.util.concurrent.CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(config(loops).build()).maxPendingPerSource(1)
                    .onReject(i -> { infos.add(i); return RejectInfo.Action.STALL; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc); SocketChannel held = SocketChannel.open(l.localAddress())) {
                Thread.sleep(300); // the held carrier takes the source's one slot
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    long ms = silentFor(ch);
                    assertTrue(ms < 3000, "closed at once, the source has no slot to stall in: " + ms + " ms");
                }
                assertEquals(1, infos.size());
                assertEquals("pending_limit", infos.getFirst().reason());
                assertEquals(0, infos.getFirst().profile());
            }
        }
    }

    /** A known Locator declined under memory pressure goes to OnReject as memory_pressure. */
    @Test void aJoinDeclinedUnderPressureIsARejection() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).joinTimeout(Duration.ofMillis(600)).build();
            var pressure = new java.util.concurrent.CountDownLatch(1);
            List<String> reasons = new java.util.concurrent.CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(cfg).memoryBudget(128 << 10)
                    .memoryPressureListener((used, budget) -> { if (used >= budget) pressure.countDown(); })
                    .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.STALL; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                var a = c.openStream();
                a.write(new byte[160 << 10]);
                var held = s.acceptStream(WAIT);
                assertTrue(pressure.await(10, java.util.concurrent.TimeUnit.SECONDS), "pressure");
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap(helloFor(c, 20, Mode.SEALED, new byte[0], null)));
                    long ms = silentFor(ch);
                    assertTrue(ms >= 400 && ms < 5000, "stalled " + ms + " ms");
                }
                assertEquals(List.of("memory_pressure"), reasons);
                held.close();
            }
        }
    }

    /** The listener's maxPendingPerSession bounds a session's Unconfirmed carriers: one more closes the oldest. */
    @Test void maxPendingPerSessionEvictsTheOldestUnconfirmed() throws Exception {
        assertEquals(4, ListenerConfig.defaults().maxPendingPerSession());
        assertThrows(IllegalArgumentException.class, () -> ListenerConfig.builder().maxPendingPerSession(0));
        assertThrows(IllegalArgumentException.class, () -> ListenerConfig.builder().maxPendingPerSession(-1));
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = config(loops).build();
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSession(2).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                SocketChannel[] held = new SocketChannel[3];
                try {
                    for (int i = 0; i < held.length; i++) {
                        held[i] = SocketChannel.open(l.localAddress());
                        held[i].write(ByteBuffer.wrap(helloFor(c, 20 + 2L * i, Mode.SEALED, new byte[0], null)));
                        assertTrue(held[i].read(ByteBuffer.allocate(4096)) > 0, "a Reply");
                    }
                    assertEquals(-1, held[0].read(ByteBuffer.allocate(64)), "the oldest is evicted");
                    for (int i = 1; i < held.length; i++) {
                        held[i].configureBlocking(false);
                        assertEquals(0, held[i].read(ByteBuffer.allocate(64)), "carrier " + i + " is kept");
                    }
                } finally {
                    for (SocketChannel ch : held) if (ch != null) ch.close();
                }
            }
        }
    }

    /** A Confirm whose write reports after the joiner's deadline leaves the Proven attachment alone. */
    @Test void aConfirmWrittenAfterTheDeadlineKeepsTheAttachment() throws Exception {
        java.util.concurrent.ScheduledExecutorService timer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig server = config(loops).build();
            ZmuxConfig client = config(loops).joinTimeout(Duration.ofMillis(400)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(server).build());
                 var c = Zmux.connect(l.localAddress(), client); var s = l.accept(WAIT)) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1], JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                PipeWriter w = pair[0].writer();
                var writes = new java.util.concurrent.atomic.AtomicInteger();
                PipeWriter late = new PipeWriter() {
                    @Override public void write(OutboundBatch b, Completion d) {
                        if (writes.incrementAndGet() != 2) { w.write(b, d); return; } // the Hello, then the Confirm
                        w.write(b, new Completion() {
                            @Override public void completed() { timer.schedule(d::completed, 1000, java.util.concurrent.TimeUnit.MILLISECONDS); }
                            @Override public void failed(Throwable t) { d.failed(t); }
                        });
                    }
                    @Override public void close() { w.close(); }
                    @Override public void abort(Throwable t) { w.abort(t); }
                };
                // Not waiting for the peer's attachment: its proof (an acknowledged record on the
                // pipe) can only follow the Confirm's late report, after the join deadline.
                ZmuxPipe p = c.addPipe(new PipeIo(pair[0].reader(), late, Framing.BYTE_STREAM, "late", null),
                        PipeOptions.builder().mode(RecordMode.SEALED).noWaitAttached(true).build());
                assertTrue(p.id() > 0);
                Thread.sleep(300);
                assertEquals(0, c.stats().pipeFailures(), "joiner");
                assertEquals(2, c.pipes().size());
                until("acceptor", () -> s.pipes().size() == 2);
                p.awaitPeerAttached(Duration.ofSeconds(5));
                assertEquals(0, c.stats().pipeFailures(), "joiner after the report");
                alive(c, s);
            }
        } finally {
            timer.shutdownNow();
        }
    }
}
