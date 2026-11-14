package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Datagrams on the QUIC connection's datagram channel. */
@Timeout(60)
class QuicChannelTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
    }

    /**
     * Forwards UDP between one client and a server with a one-way delay. When armed it drops the
     * next client-to-server packets of at least 1000 bytes, and holds every later one of that size
     * until released: the lost stream data stays missing at the server, whatever its timers do.
     */
    static final class LossyRelay implements AutoCloseable {
        final DatagramSocket front = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final DatagramSocket back = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final SocketAddress server;
        final long delayNanos;
        final AtomicInteger dropped = new AtomicInteger();
        volatile SocketAddress client;
        final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        final List<Thread> pumps = new ArrayList<>();
        // Guarded by this.
        private int drop;
        private boolean holding;
        private final List<byte[]> held = new ArrayList<>();
        /** Client packets received, and of them those settled: sent on to the server or dropped. */
        private long fromClient, toServer;
        private long barrierAt = -1;
        private CompletableFuture<Void> barrier;

        LossyRelay(SocketAddress server, Duration delay) throws Exception {
            this.server = server;
            this.delayNanos = delay.toNanos();
            pumps.add(Thread.ofVirtual().start(() -> pump(true)));
            pumps.add(Thread.ofVirtual().start(() -> pump(false)));
        }

        InetSocketAddress address() { return (InetSocketAddress) front.getLocalSocketAddress(); }

        /** Drops the next n large client packets and holds the later ones. */
        synchronized void arm(int n) { drop = n; holding = true; }

        /** Sends the held packets on, in order, and holds no more. */
        void release() {
            List<byte[]> out;
            synchronized (this) {
                holding = false;
                out = new ArrayList<>(held);
                held.clear();
            }
            for (byte[] b : out) forward(b, true);
        }

        /**
         * Completes once the server answered after every client packet received so far (and not
         * held) reached it: the server has processed them.
         */
        synchronized CompletableFuture<Void> barrier() {
            barrierAt = fromClient - held.size();
            barrier = new CompletableFuture<>();
            return barrier;
        }

        private void pump(boolean up) {
            DatagramSocket in = up ? front : back;
            byte[] buf = new byte[65536];
            while (!in.isClosed()) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                try { in.receive(p); } catch (Exception e) { return; }
                byte[] copy = Arrays.copyOf(p.getData(), p.getLength());
                if (up) {
                    client = p.getSocketAddress();
                    synchronized (this) {
                        fromClient++;
                        if (copy.length >= 1000 && drop > 0) {
                            drop--;
                            dropped.incrementAndGet();
                            toServer++; // settled: it never reaches the server
                            continue;
                        }
                        if (copy.length >= 1000 && holding) { held.add(copy); continue; }
                    }
                } else {
                    CompletableFuture<Void> b = null;
                    synchronized (this) {
                        if (barrier != null && toServer >= barrierAt) { b = barrier; barrier = null; }
                    }
                    if (b != null) b.complete(null);
                }
                forward(copy, up);
            }
        }

        private void forward(byte[] copy, boolean up) {
            SocketAddress to = up ? server : client;
            DatagramSocket out = up ? back : front;
            try {
                timer.schedule(() -> {
                    try { out.send(new DatagramPacket(copy, copy.length, to)); } catch (Exception ignored) { }
                    if (up) synchronized (this) { toServer++; }
                }, delayNanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException ignored) {
                // Closed.
            }
        }

        @Override public void close() throws Exception {
            front.close();
            back.close();
            timer.shutdownNow();
            for (Thread t : pumps) t.join(5000);
        }
    }

    static byte[] stamp(int seq) {
        return ByteBuffer.allocate(100).putLong(System.nanoTime()).put((byte) seq).array();
    }

    /** The stream burst: within the initial congestion window of the QUIC stack, so that only the lost packets delay anything. */
    static final int BURST = 8 << 10;

    /**
     * Of the datagrams sent right after a stream burst whose first large packets were lost:
     * those the server received while the lost stream data was still held back, and in all.
     */
    record Run(int whileHeld, int received, SessionStats client, int dropped) {}

    /** Waits until relay dropped the burst's first n large packets. */
    static void awaitDropped(LossyRelay relay, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (relay.dropped.get() < n) {
            assertTrue(System.nanoTime() < deadline, "the burst's head was not lost");
            Thread.sleep(1);
        }
    }

    /**
     * A session through a relay with one-way delay d: a burst of stream data whose first large
     * packets are lost and whose later ones (retransmissions among them) are held at the relay,
     * then 10 datagrams. With channel datagrams they are waited for before the relay lets the
     * stream go on; otherwise the server's count is taken once it has processed every packet
     * that carried them. lanes is the endpoints' maximum lane count (0: the default).
     */
    static Run lossLatencies(Duration d, QuicOptions clientOptions, long lanes) throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var b = ZmuxConfig.builder().eventLoops(loops).datagrams(true);
            if (lanes > 0) b.maxLanes(lanes);
            var config = b.build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var relay = new LossyRelay(server.address(), d);
                 var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                ZmuxSession c = endpoint.connectAsync(relay.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                assertNotNull(s);
                try {
                    ZmuxStream st = c.openStream();
                    st.write(ByteBuffer.wrap("warm".getBytes()));
                    ZmuxStream in = s.acceptStream();
                    Future<byte[]> read = workers.submit(() -> QuicTest.read(in));
                    for (int i = 0; i < 5; i++) {
                        c.datagrams().send(ByteBuffer.wrap(new byte[100]));
                        assertNotNull(s.datagrams().receive(Duration.ofSeconds(5)));
                    }
                    relay.arm(2);
                    st.write(ByteBuffer.wrap(new byte[BURST]));
                    awaitDropped(relay, 2);
                    for (int i = 0; i < 10; i++) c.datagrams().send(ByteBuffer.wrap(new byte[100]));
                    relay.barrier().get(10, TimeUnit.SECONDS);
                    int whileHeld = 0;
                    if (clientOptions.isDatagrams()) {
                        for (; whileHeld < 10; whileHeld++) {
                            try { s.datagrams().receive(Duration.ofSeconds(2)); }
                            catch (java.net.SocketTimeoutException e) { break; }
                        }
                    } else {
                        whileHeld = (int) (s.stats().datagramsReceived() - 5);
                    }
                    relay.release();
                    int received = clientOptions.isDatagrams() ? whileHeld : 0;
                    for (; received < 10; received++) {
                        try { s.datagrams().receive(Duration.ofSeconds(clientOptions.isDatagrams() ? 1 : 10)); }
                        catch (java.net.SocketTimeoutException e) { break; }
                    }
                    st.finish();
                    assertEquals(BURST + 4, read.get(20, TimeUnit.SECONDS).length);
                    in.close();
                    st.close();
                    return new Run(whileHeld, received, c.stats(), relay.dropped.get());
                } finally {
                    QuicEndpointTest.closed(c, s);
                }
            }
        }
    }

    @Test void channelDatagramsAvoidHeadOfLineBlocking() throws Exception {
        Duration d = Duration.ofMillis(40);
        Run channel = lossLatencies(d, QuicOptions.defaults(), 0);
        assertEquals(2, channel.dropped);
        assertTrue(channel.client.datagramsSentChannel() >= 15, "not on the channel: " + channel.client);
        // They arrive while the stream's lost data is still missing.
        assertTrue(channel.whileHeld >= 5, channel.whileHeld + " datagrams arrived while the stream was held");

        // Without lanes the stream data and the records share one QUIC stream; with them the data goes on a lane of
        // its own once the peer has acknowledged that lane, and the records on lane 0 are not held.
        Run records = lossLatencies(d, QuicOptions.builder().datagrams(false).build(), 1);
        assertEquals(2, records.dropped);
        assertEquals(0, records.client.datagramsSentChannel());
        // In records on the same QUIC stream they wait behind the stream's lost data, however long it is missing.
        assertEquals(0, records.whileHeld, "a datagram in records was not held");
        assertEquals(10, records.received);
    }

    /** Larger datagrams, a peer without QUIC datagrams and sessions without datagrams use records. */
    @Test void fallbackToRecords() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = config(loops);
            for (QuicOptions clientOptions : List.of(QuicOptions.defaults(), QuicOptions.builder().datagrams(false).build())) {
                try (var listener = ZmuxListener.of(ListenerConfig.of(config));
                     var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                     var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                    ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                    ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                    try {
                        byte[] small = {1, 2, 3}, large = new byte[4000];
                        c.datagrams().send(ByteBuffer.wrap(small));
                        assertArrayEquals(small, s.datagrams().receive(Duration.ofSeconds(5)));
                        c.datagrams().send(ByteBuffer.wrap(large));
                        assertArrayEquals(large, s.datagrams().receive(Duration.ofSeconds(5)));
                        s.datagrams().send(ByteBuffer.wrap(small));
                        assertArrayEquals(small, c.datagrams().receive(Duration.ofSeconds(5)));
                        var cs = c.stats();
                        assertEquals(2, cs.datagramsSent());
                        assertEquals(clientOptions.isDatagrams() ? 1 : 0, cs.datagramsSentChannel(), cs.toString());
                        assertEquals(clientOptions.isDatagrams() ? 1 : 0, cs.datagramsReceivedChannel(), cs.toString());
                        assertEquals(clientOptions.isDatagrams() ? 1 : 0, s.stats().datagramsReceivedChannel());
                    } finally {
                        QuicEndpointTest.closed(c, s);
                    }
                }
            }
            var none = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(none));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", none).get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                try {
                    assertThrows(NotNegotiatedException.class, () -> c.datagrams().send(ByteBuffer.wrap(new byte[1])));
                } finally {
                    QuicEndpointTest.closed(c, s);
                }
            }
        }
    }

    /** One connection carries the pipes of two sessions; its channel routes each datagram to its own. */
    @Test void sessionsShareTheConnectionsChannel() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = config(loops);
            try (var local = ZmuxListener.of(ListenerConfig.of(config));
                 var remote = ZmuxListener.of(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), remote);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults(), local);
                 var pool = new QuicPool(endpoint, server.address(), "localhost")) {
                var p1 = pool.openPipeAsync().get(10, TimeUnit.SECONDS);
                var p2 = pool.openPipeAsync().get(10, TimeUnit.SECONDS);
                assertSame(p1.carrierConn(), p2.carrierConn());
                ZmuxSession c1 = Zmux.open(p1, Role.CLIENT, config), c2 = Zmux.open(p2, Role.CLIENT, config);
                ZmuxSession s1 = remote.accept(Duration.ofSeconds(10)), s2 = remote.accept(Duration.ofSeconds(10));
                assertNotNull(s1);
                assertNotNull(s2);
                try {
                    // Which accepted session belongs to which client session is learnt from a datagram.
                    c1.datagrams().send(ByteBuffer.wrap(new byte[] {1}));
                    byte[] first = s1.datagrams().receive(Duration.ofSeconds(5));
                    if (first[0] != 1) { var t = s1; s1 = s2; s2 = t; assertArrayEquals(new byte[] {1}, s1.datagrams().receive(Duration.ofSeconds(5))); }
                    for (int i = 0; i < 20; i++) {
                        c1.datagrams().send(ByteBuffer.wrap(("one " + i).getBytes()));
                        c2.datagrams().send(ByteBuffer.wrap(("two " + i).getBytes()));
                        s1.datagrams().send(ByteBuffer.wrap(("back one " + i).getBytes()));
                        s2.datagrams().send(ByteBuffer.wrap(("back two " + i).getBytes()));
                    }
                    for (int i = 0; i < 20; i++) {
                        assertTrue(new String(s1.datagrams().receive(Duration.ofSeconds(5))).startsWith("one "));
                        assertTrue(new String(s2.datagrams().receive(Duration.ofSeconds(5))).startsWith("two "));
                        assertTrue(new String(c1.datagrams().receive(Duration.ofSeconds(5))).startsWith("back one "));
                        assertTrue(new String(c2.datagrams().receive(Duration.ofSeconds(5))).startsWith("back two "));
                    }
                    assertEquals(21, c1.stats().datagramsSentChannel());
                    assertEquals(20, c2.stats().datagramsSentChannel());
                    assertEquals(20, c1.stats().datagramsReceivedChannel());
                    assertEquals(21, s1.stats().datagramsReceivedChannel());
                    assertEquals(20, s2.stats().datagramsReceivedChannel());
                } finally {
                    c1.close(); c2.close(); s1.close(); s2.close();
                    c1.awaitClosed(); c2.awaitClosed(); s1.awaitClosed(); s2.awaitClosed();
                }
            }
        }
    }

    /** A datagram naming no pipe is dropped and counted; a malformed one fails that session. */
    @Test void unknownAndMalformedChannelDatagrams() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = config(loops);
            try (var listener = ZmuxListener.of(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                try {
                    c.datagrams().send(ByteBuffer.wrap(new byte[] {7}));
                    assertArrayEquals(new byte[] {7}, s.datagrams().receive(Duration.ofSeconds(5)));
                    QuicChannel qc = endpoint.connections().getFirst();
                    ConnectionCarrier sc = server.connections().getFirst().attr(ConnectionCarrier.KEY).get();
                    qc.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0x40, 64, 0x30, 0, 1})).sync(); // pipe stream 64
                    for (long end = System.nanoTime() + 5_000_000_000L; sc.channelDropped() == 0 && System.nanoTime() < end; ) Thread.sleep(5);
                    assertEquals(1, sc.channelDropped());
                    assertTrue(s.error().isEmpty());
                    qc.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0, 0x31, 1, 0})).sync(); // DATAGRAM with Length
                    SessionError e = s.closed().get(5, TimeUnit.SECONDS);
                    assertInstanceOf(SessionError.TransportClosed.class, e);
                    assertEquals(0x0a, ((SessionError.TransportClosed) e).code(), e.toString());
                } finally {
                    QuicEndpointTest.closed(c, s);
                }
            }
        }
    }
}
