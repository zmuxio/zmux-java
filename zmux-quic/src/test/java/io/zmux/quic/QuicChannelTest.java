package io.zmux.quic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.*;
import io.zmux.internal.core.Time;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
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
        return ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
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
        private CompletableFuture<Void> burstDropped;
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
        synchronized void arm(int n) {
            drop = n;
            holding = true;
            burstDropped = new CompletableFuture<>();
        }

        synchronized CompletableFuture<Void> burstDropped() { return burstDropped; }

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
                            if (drop == 0) burstDropped.complete(null);
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

        @Override public void close() throws IOException {
            front.close();
            back.close();
            timer.shutdownNow();
            try { for (Thread t : pumps) t.join(5000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.io.InterruptedIOException("interrupted"); }
        }
    }

    /** Timestamped payload for the cross-language latency measurement. */
    static byte[] stamp(int seq) {
        return ByteBuffer.allocate(100).putLong(System.nanoTime()).put((byte) seq).array();
    }

    static void awaitDropped(LossyRelay relay, int n) throws Exception {
        relay.burstDropped().get(20, TimeUnit.SECONDS);
        assertEquals(n, relay.dropped.get());
    }

    /** The stream burst: within the initial congestion window of the QUIC stack, so that only the lost packets delay anything. */
    static final int BURST = 8 << 10;

    /**
     * Of the datagrams sent right after a stream burst whose first large packets were lost:
     * those the server received while the lost stream data was still held back, and in all.
     */
    record Run(int whileHeld, int received, SessionStats client, int dropped) {}

    /**
     * A session through a relay with one-way delay d: a burst of stream data whose first large
     * packets are lost and whose later ones (retransmissions among them) are held at the relay,
     * then 10 datagrams. With channel datagrams they are waited for before the relay lets the
     * stream go on; otherwise the server's count is taken once it has processed every packet
     * that carried them. lanes is the endpoints' maximum lane count (0: the default).
     */
    static Run lossLatencies(Duration d, QuicOptions clientOptions, long lanes) throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var b = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true);
            if (lanes > 0) b.maxLanes(lanes);
            var config = b.build();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var relay = new LossyRelay(server.address(), d);
                 var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                ZmuxSession c = endpoint.connectAsync(relay.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                assertNotNull(s);
                try {
                    ZmuxStream st = c.openStream();
                    st.write(ByteBuffer.wrap("warm".getBytes(UTF_8)));
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
                        for (; whileHeld < 5; whileHeld++) assertNotNull(s.datagrams().receive(Duration.ofSeconds(20)));
                    } else {
                        whileHeld = (int) (s.stats().datagramsReceived() - 5);
                    }
                    relay.release();
                    if (!clientOptions.isDatagrams()) {
                        for (int i = 0; i < 10; i++) assertNotNull(s.datagrams().receive(Duration.ofSeconds(20)));
                    }
                    st.finish();
                    assertEquals(BURST + 4, read.get(20, TimeUnit.SECONDS).length);
                    in.close();
                    st.close();
                    return new Run(whileHeld, (int) s.stats().datagramsReceived() - 5, c.stats(), relay.dropped.get());
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
            // Exercise routing after both endpoints establish. The JUnit timeout guards the test.
            var sessions = ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().noMaxIdleTimeout()
                    .noDatagramQueueDelay();
            var listenerConfig = ListenerConfig.builder().firstRecordTimeout(Duration.ofNanos(Time.NEVER));
            var serverOptions = QuicOptions.builder().idleTimeout(Duration.ZERO).build();
            var none = sessions.build();
            var config = sessions.enableDatagrams(true).build();
            for (QuicOptions clientOptions : List.of(serverOptions, serverOptions.toBuilder().datagrams(false).build())) {
                try (var listener = Zmux.listener(listenerConfig.session(config).build());
                     var server = new QuicServer(QuicTest.any(), TestTls.server, serverOptions, listener);
                     var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                    ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", config).get();
                    ZmuxSession s = listener.accept();
                    try {
                        byte[] small = {1, 2, 3}, large = new byte[4000];
                        c.datagrams().send(ByteBuffer.wrap(small));
                        assertArrayEquals(small, s.datagrams().receive());
                        c.datagrams().send(ByteBuffer.wrap(large));
                        assertArrayEquals(large, s.datagrams().receive());
                        s.datagrams().send(ByteBuffer.wrap(small));
                        assertArrayEquals(small, c.datagrams().receive());
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
            try (var listener = Zmux.listener(listenerConfig.session(none).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, serverOptions, listener);
                 var endpoint = new QuicClient(TestTls.client, serverOptions)) {
                ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", none).get();
                ZmuxSession s = listener.accept();
                try {
                    assertThrows(NotNegotiatedException.class, () -> c.datagrams().send(ByteBuffer.wrap(new byte[1])));
                } finally {
                    QuicEndpointTest.closed(c, s);
                }
            }
        }
    }

    /** Model a successful local DATAGRAM write lost before delivery. Stream writes use child channels. */
    private static CompletableFuture<Void> loseNextDatagram(QuicChannel connection) throws Exception {
        var lost = new CompletableFuture<Void>();
        connection.eventLoop().submit(() -> connection.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise result) {
                if (message instanceof ByteBuf datagram) {
                    ctx.pipeline().remove(this);
                    datagram.release();
                    result.setSuccess();
                    lost.complete(null);
                } else {
                    ctx.write(message, result);
                }
            }
        })).get(5, TimeUnit.SECONDS);
        return lost;
    }

    private record Route(ZmuxSession sender, ZmuxSession receiver, String marker, java.util.Set<String> outstanding) {
        Route(ZmuxSession sender, ZmuxSession receiver, String marker) {
            this(sender, receiver, marker, new java.util.HashSet<>());
        }

        void probe(int round) throws java.io.IOException {
            String payload = marker + "/" + round;
            assertTrue(outstanding.add(payload));
            sender.datagrams().send(ByteBuffer.wrap(payload.getBytes(UTF_8)));
        }

        void receive(Duration wait) throws java.io.IOException {
            String received = new String(receiver.datagrams().receive(wait), UTF_8);
            assertTrue(outstanding.remove(received), () -> marker + " received a foreign or duplicate probe: " + received);
        }
    }

    /** One connection carries the pipes of two sessions; its channel routes each datagram to its own. */
    @Test void sessionsShareTheConnectionsChannel() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = config(loops);
            try (var local = Zmux.listener(ListenerConfig.of(config));
                 var remote = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), remote);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults(), local);
                 var pool = QuicPool.builder(endpoint, server.address(), "localhost").build()) {
                var p1 = pool.openPipeAsync().get(10, TimeUnit.SECONDS);
                var p2 = pool.openPipeAsync().get(10, TimeUnit.SECONDS);
                assertSame(p1.carrierConn(), p2.carrierConn());
                ZmuxSession c1 = Zmux.open(p1, Role.CLIENT, config);
                ZmuxSession s1 = remote.accept(Duration.ofSeconds(10));
                assertNotNull(s1);
                ZmuxSession c2 = Zmux.open(p2, Role.CLIENT, config);
                ZmuxSession s2 = remote.accept(Duration.ofSeconds(10));
                assertNotNull(s2);
                try {
                    var allRoutes = List.of(new Route(c1, s1, "one"), new Route(c2, s2, "two"),
                            new Route(s1, c1, "back one"), new Route(s2, c2, "back two"));
                    var routes = new ArrayList<>(allRoutes);
                    var lost = loseNextDatagram(endpoint.connections().getFirst());
                    for (int round = 0; round < 5 && !routes.isEmpty(); round++) {
                        for (Route route : routes) {
                            route.probe(round);
                            if (round == 0 && route.sender == c1) lost.get(5, TimeUnit.SECONDS);
                        }
                        for (var it = routes.iterator(); it.hasNext(); ) {
                            Route route = it.next();
                            try {
                                route.receive(Duration.ofMillis(500));
                                it.remove();
                            } catch (SocketTimeoutException timeout) {
                                // A lost probe can be sent again. A probe from another session fails above.
                            }
                        }
                    }
                    assertTrue(routes.isEmpty(), () -> "Undelivered QUIC routes: " + routes.stream()
                            .map(route -> route.marker + " sent=" + route.sender.stats().datagramsSentChannel()
                                    + " received=" + route.receiver.stats().datagramsReceivedChannel()).toList());
                    assertTrue(c1.stats().datagramsSentChannel() >= 2, "the lost probe had to be sent again");
                    long totalSent = 0, totalReceived = 0;
                    for (var route : allRoutes) {
                        try {
                            while (true) route.receive(Duration.ofMillis(50));
                        } catch (SocketTimeoutException drained) { /* End of this receiver's probe queue. */ }
                        long sent = route.sender.stats().datagramsSentChannel();
                        long received = route.receiver.stats().datagramsReceivedChannel();
                        assertTrue(received <= sent, route.marker);
                        totalSent += sent;
                        totalReceived += received;
                    }
                    assertTrue(totalReceived <= totalSent - 1, "the injected loss cannot be delivered");
                    for (var session : List.of(c1, c2, s1, s2)) {
                        var stats = session.stats();
                        assertEquals(stats.datagramsSent(), stats.datagramsSentChannel());
                        assertEquals(stats.datagramsReceived(), stats.datagramsReceivedChannel());
                        assertTrue(stats.datagramsReceivedChannel() > 0);
                    }
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
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connectAsync(server.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                try {
                    c.datagrams().send(ByteBuffer.wrap(new byte[] {7}));
                    assertArrayEquals(new byte[] {7}, s.datagrams().receive(Duration.ofSeconds(5)));
                    QuicChannel qc = endpoint.connections().getFirst();
                    QuicChannel peer = server.connections().getFirst();
                    ConnectionCarrier sc = peer.attr(ConnectionCarrier.KEY).get();
                    var dropped = new CompletableFuture<Long>();
                    peer.eventLoop().submit(() -> peer.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
                        @Override public void channelReadComplete(ChannelHandlerContext ctx) {
                            if (sc.channelDropped() > 0) dropped.complete(sc.channelDropped());
                            ctx.fireChannelReadComplete();
                        }
                    })).get(20, TimeUnit.SECONDS);
                    qc.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0x40, 64, 0x30, 0, 1})).sync(); // pipe stream 64
                    assertEquals(1L, dropped.get(20, TimeUnit.SECONDS));
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
