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

/** Datagrams on the QUIC connection's datagram channel (EXTENSIONS.md Section 6.8). */
@Timeout(60)
class QuicChannelTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
    }

    /**
     * Forwards UDP between one client and a server with a one-way delay, and drops the next
     * client-to-server packets of at least 1000 bytes when armed.
     */
    static final class LossyRelay implements AutoCloseable {
        final DatagramSocket front = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final DatagramSocket back = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final SocketAddress server;
        final long delayNanos;
        final AtomicInteger drop = new AtomicInteger(), dropped = new AtomicInteger();
        volatile SocketAddress client;
        final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        final List<Thread> pumps = new ArrayList<>();

        LossyRelay(SocketAddress server, Duration delay) throws Exception {
            this.server = server;
            this.delayNanos = delay.toNanos();
            pumps.add(Thread.ofVirtual().start(() -> pump(true)));
            pumps.add(Thread.ofVirtual().start(() -> pump(false)));
        }

        InetSocketAddress address() { return (InetSocketAddress) front.getLocalSocketAddress(); }

        private void pump(boolean toServer) {
            DatagramSocket in = toServer ? front : back;
            byte[] buf = new byte[65536];
            while (!in.isClosed()) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                try { in.receive(p); } catch (Exception e) { return; }
                if (toServer) client = p.getSocketAddress();
                if (toServer && p.getLength() >= 1000 && drop.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    dropped.incrementAndGet();
                    continue;
                }
                byte[] copy = Arrays.copyOf(p.getData(), p.getLength());
                SocketAddress to = toServer ? server : client;
                DatagramSocket out = toServer ? back : front;
                try {
                    timer.schedule(() -> {
                        try { out.send(new DatagramPacket(copy, copy.length, to)); } catch (Exception ignored) { }
                    }, delayNanos, TimeUnit.NANOSECONDS);
                } catch (RejectedExecutionException e) { return; }
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

    static long latencyMillis(byte[] b) {
        return (System.nanoTime() - ByteBuffer.wrap(b).getLong()) / 1_000_000;
    }

    /** The stream burst: within the initial congestion window of the QUIC stack, so that only the lost packets delay anything. */
    static final int BURST = 8 << 10;

    record Run(List<Long> latencies, SessionStats client, int dropped) {}

    /**
     * A session through a relay with one-way delay d: a burst of stream data whose first large
     * packets are lost, then datagrams right after it; their latencies and the client's counts.
     */
    static Run lossLatencies(Duration d, QuicOptions clientOptions) throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = config(loops);
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var relay = new LossyRelay(server.address(), d);
                 var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                ZmuxSession c = endpoint.connect(relay.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession s = listener.accept(Duration.ofSeconds(10));
                assertNotNull(s);
                try {
                    ZmuxStream st = c.openStream();
                    st.write(ByteBuffer.wrap("warm".getBytes()));
                    ZmuxStream in = s.acceptStream();
                    Future<byte[]> read = workers.submit(() -> QuicTest.read(in));
                    for (int i = 0; i < 5; i++) {
                        c.datagrams().send(ByteBuffer.wrap(stamp(i)));
                        assertNotNull(s.datagrams().receive(Duration.ofSeconds(5)));
                    }
                    Thread.sleep(4 * d.toMillis());
                    relay.drop.set(2);
                    st.write(ByteBuffer.wrap(new byte[BURST]));
                    Thread.sleep(5); // the burst is on the wire, its head lost
                    for (int i = 0; i < 10; i++) {
                        c.datagrams().send(ByteBuffer.wrap(stamp(i)));
                        Thread.sleep(1);
                    }
                    List<Long> lat = new ArrayList<>();
                    for (int i = 0; i < 10; i++) {
                        byte[] p;
                        try { p = s.datagrams().receive(Duration.ofSeconds(2)); }
                        catch (java.net.SocketTimeoutException e) { break; }
                        lat.add(latencyMillis(p));
                    }
                    st.finish();
                    assertEquals((BURST) + 4, read.get(20, TimeUnit.SECONDS).length);
                    in.close();
                    st.close();
                    return new Run(lat, c.stats(), relay.dropped.get());
                } finally {
                    QuicEndpointTest.closed(c, s);
                }
            }
        }
    }

    /** Channel datagrams are not held behind a lost packet of the pipe's stream; datagrams in records are. */
    @Test void channelDatagramsAvoidHeadOfLineBlocking() throws Exception {
        Duration d = Duration.ofMillis(40);
        Run channel = lossLatencies(d, QuicOptions.defaults());
        assertTrue(channel.dropped > 0, "no packet lost");
        assertTrue(channel.client.datagramsSentChannel() >= 15, "not on the channel: " + channel.client);
        assertTrue(channel.latencies.size() >= 5, channel.latencies.size() + " datagrams arrived");
        for (long l : channel.latencies) assertTrue(l < 2 * d.toMillis(), "channel datagram took " + l + " ms: " + channel.latencies);

        Run records = lossLatencies(d, QuicOptions.defaults().withDatagrams(false));
        assertTrue(records.dropped > 0, "no packet lost");
        assertEquals(0, records.client.datagramsSentChannel());
        assertEquals(10, records.latencies.size(), records.latencies.toString());
        // Each waits for the retransmission of the stream's lost packet: at least a round trip more.
        for (long l : records.latencies) assertTrue(l >= 2 * d.toMillis(), "a datagram in records was not held: " + records.latencies);
        System.out.println("channel " + channel.latencies + " records " + records.latencies);
    }

    /** Larger datagrams, a peer without QUIC datagrams and sessions without datagrams use records. */
    @Test void fallbackToRecords() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = config(loops);
            for (QuicOptions clientOptions : List.of(QuicOptions.defaults(), QuicOptions.defaults().withDatagrams(false))) {
                try (var listener = Zmux.listener(ListenerConfig.of(config));
                     var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                     var endpoint = new QuicClient(TestTls.client, clientOptions)) {
                    ZmuxSession c = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
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
                        assertEquals(clientOptions.datagrams() ? 1 : 0, cs.datagramsSentChannel(), cs.toString());
                        assertEquals(clientOptions.datagrams() ? 1 : 0, cs.datagramsReceivedChannel(), cs.toString());
                        assertEquals(clientOptions.datagrams() ? 1 : 0, s.stats().datagramsReceivedChannel());
                    } finally {
                        QuicEndpointTest.closed(c, s);
                    }
                }
            }
            var none = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.of(none));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connect(server.address(), "localhost", none).toCompletableFuture().get(10, TimeUnit.SECONDS);
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
            try (var local = Zmux.listener(ListenerConfig.of(config));
                 var remote = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), remote);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults(), local);
                 var pool = new QuicPool(endpoint, server.address(), "localhost")) {
                var p1 = pool.openPipe().toCompletableFuture().get(10, TimeUnit.SECONDS);
                var p2 = pool.openPipe().toCompletableFuture().get(10, TimeUnit.SECONDS);
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
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
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
