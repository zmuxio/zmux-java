package io.zmux.quic;

import io.zmux.*;
import static org.junit.jupiter.api.Assertions.*;
import java.net.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class UdpBottleneckTest {
    @Test void serializationDelayAndDropTailUseOneBoundedQueue() {
        var queue = new UdpBottleneck.Queue(new UdpBottleneck.Config(1000, Duration.ofMillis(20), 100));
        var first = queue.reserve(50, 0); var second = queue.reserve(50, 0);
        assertEquals(50_000_000, first.serializationDelay);
        assertEquals(70_000_000, first.deliveryDelay);
        assertEquals(120_000_000, second.deliveryDelay);
        assertNull(queue.reserve(1, 0));
        assertEquals(100, queue.peak());
        // At 50 ms the first packet is still propagating, but has left the send queue.
        queue.complete(first); queue.complete(first); // release is idempotent
        var third = queue.reserve(10, 50_000_000);
        assertNotNull(third); assertEquals(80_000_000, third.deliveryDelay);
        queue.complete(second); queue.complete(third);
        assertEquals(30_000_000, queue.reserve(10, 500_000_000).deliveryDelay);
    }
    @Test void actualDatagramsCrossBothDirectionsAndCloseStopsWorkers() throws Exception {
        try (var server = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
             var client = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
             var relay = new UdpBottleneck((InetSocketAddress) server.getLocalSocketAddress(),
                     new UdpBottleneck.Config(1_000_000, Duration.ofMillis(1), 4096))) {
            server.setSoTimeout(2000); client.setSoTimeout(2000);
            client.send(new DatagramPacket(new byte[] {1, 2, 3}, 3, relay.address()));
            var incoming = new DatagramPacket(new byte[100], 100); server.receive(incoming);
            assertArrayEquals(new byte[] {1, 2, 3}, java.util.Arrays.copyOf(incoming.getData(), incoming.getLength()));
            server.send(new DatagramPacket(new byte[] {4, 5}, 2, incoming.getSocketAddress()));
            var reply = new DatagramPacket(new byte[100], 100); client.receive(reply);
            assertArrayEquals(new byte[] {4, 5}, java.util.Arrays.copyOf(reply.getData(), reply.getLength()));
            assertEquals(1, relay.forward().received()); assertEquals(1, relay.reverse().received());
            assertEquals(0, relay.forward().dropped()); assertEquals(0, relay.reverse().dropped());
            relay.check();
        }
    }
}
