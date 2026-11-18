package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A zero or negative timed wait takes only what is ready and otherwise times out at once; it never waits unbounded. */
@Timeout(60)
class ZeroTimeoutTest {
    static final Duration FAST = Duration.ofSeconds(2);

    @Test void zeroAndNegativeWaitsTakeWhatIsReady() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
            try (ZmuxListener l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    ListenerConfig.builder().session(cfg).build());
                 ZmuxSession c = Zmux.connect(l.localAddress(), cfg); ZmuxSession s = l.accept(Duration.ofSeconds(5))) {
                // accept: nothing pending
                assertThrows(SocketTimeoutException.class, () -> s.acceptStream(Duration.ZERO));
                assertThrows(SocketTimeoutException.class, () -> s.acceptStream(Duration.ofNanos(1)));
                assertThrows(SocketTimeoutException.class, () -> s.acceptStream(Duration.ofSeconds(-1)));
                // session datagrams
                assertThrows(SocketTimeoutException.class, () -> s.datagrams().receive(Duration.ZERO));
                assertTrue(c.datagrams().send(ByteBuffer.allocate(3)));
                assertEquals(3, s.datagrams().receive(Duration.ofSeconds(5)).length);
                assertTrue(c.datagrams().send(ByteBuffer.allocate(4)));
                long end = System.nanoTime() + FAST.toNanos();
                byte[] got = null;
                while (got == null && System.nanoTime() < end) {
                    try { got = s.datagrams().receive(Duration.ZERO); } catch (SocketTimeoutException again) { Thread.onSpinWait(); }
                }
                assertNotNull(got, "a queued datagram is returned by a zero wait");
                assertEquals(4, got.length);

                // stream datagrams
                ZmuxStream st = c.openStream(); st.flush();
                ZmuxStream peer = s.acceptStream(Duration.ofSeconds(5));
                long t0 = System.nanoTime();
                assertThrows(SocketTimeoutException.class, () -> peer.receiveDatagram(Duration.ZERO));
                assertThrows(SocketTimeoutException.class, () -> peer.receiveDatagram(Duration.ofSeconds(-5)));
                assertTrue(System.nanoTime() - t0 < FAST.toNanos(), "zero waits return at once");
                assertTrue(st.sendDatagram(7, ByteBuffer.allocate(5)));
                StreamDatagram d = null;
                end = System.nanoTime() + FAST.toNanos();
                while (d == null && System.nanoTime() < end) {
                    try { d = peer.receiveDatagram(Duration.ZERO); } catch (SocketTimeoutException again) { Thread.onSpinWait(); }
                }
                assertNotNull(d, "a queued stream datagram is returned by a zero wait");
                assertEquals(7, d.context());
                st.close(); peer.close();
            }
        }
    }
}
