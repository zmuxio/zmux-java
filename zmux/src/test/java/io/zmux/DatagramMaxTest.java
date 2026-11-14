package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The advertised datagram maximum is delivered exactly, and one byte more is refused, over TCP and TLS. */
@Timeout(60)
class DatagramMaxTest {
    interface Dial { ZmuxSession dial(InetSocketAddress a, ZmuxConfig c) throws Exception; }

    static void check(ListenerConfig.Builder listener, ZmuxConfig cfg, Dial dial) throws Exception {
        try (ZmuxListener l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), listener.session(cfg).build());
             ZmuxSession c = dial.dial(l.localAddress(), cfg); ZmuxSession s = l.accept(Duration.ofSeconds(5))) {
            int max = c.datagrams().maxPayload();
            assertTrue(max > 0);
            assertTrue(c.datagrams().send(ByteBuffer.allocate(max)));
            assertEquals(max, s.datagrams().receive(Duration.ofSeconds(5)).length);
            assertThrows(DatagramTooLargeException.class, () -> c.datagrams().send(ByteBuffer.allocate(max + 1)));

            ZmuxStream st = c.openStream(); st.flush();
            ZmuxStream peer = s.acceptStream(Duration.ofSeconds(5));
            int smax = st.maxDatagramPayload(300);
            assertTrue(st.sendDatagram(300, ByteBuffer.allocate(smax)));
            StreamDatagram d = peer.receiveDatagram(Duration.ofSeconds(5));
            assertEquals(300, d.context()); assertEquals(smax, d.payload().length);
            assertThrows(DatagramTooLargeException.class, () -> st.sendDatagram(300, ByteBuffer.allocate(smax + 1)));
            assertTrue(c.datagrams().send(ByteBuffer.allocate(1)), "nothing is stuck");
            assertEquals(1, s.datagrams().receive(Duration.ofSeconds(5)).length);
            st.close(); peer.close();
        }
    }

    @Test void tcp() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            check(ListenerConfig.builder(), ZmuxConfig.builder().eventLoops(loops).datagrams(true).build(), Zmux::connect);
        }
    }

    @Test void tls() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            check(ListenerConfig.builder().tls(TlsOptions.server(TestTls.server())), ZmuxConfig.builder().eventLoops(loops).datagrams(true).build(),
                    (a, c) -> Zmux.connectTls(a, TlsOptions.client(TestTls.client(), "localhost"), c));
        }
    }
}
