package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A TLS dial cancelled during its handshake closes the connection once the handshake completes. */
@Timeout(30)
class DialTlsCancelTest {
    @Test void aCancelledDialClosesTheConnectionItEstablishes() throws Exception {
        TestTls.init();
        try (EventLoops loops = EventLoops.create(1); ServerSocket ss = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var f = Zmux.dialTlsPipeAsync(new InetSocketAddress(ss.getInetAddress(), ss.getLocalPort()),
                    TlsOptions.client(TestTls.client(), "localhost"), cfg);
            try (Socket raw = ss.accept()) {
                // The handshake waits on this side: cancel the dial before answering it.
                assertTrue(f.cancel(false));
                SSLSocket tls = (SSLSocket) TestTls.server().getSocketFactory().createSocket(raw, null, raw.getPort(), true);
                tls.setUseClientMode(false);
                tls.setSoTimeout(5_000);
                tls.startHandshake();
                // Nobody took the connection: the client closes it (with or without close_notify).
                try {
                    assertEquals(-1, tls.getInputStream().read());
                } catch (java.net.SocketTimeoutException e) {
                    fail("the cancelled dial kept the connection open");
                } catch (java.io.IOException e) {
                    // Closed without close_notify.
                }
            }
            assertTrue(f.isCancelled());
        }
    }
}
