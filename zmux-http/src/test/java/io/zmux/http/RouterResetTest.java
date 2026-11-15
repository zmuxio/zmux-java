package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** A peer's reset on a plain HTTP connection, in HTTP/1.1, HTTP/2 or before either is known, ends the connection; the exception reaches nobody. */
class RouterResetTest {
    @Test void aResetMidRequestClosesTheConnectionQuietly() throws Exception {
        try (var f = Fixture.plain(b -> b)) {
            String[] starts = {"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\npart", "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", "GE"};
            for (String start : starts) {
                try (Socket s = new Socket(f.base.getHost(), f.base.getPort())) {
                    s.setSoLinger(true, 0); // close sends RST
                    s.getOutputStream().write(start.getBytes(StandardCharsets.ISO_8859_1));
                    s.getOutputStream().flush();
                    Thread.sleep(100);
                }
            }
            Thread.sleep(300);
            assertTrue(f.server.isActive()); // LeakCheck fails the class if the reset reached the pipeline's tail
        }
    }
}
