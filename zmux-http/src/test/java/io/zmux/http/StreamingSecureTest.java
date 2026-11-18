package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.RecordMode;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A bidirectional request on a TLS connection this process terminates is a Secure carrier, on both ends. */
@Timeout(60)
class StreamingSecureTest {
    private static final PipeOptions.Direction BIDI = PipeOptions.Direction.BIDIRECTIONAL;

    @Test void http2OverTlsTakesABoundJoin() throws Exception { boundJoin(false); }

    @Test void http1OverTlsTakesABoundJoin() throws Exception { boundJoin(true); }

    private static void boundJoin(boolean h1) throws Exception {
        try (var f = new Fixture(b -> b.pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((c, m, p) -> true).maxPipes(4),
                b -> b, b -> b.streaming(), true, null);
             var http = f.client(b -> h1 ? b.http1() : b.http2())) {
            var io = http.pipe(f.base, Map.of(), BIDI, null);
            assertTrue(io.isCarrierSecure(), "the client's pipe declares Secure");
            assertNotNull(io.capabilities().exporter());
            try (ZmuxSession c = Zmux.open(io, Role.CLIENT, f.config); ZmuxSession s = f.listener.accept(Duration.ofSeconds(5))) {
                assertNotNull(s);
                var p = c.addPipe(http.pipe(f.base, Map.of(), BIDI, null), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                assertTrue(p.id().value() > 0);
                assertEquals(RecordMode.BOUND, p.stats().orElseThrow().recordMode());
                Fixture.exchange(c, s, 100_000);
            }
        }
    }

    @Test void h2cIsNotSecure() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming()); var http = f.client(b -> b.http2())) {
            var io = http.pipe(f.base, Map.of(), BIDI, null);
            assertFalse(io.isCarrierSecure());
            io.writer().close();
            io.reader().close();
        }
    }

    @Test void splitHalvesOverTlsAreNotBound() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.streaming(), true, null); var http = f.client(b -> b.http2())) {
            var io = http.pipe(f.base, Map.of(), PipeOptions.Direction.SEND_ONLY, null);
            assertFalse(io.isCarrierSecure());
            io.writer().close();
        }
    }

    @Test void tls12IsNotSecure() throws Exception {
        var tls12 = io.netty.handler.ssl.SslContextBuilder.forClient()
                .sslProvider(io.netty.handler.ssl.SslProvider.JDK).trustManager(Fixture.tmf()).protocols("TLSv1.2");
        try (var f = new Fixture(b -> b, b -> b, b -> b.streaming(), true, null); var http = f.client(b -> b.http1().tls(tls12))) {
            var first = http.pipe(f.base, Map.of(), BIDI, null);
            assertFalse(first.isCarrierSecure(), "a full TLS 1.2 handshake does not declare Secure");
            var again = http.pipe(f.base, Map.of(), BIDI, null);
            assertFalse(again.isCarrierSecure(), "a resumed TLS 1.2 session does not either");
            for (var io : new io.zmux.carrier.PipeIo[] {first, again}) { io.writer().close(); io.reader().close(); }
        }
    }
}
