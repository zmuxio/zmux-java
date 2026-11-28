package io.zmux.http;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.RecordMode;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A WebSocket over TLS this process runs is a Secure carrier with its exporter, on both ends. */
@Timeout(60)
class WebSocketSecureTest {
    @Test void wssCarriesTheExporterAndTakesABoundJoin() throws Exception {
        try (var f = new Fixture(b -> b.pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((c, m, p) -> true).sessionPolicy(new SessionPolicy(new CapacityLimits(4))),
                b -> b, b -> b.webSocket(), true, null);
             var http = f.client(b -> b.http1())) {
            URI url = URI.create(f.base.toString().replace("https://", "wss://"));
            var io = http.webSocketPipe(url, Map.of(), List.of(), null);
            assertTrue(io.isCarrierSecure(), "the client's wss pipe declares Secure");
            assertTrue(io.capabilities().exporter() != null, "the client's wss pipe has the TLS exporter");
            try (ZmuxSession c = Zmux.open(io, Role.CLIENT, f.config); ZmuxSession s = f.listener.accept(Duration.ofSeconds(5))) {
                assertNotNull(s);
                var p = c.addPipe(http.webSocketPipe(url, Map.of(), List.of(), null), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                assertTrue(p.id().value() > 0);
                Fixture.exchange(c, s, 100_000);
            }
        }
    }

    @Test void plainWebSocketIsNotSecure() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var http = f.client(b -> b.http1())) {
            URI url = URI.create(f.base.toString().replace("http://", "ws://"));
            var io = http.webSocketPipe(url, Map.of(), List.of(), null);
            assertFalse(io.isCarrierSecure());
            io.writer().close();
            io.reader().close();
        }
    }
}
