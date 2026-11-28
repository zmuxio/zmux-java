package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Role;
import io.zmux.SessionClosedException;
import io.zmux.SessionError;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Close delivery over WebSocket: a coded close delivers the data written before it, then the code. */
@Timeout(60)
class WebSocketCloseTest {
    static void closeDelivers(ZmuxSession client, ZmuxSession server, long code, String reason) throws Exception {
        byte[] data = new byte[1 << 20];
        new java.util.Random(code).nextBytes(data);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var st = client.openStream();
            st.write(data);
            var second = client.openStream();
            second.write(new byte[] {1, 2, 3});
            var in = server.acceptStream(Duration.ofSeconds(5));
            assertNotNull(in);
            Future<byte[]> read = workers.submit(() -> {
                var got = new java.io.ByteArrayOutputStream();
                byte[] b = new byte[1 << 16];
                try {
                    for (int n; (n = in.read(b, 0, b.length)) > 0; ) got.write(b, 0, n);
                } catch (IOException ended) { /* The session's end. */ }
                return got.toByteArray();
            });
            // Synchronize the carrier handoff before checking its ordering against the coded close.
            st.flush();
            // With a linger of an hour, an end within the guard means the peer read the close and ended its side.
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofHours(1));
            SessionError.ApplicationClosed mine;
            try {
                if (code == 0 && reason.isEmpty()) client.close(); else client.close(code, reason);
                mine = assertInstanceOf(SessionError.ApplicationClosed.class, client.closed().get(30, TimeUnit.SECONDS));
                assertEquals(new SessionError.ApplicationClosed(code, reason, true), server.closed().get(30, TimeUnit.SECONDS));
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
            assertEquals(code, mine.code());
            assertFalse(mine.remote());
            var e = assertThrows(SessionClosedException.class, () -> second.write(new byte[] {4}));
            assertEquals(mine, e.error());
            assertTrue(Arrays.equals(data, read.get(5, TimeUnit.SECONDS)), "the peer read the whole 1 MiB in order");
        }
    }

    @Test void aCodedCloseOverWebSocketDeliversTheDataThenTheCode() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            for (long code : new long[] {0x2a, 0}) {
                var p = c.webSocketPipe(WebSocketTest.ws(f.base), Map.of(), List.of(), null);
                var client = Zmux.open(p, Role.CLIENT, f.config);
                closeDelivers(client, f.listener.accept(Duration.ofSeconds(5)), code, code == 0 ? "" : "bye");
            }
        }
    }
}
