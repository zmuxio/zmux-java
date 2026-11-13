package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.seal.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A Bound handshake whose two ends see different exporters is a failed authentication, which
 * the listener's OnReject decides: by default an Establish is stalled and a Join closed.
 */
@Timeout(60)
class BindingMismatchTest {
    static final long STALL_MILLIS = 1_500;
    /** A stall far longer than any test's guard: a carrier ended within the guard was closed, not stalled. */
    static final long LONG_STALL_MILLIS = 120_000;

    static byte[] filled(int n, int v) { byte[] o = new byte[n]; Arrays.fill(o, (byte) v); return o; }

    /** Runs a Bound Establish whose exporters differ; returns how long the client waited for the end, in ms. */
    private static long mismatchedEstablish(ZmuxListener l) throws Exception {
        PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        l.handle(pair[1].withExporter((a, b, n) -> filled(n, 0)).withSecure(true), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
        PipeIo near = pair[0].withExporter((a, b, n) -> filled(n, 1)).withSecure(true);
        long t0 = System.nanoTime();
        var f = Seal.establish(near, SealTest.client().mode(RecordMode.BOUND).handshakeTimeout(Duration.ofSeconds(20)).build()).toCompletableFuture();
        var e = assertThrows(ExecutionException.class, () -> f.get(20, TimeUnit.SECONDS));
        assertInstanceOf(NoReplyException.class, e.getCause(), "nothing was written");
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    private static ZmuxConfig sessions(EventLoops loops) {
        return sessions(loops, STALL_MILLIS);
    }

    private static ZmuxConfig sessions(EventLoops loops, long stallMillis) {
        return SealTest.sessions(loops).handshakeTimeout(Duration.ofMillis(stallMillis)).allowRecordMode((c, m, p) -> true).build();
    }

    private static ServerConfig server() {
        return ServerConfig.builder(SealTest.gate(null), SealTest.NODE).allowRecordMode((c, m, p) -> true).build();
    }

    @Test void byDefaultAMismatchedEstablishIsStalled() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var lc = ListenerConfig.builder().session(sessions(loops)).inboundTransform(Seal.server(server())).build();
            try (var l = ZmuxListener.of(lc)) {
                long ms = mismatchedEstablish(l);
                assertTrue(ms >= STALL_MILLIS - 200, "ended before the stall: " + ms + " ms");
            }
        }
    }

    @Test void onRejectReturningCloseEndsItAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(sessions(loops, LONG_STALL_MILLIS)).inboundTransform(Seal.server(server()))
                    .onReject(i -> { rejected.add(i); return RejectInfo.Action.CLOSE; }).build();
            try (var l = ZmuxListener.of(lc)) {
                long ms = mismatchedEstablish(l);
                assertTrue(ms < LONG_STALL_MILLIS / 4, "closed after " + ms + " ms");
                assertEquals(1, rejected.size());
                assertEquals("auth", rejected.getFirst().reason());
                assertEquals(RecordMode.BOUND, rejected.getFirst().recordMode());
                assertEquals(1, l.stats().rejected());
            }
        }
    }

    @Test void byDefaultAMismatchedJoinIsClosedAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = sessions(loops, LONG_STALL_MILLIS);
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(server())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                PipeIo p = SealTest.establish(SealTest.tcp(l.localAddress()), SealTest.client().build());
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(10))) {
                    assertNotNull(s);
                    PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    l.handle(pair[1].withExporter((a, b, n) -> filled(n, 0)).withSecure(true), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 2)));
                    PipeIo near = pair[0].withExporter((a, b, n) -> filled(n, 1)).withSecure(true);
                    long t0 = System.nanoTime();
                    assertThrows(Exception.class, () -> c.addPipe(near, PipeOptions.builder().recordMode(RecordMode.BOUND).build()));
                    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue(ms < LONG_STALL_MILLIS / 4, "the join ended after " + ms + " ms");
                    assertFalse(c.isClosed(), "the session goes on");
                }
            }
        }
    }
}
