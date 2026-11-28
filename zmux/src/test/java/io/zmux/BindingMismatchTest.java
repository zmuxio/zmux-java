package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.seal.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A mismatched Bound exporter is a local authentication rejection. The listener closes
 * silently by default and can be configured to stall.
 */
@Timeout(60)
class BindingMismatchTest {
    /** A stall far longer than any test's guard: a carrier ended within the guard was closed, not stalled. */
    static final long LONG_STALL_MILLIS = 120_000;

    static byte[] filled(int n, int v) { byte[] o = new byte[n]; Arrays.fill(o, (byte) v); return o; }

    private static ZmuxFuture<PipeIo> mismatchedEstablish(ZmuxListener l) {
        PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        l.adapter().handle(pair[1].withExporter((a, b, n) -> filled(n, 0)).withCarrierSecure(), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
        PipeIo near = pair[0].withExporter((a, b, n) -> filled(n, 1)).withCarrierSecure();
        return Seal.establish(near, SealTest.client().mode(RecordMode.BOUND).noHandshakeTimeout().build());
    }

    private static void assertNoReply(ZmuxFuture<PipeIo> attempt) {
        var e = assertThrows(ExecutionException.class, () -> attempt.get(20, TimeUnit.SECONDS));
        assertInstanceOf(NoReplyException.class, e.getCause(), "nothing was written");
    }

    private static ZmuxConfig sessions(EventLoops loops, long stallMillis) {
        return SealTest.sessions(loops).handshakeTimeout(Duration.ofMillis(stallMillis)).allowRecordMode((c, m, p) -> true).build();
    }

    private static ServerConfig server() {
        return ServerConfig.builder(SealTest.gate(null), SealTest.NODE).allowRecordMode((c, m, p) -> true).build();
    }

    @Test void byDefaultAMismatchedEstablishIsClosedSilently() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var rejected = new CompletableFuture<RejectInfo>();
            var lc = ListenerConfig.builder().session(sessions(loops, LONG_STALL_MILLIS)).inboundTransform(Seal.server(server()))
                    .onReject(rejected::complete).build();
            try (var l = Zmux.listener(lc)) {
                assertNoReply(mismatchedEstablish(l));
                var info = rejected.get(20, TimeUnit.SECONDS);
                assertEquals("auth", info.reason());
                assertEquals(RecordMode.BOUND, info.recordMode());
                assertEquals(1, l.stats().rejected());
            }
        }
    }

    @Test void anExplicitStallKeepsAMismatchedEstablishOpenUntilTheTimer() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var parked = new CompletableFuture<Void>();
            var owner = loops.next();
            var lc = ListenerConfig.builder().session(sessions(loops, LONG_STALL_MILLIS)).inboundTransform(Seal.server(server()))
                    .rejection(RejectInfo.Action.STALL)
                    .onReject(info -> owner.execute(() -> parked.complete(null))).build();
            try (var l = Zmux.listener(lc)) {
                var attempt = mismatchedEstablish(l);
                parked.get(20, TimeUnit.SECONDS);
                assertFalse(attempt.isDone());
                owner.advanceTimersForTests(TimeUnit.MILLISECONDS.toNanos(LONG_STALL_MILLIS)).get(20, TimeUnit.SECONDS);
                assertNoReply(attempt);
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
                    l.adapter().handle(pair[1].withExporter((a, b, n) -> filled(n, 0)).withCarrierSecure(), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 2)));
                    PipeIo near = pair[0].withExporter((a, b, n) -> filled(n, 1)).withCarrierSecure();
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
