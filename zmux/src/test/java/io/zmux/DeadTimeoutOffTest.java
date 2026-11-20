package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.loop.TransportInfo;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An explicit "off" for the dead timeout, at session and pipe level, and the carrier capability that reports its
 * own failure: declared by nested stream carriers, kept through split halves and transport properties.
 */
@Timeout(30)
class DeadTimeoutOffTest {
    @Test void offIsItsOwnSettingAtSessionAndPipeLevel() {
        Liveness off = Liveness.builder().noDeadTimeout().build();
        assertTrue(off.isNoDeadTimeout());
        assertTrue(off.deadTimeout().isEmpty());
        assertEquals(-1, off.deadTimeoutNanos());
        assertEquals(0, Liveness.defaults().deadTimeoutNanos(), "derived");
        assertNotEquals(Liveness.defaults(), off);
        assertEquals(off, off.toBuilder().build());
        Liveness back = off.toBuilder().deadTimeout(Duration.ofSeconds(3)).build();
        assertFalse(back.isNoDeadTimeout(), "a value replaces off");
        assertEquals(3_000_000_000L, back.deadTimeoutNanos());
        assertEquals(Long.MAX_VALUE, Liveness.builder().deadTimeout(ChronoUnit.FOREVER.getDuration()).build().deadTimeoutNanos(),
                "a huge value saturates");
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().deadTimeout(Duration.ofMillis(999)));

        PipeLiveness p = PipeLiveness.builder().noDeadTimeout().build();
        assertTrue(p.isNoDeadTimeout());
        assertEquals(-1, p.deadTimeoutNanos());
        assertEquals(0, PipeLiveness.inherit().deadTimeoutNanos(), "the carrier's, then the session's");
        assertFalse(p.isInherit());
        assertEquals(-1, PipeLiveness.inherit().over(p).deadTimeoutNanos(), "off overrides");
        assertEquals(2_000_000_000L, p.over(PipeLiveness.builder().deadTimeout(Duration.ofSeconds(2)).build()).deadTimeoutNanos());
        assertEquals(-1, p.over(PipeLiveness.inherit()).deadTimeoutNanos(), "inherit keeps off");
    }

    @Test void aSessionHoldsOffAndTakesItWhileRunning() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).liveness(Liveness.builder().noDeadTimeout().build()).build();
            try (var pair = Pair.memory(cfg)) {
                assertTrue(pair.client().liveness().isNoDeadTimeout());
                pair.client().setLiveness(Liveness.defaults());
                assertFalse(pair.client().liveness().isNoDeadTimeout());
                pair.client().setLiveness(Liveness.builder().noDeadTimeout().build());
                assertTrue(pair.client().liveness().isNoDeadTimeout());
            }
        }
    }

    @Test void nestedStreamCarriersReportTheirOwnFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var outer = Pair.memory(cfg)) {
                var accepted = workers.submit(() -> Zmux.open(outer.server().acceptStream().asPipe(), Role.SERVER, cfg));
                PipeIo carrier = outer.client().openPipeStream();
                assertTrue(carrier.capabilities().reportsFailure());
                assertTrue(TransportInfo.of(carrier).carrier().ownFailure(), "the core sees it");
                try (var client = Zmux.open(carrier, Role.CLIENT, cfg); var server = accepted.get(5, TimeUnit.SECONDS)) {
                    assertFalse(client.isClosed() || server.isClosed());
                }
            }
        }
        PipeIo plain = MemoryPipe.pair(Framing.BYTE_STREAM)[0];
        assertFalse(plain.capabilities().reportsFailure());
        assertFalse(TransportInfo.of(plain).carrier().ownFailure());
        assertTrue(plain.withReportsFailure().withLabel("x").withHalves(plain.reader(), plain.writer()).capabilities().reportsFailure(),
                "kept through the with methods");
        PipeIo[] a = MemoryPipe.pair(Framing.BYTE_STREAM), b = MemoryPipe.pair(Framing.BYTE_STREAM);
        PipeIo up = a[0].withHalves(null, a[0].writer()).withReportsFailure(), down = b[0].withHalves(b[0].reader(), null);
        assertTrue(PipeIo.split(up, down).capabilities().reportsFailure(), "either half of a split reports it");
    }
}
