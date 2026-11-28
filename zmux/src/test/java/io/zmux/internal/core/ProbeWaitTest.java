package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class ProbeWaitTest {
    static final long K = 15 * Time.SECOND;

    @Test void noJitterWaitsExactlyTheInterval() {
        assertEquals(K, PathHealth.probeWait(K, 0, 0.999));
        assertEquals(K, PathHealth.probeWait(K, 0, 0));
    }

    @Test void jitteredWaitsSpanTheirRange() {
        var r = new SplittableRandom(7);
        double j = 0.4, lo = K * (1 - j);
        boolean low = false, high = false;
        for (int i = 0; i < 1000; i++) {
            long w = PathHealth.probeWait(K, j, r.nextDouble());
            assertTrue(w >= lo && w <= K, "wait " + w);
            low |= w < lo + 0.1 * (K - lo);
            high |= w > K - 0.1 * (K - lo);
        }
        assertTrue(low && high, "both ends of [k(1-j), k] reached");
    }

    @Test void anAttachmentWithoutAPeerOutboundDirectionProbesAtMost15s() {
        assertEquals(15 * Time.SECOND, PathHealth.probeInterval(60 * Time.SECOND, 15 * Time.SECOND, true));
        assertEquals(15 * Time.SECOND, PathHealth.probeInterval(-1, 0, true), "session 0 does not disable it there");
        assertEquals(5 * Time.SECOND, PathHealth.probeInterval(5 * Time.SECOND, 0, true));
        assertEquals(0, PathHealth.probeInterval(-1, 0, false), "session 0 disables pipe-pair probes");
        assertEquals(60 * Time.SECOND, PathHealth.probeInterval(60 * Time.SECOND, 15 * Time.SECOND, false));
        assertEquals(7 * Time.SECOND, PathHealth.probeInterval(-1, 7 * Time.SECOND, false));
        assertEquals(0, PathHealth.probeInterval(0, 7 * Time.SECOND, false), "a pipe's own 0 turns its pair probes off");
        assertEquals(15 * Time.SECOND, PathHealth.probeInterval(0, 7 * Time.SECOND, true), "but not an unpaired attachment's");
    }

    @Test void onlyTheSelectedPipeUsesItsOwnIdleProbeIntervalAndJitter() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().probeInterval(K).probeJitter(0));
        var other = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        var path = session.mp();
        long interval = 2 * Time.SECOND;
        session.setPipeLiveness(other.id, 0, 0, interval, 0);
        assertEquals(interval, path.health.probeWait(other));
        session.setPipeLiveness(other.id, 0, 0, interval, 0.9);
        Poke.set(other.mp().health, "probeDraw", 0.5);
        assertEquals(1100 * Time.MILLISECOND, path.health.probeWait(other));
        assertEquals(K, path.health.probeWait(session.pipes.get(0)));
    }

    @Test void theIntervalIsMeasuredToTheCarrier() {
        assertEquals(K, PathHealth.effectiveInterval(K, 0));
        assertEquals(13 * Time.SECOND, PathHealth.effectiveInterval(K, 2 * Time.SECOND), "k - H_p");
        assertEquals(Time.SECOND, PathHealth.effectiveInterval(K, 30 * Time.SECOND), "at least 1 s");
        long half = 500 * Time.MILLISECOND;
        assertEquals(half, PathHealth.effectiveInterval(half, 0), "k below 1 s is not raised");
        assertEquals(half, PathHealth.effectiveInterval(half, 2 * Time.SECOND), "k below 1 s stays k under any hold");
    }

    @Test void aBootstrapPipesFirstWaitSpansTheWholeInterval() {
        var r = new SplittableRandom(11);
        boolean early = false;
        for (int i = 0; i < 1000; i++) {
            long w = PathHealth.probeWait(K, 1, r.nextDouble());
            assertTrue(w > 0 && w <= K, "wait " + w);
            early |= w < K / 10;
        }
        assertTrue(early, "draws from [0, k]");
    }

    @Test void onlyTheBootstrapPipesFirstWaitIsDrawnFromTheWholeInterval() {
        assertEquals(1, PathHealth.drawJitter(0, 0, 0.2), "bootstrap pipe, no PIPE_PING yet");
        assertEquals(0.2, PathHealth.drawJitter(0, 1, 0.2), "bootstrap pipe after its first probe");
        assertEquals(0.2, PathHealth.drawJitter(3, 0, 0.2), "a joined pipe's acceptor end: the join carried a PIPE_PING");
        assertEquals(0.2, PathHealth.drawJitter(3, 1, 0.2));
    }

    @Test void quietSamplesStayOutOfTheMinimum() {
        var r = new RttEstimator();
        r.update(50 * Time.MILLISECOND, 0, 0, 0, true);
        assertEquals(50 * Time.MILLISECOND, r.min(), "the first sample sets it even when quiet");
        long before = r.smoothed();
        r.update(20 * Time.MILLISECOND, 0, 0, Time.SECOND, true);
        assertEquals(50 * Time.MILLISECOND, r.min(), "a quiet sample does not lower it");
        assertNotEquals(before, r.smoothed(), "smoothed_rtt takes it");
        r.update(20 * Time.MILLISECOND, 0, 0, 2 * Time.SECOND, false);
        assertEquals(20 * Time.MILLISECOND, r.min());
    }
}
