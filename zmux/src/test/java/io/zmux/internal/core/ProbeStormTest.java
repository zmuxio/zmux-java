package io.zmux.internal.core;

import io.zmux.CapacityLimits;

import static io.zmux.internal.core.Time.MILLISECOND;
import static io.zmux.internal.core.Time.SECOND;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.PipeState;
import io.zmux.PipeStats;
import io.zmux.internal.wire.TransportParams;
import io.zmux.shaping.Capability;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Event;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;

import java.time.Duration;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Idle PIPE_PING exchanges must not keep generating more probes. */
@Timeout(30)
class ProbeStormTest {
    private static final long PROBE_INTERVAL = 3 * SECOND;
    private static final long CHECK_INTERVAL = 5 * PROBE_INTERVAL;
    private static final long SHAPING_DELAY = 2 * SECOND;
    // At most five periodic probes, their answers, and one exchange crossing a window edge.
    private static final long PING_LIMIT = 12;

    private static AckSimTest.End endpoint(CoreConfig.Role role, boolean client, boolean queueSignal, boolean boundedWrites) {
        var random = new Random(client ? 1 : 2);
        var cfg = CoreConfig.builder().role(role).transportRole(role).random(random)
                .sessionPolicy(new CoreSessionPolicy(new CapacityLimits(2))).psk(new byte[32]).declaredEndToEnd(true).keepAlive(CoreConfig.OFF)
                .probeInterval(client ? PROBE_INTERVAL : 3600 * SECOND).probeJitter(0);
        var params = cfg.local();
        params.present |= TransportParams.MULTIPATH | TransportParams.SESSION_CRYPTO;
        params.secretFlags |= TransportParams.SECRET_PSK;
        params.sessionCrypto = io.zmux.internal.join.SessionCrypto.supported();
        params.maxIdleTimeoutMillis = 0;
        random.nextBytes(params.sessionNonce);
        if (boundedWrites) {
            params.maxStreamDataBidiRemote = 4L << 20;
            params.initialMaxData = 8L << 20;
        }
        var session = new Session(cfg.build(), SECOND);
        var info = new CarrierInfo(Framing.BYTES, false, true, false, queueSignal);
        session.attachBootstrap(info, info);
        return new AckSimTest.End(session, new AckSimTest.Link(boundedWrites ? 4e6 : 1e9, 10 * MILLISECOND), Framing.BYTES, boundedWrites);
    }

    static final class Path {
        final AckSimTest.End client, server;

        Path() { this(true, false); }

        Path(boolean queueSignal, boolean boundedWrites) {
            client = endpoint(CoreConfig.Role.INITIATOR, true, queueSignal, boundedWrites);
            server = endpoint(CoreConfig.Role.RESPONDER, false, queueSignal, boundedWrites);
        }
        long now = SECOND;

        void advance(long elapsed) throws Exception {
            long end = now + elapsed;
            for (;;) {
                client.s.setNow(now);
                server.s.setNow(now);
                client.deliver(now, server.out);
                server.deliver(now, client.out);
                for (var side : new AckSimTest.End[] {client, server}) {
                    if (DeadlineFixture.next(side.s) <= now) side.s.handleTimeout(now);
                    side.flush(now);
                    assertNull(side.s.error());
                }
                if (now == end) return;
                long timer = Math.min(DeadlineFixture.next(client.s), DeadlineFixture.next(server.s));
                long delivery = Math.min(client.out.head(), server.out.head());
                long next = Math.min(end, Math.min(timer, delivery));
                assertTrue(next > now, "virtual time stalled at " + now);
                now = next;
            }
        }

        PipeStats stats(AckSimTest.End side) { return side.s.pipeStats(0, now, now); }
        long pings() { return stats(client).pingsSent() + stats(server).pingsSent(); }

        void assertProbeTraffic(long previous) {
            long count = pings() - previous;
            assertTrue(count >= 5 && count <= PING_LIMIT, count + " PIPE_PING in five probe intervals");
        }
    }

    @Test void probesNeitherResetSinceLastTrafficNorKeepTheSessionAlive() throws Exception {
        var path = new Path();
        var client = path.client.s;
        path.advance(SECOND);
        assertTrue(client.established());
        long idle = 4 * PROBE_INTERVAL;
        client.keepAlive().start(idle, path.now);
        long deadline = path.now + idle;
        var before = path.stats(path.client);
        path.advance(2 * PROBE_INTERVAL);
        var after = path.stats(path.client);
        assertTrue(after.pingsAnswered() > before.pingsAnswered());
        assertEquals(before.sinceLastTraffic().plusNanos(2 * PROBE_INTERVAL), after.sinceLastTraffic());
        assertEquals(deadline, client.keepAlive().deadline(true));
        path.advance(deadline - path.now - 1);
        assertEquals(SessionState.ACTIVE, client.state());
        client.handleTimeout(deadline);
        assertEquals(SessionError.Kind.IDLE, client.error().kind());
    }

    @Test void aPeerWithProbingDisabledStillChecksTheReturnPath() throws Exception {
        var path = new Path();
        var server = path.server.s;
        server.setLiveness(new CoreConfig.Live(CoreConfig.OFF, MILLISECOND, 0, 0, 0, 0));
        path.advance(PROBE_INTERVAL);
        long sent = path.stats(path.server).pingsSent();
        long answered = path.stats(path.client).pingsAnswered();
        path.advance(2 * PROBE_INTERVAL + 100 * MILLISECOND);
        assertTrue(path.stats(path.server).pingsSent() >= sent + 2, "the server sent its return-path probes");
        assertTrue(path.stats(path.client).pingsAnswered() >= answered + 2, "the client's probes received PIPE_ACKs");
        assertEquals(PipeState.Outbound.ACTIVE, path.stats(path.server).outbound());
    }

    @Test void shapingDelayKeepsAcknowledgementsInTime() throws Exception {
        var path = new Path();
        path.advance(PROBE_INTERVAL);
        for (var side : new AckSimTest.End[] {path.client, path.server}) {
            assertTrue(side.s.features().multipath());
            side.s.setShaper(null, new io.zmux.shaping.ShapingBudget(null, new io.zmux.shaping.ProfileLimits(10, 0, 0, Duration.ofNanos(SHAPING_DELAY)), true));
            side.s.setShaper(pipe -> new Shaper() {
                @Override public Set<Capability> capabilities() { return Set.of(Capability.PLAN); }
                @Override public void plan(RecordView view, RecordPlan plan) { plan.delayNanos(SHAPING_DELAY); }
                @Override public void onEvent(Event event, Effects effects) {}
            });
        }
        path.advance(2 * PROBE_INTERVAL);
        long pings = path.pings();
        path.advance(CHECK_INTERVAL);
        for (var side : new AckSimTest.End[] {path.client, path.server}) {
            var stats = path.stats(side);
            assertEquals(PipeState.Outbound.ACTIVE, stats.outbound());
            assertEquals(0, stats.suspectCount(), "delayed records missed the ACK deadline");
        }
        assertEquals(Duration.ofNanos(SHAPING_DELAY), path.client.s.shapingMeters().maxAddedDelay());
        path.assertProbeTraffic(pings);
    }

    @Test void longRoundTripLearnedDespiteSuspicion() throws Exception {
        var path = new Path();
        path.advance(PROBE_INTERVAL + SECOND);
        var before = path.stats(path.client);
        assertEquals(0, before.suspectCount());
        assertTrue(before.smoothedRtt().compareTo(Duration.ofMillis(25)) < 0);
        path.client.out.delay = path.server.out.delay = 1500 * MILLISECOND;

        path.advance(CHECK_INTERVAL);
        long suspicions = path.stats(path.client).suspectCount();
        assertTrue(suspicions > 0, "the RTT increase should initially make the pipe suspect");
        long pings = path.pings();
        path.advance(CHECK_INTERVAL);
        var after = path.stats(path.client);
        assertTrue(after.smoothedRtt().compareTo(Duration.ofSeconds(2)) >= 0, "smoothed RTT " + after.smoothedRtt());
        assertEquals(suspicions, after.suspectCount(), "suspicions after learning the new RTT");
        assertEquals(PipeState.Outbound.ACTIVE, after.outbound());
        path.assertProbeTraffic(pings);
    }
}
