package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import io.zmux.shaping.Capability;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Event;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Idle probing on one multipath pipe whose exchanges take longer than T_suspect outside the RTT estimate: PIPE_PING
 * exchanges do not feed themselves, and the pipe stays active.
 */
@Timeout(120)
class ProbeStormTest {
    /** The client probes every 3 s; the server at an hour, so the client's probes are the only exchanges started. */
    private static ZmuxConfig config(EventLoops loops, boolean client) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noIdleTimeout()
                .liveness(Liveness.builder().noKeepAlive().probeInterval(client ? Duration.ofSeconds(3) : Duration.ofHours(1)).probeJitter(0).build())
                .build();
    }

    /**
     * Each probe is answered by a PIPE_ACK with at most one PIPE_PING of its own: in 15 s at most 5 probes and their
     * answers, and the window's edges.
     */
    private static final long LIMIT = 12;

    private record Traffic(long pings, Duration srtt) {}

    private static Traffic traffic(ZmuxSession client, ZmuxSession server) {
        long n = 0;
        Duration srtt = Duration.ZERO;
        for (ZmuxSession s : new ZmuxSession[] {client, server}) {
            for (PipeStats st : s.pipeStats()) {
                n += st.pingsSent();
                if (s == client && st.smoothedRtt() != null) srtt = st.smoothedRtt();
            }
        }
        return new Traffic(n, srtt);
    }

    private static void assertActive(ZmuxSession... sessions) {
        for (ZmuxSession s : sessions) {
            for (PipeStats st : s.pipeStats()) {
                assertEquals(PipeState.Outbound.ACTIVE, st.outbound(), "pipe " + st.pipeId() + " " + st.suspectCount() + " suspicions");
                assertEquals(0, st.suspectCount(), "pipe " + st.pipeId() + " turned suspect");
            }
        }
    }

    /**
     * Both ends shape every record with a 2 s delay. A record that carries a PIPE_ACK goes no later than
     * zmux_max_ack_delay after the acknowledgement became due, so the peer's working pipe never turns suspect. Held
     * for the whole delay, every exchange made the pipe suspect, the suspicion's PIPE_PING was answered as late, and
     * the pair kept exchanging PIPE_PING about once a second.
     */
    @Test void shapingDelayKeepsAcknowledgementsInTime() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cc = config(loops, true), sc = config(loops, false);
            try (var listener = ZmuxListener.of(ListenerConfig.of(sc))) {
                PipeIo[] io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("p")));
                try (var client = Zmux.open(io[0], Role.CLIENT, cc); var server = listener.accept(Duration.ofSeconds(10))) {
                    client.setShaper(pipe -> delayShaper(2 * 1_000_000_000L));
                    server.setShaper(pipe -> delayShaper(2 * 1_000_000_000L));
                    Thread.sleep(8_000);
                    long p0 = traffic(client, server).pings();
                    Thread.sleep(15_000);
                    long n = traffic(client, server).pings() - p0;
                    assertTrue(n <= LIMIT, n + " PIPE_PING in 15 s of idle probing");
                    assertActive(client, server);
                }
            }
        }
    }

    /**
     * A path whose round trip grows from 20 ms to 3 s, above T_suspect of the estimate: the next probes turn the pipe
     * suspect before their answers. The estimate learns the path all the same and the suspicions stop. Whether the
     * suspicion's PIPE_PING is sampled despite a later suspicion depends here on a race between that suspicion and the
     * answer; SuspicionPingSampleTest checks that rule deterministically.
     */
    @Test void longRoundTripLearnedDespiteSuspicion() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cc = config(loops, true), sc = config(loops, false);
            try (var listener = ZmuxListener.of(ListenerConfig.of(sc))) {
                PipeIo[] io = SlowPathTest.linked(Duration.ofMillis(10), 1e9, 1 << 30);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("p")));
                try (var client = Zmux.open(io[0], Role.CLIENT, cc); var server = listener.accept(Duration.ofSeconds(10))) {
                    Thread.sleep(4_000);
                    for (PipeIo p : io) ((SlowPathTest.Link) p.writer()).delay(Duration.ofMillis(1500));
                    Thread.sleep(15_000);
                    long p0 = traffic(client, server).pings();
                    long s0 = client.pipeStats().get(0).suspectCount();
                    Thread.sleep(15_000);
                    Traffic t = traffic(client, server);
                    assertTrue(t.srtt().compareTo(Duration.ofSeconds(2)) >= 0, "smoothed RTT " + t.srtt() + " 30 s after the path's round trip became 3 s");
                    long n = t.pings() - p0;
                    assertTrue(n <= LIMIT, n + " PIPE_PING in 15 s of idle probing");
                    assertEquals(s0, client.pipeStats().get(0).suspectCount(), "suspicions once the path is learned");
                }
            }
        }
    }

    /** A shaper that delays every record by nanos. */
    private static Shaper delayShaper(long nanos) {
        return new Shaper() {
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) { plan.delayNanos(nanos); }
            @Override public void onEvent(Event event, Effects effects) {}
        };
    }
}
