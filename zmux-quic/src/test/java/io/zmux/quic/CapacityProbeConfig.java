package io.zmux.quic;

import io.zmux.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

/** One configuration authority for the explicitly selected capacity diagnostic. */
record CapacityProbeConfig(UdpBottleneck.Config link, int repetitions, int bytes, int mode,
                           boolean crossSession, boolean trainEachPipe, Windows windows,
                           Scheduler scheduler, long listenerBudget) {
    enum Windows { DEFAULT, CARRIER, BDP }
    record Plan(long nominalBdp, long nativeWindow, long streamWindow, long connectionWindow, long sendBuffer) {}

    static CapacityProbeConfig read(Properties p) {
        long rate = number(p, "rate", 0);
        if (rate < 0) throw new IllegalArgumentException("negative probe rate");
        var link = rate == 0 ? null : new UdpBottleneck.Config(rate,
                Duration.ofMillis(number(p, "delayMillis", 20)), Math.toIntExact(number(p, "queue", 128 << 10)));
        int repetitions = Math.toIntExact(number(p, "repetitions", link == null ? 3 : 1));
        int bytes = Math.toIntExact(number(p, "bytes", link == null ? 4 << 20 : 1 << 20));
        int mode = Math.toIntExact(number(p, "mode", -1));
        boolean cross = Boolean.parseBoolean(p.getProperty("zmux.probe.crossSession", "false"));
        boolean train = Boolean.parseBoolean(p.getProperty("zmux.probe.trainEachPipe", "false"));
        Windows windows = Windows.valueOf(p.getProperty("zmux.probe.windows", "default").toUpperCase(Locale.ROOT));
        Scheduler scheduler = switch (p.getProperty("zmux.probe.scheduler", "eta")) {
            case "eta" -> Scheduler.SPREAD_ETA;
            case "round-robin" -> Scheduler.SPREAD_ROUND_ROBIN;
            case "affinity" -> Scheduler.AFFINITY;
            default -> throw new IllegalArgumentException("unknown probe scheduler");
        };
        long budget = number(p, "listenerBudget", 1L << 30);
        if (repetitions < 1 || bytes < 1 || mode < -1 || mode > 3 || budget < 1 || cross && mode == 0)
            throw new IllegalArgumentException("invalid probe workload, mode or memory budget");
        if (windows != Windows.DEFAULT && (link == null || link.oneWayDelay().isZero()))
            throw new IllegalArgumentException("BDP window controls require a positive link rate and delay");
        return new CapacityProbeConfig(link, repetitions, bytes, mode, cross, train, windows, scheduler, budget);
    }

    private static long number(Properties p, String name, long fallback) {
        return Long.parseLong(p.getProperty("zmux.probe." + name, Long.toString(fallback)));
    }

    /** Ceiling of the nominal byte-flight budget, computed without overflowing intermediate products. */
    private long nominalBdp(int independentLinks) {
        if (independentLinks < 1) throw new IllegalArgumentException("positive independent-link count required");
        if (link == null) return 0;
        BigInteger divisor = BigInteger.valueOf(1_000_000_000);
        return BigInteger.valueOf(link.bytesPerSecond()).multiply(BigInteger.valueOf(link.oneWayDelay().toNanos()))
                .multiply(BigInteger.valueOf(2L * independentLinks)).add(divisor.subtract(BigInteger.ONE))
                .divide(divisor).longValueExact();
    }

    Plan plan(int independentLinks) {
        long bdp = nominalBdp(independentLinks);
        long nativeWindow = windows == Windows.DEFAULT ? 1_250_000 : Math.max(1_250_000, Math.multiplyExact(2, nominalBdp(1)));
        long window = windows == Windows.BDP ? Math.multiplyExact(2, bdp) : 0;
        return new Plan(bdp, nativeWindow, window == 0 ? 0 : Math.max(256 << 10, window),
                window == 0 ? 0 : Math.max(1 << 20, window), window == 0 ? 0 : Math.max(4 << 20, window));
    }

    QuicOptions nativeOptions() {
        return new QuicOptions(plan(1).nativeWindow(), 16, 16, Duration.ofSeconds(30));
    }

    ZmuxConfig sessionConfig(EventLoops loops, int independentLinks) {
        var builder = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(8).withAuth(PipeAuth.psk(new byte[32])).withScheduler(scheduler).withProbation(Duration.ZERO));
        Plan plan = plan(independentLinks);
        if (windows == Windows.BDP) {
            builder.flowControl(FlowControl.defaults().withInitialStreamWindow(plan.streamWindow()).withMaxStreamWindow(plan.streamWindow())
                    .withInitialConnectionWindow(plan.connectionWindow()).withMaxConnectionWindow(plan.connectionWindow())
                    .withSendBuffer(plan.sendBuffer()));
        }
        return builder.build();
    }

    ListenerConfig listenerConfig(ZmuxConfig session) {
        return ListenerConfig.builder().session(session).memoryBudget(listenerBudget).build();
    }

    void checkBudget(int peakConnections, int independentLinks) {
        // Native connection windows are reserved in the listener's existing memory account.
        // Leave room for stream receive buffers, retained data and core state as well.
        long nativeCredit = Math.multiplyExact(peakConnections, nativeOptions().connectionWindow());
        long extra = Math.addExact(Math.multiplyExact(4, plan(independentLinks).connectionWindow()), 128L << 20);
        if (nativeCredit > listenerBudget || extra > listenerBudget - nativeCredit)
            throw new IllegalArgumentException("probe listener budget cannot cover native credits and transfer headroom");
    }

    String describe(int independentLinks) {
        Plan p = plan(independentLinks);
        return String.format(Locale.ROOT, "windows=%s,scheduler=%s,nominal_bdp_B=%d,native_stream_window_B=%d,zmux_stream_window_B=%d,zmux_connection_window_B=%d,send_buffer_B=%d,listener_budget_B=%d",
                windows, scheduler, p.nominalBdp(), p.nativeWindow(), p.streamWindow(), p.connectionWindow(), p.sendBuffer(), listenerBudget);
    }
}
