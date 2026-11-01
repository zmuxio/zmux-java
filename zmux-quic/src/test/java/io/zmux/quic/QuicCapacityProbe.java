package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Explicitly selected diagnostic, not a throughput gate or part of the default test suite. */
@Timeout(300)
class QuicCapacityProbe {
    private final CapacityProbeConfig settings = CapacityProbeConfig.read(System.getProperties());

    @Test void measure() throws Exception {
        var link = settings.link();
        int repetitions = settings.repetitions(), bytes = settings.bytes();
        int modes = link == null && settings.mode() != 3 ? 3 : 4;
        boolean crossSession = settings.crossSession(), trainEachPipe = settings.trainEachPipe();
        TestTls.init();
        var results = new ArrayList<String>();
        for (int round = 0; round < repetitions; round++) {
            // Rotate to reduce systematic JIT/thermal bias; each row owns fresh connections.
            for (int j = 0; j < modes; j++) {
                int mode = (round + j) % modes;
                if (!crossSession && (settings.mode() < 0 || settings.mode() == mode))
                    results.addAll(measure(round, mode, bytes, link, trainEachPipe));
            }
            if (crossSession) {
                for (int j = 0; j < 3; j++) {
                    int mode = 1 + (round + j) % 3;
                    if (settings.mode() < 0 || settings.mode() == mode)
                        results.addAll(measureSessions(round, mode, bytes, link, trainEachPipe));
                }
            }
        }
        System.out.printf("CAPACITY_ENV,java=%s,os=%s,arch=%s,transport=loopback,native=Netty%n",
                System.getProperty("java.version"), System.getProperty("os.name"), System.getProperty("os.arch"));
        System.out.println("CAPACITY_HEADER,round,mode,direction,pipes,connections,application_bytes,elapsed_ns,goodput_Bps,mean_estimate_Bps,max_estimate_Bps,samples,acknowledged_delta");
        if (link != null) System.out.printf("CAPACITY_LINK,bytes_per_second=%d,one_way_delay_ns=%d,queue_bytes=%d%n",
                link.bytesPerSecond(), link.oneWayDelay().toNanos(), link.queueBytes());
        results.forEach(System.out::println);
    }

    private List<String> measure(int round, int mode, int bytes, UdpBottleneck.Config link, boolean trainEachPipe) throws Exception {
        var results = new ArrayList<String>();
        int pipes = mode == 0 ? 1 : 4;
        String name = switch (mode) { case 0 -> "single"; case 1 -> "shared4";
            case 2 -> link == null ? "independent4" : "separate4_same_link"; default -> "independent4"; };
        try (var loops = EventLoops.create(4); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int links = mode == 3 ? pipes : 1;
            settings.checkBudget(1 + (mode >= 2 ? pipes : 1), links);
            var config = settings.sessionConfig(loops, links);
            var options = settings.nativeOptions();
            results.add("WINDOW_CONFIG," + round + "," + name + "," + settings.describe(links));
            try (var local = Zmux.listener(settings.listenerConfig(config)); var remote = Zmux.listener(settings.listenerConfig(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, remote);
                 var client = new QuicClient(TestTls.client, options, local);
                 var topology = new Topology(client, server.address(), mode, pipes, link)) {
                try (var pair = connect(client, server, remote)) {
                    var a = pair.local(); var b = pair.remote();
                    for (int i = 0; i < pipes; i++) {
                        var io = QuicEndpointTest.awaitPipe(topology.pool(i).openPipe());
                        a.paths().add(io, PipeOptions.defaults());
                    }
                    // Exclude the separate bootstrap connection from every measured topology.
                    a.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow().abort();
                    QuicPoolTest.until(() -> a.paths().stats().stream().filter(p -> p.pipe() != 0 && p.eligible()).count() == pipes
                            && b.paths().stats().stream().filter(p -> p.pipe() != 0 && p.eligible()).count() == pipes);
                    int connections = topology.pools.stream().mapToInt(p -> p.connections().size()).sum();
                    assertEquals(mode >= 2 ? pipes : 1, connections);
                    for (int direction = 0; direction < 2; direction++) {
                        System.out.printf("PROBE_PROGRESS,round=%d,mode=%s,direction=%d,phase=warmup%n", round, name, direction);
                        var sender = direction == 0 ? a : b;
                        var receiver = direction == 0 ? b : a;
                        QuicTest.transfer(workers, sender, receiver, 4, 256 << 10);
                        if (trainEachPipe) {
                            var paths = sender.paths().list().stream().filter(p -> p.id() != 0).toList();
                            try {
                                for (var selected : paths) {
                                    selected.setSendStandby(false);
                                    for (var other : paths) if (other.id() != selected.id()) other.setSendStandby(true);
                                    long before = selected.stats().orElseThrow().bytesSent();
                                    QuicTest.transfer(workers, sender, receiver, 1, 512 << 10);
                                    assertTrue(selected.stats().orElseThrow().bytesSent() - before >= (512 << 10), "selected pipe did not carry training payload");
                                }
                            } finally { for (var path : paths) path.setSendStandby(false); }
                            results.add(String.format(Locale.ROOT, "TRAINING_ESTIMATE,%d,%s,%s,%.3f", round, name,
                                    direction == 0 ? "forward" : "reverse", sender.paths().stats().stream()
                                            .filter(p -> p.pipe() != 0 && p.eligible()).mapToDouble(p -> p.deliveryRate().orElse(0)).sum()));
                        }
                        System.out.printf("PROBE_PROGRESS,round=%d,mode=%s,direction=%d,phase=measure%n", round, name, direction);
                        var observed = measureTransfers(workers, List.of(new Traffic(sender, receiver)), bytes);
                        results.add(observed.row(round, name, direction, pipes, connections));
                        for (var row : observed.pipeTraffic()) results.add("PIPE_TRAFFIC," + round + "," + name + "," + (direction == 0 ? "forward" : "reverse") + "," + row);
                        results.add(nativeStats(round, name, direction, "client", client.connections()));
                        results.add(nativeStats(round, name, direction, "server", server.connections()));
                    }
                }
                for (int index = 0; index < topology.relays.size(); index++) {
                    var relay = topology.relays.get(index); relay.check();
                    for (int direction = 0; direction < 2; direction++) {
                        var stats = direction == 0 ? relay.forward() : relay.reverse();
                        results.add(String.format(Locale.ROOT, "LINK_STATS,%d,%s,%s,%d,%d,%d,%d,%d,%d",
                                round, name, direction == 0 ? "forward" : "reverse", index,
                                stats.received(), stats.sent(), stats.dropped(), stats.sentBytes(), stats.peakQueued()));
                    }
                }
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
        return results;
    }

    private record Traffic(ZmuxSession sender, ZmuxSession receiver) {}

    private record Observed(long bytes, long elapsed, double mean, double max, int samples, long acknowledged, List<String> pipeTraffic) {
        String row(int round, String name, int direction, int pipes, int connections) {
            return String.format(Locale.ROOT, "CAPACITY,%d,%s,%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%d,%d",
                    round, name, direction == 0 ? "forward" : "reverse", pipes, connections,
                    bytes, elapsed, bytes * 1e9 / elapsed, mean, max, samples, acknowledged);
        }
    }

    /** One measurement path for single-session and simultaneous cross-session traffic. */
    private Observed measureTransfers(ExecutorService workers, List<Traffic> traffic, int bytes) throws Exception {
        var samples = new ArrayList<Double>();
        var before = new ArrayList<List<PipeStats>>();
        for (var flow : traffic) before.add(flow.sender().paths().stats());
        long acknowledged = 0;
        for (var flow : traffic) acknowledged += acknowledged(flow.sender());
        long begin = System.nanoTime();
        Future<?> transfer = workers.submit(() -> {
            var jobs = new ArrayList<Future<?>>();
            try {
                for (var flow : traffic) jobs.add(workers.submit(() -> {
                    QuicTest.transfer(workers, flow.sender(), flow.receiver(), 4, bytes); return null;
                }));
                for (var job : jobs) job.get();
            } finally { for (var job : jobs) if (!job.isDone()) job.cancel(true); }
            return null;
        });
        long deadline = begin + TimeUnit.SECONDS.toNanos(30);
        try {
            while (!transfer.isDone()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("measured transfer exceeded 30 seconds");
                double rate = 0;
                for (var flow : traffic) rate += rateSum(flow.sender());
                samples.add(rate);
                try { transfer.get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS); }
                catch (TimeoutException expected) { /* the same overall deadline still applies */ }
            }
            transfer.get();
        } finally { if (!transfer.isDone()) transfer.cancel(true); }
        long elapsed = System.nanoTime() - begin;
        long delta = -acknowledged;
        for (var flow : traffic) delta += acknowledged(flow.sender());
        var pipeTraffic = new ArrayList<String>();
        for (int i = 0; i < traffic.size(); i++) {
            var prior = before.get(i);
            var after = traffic.get(i).sender().paths().stats();
            for (var old : prior) {
                if (old.pipe() != 0 && after.stream().noneMatch(p -> p.pipe() == old.pipe()))
                    pipeTraffic.add(i + "," + old.pipe() + ",-1,-1");
            }
            for (var pipe : after) {
                if (pipe.pipe() == 0) continue;
                var old = prior.stream().filter(p -> p.pipe() == pipe.pipe()).findFirst();
                long sent = old.isPresent() ? pipe.bytesSent() - old.get().bytesSent() : -1;
                long reinjected = old.isPresent() ? pipe.bytesReinjected() - old.get().bytesReinjected() : -1;
                pipeTraffic.add(i + "," + pipe.pipe() + "," + sent + "," + reinjected);
            }
        }
        return new Observed(4L * bytes * traffic.size(), elapsed,
                samples.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                samples.stream().mapToDouble(Double::doubleValue).max().orElse(0), samples.size(), delta, List.copyOf(pipeTraffic));
    }

    private double rateSum(ZmuxSession session) throws Exception {
        return session.paths().stats().stream().filter(p -> p.pipe() != 0 && p.eligible()).mapToDouble(p -> p.deliveryRate().orElse(0)).sum();
    }

    private record Pair(ZmuxSession local, ZmuxSession remote) implements AutoCloseable {
        @Override public void close() throws Exception { QuicEndpointTest.closed(local, remote); }
    }

    private Pair connect(QuicClient client, QuicServer server, ZmuxListener listener) throws Exception {
        var a = client.connect(server.address(), "localhost").toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            var b = listener.accept(Duration.ofSeconds(5)); assertNotNull(b); return new Pair(a, b);
        } catch (Exception | AssertionError error) { a.close(); throw error; }
    }

    private List<String> measureSessions(int round, int mode, int bytes, UdpBottleneck.Config link, boolean train) throws Exception {
        var results = new ArrayList<String>();
        String name = switch (mode) { case 1 -> "sessions2_shared_connection";
            case 2 -> "sessions2_separate_connections_same_link"; default -> "sessions2_independent_links"; };
        try (var loops = EventLoops.create(4); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            settings.checkBudget(2 + (mode == 1 ? 1 : 2), 1);
            var config = settings.sessionConfig(loops, 1);
            var options = settings.nativeOptions();
            results.add("WINDOW_CONFIG," + round + "," + name + "," + settings.describe(1));
            try (var local = Zmux.listener(settings.listenerConfig(config)); var remote = Zmux.listener(settings.listenerConfig(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, remote);
                 var client = new QuicClient(TestTls.client, options, local);
                 var topology = new Topology(client, server.address(), mode, 2, link);
                 var first = connect(client, server, remote); var second = connect(client, server, remote)) {
                var pairs = List.of(first, second);
                Object identity = null;
                for (int i = 0; i < pairs.size(); i++) {
                    var pair = pairs.get(i);
                    var io = QuicEndpointTest.awaitPipe(topology.pool(i).openPipe());
                    if (i == 0) identity = io.carrierConn();
                    else if (mode == 1) assertSame(identity, io.carrierConn());
                    else assertNotSame(identity, io.carrierConn());
                    pair.local().paths().add(io, PipeOptions.defaults());
                    pair.local().paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow().abort();
                    QuicPoolTest.until(() -> pair.local().paths().stats().stream().anyMatch(p -> p.pipe() != 0 && p.eligible())
                            && pair.remote().paths().stats().stream().anyMatch(p -> p.pipe() != 0 && p.eligible()));
                }
                int connections = topology.pools.stream().mapToInt(p -> p.connections().size()).sum();
                assertEquals(mode == 1 ? 1 : 2, connections);
                for (int direction = 0; direction < 2; direction++) {
                    System.out.printf("PROBE_PROGRESS,round=%d,mode=%s,direction=%d,phase=warmup%n", round, name, direction);
                    var traffic = new ArrayList<Traffic>();
                    for (var pair : pairs) traffic.add(direction == 0 ? new Traffic(pair.local(), pair.remote()) : new Traffic(pair.remote(), pair.local()));
                    // Each session is trained separately before their simultaneous transaction.
                    for (var flow : traffic) QuicTest.transfer(workers, flow.sender(), flow.receiver(), 1, train ? 512 << 10 : 256 << 10);
                    double rate = 0;
                    for (var flow : traffic) rate += rateSum(flow.sender());
                    results.add(String.format(Locale.ROOT, "TRAINING_ESTIMATE,%d,%s,%s,%.3f", round, name,
                            direction == 0 ? "forward" : "reverse", rate));
                    System.out.printf("PROBE_PROGRESS,round=%d,mode=%s,direction=%d,phase=measure%n", round, name, direction);
                    var observed = measureTransfers(workers, traffic, bytes);
                    results.add(observed.row(round, name, direction, 2, connections));
                    for (var row : observed.pipeTraffic()) results.add("PIPE_TRAFFIC," + round + "," + name + "," + (direction == 0 ? "forward" : "reverse") + "," + row);
                    results.add(nativeStats(round, name, direction, "client", client.connections()));
                    results.add(nativeStats(round, name, direction, "server", server.connections()));
                }
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
        return results;
    }

    /** Pools and relays are experiment resources; the application client remains their owner. */
    private static final class Topology implements AutoCloseable {
        final List<QuicPool> pools = new ArrayList<>();
        final List<UdpBottleneck> relays = new ArrayList<>();
        Topology(QuicClient client, java.net.InetSocketAddress server, int mode, int pipes, UdpBottleneck.Config link) throws Exception {
            try {
                int count = mode == 3 ? pipes : 1;
                for (int i = 0; i < count; i++) {
                    var address = server;
                    if (link != null) {
                        var relay = new UdpBottleneck(server, link); relays.add(relay); address = relay.address();
                    }
                    pools.add(new QuicPool(client, address, "localhost", new QuicPool.Limits(mode >= 2 ? 1 : pipes, 0, Duration.ZERO)));
                }
            } catch (Exception error) {
                try { close(); } catch (Exception cleanup) { error.addSuppressed(cleanup); }
                throw error;
            }
        }
        QuicPool pool(int index) { return pools.get(pools.size() == 1 ? 0 : index); }
        @Override public void close() throws Exception {
            for (var pool : pools) pool.close();
            Exception error = null;
            for (var relay : relays) {
                try { relay.close(); }
                catch (Exception failure) { if (error == null) error = failure; else error.addSuppressed(failure); }
            }
            if (error != null) throw error;
        }
    }

    /** Cumulative native counters, sampled outside the timed transfer; includes an idle bootstrap if still live. */
    private String nativeStats(int round, String mode, int direction, String role,
                               List<io.netty.handler.codec.quic.QuicChannel> channels) throws Exception {
        long sent = 0, lost = 0, lostBytes = 0, retransmitted = 0;
        int collected = 0, skipped = 0;
        for (var channel : channels) {
            if (!channel.isActive()) { skipped++; continue; }
            io.netty.handler.codec.quic.QuicConnectionStats stats;
            try { stats = channel.collectStats().get(5, TimeUnit.SECONDS); }
            catch (ExecutionException error) {
                if (!channel.isActive()) { skipped++; continue; }
                throw error;
            }
            collected++; sent += stats.sent(); lost += stats.lost();
            lostBytes += stats.lostBytes(); retransmitted += stats.streamRetransBytes();
        }
        return String.format(Locale.ROOT, "NATIVE_STATS,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d", round, mode,
                direction == 0 ? "forward" : "reverse", role, collected, skipped, sent, lost, lostBytes, retransmitted);
    }

    private long acknowledged(ZmuxSession session) throws Exception {
        return session.paths().stats().stream().filter(p -> p.pipe() != 0).mapToLong(p -> p.bytesAcked().orElse(0)).sum();
    }
}
