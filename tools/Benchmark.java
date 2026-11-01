import io.zmux.*;
import io.zmux.carrier.*;
import java.lang.management.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Standalone public-API measurements, not a conformance or performance pass gate. */
public final class Benchmark {
    private static final Map<String, Number> samples = new LinkedHashMap<>();
    private static boolean quick, loadedOnly, footprintOnly;
    private record Pair(ZmuxSession client, ZmuxSession server) implements AutoCloseable {
        public void close() throws Exception {
            client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
        }
    }
    private static Pair memory(PipeIo[] pipes, ZmuxConfig config, ExecutorService workers) throws Exception {
        var accepting = workers.submit(() -> Zmux.open(pipes[1], Role.SERVER, config));
        var client = Zmux.open(pipes[0], Role.CLIENT, config);
        return new Pair(client, accepting.get(10, TimeUnit.SECONDS));
    }
    private static void control(ZmuxSession session) throws Exception {
        int warmup = quick ? 2000 : 20000;
        int count = quick ? 5000 : 50000;
        for (int i = 0; i < warmup; i++) session.paths().joinReady();
        long[] ns = new long[count];
        for (int i = 0; i < count; i++) {
            long start = System.nanoTime(); session.paths().joinReady(); ns[i] = System.nanoTime() - start;
        }
        Arrays.sort(ns);
        samples.put("control_samples", count);
        samples.put("control_p50_ns", ns[count / 2]);
        samples.put("control_p99_ns", ns[(count * 99) / 100]);
    }
    private static long transfer(Pair pair, ExecutorService workers, int chunk, long bytes, boolean owned) throws Exception {
        var received = workers.submit(() -> {
            try (var stream = pair.server.acceptStream(Duration.ofSeconds(10))) {
                long count = 0;
                for (Chunk data; (data = stream.readChunk(1 << 20)) != null;) {
                    try (Chunk receivedChunk = data) { count += receivedChunk.buffer().remaining(); }
                }
                return count;
            }
        });
        byte[] payload = new byte[chunk]; Arrays.fill(payload, (byte) 17);
        long start = System.nanoTime();
        try (var stream = pair.client.openStream()) {
            for (long sent = 0; sent < bytes;) {
                int n = (int)Math.min(chunk, bytes - sent);
                if (owned) stream.writeChunk(Chunk.of(ByteBuffer.wrap(payload, 0, n)));
                else stream.write(ByteBuffer.wrap(payload, 0, n));
                sent += n;
            }
            stream.finish();
            if (received.get(30, TimeUnit.SECONDS) != bytes) throw new AssertionError("incomplete transfer");
        }
        return System.nanoTime() - start;
    }
    private static void bulk(EventLoops loops, ExecutorService workers) throws Exception {
        var config = ZmuxConfig.builder().eventLoops(loops).build();
        try (var listener = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), ListenerConfig.of(config));
             var pair = new Pair(Zmux.connect(listener.localAddress(), config), listener.accept(Duration.ofSeconds(10)))) {
            for (int chunk : new int[] {4096, 65536, 1048576}) {
                for (boolean owned : new boolean[] {false, true}) {
                    transfer(pair, workers, chunk, 4L << 20, owned);
                    long bytes = quick ? 8L << 20 : 128L << 20;
                    long ns = transfer(pair, workers, chunk, bytes, owned);
                    String key = "tcp_" + chunk + (owned ? "_owned" : "_copied");
                    samples.put(key + "_bytes", bytes);
                    samples.put(key + "_ns", ns);
                    samples.put(key + "_bytes_per_second", bytes * 1e9 / ns);
                }
            }
        }
    }
    private record BulkResult(long bytes, long checksum) {}

    private static void ioTimeouts(ZmuxStream stream) {
        stream.setReadTimeout(Duration.ofSeconds(10));
        stream.setWriteTimeout(Duration.ofSeconds(10));
    }
    private static byte[] request(int sequence) {
        byte[] b = new byte[32];
        new Random(sequence).nextBytes(b);
        return b;
    }
    private static void readExactly(ZmuxStream stream, byte[] b) throws Exception {
        var dst = ByteBuffer.wrap(b);
        while (dst.hasRemaining()) if (stream.read(dst) < 0) throw new AssertionError("truncated response");
        if (stream.read(ByteBuffer.allocate(1)) != -1) throw new AssertionError("unexpected trailing data");
    }
    private static long[] roundTrips(Pair pair, ExecutorService workers, int count) throws Exception {
        var echo = workers.submit(() -> {
            for (int i = 0; i < count; i++) {
                try (var stream = pair.server.acceptStream(Duration.ofSeconds(10))) {
                    ioTimeouts(stream);
                    byte[] b = new byte[32]; readExactly(stream, b);
                    if (!Arrays.equals(b, request(i))) throw new AssertionError("request mismatch");
                    stream.write(ByteBuffer.wrap(b)); stream.finish();
                }
            }
            return true;
        });
        try {
            long[] ns = new long[count];
            for (int i = 0; i < count; i++) {
                byte[] sent = request(i), got = new byte[32];
                long start = System.nanoTime();
                try (var stream = pair.client.openStream()) {
                    ioTimeouts(stream); stream.setPriority(new Priority(0, false));
                    stream.write(ByteBuffer.wrap(sent)); stream.finish(); readExactly(stream, got);
                    if (!Arrays.equals(sent, got)) throw new AssertionError("response mismatch");
                }
                ns[i] = System.nanoTime() - start;
            }
            echo.get(15, TimeUnit.SECONDS);
            Arrays.sort(ns);
            return ns;
        } finally { echo.cancel(true); }
    }
    private static void latencySamples(String key, long[] ns) {
        samples.put(key + "_samples", ns.length);
        samples.put(key + "_p50_ns", ns[ns.length / 2]);
        samples.put(key + "_p99_ns", ns[(ns.length * 99) / 100]);
        samples.put(key + "_max_ns", ns[ns.length - 1]);
    }
    private static void loadedCase(EventLoops loops, ExecutorService workers, int pipes,
                                   Scheduler scheduler, int round, boolean reverse) throws Exception {
        var builder = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withScheduler(scheduler));
        if (pipes > 1) builder.multipath(MultipathConfig.of(pipes, PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO));
        var config = builder.build();
        String key = "rr_tcp" + pipes + "_" + scheduler + (reverse ? "_reverse" : "_forward") + "_round" + round;
        System.out.println("BENCH_PROGRESS," + key);
        try (var listener = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), ListenerConfig.of(config));
             var connected = new Pair(Zmux.connect(listener.localAddress(), config), listener.accept(Duration.ofSeconds(10)))) {
            var pair = reverse ? new Pair(connected.server, connected.client) : connected;
            for (int i = 1; i < pipes; i++) connected.client.paths().add(listener.localAddress(), PipeOptions.defaults());
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (pipes > 1 && (pair.client.paths().stats().stream().filter(p -> p.sending().eligible()).count() != pipes
                    || pair.server.paths().stats().stream().filter(p -> p.sending().eligible()).count() != pipes)) {
                if (System.nanoTime() >= readyDeadline) throw new AssertionError("pipes not eligible");
                Thread.sleep(1);
            }
            int warmup = quick ? 20 : 200, count = quick ? 100 : 2000;
            roundTrips(pair, workers, warmup);
            latencySamples(key + "_idle", roundTrips(pair, workers, count));
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var receiving = new CountDownLatch(1);
            var progress = new java.util.concurrent.atomic.AtomicLong();
            var received = workers.submit(() -> {
                try (var stream = pair.server.acceptStream(Duration.ofSeconds(10))) {
                    ioTimeouts(stream);
                    var crc = new java.util.zip.CRC32(); long bytes = 0;
                    for (Chunk data; (data = stream.readChunk(1 << 20)) != null;) {
                        try (Chunk chunk = data) {
                            var b = chunk.buffer(); bytes += b.remaining(); crc.update(b); progress.set(bytes);
                        }
                        receiving.countDown();
                    }
                    return new BulkResult(bytes, crc.getValue());
                }
            });
            var sent = workers.submit(() -> {
                try (var stream = pair.client.openStream()) {
                    ioTimeouts(stream); stream.setPriority(new Priority(7, true));
                    byte[] b = new byte[65536]; new Random(17).nextBytes(b);
                    var crc = new java.util.zip.CRC32(); long bytes = 0;
                    while (!stop.get()) {
                        stream.write(ByteBuffer.wrap(b)); crc.update(b); bytes += b.length;
                    }
                    stream.finish(); return new BulkResult(bytes, crc.getValue());
                }
            });
            try {
                if (!receiving.await(10, TimeUnit.SECONDS)) throw new AssertionError("bulk never became active");
                roundTrips(pair, workers, warmup);
                var before = pair.client.paths().stats();
                long bulkBefore = progress.get();
                long start = System.nanoTime();
                latencySamples(key + "_loaded", roundTrips(pair, workers, count));
                long elapsed = System.nanoTime() - start;
                var after = pair.client.paths().stats();
                long bytes = 0;
                for (var a : after) {
                    var b = before.stream().filter(x -> x.id() == a.id()).findFirst().orElseThrow();
                    long delta = a.traffic().bytesSent() - b.traffic().bytesSent(); bytes += delta;
                    samples.put(key + "_pipe" + a.id() + "_sent_bytes", delta);
                }
                long bulkDelta = progress.get() - bulkBefore;
                if (bulkDelta <= 0) throw new AssertionError("bulk made no progress");
                samples.put(key + "_bulk_received_interval_bytes", bulkDelta);
                samples.put(key + "_interval_ns", elapsed);
                samples.put(key + "_wire_bytes", bytes);
                stop.set(true);
                var written = sent.get(15, TimeUnit.SECONDS); var read = received.get(15, TimeUnit.SECONDS);
                if (!written.equals(read) || written.bytes() == 0) throw new AssertionError("bulk mismatch");
                samples.put(key + "_bulk_total_bytes", written.bytes());
            } finally {
                stop.set(true);
                pair.close(); // Wake blocked I/O before cancelling workers or closing their executor.
                sent.cancel(true); received.cancel(true);
            }
        }
    }
    private static void loaded(EventLoops loops, ExecutorService workers) throws Exception {
        for (int round = 0; round < (quick ? 1 : 3); round++) {
            for (boolean reverse : new boolean[] {false, true}) {
                loadedCase(loops, workers, 1, Scheduler.AFFINITY, round, reverse);
                loadedCase(loops, workers, 4, Scheduler.SPREAD_ETA, round, reverse);
                loadedCase(loops, workers, 4, Scheduler.SPREAD_ROUND_ROBIN, round, reverse);
            }
        }
    }
    private static long heap() throws InterruptedException {
        // A retained-heap estimate. GC/JIT noise is reported as measured, including negative deltas.
        for (int i = 0; i < 3; i++) { System.gc(); Thread.sleep(100); }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
    private static long direct() {
        return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> pool.getName().equals("direct")).mapToLong(BufferPoolMXBean::getMemoryUsed).sum();
    }
    private static void warmFootprint(EventLoops loops, ExecutorService workers, ZmuxConfig config) throws Exception {
        for (int i = 0; i < 8; i++) {
            try (var pair = memory(MemoryPipe.pair(Framing.BYTE_STREAM), config, workers)) {
                for (int j = 0; j < 32; j++) pair.client.openStream().close();
            }
        }
    }
    private static void footprint(EventLoops loops, ExecutorService workers, boolean multipath) throws Exception {
        int count = quick ? 20 : 200;
        var builder = ZmuxConfig.builder().eventLoops(loops);
        if (multipath) builder.multipath(MultipathConfig.of(8, PipeAuth.psk(new byte[32])));
        var config = builder.build();
        warmFootprint(loops, workers, config);
        String prefix = multipath ? "mp_bootstrap_" : "";
        var pipes = new ArrayList<PipeIo[]>();
        var pairs = new ArrayList<Pair>();
        var streams = new ArrayList<ZmuxStream>();
        for (int i = 0; i < count; i++) pipes.add(MemoryPipe.pair(Framing.BYTE_STREAM));
        long baseline = heap(), directBaseline = direct();
        try {
            for (var pipe : pipes) pairs.add(memory(pipe, config, workers));
            long sessions = heap(), directSessions = direct();
            for (int i = 0; i < 200; i++) streams.add(pairs.getFirst().client.openStream());
            long withStreams = heap();
            java.lang.ref.Reference.reachabilityFence(pipes);
            java.lang.ref.Reference.reachabilityFence(pairs);
            java.lang.ref.Reference.reachabilityFence(streams);
            samples.put(prefix + "footprint_session_count", 2 * count);
            samples.put(prefix + "retained_heap_per_session_estimate", (sessions - baseline) / (2 * count));
            samples.put(prefix + "direct_bytes_per_session_estimate", (directSessions - directBaseline) / (2 * count));
            samples.put(prefix + "footprint_unassigned_stream_count", streams.size());
            samples.put(prefix + "retained_heap_per_unassigned_stream_estimate", (withStreams - sessions) / streams.size());
        } finally {
            for (var stream : streams) stream.close();
            for (var pair : pairs) pair.close();
        }
    }
    private static String quote(String s) { return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""; }
    public static void main(String[] args) throws Exception {
        Path result = Path.of("target/benchmark.json");
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--quick")) quick = true;
            else if (args[i].equals("--loaded-only")) loadedOnly = true;
            else if (args[i].equals("--footprint-only")) footprintOnly = true;
            else if (args[i].equals("--result-file") && i + 1 < args.length) result = Path.of(args[++i]);
            else throw new IllegalArgumentException("usage: [--quick] [--loaded-only|--footprint-only] --result-file PATH");
        }
        if (loadedOnly && footprintOnly) throw new IllegalArgumentException("choose one measurement family");
        Thread watchdog = Thread.ofPlatform().daemon(true).start(() -> {
            try { Thread.sleep(Duration.ofMinutes(5)); Runtime.getRuntime().halt(124); }
            catch (InterruptedException expected) { /* Measurements completed. */ }
        });
        try {
            try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                if (loadedOnly) loaded(loops, workers);
                else if (footprintOnly) {
                    footprint(loops, workers, false);
                    footprint(loops, workers, true);
                }
                else {
                    var config = ZmuxConfig.builder().eventLoops(loops).build();
                    try (var pair = memory(MemoryPipe.pair(Framing.BYTE_STREAM), config, workers)) { control(pair.client); }
                    bulk(loops, workers);
                    footprint(loops, workers, false);
                }
                if (io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops) != 0) throw new AssertionError("outstanding receive buffers");
            }
            StringBuilder json = new StringBuilder("{\n  \"java\": ").append(quote(System.getProperty("java.version")))
                    .append(",\n  \"os\": ").append(quote(System.getProperty("os.name") + " " + System.getProperty("os.arch")))
                    .append(",\n  \"profile\": ").append(quote(quick ? "quick" : "full"));
            json.append(",\n  \"family\": ");
            json.append(quote(loadedOnly ? "loaded" : footprintOnly ? "footprint" : "standard"));
            for (var entry : samples.entrySet()) json.append(",\n  ").append(quote(entry.getKey())).append(": ").append(entry.getValue());
            json.append("\n}\n");
            if (result.toAbsolutePath().getParent() != null) Files.createDirectories(result.toAbsolutePath().getParent());
            Files.writeString(result, json); System.out.print(json);
        } finally { watchdog.interrupt(); }
    }
}
