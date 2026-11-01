package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.shaping.*;
import io.zmux.testutil.Json;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ShapingTest {
    /** Records the bytes a session writes to its carrier. */
    static final class Recorder implements PipeWriter {
        private final PipeWriter inner;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private volatile boolean on;
        /** System.nanoTime of each recorded write. */
        final List<Long> writes = Collections.synchronizedList(new ArrayList<>());
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            if (on) writes.add(System.nanoTime());
            if (on) synchronized (out) { for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); out.writeBytes(a); } }
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }
        /** Records from now on, keeping what was recorded. */
        void startNow() { on = true; }
        void start() throws InterruptedException {
            Thread.sleep(50);
            synchronized (out) { out.reset(); }
            writes.clear();
            on = true;
        }
        /** Frames lengths and trailing zero bytes of the recorded records. */
        List<int[]> records() {
            byte[] b; synchronized (out) { b = out.toByteArray(); }
            List<int[]> r = new ArrayList<>();
            for (int i = 0; i < b.length;) {
                int first = b[i] & 0xff, len = 1 << (first >> 6);
                long v = first & 0x3f;
                for (int k = 1; k < len; k++) v = v << 8 | (b[i + k] & 0xff);
                int start = i + len, end = (int) (start + v);
                assertTrue(end <= b.length, "truncated record");
                int z = 0;
                for (int k = end - 1; k >= start && b[k] == 0; k--) z++;
                r.add(new int[] {(int) v, z});
                i = end;
            }
            return r;
        }
    }

    record Pair(ZmuxSession shaped, ZmuxSession other, Recorder rec) implements AutoCloseable {
        @Override public void close() { shaped.close(); other.close(); }
    }

    /** A pair over memory carriers whose shaped endpoint is the client, or the server when server is set. */
    static Pair pair(ZmuxConfig shapedCfg, ZmuxConfig otherCfg, boolean server) throws Exception {
        var m = MemoryPipe.pair(Framing.BYTE_STREAM);
        var rec = new Recorder(server ? m[1].writer() : m[0].writer());
        PipeIo c = server ? m[0] : new PipeIo(m[0].reader(), rec, Framing.BYTE_STREAM, "memory", null);
        PipeIo s = server ? new PipeIo(m[1].reader(), rec, Framing.BYTE_STREAM, "memory", null) : m[1];
        ZmuxConfig ccfg = server ? otherCfg : shapedCfg, scfg = server ? shapedCfg : otherCfg;
        var ex = Executors.newVirtualThreadPerTaskExecutor();
        try {
            Future<ZmuxSession> sf = ex.submit(() -> Zmux.open(s, Role.SERVER, scfg));
            ZmuxSession client = Zmux.open(c, Role.CLIENT, ccfg);
            ZmuxSession srv = sf.get(5, TimeUnit.SECONDS);
            return server ? new Pair(srv, client, rec) : new Pair(client, srv, rec);
        } finally { ex.shutdown(); }
    }

    /** Writes n bytes on a new stream of from and checks that they arrive at to. */
    static void transfer(ZmuxSession from, ZmuxSession to, int n) throws Exception {
        byte[] data = new byte[n];
        Arrays.fill(data, (byte) 7);
        var ex = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var st = from.openStream();
            Future<?> w = ex.submit(() -> { st.write(data); st.finish(); return null; });
            try (var in = to.acceptStream(Duration.ofSeconds(10))) {
                assertNotNull(in);
                assertArrayEquals(data, in.readAll(1 << 24));
            }
            w.get(10, TimeUnit.SECONDS);
            st.close();
        } finally { ex.shutdown(); }
    }

    static ShaperFactory planShaper(Consumer<RecordPlan> f) {
        return pipe -> new Shaper() {
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) { f.accept(plan); }
            @Override public void onEvent(Event event, Reply reply) {}
        };
    }

    static int num(Map<String, Object> m, String k) {
        Object v = m == null ? null : m.get(k);
        return v == null ? 0 : ((Number) v).intValue();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> contract() throws Exception {
        return Json.object(Files.readString(Path.of("src", "test", "resources", "shaping", "contract.json")));
    }

    static void await(BooleanSupplierX c) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!c.get() && System.nanoTime() < until) Thread.sleep(5);
    }
    @FunctionalInterface interface BooleanSupplierX { boolean get() throws Exception; }

    @SuppressWarnings("unchecked")
    @TestFactory Collection<DynamicTest> contractRecords() throws Exception {
        List<DynamicTest> tests = new ArrayList<>();
        for (Object o : (List<Object>) contract().get("records")) {
            Map<String, Object> v = (Map<String, Object>) o;
            tests.add(DynamicTest.dynamicTest((String) v.get("name"), () -> {
                var limits = (Map<String, Object>) v.get("limits");
                var plan = (Map<String, Object>) v.get("plan");
                var e = (Map<String, Object>) v.get("expect");
                try (EventLoops loops = EventLoops.create(2)) {
                    var shaped = ZmuxConfig.builder().eventLoops(loops)
                            .shapingLimits(limits == null || !limits.containsKey("max_padding_bytes") ? ShapingLimits.none()
                                    : ShapingLimits.none().withMaxPaddingBytes(num(limits, "max_padding_bytes"))).build();
                    var other = ZmuxConfig.builder().eventLoops(loops).build();
                    try (var p = pair(shaped, other, "server".equals(v.get("side")))) {
                        p.shaped().setShaper(planShaper(pl -> pl.maxLength(num(plan, "max_len")).padTo(num(plan, "pad_to"))
                                .padMultiple(num(plan, "pad_multiple")).pad(num(plan, "pad"))));
                        p.rec().start();
                        for (int i = 0; i < Math.max(num(v, "repeat"), 1); i++) transfer(p.shaped(), p.other(), num(v, "send"));
                        var recs = p.rec().records();
                        assertFalse(recs.isEmpty());
                        assertTrue(recs.size() >= num(e, "min_records"), recs.size() + " records");
                        for (int[] r : recs) {
                            int n = r[0];
                            boolean ok = n >= num(e, "min_len") && (num(e, "max_len") == 0 || n <= num(e, "max_len"))
                                    && (num(e, "mod") == 0 || n % num(e, "mod") == num(e, "rem")) && r[1] >= num(e, "min_pad");
                            assertTrue(ok, "record of " + n + " bytes, " + r[1] + " trailing zero bytes");
                        }
                        int records = recs.size();
                        await(() -> statsOk(p.shaped().shapingStats(), e, records));
                        assertTrue(statsOk(p.shaped().shapingStats(), e, records), p.shaped().shapingStats().toString());
                    }
                }
            }));
        }
        return tests;
    }

    static boolean statsOk(ShapingStats st, Map<String, Object> e, int records) {
        return !(num(e, "max_padding_bytes") > 0 && st.paddingBytes() > num(e, "max_padding_bytes")
                || Boolean.TRUE.equals(e.get("clamped")) && st.clamped() == 0
                || Boolean.TRUE.equals(e.get("padding")) && st.paddingBytes() == 0
                || st.paddingBytes() < (long) records * num(e, "min_pad"));
    }

    /**
     * A record whose frames reach the target on their own takes no PADDING, and one whose target
     * falls within the last frame's Length field takes its PADDING before that frame: either way
     * every record hits its target exactly.
     */
    @Test void paddedRecordsHitTheirTargetsExactly() throws Exception {
        int[][] plans = {{100, 100, 0}, {200, 200, 0}, {1000, 1000, 0}, {1000, 200, 0}, {448, 0, 50}, {301, 0, 7}, {0, 0, 50}};
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            for (int[] pl : plans) {
                try (var p = pair(cfg, cfg, false)) {
                    p.shaped().setShaper(planShaper(r -> r.maxLength(pl[0]).padTo(pl[1]).padMultiple(pl[2])));
                    p.rec().start();
                    for (int n = 1; n < 700; n += 37) transfer(p.shaped(), p.other(), n);
                    transfer(p.shaped(), p.other(), 30_000);
                    var recs = p.rec().records();
                    assertTrue(recs.size() >= 20, recs.size() + " records");
                    for (int[] r : recs) {
                        String what = "plan " + Arrays.toString(pl) + ": record of " + r[0] + " bytes";
                        if (pl[1] > 0) assertTrue(r[0] >= pl[1], what);
                        if (pl[1] > 0 && pl[0] == pl[1]) assertEquals(pl[1], r[0], what);
                        if (pl[2] > 0) assertEquals(0, r[0] % pl[2], what);
                    }
                }
            }
        }
    }

    @Test void injectionTowardAZmuxPeerSendsPaddingOnlyRecords() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                var sent = new AtomicInteger();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Reply reply) {
                        if (event.kind() == EventKind.START || event.kind() == EventKind.INJECTED && sent.incrementAndGet() < 40) {
                            reply.inject(0, 100);
                        }
                    }
                });
                await(() -> p.shaped().shapingStats().injected() >= 39);
                assertTrue(p.shaped().shapingStats().injected() >= 39, p.shaped().shapingStats().toString());
                transfer(p.shaped(), p.other(), 1000);
            }
        }
    }

    @Test void delayedRecordsResumeAndPausesCount() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                p.shaped().setShaper(planShaper(pl -> pl.delayNanos(2_000_000)));
                long t0 = System.nanoTime();
                transfer(p.shaped(), p.other(), 3000);
                assertTrue(System.nanoTime() - t0 >= 2_000_000);
                await(() -> p.shaped().shapingStats().pauseTotal().toNanos() > 0);
                assertTrue(p.shaped().shapingStats().pauseTotal().toNanos() > 0);
            }
        }
    }

    /** Delays the first record of each pipe it shapes, or every record with all. */
    static ShaperFactory firstDelay(long delayNanos, boolean all) {
        return pipe -> new Shaper() {
            private long n;
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) {
                if (n++ == 0 || all) plan.delayNanos(delayNanos);
            }
            @Override public void onEvent(Event event, Reply reply) {}
        };
    }

    /**
     * A planned delay of the bootstrap pipe's first record, from the session's creation on both
     * ends, holds QX_TRANSPORT_PARAMETERS, which still lead the pipe once it ends (the delay is the
     * whole record's earliest send time, the first record included): both ends establish and
     * carry a stream.
     */
    @Test void aDelayedFirstRecordKeepsTheParametersFirst() throws Exception {
        for (boolean all : new boolean[] {false, true}) {
            try (EventLoops loops = EventLoops.create(2)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).shaper(firstDelay(20_000_000, all)).build();
                long t0 = System.nanoTime();
                try (var p = pair(cfg, cfg, false)) {
                    transfer(p.shaped(), p.other(), 3000);
                    transfer(p.other(), p.shaped(), 3000);
                    assertTrue(System.nanoTime() - t0 >= 20_000_000, "all=" + all);
                    assertTrue(p.shaped().error().isEmpty() && p.other().error().isEmpty(), "all=" + all);
                }
            }
        }
    }

    /**
     * A planned delay holds the whole record: control frames the shaped side owes for data it
     * receives wait with it instead of leaving early in a record of their own, which would use up
     * the plan and make the next record wait a second delay.
     */
    @Test void aDelayedRecordHoldsItsControlFramesToo() throws Exception {
        long delay = 5_000_000;
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                p.shaped().setShaper(planShaper(pl -> pl.maxLength(4000).delayNanos(delay)));
                p.rec().start();
                var ex = Executors.newVirtualThreadPerTaskExecutor();
                try {
                    Future<?> in = ex.submit(() -> { transfer(p.other(), p.shaped(), 2 << 20); return null; });
                    transfer(p.shaped(), p.other(), 60_000);
                    in.get(20, TimeUnit.SECONDS);
                } finally { ex.shutdown(); }
                List<Long> w;
                synchronized (p.rec().writes) { w = new ArrayList<>(p.rec().writes); }
                assertTrue(w.size() >= 15, w.size() + " writes");
                for (int i = 1; i < w.size(); i++) {
                    long gap = w.get(i) - w.get(i - 1);
                    assertTrue(gap >= delay - 1_000_000, "records " + gap / 1000 + " us apart, delay 5 ms");
                }
            }
        }
    }

    @Test void rateGateBoundsThroughput() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Reply reply) {
                        if (event.kind() == EventKind.START) reply.gate(0, 200_000, 4096);
                    }
                });
                long t0 = System.nanoTime();
                transfer(p.shaped(), p.other(), 60_000);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ms >= 200, "60000 bytes at 200000 B/s took " + ms + " ms");
            }
        }
    }

    @Test void timersFireAndReplacementNotifiesTheOldInstance() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                var events = new ConcurrentLinkedQueue<EventKind>();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Reply reply) {
                        events.add(event.kind());
                        if (event.kind() == EventKind.START) reply.timer(1, 1_000_000);
                    }
                });
                await(() -> events.contains(EventKind.TIMER));
                assertTrue(events.contains(EventKind.TIMER));
                var second = new ConcurrentLinkedQueue<EventKind>();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Reply reply) { second.add(event.kind()); }
                });
                await(() -> events.contains(EventKind.REPLACED) && second.contains(EventKind.START));
                assertTrue(events.contains(EventKind.REPLACED));
                assertTrue(second.contains(EventKind.START));
                assertTrue(p.shaped().setPipeShaper(0, null));
                assertFalse(p.shaped().setPipeShaper(99, null));
                transfer(p.shaped(), p.other(), 100);
            }
        }
    }

    @Test void aThrowingShaperEndsShapingOfItsPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                p.shaped().setShaper(planShaper(pl -> { throw new IllegalStateException("plan failure"); }));
                transfer(p.shaped(), p.other(), 5000);
                await(() -> p.shaped().shapingStats().faults() == 1);
                assertEquals(1, p.shaped().shapingStats().faults());
            }
        }
    }
}
