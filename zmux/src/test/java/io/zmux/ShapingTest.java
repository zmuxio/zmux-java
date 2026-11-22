package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
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
        /** Per write: whether it carries a PIPE_ACK (written together with the stamp, same index). */
        final List<Boolean> withAck = Collections.synchronizedList(new ArrayList<>());
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            if (on) {
                writes.add(System.nanoTime());
                var batch = new ByteArrayOutputStream();
                for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); batch.writeBytes(a); }
                byte[] bytes = batch.toByteArray();
                withAck.add(carriesPipeAck(bytes));
                synchronized (out) { out.writeBytes(bytes); }
            }
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }
        /** Whether a record of the BYTE_STREAM bytes carries a PIPE_ACK frame. */
        static boolean carriesPipeAck(byte[] b) {
            return carriesPipeAck(b, entry -> true);
        }

        static boolean carriesPipeAck(byte[] b, java.util.function.Predicate<io.zmux.internal.wire.Frame.AckEntries> matches) {
            var buf = ByteBuffer.wrap(b);
            var dec = new io.zmux.internal.wire.FrameDecoder();
            var f = new io.zmux.internal.wire.Frame();
            try {
                for (int i = 0; i < b.length;) {
                    int len = 1 << (b[i] >> 6 & 3);
                    long v = b[i] & 0x3f;
                    for (int k = 1; k < len; k++) v = v << 8 | (b[i + k] & 0xff);
                    int pos = i + len, end = (int) (pos + v);
                    while (pos < end) {
                        int n = dec.decode(buf, pos, end, f);
                        if (f.type == io.zmux.internal.wire.FrameType.PIPE_ACK) {
                            var entries = f.entries(new io.zmux.internal.wire.Frame.AckEntries());
                            while (entries.next()) if (matches.test(entries)) return true;
                        }
                        pos += n;
                    }
                    i = end;
                }
            } catch (io.zmux.internal.wire.WireException | RuntimeException e) {
                throw new AssertionError("record does not parse", e);
            }
            return false;
        }
        /** Records from now on, keeping what was recorded. */
        void startNow() { on = true; }
        void start() throws InterruptedException {
            Thread.sleep(50);
            synchronized (out) { out.reset(); }
            writes.clear();
            withAck.clear();
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
            @Override public void onEvent(Event event, Effects effects) {}
        };
    }

    static void await(BooleanSupplierX c) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!c.get() && System.nanoTime() < until) Thread.sleep(5);
    }
    @FunctionalInterface interface BooleanSupplierX { boolean get() throws Exception; }

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
                        int wire = VarInt.length(r[0]) + r[0];
                        String what = "plan " + Arrays.toString(pl) + ": record of " + wire + " bytes";
                        if (pl[1] > 0) assertTrue(wire >= pl[1], what);
                        if (pl[1] > 0 && pl[0] == pl[1]) assertEquals(pl[1], wire, what);
                        if (pl[2] > 0) assertEquals(0, wire % pl[2], what);
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
                    @Override public void onEvent(Event event, Effects effects) {
                        if (event.kind() == EventKind.START || (event.kind() == EventKind.INJECTED && sent.incrementAndGet() < 40)) {
                            effects.inject(event.nowNanos(), 100);
                        } else if (event.kind() == EventKind.INJECTED) {
                            effects.noInject();
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
                await(() -> p.shaped().shapingStats().stallSpent().toNanos() > 0);
                assertTrue(p.shaped().shapingStats().stallSpent().toNanos() > 0);
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
            @Override public void onEvent(Event event, Effects effects) {}
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
     * the plan and make the next record wait a second delay. The one bound a delay never passes is
     * the deadline of a PIPE_ACK the record carries (zmux_max_ack_delay after the first byte it
     * reports): a record released short of its delay by that deadline carries the PIPE_ACK, any
     * other record keeps the whole delay after the one before it.
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
                List<Long> w; List<Boolean> acked;
                synchronized (p.rec().writes) { w = new ArrayList<>(p.rec().writes); }
                synchronized (p.rec().withAck) { acked = new ArrayList<>(p.rec().withAck); }
                while (acked.size() > w.size()) acked.remove(acked.size() - 1);
                while (w.size() > acked.size()) w.remove(w.size() - 1);
                assertTrue(w.size() >= 15, w.size() + " writes");
                for (int i = 1; i < w.size(); i++) {
                    long gap = w.get(i) - w.get(i - 1);
                    assertTrue(gap >= delay - 1_000_000 || acked.get(i), "record " + i + " without a PIPE_ACK " + gap / 1000 + " us after the one before, delay 5 ms");
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
                    @Override public void onEvent(Event event, Effects effects) {
                        if (event.kind() == EventKind.START) effects.gate(0, 200_000, 4096);
                    }
                });
                long t0 = System.nanoTime();
                transfer(p.shaped(), p.other(), 60_000);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ms >= 200, "60000 bytes at 200000 B/s took " + ms + " ms");
            }
        }
    }

    /** What an answer leaves alone stays: the session goes on with the old value until an answer clears it. */
    @Test void anInjectionAndAWakeUpStayUntilAnAnswerChangesThem() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                var injected = new AtomicInteger();
                var wakes = new AtomicInteger();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Effects effects) {
                        switch (event.kind()) {
                            case START -> effects.inject(event.nowNanos(), 100).wake(event.nowNanos());
                            case INJECTED -> { if (injected.incrementAndGet() >= 3) effects.noInject(); }
                            case WAKE -> { if (wakes.incrementAndGet() >= 3) effects.noWake(); }
                            default -> { }
                        }
                    }
                });
                await(() -> injected.get() >= 3 && wakes.get() >= 3);
                assertEquals(3, Math.min(injected.get(), 3), "the slot was used up by its first injection");
                assertEquals(3, Math.min(wakes.get(), 3), "the wake-up was used up by its first event");
                Thread.sleep(100);
                assertEquals(3, injected.get(), "cleared by the answer");
                assertEquals(3, wakes.get(), "cleared by the answer");
                transfer(p.shaped(), p.other(), 1000);
            }
        }
    }

    @Test void wakeUpsComeAndReplacementNotifiesTheOldInstance() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                var events = new ConcurrentLinkedQueue<EventKind>();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Effects effects) {
                        events.add(event.kind());
                        if (event.kind() == EventKind.START) effects.wake(event.nowNanos() + 1_000_000);
                    }
                });
                await(() -> events.contains(EventKind.WAKE));
                assertTrue(events.contains(EventKind.WAKE));
                var second = new ConcurrentLinkedQueue<EventKind>();
                p.shaped().setShaper(pipe -> new Shaper() {
                    @Override public Set<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
                    @Override public void plan(RecordView view, RecordPlan plan) {}
                    @Override public void onEvent(Event event, Effects effects) { second.add(event.kind()); }
                });
                await(() -> events.contains(EventKind.REPLACED) && second.contains(EventKind.START));
                assertTrue(events.contains(EventKind.REPLACED));
                assertTrue(second.contains(EventKind.START));
                p.shaped().pipes().getFirst().setShaper(null);
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
