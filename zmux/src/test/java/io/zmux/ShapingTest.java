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
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            if (on) {
                var batch = new ByteArrayOutputStream();
                for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); batch.writeBytes(a); }
                byte[] bytes = batch.toByteArray();
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
                    p.shaped().setShaper(planShaper(r -> r.maxLength(pl[0]).padTo(pl[1]).padMultiple(pl[2])),
                            () -> new ShapingBudget(null, new ProfileLimits(10, 0, 1 << 20, Duration.ZERO), true));
                    p.rec().startNow();
                    for (int n = 1; n < 700; n += 37) transfer(p.shaped(), p.other(), n);
                    transfer(p.shaped(), p.other(), 30_000);
                    var recs = p.rec().records();
                    assertTrue(recs.size() >= 20, recs.size() + " records");
                    for (int[] r : recs) {
                        int wire = VarInt.length(r[0]) + r[0];
                        String what = "plan " + Arrays.toString(pl) + ": record of " + wire + " bytes";
                        if (pl[1] > 0) assertTrue(wire >= pl[1], what);
                        if (pl[2] > 0) assertEquals(0, wire % pl[2], what);
                    }
                }
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

    @Test void aThrowingShaperEndsShapingOfItsPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var p = pair(cfg, cfg, false)) {
                p.shaped().setShaper(planShaper(pl -> { throw new IllegalStateException("plan failure"); }));
                transfer(p.shaped(), p.other(), 5000);
                p.shaped().shapingMeters(); // Synchronize with the session owner.
                assertEquals(1, p.shaped().shapingStats().faults());
            }
        }
    }
}
