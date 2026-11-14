package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import io.zmux.carrier.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SessionMachineTest {
    /** A timer armed again for the deadline last reported is no change: the core still has that deadline, and wakes the machine. */
    @Test void aTimerOfTheSameDeadlineIsNotReportedAndStillFires() throws Exception {
        String d = "{\"machines\":[{\"start\":0,\"timers\":2,\"states\":[{\"enter\":[{\"op\":\"timer\",\"slot\":0,\"after\":1000}],\"next\":[{\"on\":\"timer\",\"slot\":0,\"to\":1}]},"
                + "{\"enter\":[{\"op\":\"timer\",\"slot\":1,\"after\":0}],\"next\":[{\"on\":\"timer\",\"slot\":1,\"to\":2}]},{}]}]}";
        var m = new Machines.Compiled(Definition.parse(d), Sampler.seeded(), MachineOptions.defaults()).newShaper();
        var e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.START, io.zmux.PipeId.BOOTSTRAP, 0, 0, 0, false, 0), e);
        assertEquals(1_000_000, e.wakeAtNanos());
        e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.WAKE, io.zmux.PipeId.BOOTSTRAP, 0, 1_000_000, 0, false, 0), e);
        assertEquals(1, m.state(0));
        assertFalse(e.isWakeSet());
        e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.WAKE, io.zmux.PipeId.BOOTSTRAP, 0, 1_000_000, 0, false, 0), e);
        assertEquals(2, m.state(0));
        assertTrue(e.isWakeSet() && e.isWakeNone());
    }

    @Test void aDefinitionShapesTheRecordsOfASession() throws Exception {
        var f = Machines.factory("{\"machines\":[{\"start\":0,\"states\":[{\"record\":{\"pad_to\":200}}]}]}", MachineOptions.defaults());
        var out = new ByteArrayOutputStream();
        var on = new java.util.concurrent.atomic.AtomicBoolean();
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var m = MemoryPipe.pair(Framing.BYTE_STREAM);
            PipeWriter inner = m[0].writer();
            PipeWriter rec = new PipeWriter() {
                @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
                    if (on.get()) synchronized (out) { for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); out.writeBytes(a); } }
                    inner.write(data, done);
                }
                @Override public void close() { inner.close(); }
                @Override public void abort(Throwable cause) { inner.abort(cause); }
            };
            Future<ZmuxSession> sf = ex.submit(() -> Zmux.open(m[1], Role.SERVER, cfg));
            try (var c = Zmux.open(new PipeIo(m[0].reader(), rec, Framing.BYTE_STREAM, "memory", null), Role.CLIENT, cfg);
                 var s = sf.get(5, TimeUnit.SECONDS)) {
                c.setShaper(f);
                Thread.sleep(50);
                on.set(true);
                try (var st = c.openStream()) {
                    st.write(new byte[10]);
                    st.finish();
                    try (var in = s.acceptStream(Duration.ofSeconds(5))) { assertEquals(10, in.readAll(1 << 20).length); }
                }
            }
        }
        byte[] b;
        synchronized (out) { b = out.toByteArray(); }
        assertTrue(b.length > 0);
        for (int i = 0; i < b.length;) {
            int first = b[i] & 0xff, len = 1 << (first >> 6);
            long v = first & 0x3f;
            for (int k = 1; k < len; k++) v = v << 8 | (b[i + k] & 0xff);
            assertTrue(len + v >= 200, "record of " + (len + v) + " bytes");
            i += len + (int) v;
        }
    }
}
