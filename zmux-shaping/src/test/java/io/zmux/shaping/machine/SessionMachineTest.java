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
    @Test void aDefinitionShapesTheRecordsOfASession() throws Exception {
        var f = Machines.factory("{\"version\":1,\"machines\":[{\"states\":[{\"record\":{\"pad_to\":{\"const\":200}}}]}]}", Options.defaults());
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
            assertTrue(v >= 200, "record of " + v + " bytes");
            i += len + (int) v;
        }
    }
}
