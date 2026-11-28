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
    @Test void aTimerCanRearmTheSameDeadlineAfterConsumption() throws Exception {
        String d = "{\"machines\":[{\"start\":0,\"timers\":2,\"states\":[{\"enter\":[{\"op\":\"timer\",\"slot\":0,\"after\":1000}],\"next\":[{\"on\":\"timer\",\"slot\":0,\"to\":1}]},"
                + "{\"enter\":[{\"op\":\"timer\",\"slot\":1,\"after\":0}],\"next\":[{\"on\":\"timer\",\"slot\":1,\"to\":2}]},{}]}]}";
        var m = new Machines.Compiled(Definition.parse(d), Sampler.seeded(), MachineOptions.defaults()).newShaper();
        var e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.START, io.zmux.PipeId.BOOTSTRAP, 0, 0, 0, 0, io.zmux.shaping.ShapingMeters.ZERO), e);
        assertEquals(1_000_000, e.wakeAtNanos());
        e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.TIMER, io.zmux.PipeId.BOOTSTRAP, 0, 1_000_000, 0, 0, io.zmux.shaping.ShapingMeters.ZERO), e);
        assertEquals(1, m.state(0));
        assertTrue(e.isWakeSet());
        assertEquals(1_000_000, e.wakeAtNanos());
        e = new io.zmux.shaping.Effects();
        m.onEvent(new io.zmux.shaping.Event(io.zmux.shaping.EventKind.TIMER, io.zmux.PipeId.BOOTSTRAP, 0, 1_000_000, 0, 0, io.zmux.shaping.ShapingMeters.ZERO), e);
        assertEquals(2, m.state(0));
        assertTrue(e.isWakeSet() && e.isWakeNone());
    }

    @Test void budgetsRejectOldSchemaAndUnsafeSampleCombinations() {
        var options = MachineOptions.defaults();
        var legacy = "{\"schema\":1,\"kinds\":{\"default\":[{}]}}";
        assertEquals(DefinitionException.Category.SCHEMA_VERSION,
                assertThrows(DefinitionException.class,
                        () -> Bundle.parse(legacy.getBytes(java.nio.charset.StandardCharsets.UTF_8), options)).category());
        String bundle = """
                {"schema":2,"kinds":{"default":[{"budget":{"wire":{
                  "ceiling":{"t":[[1,5,20]],"scope":"session"},
                  "initial":{"t":[[1,1,10]],"scope":"session"}
                }}}]}}
                """;
        assertEquals(DefinitionException.Category.RANGE, assertThrows(DefinitionException.class,
                () -> Bundle.parse(bundle.getBytes(java.nio.charset.StandardCharsets.UTF_8), options)).category());
        assertThrows(DefinitionException.class,
                () -> Definition.parse("{\"machines\":[{\"start\":0,\"states\":[{\"next\":[{\"on\":\"record_in\",\"to\":0}]}]}]}"));
    }

    @Test void rejectingAnInfeasibleRatePreservesTheInstalledGeneration() {
        var cfg = io.zmux.internal.core.CoreConfig.builder();
        cfg.local().present &= ~io.zmux.internal.wire.TransportParams.PREAMBLE;
        var session = new io.zmux.internal.core.Session(cfg.build(), 0);
        var gate = Machines.factory("""
                {"machines":[{"start":0,"states":[{"enter":[{"op":"rate","rate":32000,"burst":64}]}]}]}
                """, MachineOptions.defaults());
        var policy = io.zmux.shaping.ShapingBudget.NONE;
        session.setShaper(gate, policy);
        long previous = session.shapingMeters().generation();
        for (int micros : new int[]{0, 1999}) {
            var limits = new io.zmux.shaping.ProfileLimits(10, 50_000, 540, Duration.ofNanos(micros * 1000L));
            assertThrows(IllegalArgumentException.class, () ->
                    session.setShaper(gate, new io.zmux.shaping.ShapingBudget(null, limits, false)));
            assertEquals(previous, session.shapingMeters().generation());
            assertEquals(policy.profile(), session.shapingLimitsInForce().policy());
        }
    }

    @Test void datagramPlanningHasItsOwnStateTransition() {
        var definition = Definition.parse("""
                {"include_control":true,"machines":[{"start":0,"states":[
                  {"record":{"pad":7},"datagram":{"pad":3},"next":[{"on":"datagram_plan","to":1}]},
                  {"record":{"pad":9},"datagram":{"pad":5},"next":[{"on":"outbound","to":1}]}
                ]}]}
                """);
        var shaper = new Machines.Compiled(definition, Sampler.seeded(), MachineOptions.defaults()).newShaper();
        assertTrue(shaper.capabilities().contains(io.zmux.shaping.Capability.INCLUDE_CONTROL));
        var record = new io.zmux.shaping.RecordPlan();
        var datagram = new io.zmux.shaping.UnitPlan();
        var meters = io.zmux.shaping.ShapingMeters.ZERO;
        var pipe = PipeId.BOOTSTRAP;
        var recordView = new io.zmux.shaping.RecordView(pipe, 0, 0, 100, true, 0, meters);
        shaper.plan(recordView, record);
        assertEquals(7, record.pad());
        shaper.planDatagram(new io.zmux.shaping.DatagramView(pipe, 0, 5, 100, 100, true, false, meters),
                datagram, new io.zmux.shaping.Effects());
        assertEquals(3, datagram.pad());
        record.clear();
        shaper.plan(recordView, record);
        assertEquals(9, record.pad());
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
                c.setShaper(pipe -> {
                    var shaper = f.create(pipe);
                    on.set(true);
                    return shaper;
                }, () -> new io.zmux.shaping.ShapingBudget(null, new io.zmux.shaping.ProfileLimits(
                        10, 0, 8_192, Duration.ZERO), true));
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
