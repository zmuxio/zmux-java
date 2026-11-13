package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** What a short or hostile configuration text can cost its reader. */
@Timeout(60)
class ReaderBoundsTest {
    private static String machine(long counters, long timers) {
        return "{\"start\":0,\"counters\":" + counters + ",\"timers\":" + timers + ",\"states\":[{}]}";
    }

    private static byte[] bundle(String... machines) {
        return ("{\"kinds\":{\"default\":[{\"machines\":[" + String.join(",", machines) + "]}]}}").getBytes(StandardCharsets.UTF_8);
    }

    private static DefinitionException.Category failed(byte[] b, MachineOptions o) {
        return assertThrows(DefinitionException.class, () -> Bundle.parse(b, o)).category();
    }

    @Test void aHugeIntegerLiteralCostsLinearTimeAndIsOutOfRange() throws Exception {
        for (String sign : new String[] {"", "-"}) {
            Object o = DefinitionJson.parse(sign + "9".repeat(4_000_000));
            assertEquals(sign.isEmpty() ? Long.MAX_VALUE : Long.MIN_VALUE, DefinitionJson.integer(o, "x"));
        }
        assertEquals(new java.math.BigInteger("18446744073709551615"), DefinitionJson.parse("18446744073709551615"));
        assertEquals(Long.MAX_VALUE, DefinitionJson.integer(DefinitionJson.parse("1" + "0".repeat(20)), "x"));
        assertThrows(DefinitionException.class, () -> Definition.parse("{\"machines\":[{\"start\":" + "9".repeat(800_000) + ",\"states\":[{}]}]}"));
    }

    @Test void aUnicodeEscapeIsFourHexadecimalDigits() throws Exception {
        assertEquals("A\u00ff\uABCD", DefinitionJson.parse("\"\\u0041\\u00ff\\uABCD\""));
        for (String bad : new String[] {"+123", "-012", "12g4", "\u0661\u0662\u0663\u0664", "\uff11234"}) {
            var e = assertThrows(DefinitionException.class, () -> DefinitionJson.parse("\"\\u" + bad + "\""), bad);
            assertEquals(DefinitionException.Category.SYNTAX, e.category(), bad);
        }
    }

    /** The handshake hook and the pool policy take their own locks, so they draw from sources of their own. */
    @Test void theHandshakeHookAndThePoolPolicyDrawFromSourcesOfTheirOwn() throws Exception {
        var overlap = new java.util.concurrent.atomic.AtomicBoolean();
        var o = MachineOptions.defaults().withNewSource(() -> new java.util.random.RandomGenerator() {
            final java.util.concurrent.atomic.AtomicBoolean busy = new java.util.concurrent.atomic.AtomicBoolean();
            long x;
            @Override public long nextLong() {
                if (!busy.compareAndSet(false, true)) overlap.set(true);
                Thread.onSpinWait();
                long v = ++x * 0x9E3779B97F4A7C15L;
                busy.set(false);
                return v;
            }
        });
        String each = "{\"t\":\"a\"}";
        String candidate = "{\"handshakes\":{\"join\":{\"hello\":{\"pad\":" + each + "}}},"
                + "\"pool\":{\"count\":1,\"retire\":[{\"metric\":\"age\",\"above\":" + each + "}]}}";
        Bundle b = Bundle.parse(("{\"tables\":{\"a\":[[1,1,1000]]},\"kinds\":{\"default\":[" + candidate + "]}}").getBytes(StandardCharsets.UTF_8), o);
        var k = b.kind("default");
        assertNotNull(k.pool);
        var view = java.util.List.of(new MachinePoolPolicy.Metrics(1, 5, 0, 1, 0, 0));
        Thread t = Thread.ofPlatform().start(() -> {
            for (int i = 0; i < 300_000 && !overlap.get(); i++) k.plan("join", "hello");
        });
        for (int i = 0; i < 300_000 && !overlap.get(); i++) k.pool.decide(0, null, view);
        t.join();
        assertFalse(overlap.get(), "one source under two locks");
    }

    @Test void slotsOfAllMachinesAreBoundedBeforeAnyPipeAllocatesThem() throws Exception {
        MachineOptions o = MachineOptions.defaults();
        Bundle.parse(bundle(machine(4000, 96)), o);
        assertEquals(DefinitionException.Category.LIMIT, failed(bundle(machine(4000, 97)), o));
        assertEquals(DefinitionException.Category.LIMIT, failed(bundle(machine(4096, 0), machine(0, 1)), o));
        // Two thousand million counters per pipe, in a few bytes.
        assertEquals(DefinitionException.Category.LIMIT, failed(bundle(machine(2_000_000_000, 2_000_000_000)), o));
    }

    @Test void theBoundOfSlotsIsConfigured() throws Exception {
        MachineOptions less = MachineOptions.defaults().withLimits(SizeLimits.NONE.withMaxSlots(2));
        Bundle.parse(bundle(machine(1, 1)), less);
        assertEquals(DefinitionException.Category.LIMIT, failed(bundle(machine(2, 1)), less));
        Bundle.parse(bundle(machine(5000, 0)), MachineOptions.defaults().withLimits(SizeLimits.NONE.withMaxSlots(5000)));
        Bundle.parse(bundle(machine(10000, 10000)), MachineOptions.defaults().withLimits(SizeLimits.NONE.withMaxSlots(-1)));
        assertEquals(DefinitionException.Category.LIMIT, assertThrows(DefinitionException.class,
                () -> Definition.parse("{\"machines\":[" + machine(4097, 0) + "]}")).category());
        assertEquals(DefinitionException.Category.LIMIT, assertThrows(DefinitionException.class,
                () -> Machines.factory("{\"machines\":[" + machine(3, 0) + "]}", less)).category());
    }

    /** A copy per reference would take about 2.4 GB here. */
    @Test void aTableIsReadOnceForAllItsReferences() throws Exception {
        StringBuilder table = new StringBuilder("[");
        for (int i = 0; i < 10_000; i++) table.append(i == 0 ? "" : ",").append("[1,").append(i).append(',').append(i).append(']');
        table.append(']');
        StringBuilder enter = new StringBuilder();
        for (int i = 0; i < 10_000; i++) enter.append(i == 0 ? "" : ",").append("{\"op\":\"pause\",\"after\":{\"t\":\"a\"}}");
        String json = "{\"tables\":{\"a\":" + table + "},\"machines\":[{\"start\":0,\"states\":[{\"enter\":[" + enter + "]}]}]}";
        Definition d = Definition.parse(json);
        var first = d.machines.getFirst().states().getFirst().enter();
        assertSame(first.get(0).after().segments, first.get(9999).after().segments);
        assertNotSame(first.get(0).after(), first.get(1).after(), "each reference is a value of its own");
    }

    @Test void aTableOutOfTheRangeOfAReferenceIsRefusedAtEachUse() {
        String json = "{\"tables\":{\"a\":[[1,0,2000000]]},\"machines\":[{\"start\":0,\"states\":[{\"enter\":[{\"op\":\"pause\",\"after\":{\"t\":\"a\"}}],"
                + "\"next\":[{\"on\":\"enter\",\"to\":0,\"prob\":{\"t\":\"a\"}}]}]}]}";
        assertEquals(DefinitionException.Category.RANGE, assertThrows(DefinitionException.class, () -> Definition.parse(json)).category());
    }

    @Test void maxBytesCountsTheTables() throws Exception {
        String pad = " ".repeat(500);
        byte[] b = ("{\"tables\":{\"a\":[[1,1,1]" + pad + "]},\"kinds\":{\"default\":[{}]}}").getBytes(StandardCharsets.UTF_8);
        assertEquals(DefinitionException.Category.LIMIT, failed(b, MachineOptions.defaults().withLimits(SizeLimits.NONE.withMaxBytes(100))));
        Bundle.parse(b, MachineOptions.defaults().withLimits(SizeLimits.NONE.withMaxBytes(b.length)));
    }

    /** Quadratic checks take minutes at this size. */
    @Test void theStructureCheckIsLinear() throws Exception {
        int n = 200_000;
        StringBuilder s = new StringBuilder("{\"machines\":[{\"start\":0,\"states\":[");
        for (int i = 0; i < n; i++) {
            s.append(i == 0 ? "" : ",");
            if (i + 1 < n) s.append("{\"next\":[{\"on\":\"record_out\",\"to\":").append(i + 1).append("}]}");
            else s.append("{\"next\":[{\"on\":\"record_out\",\"to\":0}]}");
        }
        s.append("]}]}");
        assertEquals(n, Definition.parse(s.toString()).machines.getFirst().states().size());
        String cycle = "{\"machines\":[{\"start\":0,\"states\":[{\"next\":[{\"on\":\"enter\",\"to\":1}]},{\"next\":[{\"on\":\"zero\",\"slot\":0,\"to\":2}]},{\"next\":[{\"on\":\"enter\",\"to\":1}]}],\"counters\":1}]}";
        assertEquals(DefinitionException.Category.CYCLE, assertThrows(DefinitionException.class, () -> Definition.parse(cycle)).category());
    }

    @Test void deepNestingIsASyntaxError() {
        for (String open : new String[] {"[", "{\"a\":"}) {
            String deep = open.repeat(1_000_000);
            assertEquals(DefinitionException.Category.SYNTAX, assertThrows(DefinitionException.class, () -> Definition.parse(deep)).category());
        }
    }
}
