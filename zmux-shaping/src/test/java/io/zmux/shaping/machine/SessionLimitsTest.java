package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.shaping.ShapingLimits;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class SessionLimitsTest {
    static final Path VECTORS = Path.of("..", "zmux", "src", "test", "resources", "shaping", "session_limits.json");

    @SuppressWarnings("unchecked")
    @TestFactory Stream<DynamicTest> vectors() throws Exception {
        var root = (Map<String, Object>) DefinitionJson.parse(Files.readString(VECTORS));
        return ((List<Object>) root.get("cases")).stream().map(x -> (Map<String, Object>) x).map(c ->
                DynamicTest.dynamicTest((String) c.get("name"), () -> {
                    String json = c.get("raw") != null ? (String) c.get("raw") : MachinesTest.json(c.get("limits"));
                    byte[] b = json.getBytes(StandardCharsets.UTF_8);
                    if (!MachinesTest.wantOk(c)) {
                        MachinesTest.assertError(c, assertThrows(DefinitionException.class, () -> Machines.sessionLimits(b, Options.defaults())));
                        return;
                    }
                    ShapingLimits l = Machines.sessionLimits(b, Options.defaults());
                    var w = (Map<String, Object>) c.get("want");
                    if (w == null) return;
                    assertEquals(opt(w.get("max_padding_bytes")), l.maxPaddingBytes().isPresent() ? (double) l.maxPaddingBytes().getAsLong() : null);
                    assertEquals(opt(w.get("max_injected")), l.maxInjected().isPresent() ? (double) l.maxInjected().getAsLong() : null);
                    assertEquals(opt(w.get("max_padding_ratio")), l.maxPaddingRatio().isPresent() ? l.maxPaddingRatio().getAsDouble() : null);
                    assertEquals(opt(w.get("max_pause_total")), l.maxPauseTotal().map(SessionLimitsTest::seconds).orElse(null));
                    assertEquals(opt(w.get("max_delay")), l.maxDelay().map(SessionLimitsTest::seconds).orElse(null));
                }));
    }

    private static Double opt(Object x) { return x == null ? null : ((Number) x).doubleValue(); }

    private static Double seconds(Duration d) { return d.toNanos() / 1e9; }
}
