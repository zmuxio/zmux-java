package io.zmux.internal.core;

import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.opentest4j.AssertionFailedError;

/** Runs the dev specification's executable traces (TRACES.md) against the core. */
class TraceTest {
    /** Trace id prefixes the implementation runs so far. */
    static final List<String> ENABLED = List.of("core.", "ext.", "compat.", "findings.", "multipath.", "resume.");

    @TestFactory
    Stream<DynamicTest> traces() throws Exception {
        Path root = SpecFixtures.root().resolve("fixtures");
        Map<String, Object> profiles = Json.object(Files.readString(root.resolve("profiles.json")));
        List<Path> files;
        try (Stream<Path> w = Files.walk(root.resolve("traces"))) {
            files = w.filter(p -> p.toString().endsWith(".ndjson")).sorted().toList();
        }
        List<DynamicTest> tests = new ArrayList<>();
        for (Path f : files) {
            List<String> lines = Files.readAllLines(f).stream().filter(l -> !l.isBlank()).toList();
            Map<String, Object> h = Json.object(lines.getFirst());
            String id = (String) h.get("id");
            if (ENABLED.stream().noneMatch(id::startsWith)) continue;
            String only = System.getProperty("zmux.trace");
            if (only != null && !id.startsWith(only)) continue;
            tests.add(DynamicTest.dynamicTest(id, () -> run(h, profiles, lines)));
        }
        return tests.stream();
    }

    static void run(Map<String, Object> h, Map<String, Object> profiles, List<String> lines) {
        TraceRunner r = new TraceRunner(h, profiles);
        for (int i = 1; i < lines.size(); i++) {
            Map<String, Object> st = Json.object(lines.get(i));
            try {
                r.step(st);
            } catch (TraceRunner.Failure e) {
                throw new AssertionFailedError(h.get("id") + " step " + i + " (" + st.get("op") + "): " + e.getMessage() + "\n" + lines.get(i));
            }
        }
        if (r.s.error() != null && !r.closeCovered) {
            throw new AssertionFailedError("session closed without expect_close: " + r.s.error().message());
        }
    }
}
