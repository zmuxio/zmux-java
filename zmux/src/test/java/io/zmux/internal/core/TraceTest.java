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

/**
 * Runs the specification's executable traces against the core: the public traces (fixtures/traces)
 * and, when the repository has them, the traces of this implementation's local sender policy
 * (src/test/resources/local/spec-traces, absent from a published tree).
 */
class TraceTest {
    /** The number of public traces: none is left out. */
    static final int TRACE_COUNT = 381;

    @TestFactory
    Stream<DynamicTest> traces() throws Exception {
        Path root = SpecFixtures.root().resolve("fixtures");
        Map<String, Object> profiles = Json.object(Files.readString(root.resolve("profiles.json")));
        List<Path> files = new ArrayList<>(traceFiles(root.resolve("traces"), TRACE_COUNT));
        Path local = Path.of("src", "test", "resources", "local", "spec-traces");
        if (Files.isDirectory(local)) files.addAll(traceFiles(local, -1));
        List<DynamicTest> tests = new ArrayList<>();
        for (Path f : files) {
            List<String> lines = Files.readAllLines(f).stream().filter(l -> !l.isBlank()).toList();
            Map<String, Object> h = Json.object(lines.getFirst());
            String id = (String) h.get("id");
            String only = System.getProperty("zmux.trace");
            if (only != null && !id.startsWith(only)) continue;
            tests.add(DynamicTest.dynamicTest(id, () -> run(h, profiles, lines)));
        }
        return tests.stream();
    }

    /** The traces below dir; want, when not negative, is how many there must be. */
    private static List<Path> traceFiles(Path dir, int want) throws Exception {
        List<Path> files;
        try (Stream<Path> w = Files.walk(dir)) {
            files = w.filter(p -> p.toString().endsWith(".ndjson")).sorted().toList();
        }
        if (want >= 0 && files.size() != want) {
            throw new AssertionFailedError(dir + ": " + files.size() + " traces, want " + want);
        }
        return files;
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
