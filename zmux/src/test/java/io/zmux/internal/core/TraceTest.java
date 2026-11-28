package io.zmux.internal.core;

import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.opentest4j.AssertionFailedError;

/** Runs the specification's public executable traces against the wire core. */
class TraceTest {
    @TestFactory
    Stream<DynamicTest> traces() throws Exception {
        Path root = SpecFixtures.root();
        var files = indexedTraces(root, traceIndex(root));
        return tracesIn(files.values());
    }

    static Map<String, Object> traceIndex(Path root) throws Exception {
        return Json.object(Files.readString(root.resolve("fixtures/traces/index.json")));
    }

    static Map<String, Path> indexedTraces(Path root, Map<String, Object> index) throws Exception {
        Path dir = root.resolve("fixtures/traces").normalize();
        var files = new LinkedHashMap<String, Path>();
        var remaining = new HashSet<>(traceFiles(dir));
        for (var item : TraceFrames.list(index.get("traces"))) {
            var entry = TraceFrames.map(item);
            String id = (String) entry.get("id");
            Path file = root.resolve((String) entry.get("file")).normalize();
            if (!remaining.remove(file)) throw new AssertionFailedError("missing or repeated trace file " + file);
            if (files.putIfAbsent(id, file) != null) throw new AssertionFailedError("repeated trace ID " + id);
            try (var lines = Files.lines(file)) {
                var header = Json.object(lines.filter(line -> !line.isBlank()).findFirst().orElseThrow());
                if (!id.equals(header.get("id"))) throw new AssertionFailedError("trace ID does not match the index: " + file);
            }
        }
        if (!remaining.isEmpty()) throw new AssertionFailedError("unindexed trace files: " + remaining);
        return files;
    }

    static Stream<DynamicTest> tracesIn(Collection<Path> files) throws Exception {
        Path profilesPath = SpecFixtures.root().resolve("fixtures/profiles.json");
        Map<String, Object> profiles = Json.object(Files.readString(profilesPath));
        List<DynamicTest> tests = new ArrayList<>();
        String only = System.getProperty("zmux.trace");
        for (Path file : files) {
            List<String> lines = traceLines(file);
            Map<String, Object> h = Json.object(lines.getFirst());
            String id = (String) h.get("id");
            if (only != null && !id.startsWith(only)) continue;
            tests.add(DynamicTest.dynamicTest(id, () -> run(h, profiles, lines)));
        }
        return tests.stream();
    }

    static List<String> traceLines(Path file) throws java.io.IOException {
        return Files.readAllLines(file).stream().filter(line -> !line.isBlank()).toList();
    }

    @TestFactory
    Stream<DynamicTest> firstMessageResponses() throws Exception {
        Path root = SpecFixtures.root();
        var vectors = Json.object(Files.readString(root.resolve("secure-carrier/vectors/first_message_cases.json")));
        var profiles = Json.object(Files.readString(root.resolve("fixtures/profiles.json")));
        var files = indexedTraces(root, traceIndex(root));
        var tests = new ArrayList<DynamicTest>();
        for (Object value : (List<?>) vectors.get("responses")) {
            Map<?, ?> row = (Map<?, ?>) value;
            if (!"trace".equals(row.get("expect"))) continue;
            String id = (String) row.get("fixture");
            Path file = files.get(id);
            org.junit.jupiter.api.Assertions.assertNotNull(file, id);
            var lines = traceLines(file);
            tests.add(DynamicTest.dynamicTest((String) row.get("name"), () -> run(Json.object(lines.getFirst()), profiles, lines)));
        }
        org.junit.jupiter.api.Assertions.assertEquals(4, tests.size());
        return tests.stream();
    }

    static List<Path> traceFiles(Path dir) throws java.io.IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(path -> path.toString().endsWith(".ndjson")).sorted().toList();
        }
    }

    static void run(Map<String, Object> h, Map<String, Object> profiles, List<String> lines) {
        org.junit.jupiter.api.Assumptions.assumeFalse("compat.qmux.remembered_early_data".equals(h.get("id")),
                "The Java carriers do not expose a resumption context for plain QMux 0-RTT");
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
