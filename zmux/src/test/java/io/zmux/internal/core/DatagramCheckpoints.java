package io.zmux.internal.core;

import io.zmux.internal.wire.CorpusTest;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

import static io.zmux.internal.core.TraceFrames.list;
import static io.zmux.internal.core.TraceFrames.map;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class DatagramCheckpoints {
    private DatagramCheckpoints() {}

    static Stream<DynamicTest> tests(String name) throws Exception {
        var root = SpecFixtures.root();
        var index = TraceTest.traceIndex(root);
        Map<String, Object> profiles = Json.object(Files.readString(root.resolve("fixtures/profiles.json")));
        var checkpoint = map(map(index.get("datagram_checkpoints")).get(name));
        var traceFiles = TraceTest.indexedTraces(root, index);
        var vectors = new java.util.HashMap<String, Map<String, Object>>();
        for (String file : java.util.List.of("valid.ndjson", "invalid.ndjson")) {
            for (var vector : SpecFixtures.ndjson("wire/" + file)) vectors.put((String) vector.get("id"), vector);
        }
        return Stream.concat(list(checkpoint.get("traces")).stream().map(String.class::cast).map(id ->
                DynamicTest.dynamicTest(id, () -> {
                    var path = traceFiles.get(id);
                    assertNotNull(path, id);
                    var lines = TraceTest.traceLines(path);
                    TraceTest.run(Json.object(lines.getFirst()), profiles, lines);
                })), list(checkpoint.get("wire")).stream().map(String.class::cast).map(id ->
                DynamicTest.dynamicTest(id, () -> {
                    var vector = vectors.get(id);
                    assertNotNull(vector, id);
                    CorpusTest.verify(vector);
                })));
    }
}
