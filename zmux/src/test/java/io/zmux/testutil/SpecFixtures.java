package io.zmux.testutil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Access to the vendored dev-spec fixtures (src/test/resources/spec). */
public final class SpecFixtures {
    private SpecFixtures() {}

    public static Path root() {
        String p = System.getProperty("zmux.spec.dir");
        return p != null ? Path.of(p) : Path.of("src", "test", "resources", "spec");
    }

    /** Reads an NDJSON file under fixtures/. */
    public static List<Map<String, Object>> ndjson(String relative) throws IOException {
        return Files.readAllLines(root().resolve("fixtures").resolve(relative)).stream()
                .filter(l -> !l.isBlank()).map(Json::object).toList();
    }

    public static byte[] hex(Object s) {
        return HexFormat.of().parseHex((String) s);
    }
}
