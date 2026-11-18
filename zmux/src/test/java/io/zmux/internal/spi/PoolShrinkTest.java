package io.zmux.internal.spi;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.testutil.Json;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The pool shrink order against the vectors shared across implementations. */
@Timeout(30)
class PoolShrinkTest {
    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    @Test void sharedVectors() throws Exception {
        var v = Json.object(Files.readString(Path.of("src", "test", "resources", "pool-shrink", "vectors.json")));
        var cases = (List<Map<String, Object>>) v.get("cases");
        assertFalse(cases.isEmpty());
        for (var c : cases) {
            String name = (String) c.get("name");
            var decision = (Map<String, Object>) c.get("decision");
            var named = new ArrayList<Long>((List<Long>) decision.get("retire"));
            var members = new ArrayList<PoolShrink.Candidate<Long>>();
            for (var m : (List<Map<String, Object>>) c.get("members")) {
                long key = (Long) m.get("key");
                members.add(new PoolShrink.Candidate<>(key, key, named.contains(key), (Long) m.get("idle"), ((Long) m.get("pipes")).intValue(), (Long) m.get("age")));
            }
            int dialing = ((Long) c.get("dialing")).intValue();
            var plan = PoolShrink.plan(members, dialing, ((Long) decision.get("count")).intValue());
            var expect = (Map<String, Object>) c.get("expect");
            var cancelled = new ArrayList<Long>();
            for (int i = 0; i < plan.cancelDials(); i++) cancelled.add((long) (dialing - 1 - i));
            assertEquals(expect.get("cancel_dials"), cancelled, name + ": cancelled dials");
            assertEquals(expect.get("close"), plan.close(), name + ": closed members");
        }
    }
}
