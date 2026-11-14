package io.zmux.seal;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.zmux.internal.join.Frames;
import io.zmux.internal.join.Framing;
import io.zmux.testutil.Json;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The Establish units of the shared lengths, from the synced vectors or the directory of {@code ZMUX_SHAPING_VECTORS}. */
class EstablishLengthVectorsTest {
    private static int num(Map<String, Object> m, String k) { return ((Number) m.get(k)).intValue(); }

    @SuppressWarnings("unchecked")
    @Test void establishUnits() throws Exception {
        String external = System.getenv("ZMUX_SHAPING_VECTORS");
        Path p = Path.of(external != null ? external : "src/test/resources/spec/shaping", "pad.json");
        // Only a directory named from outside may be absent; the synced copy must be there.
        if (external != null) assumeTrue(Files.exists(p), "no shared cases at " + p);
        else assertTrue(Files.exists(p), "the synced shaping vectors are missing: " + p);
        int seen = 0;
        for (var c : (List<Map<String, Object>>) Json.object(Files.readString(p)).get("handshake_len")) {
            var u = (Map<String, Object>) c.get("unit");
            String message = (String) u.get("message");
            int slash = message.indexOf('/');
            String handshake = message.substring(0, slash);
            if (!handshake.equals(Profile.HANDSHAKE_ESTABLISH)) continue;
            Framing f = "stream".equals(u.get("framing")) ? Framing.STREAM : Framing.MESSAGE;
            int w = Frames.unitLength(f, num(u, "noise")) + num(u, "overhead");
            int seq = List.of("hello", "reply", "finish").indexOf(message.substring(slash + 1));
            // The shortest message, an empty payload: a Profile with a pre-shared key when the case names none.
            Profile profile = u.containsKey("profile") ? Profile.of(num(u, "profile")) : Profile.XXHFS_PSK0_CHACHA;
            assertEquals(num(u, "noise"), Handshake.minLength(profile, seq), c.toString());
            if (seq == 0) {
                assertEquals(u.get("has_handle"), profile.isPskBased(), c.toString());
                w += EstablishHello.headerLength(profile, new byte[profile.isPskBased() ? Handshake.HANDLE_LEN : 0]);
            }
            assertEquals(num(c, "wire"), w, c.toString());
            seen++;
        }
        assertTrue(seen > 0);
    }
}
