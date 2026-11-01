package io.zmux.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Guards the bridge between application threads and the session's loop: the objects shared
 * across threads take no locks, and every field that is neither final nor volatile has one
 * named owning context, so a new shared field cannot appear without a decision.
 */
class BridgeStructureTest {
    private static final Path LOOP = Path.of("src/main/java/io/zmux/internal/loop");
    /** The classes application threads and the loop both touch. */
    private static final List<String> SHARED = List.of("Operation", "Mailbox", "Turn", "SendLane", "RecvLane");
    private static final Pattern FORBIDDEN = Pattern.compile(
            "\\bsynchronized\\b|java\\.util\\.concurrent\\.locks\\.(?!LockSupport)|\\bReentrantLock\\b|\\.wait\\(|\\.notify");

    /** Plain mutable fields of the shared classes and the single context that reads and writes each. */
    private static final Map<String, String> OWNED = Map.ofEntries(
            Map.entry("Operation.outcome", "written before the state CAS that publishes it; read after observing it"),
            Map.entry("Operation.registered", "loop"),
            Map.entry("Operation.timer", "loop"),
            Map.entry("Operation.prevLive", "loop (the session's live list)"),
            Map.entry("Operation.nextLive", "loop (the session's live list)"),
            Map.entry("Operation.live", "loop (the session's live list)"),
            Map.entry("SendLane.tail", "turn holder (ring producer)"),
            Map.entry("SendLane.block", "turn holder (ring producer)"),
            Map.entry("SendLane.blockBytes", "turn holder (ring producer)"),
            Map.entry("SendLane.blockPos", "turn holder (ring producer)"),
            Map.entry("SendLane.finished", "turn holder (ring producer)"),
            Map.entry("SendLane.head", "loop (ring consumer)"),
            Map.entry("SendLane.headPos", "loop (ring consumer)"),
            Map.entry("SendLane.leaseListed", "loop"),
            Map.entry("SendLane.activeRound", "loop"),
            Map.entry("SendLane.endWatcher", "loop"),
            Map.entry("RecvLane.endWatcher", "loop"),
            Map.entry("SendLane.hostOwned", "loop"),
            Map.entry("SendLane.used", "loop"),
            Map.entry("SendLane.nextLeased", "loop (the session's lease list)"),
            Map.entry("RecvLane.head", "turn holder (ring consumer)"),
            Map.entry("RecvLane.unreported", "turn holder (ring consumer)"),
            Map.entry("RecvLane.tail", "loop (ring producer)"),
            Map.entry("RecvLane.ended", "loop (ring producer)"),
            Map.entry("RecvLane.endReported", "loop"),
            Map.entry("RecvLane.hostReader", "loop"),
            Map.entry("RecvLane$Piece.off", "turn holder (ring consumer); published with the piece's next link"));

    @Test
    void sharedBridgeObjectsTakeNoLocks() throws IOException {
        List<String> bad = new ArrayList<>();
        for (String c : SHARED) {
            List<String> lines = Files.readAllLines(LOOP.resolve(c + ".java"));
            for (int i = 0; i < lines.size(); i++) {
                String l = lines.get(i).strip();
                if (l.startsWith("*") || l.startsWith("//") || l.startsWith("/*")) continue;
                if (FORBIDDEN.matcher(l).find()) bad.add(c + ":" + (i + 1) + ": " + l);
            }
        }
        assertEquals(List.of(), bad);
    }

    @Test
    void everyPlainMutableFieldHasOneOwner() throws Exception {
        Map<String, String> found = new TreeMap<>();
        for (String c : SHARED) collect(Class.forName("io.zmux.internal.loop." + c), found);
        assertEquals(new TreeMap<>(OWNED).keySet(), found.keySet(),
                "a plain mutable field of a shared bridge class needs a named owner (or must be final or volatile)");
    }

    private static void collect(Class<?> c, Map<String, String> found) {
        for (Field f : c.getDeclaredFields()) {
            int m = f.getModifiers();
            if (Modifier.isStatic(m) || Modifier.isFinal(m) || Modifier.isVolatile(m) || f.isSynthetic()) continue;
            found.put(c.getName().substring(c.getPackageName().length() + 1) + "." + f.getName(), "");
        }
        for (Class<?> n : c.getDeclaredClasses()) collect(n, found);
    }
}
