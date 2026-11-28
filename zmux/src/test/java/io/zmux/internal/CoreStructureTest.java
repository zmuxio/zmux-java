package io.zmux.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards the core's structural rules: it depends on nothing above it, takes no locks, touches no channels,
 * and every mutable field is private to the class that writes it.
 */
class CoreStructureTest {
    private static final Path CORE = Path.of("src/main/java/io/zmux/internal/core");
    private static final Path CORE_CLASSES = Path.of("target/classes/io/zmux/internal/core");

    /**
     * Mutable fields that may stay visible inside the package, each with the reason. Everything else that
     * changes after construction is private: other classes read it through accessors and change it through
     * the owner's methods. A private field set by its own class's transition method (a monotonic flag such as
     * {@code Pipe.InDir.end()}, {@code Pipe.OutDir.fail()}, {@code Pipe.attach()/restart()}, or the
     * {@code Stream.replayable} setter) needs no entry.
     */
    private static final Map<String, String> VISIBLE_MUTABLE = Map.of(
            "Waiter.queued", "intrusive: set and cleared by Outbox when it queues and delivers the waiter",
            "Waiter.prev", "intrusive link: written only by WaitList (O(1) withdrawal)",
            "Waiter.next", "intrusive link: written only by WaitList (O(1) withdrawal)",
            "Waiter.list", "intrusive membership: written only by WaitList");
    // Immutable protocol observations and send options have one representation shared with the API.
    private static final java.util.Set<String> CORE_VALUES = java.util.Set.of(
            "CapacityLimits", "CapacitySnapshot", "Directions", "MemberId", "DatagramSendOptions", "DatagramSendOptions.Carriage", "Direction", "DirectionHealth",
            "DirectionHealth.ProofKind", "DirectionHealth.Reason", "DirectionHealth.State", "JoinTimeoutException", "PipeHealth",
            "PipeHealthChange", "PipeId");

    // Existing qualified dependencies outside the shared protocol value imports.
    private static final java.util.Set<String> QUALIFIED_CORE_VALUES = java.util.Set.of(
            "CarrierKind", "DatagramCopyPolicy", "Liveness", "PipeGoneException", "PipeState", "PipeStats",
            "SessionTarget", "StreamStats", "ZmuxConfig");
    private static final Pattern PUBLIC_TYPE = Pattern.compile("\\bio\\.zmux\\.([A-Z][A-Za-z0-9_]*(?:\\.[A-Z][A-Za-z0-9_]*)*)");
    private static final Pattern FORBIDDEN = Pattern.compile(
            "\\bio\\.zmux\\.internal\\.(loop|runtime|host|transport|listener)\\b"
                    + "|java\\.nio\\.channels|java\\.util\\.concurrent\\.locks|\\bsynchronized\\b|\\bnew Thread\\b");

    private static boolean forbiddenDependency(String line) {
        if (FORBIDDEN.matcher(line).find()) return true;
        return PUBLIC_TYPE.matcher(line).results().anyMatch(match -> {
            String type = match.group(1);
            if (line.startsWith("import ")) return !CORE_VALUES.contains(type);
            int nested = type.indexOf('.');
            String owner = nested < 0 ? type : type.substring(0, nested);
            return !CORE_VALUES.contains(owner) && !QUALIFIED_CORE_VALUES.contains(owner);
        });
    }

    @Test
    void rejectsFullyQualifiedUpwardDependencies() {
        assertTrue(forbiddenDependency("return new io.zmux.DatagramPathFeedback("));
        assertTrue(forbiddenDependency("io.zmux.internal.runtime.Owner owner;"));
    }

    @Test
    void coreHasNoUpwardDependencyLockOrChannel() throws IOException {
        List<String> bad = new ArrayList<>();
        try (Stream<Path> files = Files.list(CORE)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                List<String> lines = Files.readAllLines(f);
                for (int i = 0; i < lines.size(); i++) {
                    String l = lines.get(i).strip();
                    if (l.startsWith("*") || l.startsWith("//") || l.startsWith("/*")) continue;
                    if (forbiddenDependency(l)) bad.add(f.getFileName() + ":" + (i + 1) + ": " + l);
                }
            }
        }
        assertEquals(List.of(), bad);
    }

    @Test
    void mutableFieldsArePrivateToTheirWriter() throws Exception {
        Map<String, String> visible = new TreeMap<>();
        try (Stream<Path> files = Files.list(CORE_CLASSES)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".class")).sorted().toList()) {
                String name = f.getFileName().toString().replace(".class", "");
                Class<?> c = Class.forName("io.zmux.internal.core." + name, false, getClass().getClassLoader());
                if (c.isSynthetic() || c.isAnonymousClass()) continue;
                for (Field fd : c.getDeclaredFields()) {
                    int m = fd.getModifiers();
                    if (fd.isSynthetic() || Modifier.isStatic(m) || Modifier.isFinal(m) || Modifier.isPrivate(m)) continue;
                    visible.put(name.replace('$', '.') + "." + fd.getName(), fd.getType().getSimpleName());
                }
            }
        }
        assertEquals(new TreeMap<>(VISIBLE_MUTABLE).keySet(), visible.keySet(),
                "non-private mutable fields must be listed with a reason (and listed ones must still exist)");
    }
}
