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
    private static final Pattern FORBIDDEN = Pattern.compile(
            "import io\\.zmux\\.internal\\.(loop|runtime|host|transport|listener)\\b|import io\\.zmux\\.[A-Z]"
                    + "|java\\.nio\\.channels|java\\.util\\.concurrent\\.locks|\\bsynchronized\\b|\\bnew Thread\\b");

    @Test
    void coreHasNoUpwardDependencyLockOrChannel() throws IOException {
        List<String> bad = new ArrayList<>();
        try (Stream<Path> files = Files.list(CORE)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                List<String> lines = Files.readAllLines(f);
                for (int i = 0; i < lines.size(); i++) {
                    String l = lines.get(i).strip();
                    if (l.startsWith("*") || l.startsWith("//") || l.startsWith("/*")) continue;
                    if (FORBIDDEN.matcher(l).find()) bad.add(f.getFileName() + ":" + (i + 1) + ": " + l);
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
