package io.zmux.http;

import io.zmux.ZmuxSession;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A diagnostic dump for a stalled exchange: every thread's stack (virtual threads too), what
 * a test adds (a proxy's counters), and per session its statistics and the carrier and stream
 * state reachable from it, read reflectively. Only for a test that already failed or stalled:
 * it reads fields off their owners' threads, so the values are a best-effort snapshot.
 */
final class Stall {
    private Stall() {}

    /** The classes whose instances are reported, by name: the carrier, its input, the core streams. */
    private static final Set<String> REPORTED = Set.of("io.zmux.http.BodyCarrier", "io.zmux.internal.spi.LentInput",
            "io.zmux.internal.core.Stream", "io.zmux.internal.core.Session");
    private static final int MAX_OBJECTS = 200_000;

    static void dump(String why, Supplier<String> extra, Map<String, ZmuxSession> sessions) {
        var b = new StringBuilder(1 << 16);
        b.append("\n===== STALL DUMP: ").append(why).append(" =====\n");
        if (extra != null) {
            try { b.append(extra.get()).append('\n'); } catch (RuntimeException e) { b.append("extra failed: ").append(e).append('\n'); }
        }
        for (var e : sessions.entrySet()) {
            b.append("--- session ").append(e.getKey()).append(" @").append(Integer.toHexString(System.identityHashCode(e.getValue()))).append('\n');
            try { b.append("stats: ").append(e.getValue().stats()).append('\n'); } catch (RuntimeException x) { b.append("stats failed: ").append(x).append('\n'); }
            try { walk(e.getValue(), b); } catch (RuntimeException x) { b.append("walk failed: ").append(x).append('\n'); }
        }
        b.append("--- threads\n").append(threads());
        b.append("===== END STALL DUMP =====\n");
        PrintStream err = System.err;
        err.print(b);
        err.flush();
    }

    /** All threads, virtual ones included, as the JVM's thread dump has them. */
    static String threads() {
        try {
            Path f = Files.createTempFile("stall-threads", ".txt");
            Files.delete(f);
            // By reflection: the tests compile as the module, which reads no management module;
            // they run on the class path, which sees it.
            Class<?> type = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            Class<?> format = Class.forName("com.sun.management.HotSpotDiagnosticMXBean$ThreadDumpFormat");
            Object bean = Class.forName("java.lang.management.ManagementFactory").getMethod("getPlatformMXBean", Class.class).invoke(null, type);
            type.getMethod("dumpThreads", String.class, format).invoke(bean, f.toString(), format.getField("TEXT_PLAIN").get(null));
            String s = Files.readString(f);
            Files.deleteIfExists(f);
            return s;
        } catch (Exception | LinkageError e) {
            var b = new StringBuilder("dumpThreads failed (").append(e).append("); platform threads only:\n");
            for (var t : Thread.getAllStackTraces().entrySet()) b.append(stack(t.getKey(), t.getValue()));
            return b.toString();
        }
    }

    static String stack(Thread t) { return stack(t, t.getStackTrace()); }

    private static String stack(Thread t, StackTraceElement[] st) {
        var b = new StringBuilder().append('"').append(t.getName()).append("\" ").append(t).append(' ').append(t.getState()).append('\n');
        for (var e : st) b.append("    at ").append(e).append('\n');
        return b.toString();
    }

    /** Reports every instance of a {@link #REPORTED} class reachable from root, with its plain fields. */
    private static void walk(Object root, StringBuilder b) {
        var seen = new IdentityHashMap<Object, Boolean>();
        var todo = new ArrayDeque<Object>();
        todo.add(root);
        seen.put(root, true);
        while (!todo.isEmpty() && seen.size() < MAX_OBJECTS) {
            Object o = todo.poll();
            Class<?> c = o.getClass();
            if (REPORTED.contains(c.getName())) {
                b.append(describe(o));
                if (c.getName().equals("io.zmux.internal.core.Stream")) b.append(streamParts(o));
            }
            if (o instanceof io.netty.channel.Channel ch) {
                b.append("channel ").append(ch).append(" active=").append(ch.isActive()).append(" open=").append(ch.isOpen())
                        .append(" autoRead=").append(ch.config().isAutoRead()).append(" writable=").append(ch.isWritable())
                        .append(" loop=").append(ch.eventLoop()).append('\n');
                continue; // not into Netty's pipeline
            }
            for (Object n : refs(o)) {
                if (n != null && !seen.containsKey(n) && followed(n.getClass())) {
                    seen.put(n, true);
                    todo.add(n);
                }
            }
        }
    }

    private static boolean followed(Class<?> c) {
        if (c.isArray()) return !c.getComponentType().isPrimitive();
        String n = c.getName();
        return n.startsWith("io.zmux.") || n.startsWith("java.util.") || n.startsWith("io.netty.channel.");
    }

    private static List<Object> refs(Object o) {
        var r = new ArrayList<Object>();
        if (o instanceof Object[] a) {
            for (Object x : a) r.add(x);
            return r;
        }
        if (o.getClass().getName().startsWith("java.util.")) {
            try {
                if (o instanceof Collection<?> c) r.addAll(List.copyOf(c.stream().filter(java.util.Objects::nonNull).toList()));
                else if (o instanceof Map<?, ?> m) for (var e : List.copyOf(m.entrySet())) { r.add(e.getKey()); r.add(e.getValue()); }
            } catch (RuntimeException ignored) {
                // Changed while copied: skipped.
            }
            return r;
        }
        for (Class<?> c = o.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()) continue;
                try { f.setAccessible(true); r.add(f.get(o)); } catch (RuntimeException | IllegalAccessException ignored) { }
            }
        }
        return r;
    }

    /** A core stream's statistics and its send, receive and table parts (states, offsets, pending reads). */
    private static String streamParts(Object stream) {
        var b = new StringBuilder();
        try {
            b.append("    stats=").append(stream.getClass().getMethod("stats").invoke(stream)).append('\n');
        } catch (ReflectiveOperationException | RuntimeException e) {
            b.append("    stats failed: ").append(e).append('\n');
        }
        for (Field f : stream.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()) continue;
            try {
                f.setAccessible(true);
                Object v = f.get(stream);
                if (v != null && v.getClass().getName().startsWith("io.zmux.internal.core.")) b.append("    ").append(f.getName()).append(": ").append(describe(v));
            } catch (RuntimeException | IllegalAccessException ignored) {
                // Not readable: left out.
            }
        }
        return b.toString();
    }

    /** The instance with its primitive, string and enum fields, and its collections' sizes. */
    private static String describe(Object o) {
        var b = new StringBuilder(o.getClass().getSimpleName()).append('@').append(Integer.toHexString(System.identityHashCode(o))).append(" {");
        for (Class<?> c = o.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                try {
                    f.setAccessible(true);
                    Object v = f.get(o);
                    if (f.getType().isPrimitive() || v instanceof String || v instanceof Enum<?> || v instanceof Throwable) {
                        b.append(' ').append(f.getName()).append('=').append(v);
                    } else if (v instanceof Collection<?> col) {
                        b.append(' ').append(f.getName()).append(".size=").append(col.size());
                    } else if (v instanceof io.netty.channel.Channel ch) {
                        b.append(' ').append(f.getName()).append('=').append(ch.id().asShortText());
                    }
                } catch (RuntimeException | IllegalAccessException ignored) {
                    // Not readable: left out.
                }
            }
        }
        return b.append(" }\n").toString();
    }
}
