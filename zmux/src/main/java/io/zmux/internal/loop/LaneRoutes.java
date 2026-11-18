package io.zmux.internal.loop;

import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The sessions of the process by their Lane Key (MULTIPATH.md 5.8): the one table that routes a stream which names
 * its session only through that key, for a listener and for the process-wide lane entry alike.
 */
final class LaneRoutes {
    private static final ConcurrentHashMap<String, SessionRuntime> SESSIONS = new ConcurrentHashMap<>();

    private LaneRoutes() {}

    /** Routes the lanes naming driver's Lane Key to it until {@link #remove}; nothing for a session without lanes. */
    static void add(SessionRuntime driver) {
        byte[] key = driver.core().lanes().key();
        if (key != null) SESSIONS.putIfAbsent(HexFormat.of().formatHex(key), driver);
    }

    static void remove(SessionRuntime driver) {
        byte[] key = driver.core().lanes().key();
        if (key != null) SESSIONS.remove(HexFormat.of().formatHex(key), driver);
    }

    /** The session whose Lane Key is key, or null. */
    static SessionRuntime of(byte[] key) {
        return SESSIONS.get(HexFormat.of().formatHex(key)); // the session checks that it is live on its loop
    }
}
