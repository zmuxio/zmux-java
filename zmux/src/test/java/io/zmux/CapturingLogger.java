package io.zmux;

import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentLinkedQueue;

/** A {@link System.Logger} that keeps what it is given, for tests of what zmux logs. */
final class CapturingLogger implements System.Logger {
    record Entry(Level level, String message, Throwable thrown) {}

    final ConcurrentLinkedQueue<Entry> entries = new ConcurrentLinkedQueue<>();

    @Override public String getName() { return "test"; }
    @Override public boolean isLoggable(Level level) { return true; }
    @Override public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) { entries.add(new Entry(level, msg, thrown)); }
    @Override public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        entries.add(new Entry(level, params == null || params.length == 0 ? format : java.text.MessageFormat.format(format, params), null));
    }

    boolean has(String fragment) {
        for (Entry e : entries) if (e.message() != null && e.message().contains(fragment)) return true;
        return false;
    }
}
