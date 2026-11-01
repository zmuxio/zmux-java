package io.zmux;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Bounded, owned child process for explicitly selected interop tests. */
final class InteropProcess implements AutoCloseable {
    private static final String EOF = "\u0000";
    private final Process process;
    private final BlockingQueue<String> lines = new ArrayBlockingQueue<>(64);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread reader;
    private volatile boolean closing;
    static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank() || !Files.isExecutable(Path.of(value)))
            throw new IllegalStateException(name + " must name a built executable (no skipped interop cases)");
        return value;
    }
    InteropProcess(String executable, String... arguments) throws IOException {
        var command = new ArrayList<String>(); command.add(executable); command.addAll(List.of(arguments));
        process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        reader = Thread.ofVirtual().name("interop-output").start(() -> {
            try (var input = process.inputReader(StandardCharsets.UTF_8)) {
                var line = new StringBuilder();
                for (int ch; (ch = input.read()) != -1;) {
                    if (ch == '\n') {
                        if (!lines.offer(line.toString())) throw new IOException("peer output queue exceeded its bound");
                        line.setLength(0);
                    } else {
                        if (line.length() == 65536) throw new IOException("peer output line exceeded its bound");
                        line.append((char) ch);
                    }
                }
                if (!line.isEmpty() && !lines.offer(line.toString())) throw new IOException("peer output queue exceeded its bound");
            } catch (IOException error) { if (!closing) { failure.set(error); process.destroyForcibly(); } }
            finally { lines.offer(EOF); }
        });
    }
    String expect(String prefix) throws Exception {
        for (int skipped = 0; skipped < 2; skipped++) {
            String line = lines.poll(15, TimeUnit.SECONDS);
            if (failure.get() != null) throw new IOException("peer output failed", failure.get());
            if (line == null || line.equals(EOF)) throw new IOException("peer ended before " + prefix);
            if (prefix.stripTrailing().equals("CLOSED") && line.startsWith("RTT ")) continue;
            if (!line.startsWith(prefix)) throw new IOException("expected " + prefix + ", got " + line);
            return line.substring(prefix.length()).trim();
        }
        throw new IOException("no " + prefix);
    }
    void awaitExit() throws Exception {
        if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IOException("peer did not exit");
        if (process.exitValue() != 0) throw new IOException("peer exit code " + process.exitValue());
        reader.join(2000);
        if (reader.isAlive()) throw new IOException("peer output reader did not terminate");
        if (failure.get() != null) throw new IOException("peer output failed", failure.get());
    }
    @Override public void close() throws IOException {
        closing = true;
        boolean interrupted = Thread.interrupted();
        IOException error = null;
        process.destroy();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (process.isAlive() && System.nanoTime() < deadline) {
                try { process.waitFor(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (InterruptedException ignored) { interrupted = true; }
            }
            if (process.isAlive()) process.destroyForcibly();
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (process.isAlive() && System.nanoTime() < deadline) {
                try { process.waitFor(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (InterruptedException ignored) { interrupted = true; }
            }
            if (process.isAlive()) error = new IOException("peer did not terminate");
        } finally {
            try { process.getInputStream().close(); }
            catch (IOException closeError) { if (error == null) error = closeError; else error.addSuppressed(closeError); }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (reader.isAlive() && System.nanoTime() < deadline) {
                try { reader.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))); }
                catch (InterruptedException ignored) { interrupted = true; }
            }
            if (reader.isAlive()) {
                var readerError = new IOException("peer output reader did not terminate");
                if (error == null) error = readerError; else error.addSuppressed(readerError);
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        if (error != null) throw error;
    }
}
