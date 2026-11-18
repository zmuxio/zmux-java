package io.zmux.helper;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/** Application-side stream limiting through {@link Permit}s. No session lock or event-loop thread runs the limiter. */
public final class LimitedStreams {
    private LimitedStreams() {}

    /** Waits for a grant of 1 through max bytes; it is at most max, so it fits an int. */
    private static Permit allowance(Limiter limiter, int max) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("limiter on an event loop");
        var pending = Permit.acquire(limiter, 1, max);
        try {
            return pending.get();
        } catch (InterruptedException e) {
            // An allowance arriving after the cancellation goes back; one already granted is closed here.
            io.zmux.internal.spi.Spi.abandon(pending);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting for limiter");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof io.zmux.CallbackFailedException failed) throw failed;
            throw new io.zmux.CallbackFailedException("limiter failed", e.getCause());
        }
    }

    /** Takes an allowance before each read; what the read does not return goes back. */
    public static InputStream input(InputStream source, Limiter limiter) {
        Objects.requireNonNull(source); Objects.requireNonNull(limiter);
        return new FilterInputStream(source) {
            @Override public int read() throws IOException {
                try (Permit p = allowance(limiter, 1)) {
                    int b = in.read();
                    if (b >= 0) p.consume(1);
                    return b;
                }
            }
            @Override public int read(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                if (len == 0) return 0;
                try (Permit p = allowance(limiter, len)) {
                    int n = in.read(b, off, (int) p.size());
                    if (n > 0) p.consume(n);
                    return n;
                }
            }
            @Override public long transferTo(OutputStream target) throws IOException {
                Objects.requireNonNull(target);
                byte[] buffer = new byte[8192];
                long total = 0;
                for (int n; (n = read(buffer, 0, buffer.length)) >= 0;) { target.write(buffer, 0, n); total += n; }
                return total;
            }
            @Override public long skip(long n) throws IOException {
                if (n <= 0) return 0;
                try (Permit p = allowance(limiter, (int)Math.min(n, Integer.MAX_VALUE))) {
                    long consumed = in.skip(p.size());
                    if (consumed > 0) p.consume(consumed);
                    return consumed;
                }
            }
        };
    }

    /** Takes an allowance before each write; bytes not written (a partial write reported by InterruptedIOException) go back. */
    public static OutputStream output(OutputStream target, Limiter limiter) {
        Objects.requireNonNull(target); Objects.requireNonNull(limiter);
        return new FilterOutputStream(target) {
            @Override public void write(int b) throws IOException { write(new byte[] {(byte)b}, 0, 1); }
            @Override public void write(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                int completed = 0;
                while (len > 0) {
                    Permit p;
                    try { p = allowance(limiter, len); }
                    catch (InterruptedIOException e) { e.bytesTransferred = completed; throw e; }
                    try (p) {
                        int n = (int) p.size();
                        try { out.write(b, off, n); }
                        catch (InterruptedIOException e) {
                            p.consume(e.bytesTransferred); // the rest goes back
                            e.bytesTransferred += completed; throw e;
                        }
                        p.consume(n); off += n; len -= n; completed += n;
                    }
                }
            }
        };
    }
}
