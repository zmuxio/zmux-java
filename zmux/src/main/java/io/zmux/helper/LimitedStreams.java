package io.zmux.helper;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/** Application-side stream limiting. No session lock or event-loop thread runs the limiter. */
public final class LimitedStreams {
    private LimitedStreams() {}

    private static int allowance(Limiter limiter, int max) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("limiter on an event loop");
        var pending = limiter.ready(1, max).toCompletableFuture();
        try {
            int n = pending.get();
            if (n <= 0 || n > max) throw new IOException("invalid limiter allowance");
            return n;
        } catch (InterruptedException e) {
            pending.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting for limiter");
        } catch (ExecutionException e) { throw new IOException("limiter failed", e.getCause()); }
    }

    /** Charges bytes returned to the application; readiness is awaited before reading. */
    public static InputStream input(InputStream source, Limiter limiter) {
        Objects.requireNonNull(source); Objects.requireNonNull(limiter);
        return new FilterInputStream(source) {
            @Override public int read() throws IOException {
                allowance(limiter, 1);
                int b = in.read();
                if (b >= 0) limiter.charge(1);
                return b;
            }
            @Override public int read(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                if (len == 0) return 0;
                int n = in.read(b, off, allowance(limiter, len));
                if (n > 0) limiter.charge(n);
                return n;
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
                int allowed = allowance(limiter, (int)Math.min(n, Integer.MAX_VALUE));
                long consumed = in.skip(allowed);
                if (consumed > 0) limiter.charge((int)consumed);
                return consumed;
            }
        };
    }

    /** Charges completed writes, including partial writes reported by InterruptedIOException. */
    public static OutputStream output(OutputStream target, Limiter limiter) {
        Objects.requireNonNull(target); Objects.requireNonNull(limiter);
        return new FilterOutputStream(target) {
            @Override public void write(int b) throws IOException { write(new byte[] {(byte)b}, 0, 1); }
            @Override public void write(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                int completed = 0;
                while (len > 0) {
                    int n;
                    try { n = allowance(limiter, len); }
                    catch (InterruptedIOException e) { e.bytesTransferred = completed; throw e; }
                    try { out.write(b, off, n); }
                    catch (InterruptedIOException e) {
                        if (e.bytesTransferred > 0) limiter.charge(e.bytesTransferred);
                        e.bytesTransferred += completed; throw e;
                    }
                    limiter.charge(n); off += n; len -= n; completed += n;
                }
            }
        };
    }
}
