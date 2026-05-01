package io.zmux.internal;

import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;

import java.io.IOException;

final class StreamWriteCompletion {
    private int remainingFrames;
    private boolean done;
    private IOException error;
    private boolean transportProgress;
    private long signalVersion;
    private Runnable completionListener;

    private static void runCompletionListener(Runnable listener) {
        if (listener != null) {
            listener.run();
        }
    }

    private static long deadlineNanos(long waitNanos) {
        if (waitNanos <= 0L) {
            return 0L;
        }
        long now = System.nanoTime();
        return now > Long.MAX_VALUE - waitNanos ? Long.MAX_VALUE : now + waitNanos;
    }

    private static long remainingNanos(long deadlineNanos) {
        long now = System.nanoTime();
        long remaining = deadlineNanos - now;
        if (remaining <= 0L && deadlineNanos > now) {
            return Long.MAX_VALUE;
        }
        return remaining;
    }

    synchronized void retainFrame() {
        if (done) {
            return;
        }
        remainingFrames++;
    }

    synchronized boolean hasFrames() {
        return remainingFrames > 0 || done;
    }

    void completeFrameWritten() {
        Runnable listener = null;
        synchronized (this) {
            if (done) {
                return;
            }
            transportProgress = true;
            if (remainingFrames > 0) {
                remainingFrames--;
            }
            if (remainingFrames == 0) {
                done = true;
                signalVersion++;
                notifyAll();
                listener = completionListener;
                completionListener = null;
            }
        }
        runCompletionListener(listener);
    }

    void completeFailure(IOException failure) {
        Runnable listener;
        synchronized (this) {
            if (done) {
                return;
            }
            error = failure;
            done = true;
            signalVersion++;
            notifyAll();
            listener = completionListener;
            completionListener = null;
        }
        runCompletionListener(listener);
    }

    synchronized boolean done() {
        return done;
    }

    synchronized void throwIfFailed() throws IOException {
        throwIfFailedLocked();
    }

    synchronized boolean awaitSignal(long waitNanos) throws IOException {
        if (done) {
            throwIfFailedLocked();
            return true;
        }
        long observedSignalVersion = signalVersion;
        long deadlineNanos = deadlineNanos(waitNanos);
        try {
            while (!done && signalVersion == observedSignalVersion) {
                if (waitNanos <= 0L) {
                    wait();
                } else {
                    long remainingNanos = remainingNanos(deadlineNanos);
                    if (remainingNanos <= 0L) {
                        return false;
                    }
                    long millis = remainingNanos / 1_000_000L;
                    int nanos = (int) (remainingNanos % 1_000_000L);
                    wait(millis, nanos);
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw SessionRuntime.interruptedIo(
                    "zmux: interrupted while waiting for stream write",
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.WRITE,
                    interruptedException
            );
        }
        if (done) {
            throwIfFailedLocked();
            return true;
        }
        return true;
    }

    synchronized void notifyWaiters() {
        signalVersion++;
        notifyAll();
    }

    synchronized boolean canCancelQueuedFrames(int queuedFrameCount) {
        return !done && !transportProgress && remainingFrames > 0 && queuedFrameCount == remainingFrames;
    }

    boolean completeFailureIfPending(IOException failure) {
        Runnable listener;
        synchronized (this) {
            if (done) {
                return false;
            }
            error = failure;
            done = true;
            signalVersion++;
            notifyAll();
            listener = completionListener;
            completionListener = null;
        }
        runCompletionListener(listener);
        return true;
    }

    void onComplete(Runnable listener) {
        if (listener == null) {
            return;
        }
        boolean runNow;
        synchronized (this) {
            if (done) {
                runNow = true;
            } else {
                completionListener = listener;
                runNow = false;
            }
        }
        if (runNow) {
            listener.run();
        }
    }

    private void throwIfFailedLocked() throws IOException {
        if (error != null) {
            throw error;
        }
    }
}
