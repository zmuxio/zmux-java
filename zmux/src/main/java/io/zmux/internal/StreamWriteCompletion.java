package io.zmux.internal;

import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

final class StreamWriteCompletion {
    private int remainingFrames;
    private boolean done;
    private IOException error;
    private boolean transportProgress;
    private Runnable completionListener;

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
        try {
            if (waitNanos == 0L) {
                wait();
            } else {
                long millis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                int nanos = (int) (waitNanos - TimeUnit.MILLISECONDS.toNanos(millis));
                wait(millis, nanos);
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
        return false;
    }

    synchronized void notifyWaiters() {
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

    private static void runCompletionListener(Runnable listener) {
        if (listener != null) {
            listener.run();
        }
    }

    private void throwIfFailedLocked() throws IOException {
        if (error != null) {
            throw error;
        }
    }
}
