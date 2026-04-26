package io.zmux.internal;

import io.zmux.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("resource")
final class SessionEstablishmentCoordinator {
    private static final int WRITER_CARRIER_WAITING = 0;
    private static final int WRITER_CARRIER_SELECTED = 1;
    private static final int WRITER_CARRIER_EXPIRED = 2;
    private static final int WRITER_CARRIER_ABORTED = 3;
    private static final String PREFACE_WRITE_STALLED = "local preface write stalled during establishment";

    private final Owner owner;
    private final Duration failureWriteWait;
    private final Duration successWriteWait;
    private final Duration closeDrainDelay;

    SessionEstablishmentCoordinator(Owner owner,
                                    Duration failureWriteWait,
                                    Duration successWriteWait,
                                    Duration closeDrainDelay) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.failureWriteWait = failureWriteWait;
        this.successWriteWait = successWriteWait;
        this.closeDrainDelay = closeDrainDelay;
    }

    private static IOException transportFailure(String operation, String message, IOException error) {
        if (ZmuxErrors.details(error) != null) {
            return error;
        }
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                operation,
                message,
                error,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private static Thread newDaemonThread(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    private static long closeCode(IOException error) {
        return ZmuxErrors.code(error, ErrorCode.INTERNAL.code());
    }

    private static String closeReason(IOException error) {
        return ZmuxErrors.reason(error);
    }

    private static boolean awaitLatch(CountDownLatch latch, Duration duration) throws InterruptedException {
        if (latch == null) {
            return true;
        }
        TimeoutBudget budget = TimeoutBudget.fromTimeout(duration);
        if (!budget.bounded()) {
            latch.await();
            return true;
        }
        long timeoutNanos = budget.remainingNanos();
        if (timeoutNanos <= 0L) {
            return latch.getCount() == 0L;
        }
        return latch.await(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private static void sleepDuration(Duration duration) throws InterruptedException {
        long sleepNanos = SessionEstablishmentCoordinator.durationToPositiveNanosSaturated(duration);
        if (sleepNanos <= 0L) {
            return;
        }
        TimeUnit.NANOSECONDS.sleep(sleepNanos);
    }

    private static long durationToPositiveNanosSaturated(Duration duration) {
        return SessionRuntime.durationToPositiveNanosSaturated(duration);
    }

    private static Instant deadlineAfter(Duration duration) {
        if (SessionEstablishmentCoordinator.durationToPositiveNanosSaturated(duration) <= 0L) {
            return null;
        }
        try {
            return Instant.now().plus(duration);
        } catch (ArithmeticException overflow) {
            return Instant.MAX;
        }
    }

    void establish() throws IOException {
        CountDownLatch prefaceWriteDone = new CountDownLatch(1);
        CountDownLatch writerLoopDecision = new CountDownLatch(1);
        AtomicBoolean runWriterLoop = new AtomicBoolean();
        AtomicInteger writerCarrierState = new AtomicInteger(WRITER_CARRIER_WAITING);
        AtomicReference<IOException> prefaceWriteError = new AtomicReference<>();
        EstablishmentWriteDeadline writeDeadline = this.beginEstablishmentWriteDeadline(this.successWriteWait);
        Thread writerThread = SessionEstablishmentCoordinator.newDaemonThread("zmux-writer", () -> {
            try {
                FrameCodec.writePreface(this.owner.output(), this.owner.localPreface());
                this.owner.output().flush();
            } catch (IOException error) {
                prefaceWriteError.set(error);
            } finally {
                prefaceWriteDone.countDown();
            }
            try {
                if (!SessionEstablishmentCoordinator.awaitLatch(writerLoopDecision, this.successWriteWait)) {
                    if (writerCarrierState.compareAndSet(WRITER_CARRIER_WAITING, WRITER_CARRIER_EXPIRED)) {
                        return;
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            if (runWriterLoop.get()) {
                this.owner.writerLoopTask().run();
            }
        });
        writerThread.start();

        Preface remotePreface = null;
        boolean established = false;
        try {
            remotePreface = this.readPeerPreface();
            this.awaitPrefaceWrite(prefaceWriteDone, prefaceWriteError, this.successWriteWait, writeDeadline, true);
            IOException clearDeadlineError = writeDeadline.clear();
            if (clearDeadlineError != null) {
                throw SessionEstablishmentCoordinator.transportFailure(
                        "clear write deadline",
                        "zmux: transport write deadline clear failed",
                        clearDeadlineError
                );
            }
            Negotiated negotiated = FrameCodec.negotiate(this.owner.localPreface(), remotePreface);
            synchronized (this.owner.lock()) {
                this.owner.markReadyLocked(remotePreface, negotiated, System.nanoTime());
                this.owner.notifyLockWaiters();
            }
            established = true;
            runWriterLoop.set(true);
            if (writerCarrierState.compareAndSet(WRITER_CARRIER_WAITING, WRITER_CARRIER_SELECTED)) {
                writerLoopDecision.countDown();
            } else {
                writerLoopDecision.countDown();
                SessionEstablishmentCoordinator.newDaemonThread("zmux-writer", this.owner.writerLoopTask()).start();
            }
        } catch (IOException error) {
            this.finishEstablishmentFailure(prefaceWriteDone, prefaceWriteError, writeDeadline, remotePreface, error);
            throw error;
        } finally {
            if (!established) {
                writerCarrierState.compareAndSet(WRITER_CARRIER_WAITING, WRITER_CARRIER_ABORTED);
                writerLoopDecision.countDown();
            }
        }

        Thread readerThread = SessionEstablishmentCoordinator.newDaemonThread("zmux-reader", this.owner.readerLoopTask());
        readerThread.start();
    }

    private void finishEstablishmentFailure(CountDownLatch prefaceWriteDone,
                                            AtomicReference<IOException> prefaceWriteError,
                                            EstablishmentWriteDeadline writeDeadline,
                                            Preface remotePreface,
                                            IOException error) {
        boolean wroteClose = false;
        try {
            writeDeadline.expedite();
            this.awaitPrefaceWrite(prefaceWriteDone, prefaceWriteError, this.failureWriteWait, writeDeadline, false);
            writeDeadline.clearIgnoringFailure();
            this.emitEstablishmentClose(remotePreface, error);
            wroteClose = true;
        } catch (IOException ignored) {
        } finally {
            if (wroteClose) {
                this.drainEstablishmentClose();
            }
            this.owner.closeTransport();
        }
    }

    private void awaitPrefaceWrite(CountDownLatch prefaceWriteDone,
                                   AtomicReference<IOException> prefaceWriteError,
                                   Duration duration,
                                   EstablishmentWriteDeadline writeDeadline,
                                   boolean failOnStall) throws IOException {
        boolean completed;
        try {
            completed = SessionEstablishmentCoordinator.awaitLatch(prefaceWriteDone, duration);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw SessionRuntime.interruptedIo(
                    "zmux: interrupted while waiting for preface write",
                    "open",
                    io.zmux.ZmuxErrorScope.SESSION,
                    io.zmux.ZmuxErrorDirection.BOTH,
                    interruptedException
            );
        }
        if (!completed) {
            if (!failOnStall) {
                throw this.owner.sessionInternalError(
                        "write preface",
                        "local preface write did not finish before establishment failure close"
                );
            }
            throw this.owner.sessionInternalError("write preface", PREFACE_WRITE_STALLED);
        }
        IOException writeError = prefaceWriteError.get();
        if (writeError != null) {
            if (writeDeadline.armed() && ZmuxErrors.timeout(writeError)) {
                throw this.owner.sessionInternalError("write preface", PREFACE_WRITE_STALLED, writeError);
            }
            throw SessionEstablishmentCoordinator.transportFailure(
                    "write preface",
                    "zmux: transport preface write failed",
                    writeError
            );
        }
    }

    private EstablishmentWriteDeadline beginEstablishmentWriteDeadline(Duration duration) {
        Instant deadline = SessionEstablishmentCoordinator.deadlineAfter(duration);
        if (deadline == null || !this.owner.supportsWriteDeadline()) {
            return EstablishmentWriteDeadline.disabled();
        }
        try {
            this.owner.setWriteDeadline(deadline);
            return new EstablishmentWriteDeadline(this.owner);
        } catch (IOException ignored) {
            return EstablishmentWriteDeadline.disabled();
        }
    }

    private Preface readPeerPreface() throws IOException {
        try {
            return this.owner.input().readPreface();
        } catch (IOException error) {
            throw SessionEstablishmentCoordinator.transportFailure(
                    "read preface",
                    "zmux: transport preface read failed",
                    error
            );
        }
    }

    private void emitEstablishmentClose(Preface peerPreface, IOException error) throws IOException {
        long payloadLimit = peerPreface == null ? 0L : peerPreface.settings().maxControlPayloadBytes();
        if (payloadLimit <= 0L) {
            payloadLimit = this.owner.localPreface().settings().maxControlPayloadBytes();
        }
        if (payloadLimit <= 0L) {
            payloadLimit = Settings.defaults().maxControlPayloadBytes();
        }
        FrameCodec.Frame frame = new FrameCodec.Frame(
                io.zmux.FrameType.CLOSE,
                0,
                0L,
                FrameCodec.buildErrorPayload(
                        SessionEstablishmentCoordinator.closeCode(error),
                        SessionEstablishmentCoordinator.closeReason(error),
                        payloadLimit
                )
        );
        Limits outboundLimits = peerPreface == null ? this.owner.localPreface().settings().limits() : peerPreface.settings().limits();
        FrameCodec.writeFrame(this.owner.output(), frame, outboundLimits);
        this.owner.output().flush();
    }

    private void drainEstablishmentClose() {
        try {
            SessionEstablishmentCoordinator.sleepDuration(this.closeDrainDelay);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class EstablishmentWriteDeadline {
        private static final EstablishmentWriteDeadline DISABLED = new EstablishmentWriteDeadline(null);
        private final Owner owner;
        private boolean armed;

        private EstablishmentWriteDeadline(Owner owner) {
            this.owner = owner;
            this.armed = owner != null;
        }

        static EstablishmentWriteDeadline disabled() {
            return DISABLED;
        }

        boolean armed() {
            return armed;
        }

        IOException clear() {
            if (!armed || owner == null) {
                return null;
            }
            try {
                owner.setWriteDeadline(null);
                armed = false;
                return null;
            } catch (IOException error) {
                return error;
            }
        }

        void clearIgnoringFailure() {
            clear();
        }

        void expedite() {
            if (!armed || owner == null) {
                return;
            }
            try {
                owner.setWriteDeadline(Instant.now());
            } catch (IOException ignored) {
            }
        }
    }

    interface Owner {
        FrameCodec.Decoder input();

        BufferedOutputStream output();

        Preface localPreface();

        Object lock();

        void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos);

        void notifyLockWaiters();

        boolean supportsWriteDeadline();

        void setWriteDeadline(Instant deadline) throws IOException;

        Runnable readerLoopTask();

        Runnable writerLoopTask();

        IOException sessionInternalError(String operation, String message);

        IOException sessionInternalError(String operation, String message, Throwable cause);

        void closeTransport();
    }
}
