package io.zmux.internal;

import io.zmux.SessionState;
import io.zmux.Settings;

import java.io.IOException;
import java.util.*;

final class SessionWriterCoordinator {
    private static final long BLOCKED_ONLY_COALESCE_NANOS = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10L);
    private static final int MAX_BATCH_FRAMES = 32;
    private final Owner owner;
    private final SessionWriterTransport writerTransport;
    @SuppressWarnings("FieldCanBeLocal")
    private final ArrayList<SessionRuntime.OutboundFrame> pendingWindowUpdateBatch = new ArrayList<>(8);
    private final SessionWriterBatchOrderer batchOrderer;
    private final SessionWriterBatchCollector batchCollector;
    private final SessionWriterBatchFilter batchFilter;
    private long blockedOnlyPendingSinceNanos;

    SessionWriterCoordinator(Owner owner, SessionWriterTransport writerTransport) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.writerTransport = Objects.requireNonNull(writerTransport, "writerTransport");
        this.batchOrderer = new SessionWriterBatchOrderer(owner, MAX_BATCH_FRAMES);
        this.batchCollector = new SessionWriterBatchCollector(
                owner,
                this.batchOrderer,
                new ArrayList<>(MAX_BATCH_FRAMES),
                MAX_BATCH_FRAMES
        );
        this.batchFilter = new SessionWriterBatchFilter(owner);
    }

    private static boolean containsOpeningFrame(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null) {
            return false;
        }
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (outboundFrame != null && outboundFrame.openingFrame()) {
                return true;
            }
        }
        return false;
    }

    static long remainingDeadlineNanos(long deadlineNanos, long nowNanos) {
        return TimeoutBudget.positiveRemainingNanosUntil(deadlineNanos, nowNanos);
    }

    void run() {
        try {
            while (true) {
                WriterPollResult pollResult;
                synchronized (this.owner.lock()) {
                    pollResult = this.awaitNextWriterBatchLocked();
                    if (!pollResult.keepaliveTimeout() && pollResult.batch() == null) {
                        return;
                    }
                }
                if (pollResult.keepaliveTimeout()) {
                    // Keep CLOSE emission out of the synchronized section to avoid
                    // re-entering close/event machinery while still holding the session monitor.
                    this.owner.emitKeepaliveTimeoutClose();
                    continue;
                }
                List<SessionRuntime.OutboundFrame> batch = pollResult.batch();

                long batchStartedAtNanos = System.nanoTime();
                long batchBytes = this.writeBatch(batch);
                long batchCompletedAtNanos = System.nanoTime();
                synchronized (this.owner.lock()) {
                    this.owner.afterWriteBatchLocked(batch, batchBytes, batchStartedAtNanos, batchCompletedAtNanos);
                }
                this.clearRetainedBatchRefs();
                this.owner.emitPendingEvents();
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            IOException interrupted = SessionRuntime.interruptedIo(
                    "zmux: writer interrupted",
                    "write",
                    io.zmux.ZmuxErrorScope.SESSION,
                    io.zmux.ZmuxErrorDirection.BOTH,
                    interruptedException
            );
            synchronized (this.owner.lock()) {
                if (this.owner.state().terminal()) {
                    this.owner.finishSessionLocked(interrupted, this.owner.state());
                    return;
                }
                this.owner.finishSessionLocked(interrupted, SessionState.FAILED);
            }
        } catch (IOException error) {
            synchronized (this.owner.lock()) {
                boolean closeFrameInflight = this.owner.batchContainsCloseFrameLocked(this.owner.inflightBatch());
                if (closeFrameInflight) {
                    this.owner.recordCloseFrameFlushErrorLocked();
                }
                if (this.owner.state().terminal()) {
                    this.owner.finishSessionLocked(error, this.owner.state());
                    return;
                }
                if (!closeFrameInflight) {
                    this.owner.recordSkippedCloseOnDeadIOLocked();
                }
                this.owner.finishSessionLocked(error, SessionState.FAILED);
            }
        } finally {
            this.clearRetainedBatchRefs();
            this.owner.emitPendingEvents();
        }
    }

    List<SessionRuntime.OutboundFrame> collectReadyBatchLocked() throws IOException {
        return this.collectReadyBatchStateLocked(true, false).frames();
    }

    SessionRuntime.ReadyBatch collectReadyBatchStateLocked(boolean orderOrdinary, boolean trackWriterHeld)
            throws IOException {
        return this.batchCollector.collectReadyBatchStateLocked(orderOrdinary, trackWriterHeld);
    }

    List<SessionRuntime.OutboundFrame> finishOrdinaryBatchLocked(List<SessionRuntime.OutboundFrame> batch,
                                                                 long batchCost,
                                                                 long costLimit)
            throws IOException, InterruptedException {
        long coalesceNanos = this.ordinaryBatchCoalesceNanosLocked(batch, batchCost, costLimit);
        if (coalesceNanos > 0L) {
            this.waitForWriterWorkLocked(coalesceNanos);
            this.owner.expireStopSendingGracefulDrainsLocked();
            if (this.shouldDiscardStagedOrdinaryBatchLocked()) {
                this.discardCollectedBatchLocked(batch);
                return Collections.emptyList();
            }
            if (this.owner.urgentQueue().isEmpty()) {
                this.drainOrdinaryBatchLocked(batch, costLimit, batchCost, true);
            }
        }

        if (this.shouldDiscardStagedOrdinaryBatchLocked()) {
            this.discardCollectedBatchLocked(batch);
            return Collections.emptyList();
        }
        return this.orderOrdinaryBatchLocked(batch);
    }

    long writeBatch(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        return this.writerTransport.writeBatch(batch);
    }

    List<SessionRuntime.OutboundFrame> filterWritableBatchLocked(List<SessionRuntime.OutboundFrame> batch) {
        return this.batchFilter.filterWritableBatchLocked(batch);
    }

    private WriterPollResult awaitNextWriterBatchLocked() throws IOException, InterruptedException {
        while (true) {
            if (this.owner.processKeepaliveScheduledWorkLocked(System.nanoTime())) {
                return WriterPollResult.timeoutResult();
            }
            this.owner.retryReceiveReplenishLocked();
            SessionRuntime.ReadyBatch readyBatch = this.collectReadyBatchStateLocked(false, true);
            if (!readyBatch.frames().isEmpty()) {
                List<SessionRuntime.OutboundFrame> staged = this.stageReadyWriterBatchLocked(readyBatch);
                if (!staged.isEmpty()) {
                    return WriterPollResult.batch(staged);
                }
                continue;
            }

            if (this.shouldFinishWriterLocked()) {
                this.owner.finishSessionLocked(
                        null,
                        this.owner.state() == SessionState.CLOSING ? SessionState.CLOSED : this.owner.state()
                );
                return WriterPollResult.batch(null);
            }

            if (this.shouldCoalesceBlockedOnlyWindowUpdatesLocked()) {
                long remainingNanos = this.remainingBlockedOnlyCoalesceNanosLocked(System.nanoTime());
                if (remainingNanos > 0L) {
                    this.waitForWriterWorkLocked(remainingNanos);
                    continue;
                }
            } else {
                this.blockedOnlyPendingSinceNanos = 0L;
            }

            ArrayList<SessionRuntime.OutboundFrame> pendingUpdates = this.pendingWindowUpdateBatch;
            pendingUpdates.clear();
            try {
                this.owner.appendPendingWindowUpdatesLocked(pendingUpdates, MAX_BATCH_FRAMES, null, true);
            } catch (IOException error) {
                this.discardCollectedBatchLocked(pendingUpdates);
                throw error;
            }
            if (!pendingUpdates.isEmpty()) {
                this.blockedOnlyPendingSinceNanos = 0L;
                this.owner.setInflightBatch(pendingUpdates);
                return WriterPollResult.batch(pendingUpdates);
            }

            this.waitForWriterWorkLocked();
        }
    }

    private List<SessionRuntime.OutboundFrame> stageReadyWriterBatchLocked(SessionRuntime.ReadyBatch readyBatch)
            throws IOException, InterruptedException {
        List<SessionRuntime.OutboundFrame> batch = readyBatch.frames();
        if (readyBatch.ordinary()) {
            batch = this.finishOrdinaryBatchLocked(batch, readyBatch.batchCost(), readyBatch.costLimit());
            if (batch.isEmpty()) {
                return Collections.emptyList();
            }
        }

        try {
            batch = this.filterWritableBatchLocked(batch);
            if (!containsOpeningFrame(batch)) {
                this.owner.appendPendingWindowUpdatesLocked(batch, MAX_BATCH_FRAMES, null, true);
            }
        } catch (IOException error) {
            this.discardCollectedBatchLocked(batch);
            throw error;
        }
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }

        this.owner.noteTransportWriteIntentLocked(System.nanoTime());
        this.owner.setInflightBatch(batch);
        return batch;
    }

    private boolean shouldFinishWriterLocked() {
        return this.owner.state().terminal()
                && this.owner.urgentQueue().isEmpty()
                && this.owner.advisoryQueue().isEmpty()
                && this.owner.dataQueue().isEmpty();
    }

    private boolean hasQueuedWriterWorkLocked() {
        return !this.owner.urgentQueue().isEmpty()
                || !this.owner.advisoryQueue().isEmpty()
                || !this.owner.dataQueue().isEmpty();
    }

    private boolean shouldCoalesceBlockedOnlyWindowUpdatesLocked() {
        return !this.owner.state().terminal()
                && this.owner.hasPendingWindowUpdatesLocked()
                && !this.owner.hasPendingMaxDataLocked();
    }

    private long remainingBlockedOnlyCoalesceNanosLocked(long nowNanos) {
        if (this.blockedOnlyPendingSinceNanos <= 0L) {
            this.blockedOnlyPendingSinceNanos = nowNanos;
            return BLOCKED_ONLY_COALESCE_NANOS;
        }
        long elapsed = RuntimeFlow.elapsedNanos(nowNanos, this.blockedOnlyPendingSinceNanos);
        return elapsed >= BLOCKED_ONLY_COALESCE_NANOS ? 0L : BLOCKED_ONLY_COALESCE_NANOS - elapsed;
    }

    private long drainOrdinaryBatchLocked(List<SessionRuntime.OutboundFrame> batch,
                                          long costLimit,
                                          long batchCost,
                                          boolean trackWriterHeld) throws IOException {
        return this.batchCollector.drainOrdinaryBatchLocked(batch, costLimit, batchCost, trackWriterHeld);
    }

    private ArrayList<SessionRuntime.OutboundFrame> orderOrdinaryBatchLocked(List<SessionRuntime.OutboundFrame> batch) {
        return this.batchOrderer.orderOrdinary(batch);
    }

    private long ordinaryBatchCoalesceNanosLocked(List<SessionRuntime.OutboundFrame> batch,
                                                  long batchCost,
                                                  long costLimit) {
        return SessionWriterBatchPolicy.ordinaryBatchCoalesceNanos(
                batch,
                batchCost,
                costLimit,
                this.owner.urgentQueue().peekFirst() != null,
                this.shouldDiscardStagedOrdinaryBatchLocked(),
                this.owner.peerSettings(),
                MAX_BATCH_FRAMES
        );
    }

    private boolean shouldDiscardStagedOrdinaryBatchLocked() {
        return this.owner.closeFrameQueued() || this.owner.state() == SessionState.CLOSING || this.owner.state().terminal();
    }

    private void discardCollectedBatchLocked(List<SessionRuntime.OutboundFrame> batch) {
        this.batchFilter.discardCollectedBatchLocked(batch);
    }

    private void clearRetainedBatchRefs() {
        this.pendingWindowUpdateBatch.clear();
        this.batchCollector.clearRetainedBatchRefs();
    }

    private void waitForWriterWorkLocked() throws InterruptedException {
        long timeoutNanos = this.nextWriterWakeNanosLocked();
        if (timeoutNanos <= 0L) {
            this.owner.waitOnLock();
            return;
        }
        this.waitForWriterWorkLocked(timeoutNanos);
    }

    private void waitForWriterWorkLocked(long maxWaitNanos) throws InterruptedException {
        if (maxWaitNanos <= 0L) {
            return;
        }

        long timeoutNanos = maxWaitNanos;
        long deadlineNanos = this.nextWriterWakeNanosLocked();
        if (deadlineNanos > 0L) {
            timeoutNanos = Math.min(timeoutNanos, deadlineNanos);
        }

        this.owner.waitOnLockNanos(timeoutNanos);
    }

    private long nextWriterWakeNanosLocked() {
        long nowNanos = System.nanoTime();
        long gracefulWakeNanos = remainingDeadlineNanos(this.owner.nextStopSendingGracefulDeadlineLocked(), nowNanos);
        long keepaliveWakeNanos = this.owner.nextKeepaliveWakeNanosLocked(nowNanos);
        return RuntimeFlow.minNonZeroPositive(gracefulWakeNanos, keepaliveWakeNanos);
    }

    interface Owner {
        Object lock();

        void emitPendingEvents();

        SessionState state();

        boolean closeFrameQueued();

        Deque<SessionRuntime.OutboundFrame> urgentQueue();

        Deque<StreamRuntime> advisoryQueue();

        Deque<SessionRuntime.OutboundFrame> dataQueue();

        List<SessionRuntime.OutboundFrame> inflightBatch();

        void setInflightBatch(List<SessionRuntime.OutboundFrame> batch);

        void finishSessionLocked(IOException error, SessionState sessionState);

        boolean batchContainsCloseFrameLocked(List<SessionRuntime.OutboundFrame> batch);

        void recordCloseFrameFlushErrorLocked();

        void recordSkippedCloseOnDeadIOLocked();

        void retainWriterHeldFramesLocked(List<SessionRuntime.OutboundFrame> batch);

        void appendPendingWindowUpdatesLocked(List<SessionRuntime.OutboundFrame> batch,
                                              int maxFrames,
                                              Long preferredStreamId,
                                              boolean trackWriterHeld) throws IOException;

        boolean hasPendingWindowUpdatesLocked();

        boolean hasPendingMaxDataLocked();

        void noteTransportWriteIntentLocked(long nowNanos);

        void expireStopSendingGracefulDrainsLocked();

        void addBatchFrameLocked(List<SessionRuntime.OutboundFrame> batch,
                                 SessionRuntime.OutboundFrame outboundFrame,
                                 boolean trackWriterHeld);

        Long outboundSchedulingGroupLocked(StreamRuntime streamRuntime);

        Settings peerSettings();

        OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias();

        long sendRateEstimateLocked();

        long nextStopSendingGracefulDeadlineLocked();

        long nextKeepaliveWakeNanosLocked(long nowNanos);

        boolean processKeepaliveScheduledWorkLocked(long nowNanos) throws IOException;

        void retryReceiveReplenishLocked();

        SessionRuntime.OutboundFrame pollQueuedOutboundLocked(Deque<SessionRuntime.OutboundFrame> deque);

        void releaseQueuedDataLocked(SessionRuntime.OutboundFrame outboundFrame);

        void releaseWriterHeldFrameLocked(SessionRuntime.OutboundFrame outboundFrame);

        void maybeCompactStreamLocked(StreamRuntime streamRuntime);

        boolean shouldEmitPriorityUpdateLocked(StreamRuntime streamRuntime);

        SessionRuntime.OutboundFrame takePendingPriorityUpdateForBatchLocked(StreamRuntime streamRuntime)
                throws IOException;

        void afterWriteBatchLocked(List<SessionRuntime.OutboundFrame> batch,
                                   long batchBytes,
                                   long batchStartedAtNanos,
                                   long batchCompletedAtNanos) throws IOException;

        void emitKeepaliveTimeoutClose() throws IOException;

        void notifyLockWaiters();

        void waitOnLock() throws InterruptedException;

        void waitOnLockNanos(long waitNanos) throws InterruptedException;
    }

    static final class WriterPollResult {
        private final List<SessionRuntime.OutboundFrame> batch;
        private final boolean keepaliveTimeout;

        private WriterPollResult(List<SessionRuntime.OutboundFrame> batch, boolean keepaliveTimeout) {
            this.batch = batch;
            this.keepaliveTimeout = keepaliveTimeout;
        }

        static WriterPollResult batch(List<SessionRuntime.OutboundFrame> batch) {
            return new WriterPollResult(batch, false);
        }

        static WriterPollResult timeoutResult() {
            return new WriterPollResult(null, true);
        }

        List<SessionRuntime.OutboundFrame> batch() {
            return batch;
        }

        boolean keepaliveTimeout() {
            return keepaliveTimeout;
        }
    }
}
