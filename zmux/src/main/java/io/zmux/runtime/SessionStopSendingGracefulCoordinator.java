package io.zmux.runtime;

import io.zmux.ErrorCode;
import java.io.IOException;
import java.util.*;

final class SessionStopSendingGracefulCoordinator {
    private final SessionRuntime owner;
    private Map<StreamRuntime, Long> deadlines = new HashMap<>();
    private PriorityQueue<GracefulDrainDeadline> deadlineHeap =
            new PriorityQueue<>(Comparator.comparingLong(GracefulDrainDeadline::deadlineNanos));

    SessionStopSendingGracefulCoordinator(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    boolean tryGracefulStopSendingLocked(StreamRuntime streamRuntime) throws IOException {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(
                        streamRuntime.recvAbortiveLocked(),
                        streamRuntime.needsLocalOpenerLocked(),
                        streamRuntime.openedLocally(),
                        streamRuntime.sendCommittedLocked(),
                        this.owner.queuedDataBytesForStreamLocked(streamRuntime),
                        this.owner.inflightQueuedBytesForStreamLocked(streamRuntime),
                        this.owner.fragmentCapLocked(streamRuntime),
                        this.owner.sendRateEstimateLocked(),
                        this.owner.stopSendingGracefulTailCapLocked(),
                        this.owner.stopSendingGracefulDrainWindowLocked()
                )
        );
        if (!decision.attempt()) {
            return false;
        }
        long deadlineNanos = SessionRuntime.saturatingAdd(
                System.nanoTime(),
                SessionRuntime.durationToPositiveNanosSaturated(
                        StopSendingGracefulPolicy.drainWindow(this.owner.stopSendingGracefulDrainWindowLocked())
                )
        );
        if (!streamRuntime.sendTerminalLocked() && !streamRuntime.finQueuedLocked()) {
            streamRuntime.queuePeerStopGracefulFinishLocked();
        }
        streamRuntime.armStopSendingGracefulDrainLocked(deadlineNanos);
        return true;
    }

    void expireStopSendingGracefulDrainsLocked() {
        long now = System.nanoTime();
        StreamRuntime streamRuntime;
        boolean queuedReset = false;
        while ((streamRuntime = this.pollExpiredStopSendingGracefulDrainLocked(now)) != null) {
            streamRuntime.clearStopSendingGracefulDrainLocked();
            if (!streamRuntime.shouldEmitQueuedDataLocked()) {
                continue;
            }
            try {
                byte[] resetPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.CANCELLED.code(), "");
                streamRuntime.concludeStopSendingWithResetLocked();
                this.owner.enqueueResetLocked(streamRuntime, ErrorCode.CANCELLED.code(), resetPayload, false);
                queuedReset = true;
            } catch (IOException error) {
                this.owner.failSession(error);
                return;
            }
        }
        if (queuedReset) {
            this.owner.notifyWriterWaitersLocked();
        }
    }

    void updateStopSendingGracefulDeadlineLocked(StreamRuntime streamRuntime, long deadlineNanos) {
        if (streamRuntime == null) {
            return;
        }
        if (deadlineNanos <= 0L) {
            if (this.deadlines.remove(streamRuntime) != null) {
                this.owner.notifyWriterWaitersLocked();
            }
            return;
        }
        this.deadlines.put(streamRuntime, deadlineNanos);
        this.deadlineHeap.add(new GracefulDrainDeadline(streamRuntime, deadlineNanos));
        this.owner.notifyWriterWaitersLocked();
    }

    long nextStopSendingGracefulDeadlineLocked() {
        while (true) {
            GracefulDrainDeadline head = this.deadlineHeap.peek();
            if (head == null) {
                return 0L;
            }
            Long activeDeadline = this.deadlines.get(head.stream());
            if (activeDeadline == null || activeDeadline != head.deadlineNanos()) {
                this.deadlineHeap.poll();
                continue;
            }
            return head.deadlineNanos();
        }
    }

    void clear() {
        this.deadlines = new HashMap<>();
        this.deadlineHeap = new PriorityQueue<>(Comparator.comparingLong(GracefulDrainDeadline::deadlineNanos));
    }

    private StreamRuntime pollExpiredStopSendingGracefulDrainLocked(long nowNanos) {
        while (true) {
            GracefulDrainDeadline head = this.deadlineHeap.peek();
            if (head == null || head.deadlineNanos() > nowNanos) {
                return null;
            }
            this.deadlineHeap.poll();
            Long activeDeadline = this.deadlines.get(head.stream());
            if (activeDeadline == null || activeDeadline != head.deadlineNanos()) {
                continue;
            }
            return head.stream();
        }
    }

    private static final class GracefulDrainDeadline {
        private final StreamRuntime stream;
        private final long deadlineNanos;

        private GracefulDrainDeadline(StreamRuntime stream, long deadlineNanos) {
            this.stream = stream;
            this.deadlineNanos = deadlineNanos;
        }

        private StreamRuntime stream() {
            return stream;
        }

        private long deadlineNanos() {
            return deadlineNanos;
        }
    }
}
