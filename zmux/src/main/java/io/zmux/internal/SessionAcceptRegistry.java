package io.zmux.internal;

import io.zmux.ErrorCode;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionAcceptRegistry {
    private final Owner owner;
    private Deque<StreamRuntime> acceptBidi = new ArrayDeque<>();
    private Deque<StreamRuntime> acceptUni = new ArrayDeque<>();
    private long acceptBidiBytes;
    private long acceptUniBytes;
    private long nextVisibilitySequence;
    private long visibleAcceptRefused;

    SessionAcceptRegistry(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    void addQueuedBytesLocked(StreamRuntime streamRuntime, long queuedBytes) {
        if (streamRuntime == null || queuedBytes <= 0) {
            return;
        }
        if (streamRuntime.bidirectional()) {
            this.acceptBidiBytes = SessionRuntime.saturatingAdd(this.acceptBidiBytes, queuedBytes);
        } else {
            this.acceptUniBytes = SessionRuntime.saturatingAdd(this.acceptUniBytes, queuedBytes);
        }
    }

    void enqueueAcceptedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || streamRuntime.acceptQueued()) {
            return;
        }
        streamRuntime.setApplicationVisibleLocked(true);
        streamRuntime.setVisibilitySequenceLocked(++this.nextVisibilitySequence);
        streamRuntime.setAcceptQueuedLocked(true);
        this.acceptQueueLocked(streamRuntime.bidirectional()).addLast(streamRuntime);
        this.addQueuedBytesLocked(streamRuntime, streamRuntime.readBufferSizeLocked());
    }

    boolean removeAcceptedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.acceptQueued()) {
            return false;
        }
        Deque<StreamRuntime> deque = this.acceptQueueLocked(streamRuntime.bidirectional());
        if (deque.peekFirst() == streamRuntime) {
            deque.pollFirst();
            this.finishAcceptedRemovalLocked(streamRuntime);
            return true;
        }
        if (deque.peekLast() == streamRuntime) {
            deque.pollLast();
            this.finishAcceptedRemovalLocked(streamRuntime);
            return true;
        }
        Iterator<StreamRuntime> iterator = deque.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() != streamRuntime) {
                continue;
            }
            iterator.remove();
            this.finishAcceptedRemovalLocked(streamRuntime);
            return true;
        }
        this.finishAcceptedRemovalLocked(streamRuntime);
        return false;
    }

    StreamRuntime pollAcceptedHeadLocked(boolean bidirectional) {
        StreamRuntime streamRuntime = this.acceptQueueLocked(bidirectional).pollFirst();
        if (streamRuntime != null) {
            streamRuntime.markAcceptedLocked();
        }
        this.finishAcceptedRemovalLocked(streamRuntime);
        return streamRuntime;
    }

    void enforceVisibleBacklogLocked() throws IOException {
        int backlogCountLimit = this.owner.visibleAcceptBacklogHardCapLocked();
        long backlogBytesLimit = this.owner.visibleAcceptBacklogBytesHardCapLocked();
        long openInfoBudget = this.owner.retainedOpenInfoBudgetLocked();
        boolean enqueuedAbort = false;
        while (true) {
            boolean overCount = backlogCountLimit > 0 && this.pendingAcceptedCountLocked() > backlogCountLimit;
            boolean overBytes = backlogBytesLimit > 0L && this.pendingAcceptedBytesLocked() > backlogBytesLimit;
            boolean overMemory = this.owner.trackedSessionMemoryLocked() > this.owner.sessionMemoryHardCapLocked();
            boolean overOpenInfo = openInfoBudget > 0L && this.owner.retainedOpenInfoBytesLocked() > openInfoBudget;
            if (!(overCount || overBytes || overOpenInfo || overMemory)) {
                if (enqueuedAbort) {
                    this.owner.notifyAcceptWaitersLocked();
                    this.owner.notifyWriterWaitersLocked();
                }
                return;
            }
            StreamRuntime streamRuntime = this.pollNewestAcceptedLocked();
            if (streamRuntime == null) {
                if (enqueuedAbort) {
                    this.owner.notifyAcceptWaitersLocked();
                    this.owner.notifyWriterWaitersLocked();
                }
                return;
            }
            this.visibleAcceptRefused = SessionRuntime.saturatingAdd(this.visibleAcceptRefused, 1L);
            byte[] abortPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.REFUSED_STREAM.code(), "");
            streamRuntime.abortFromLocalLocked(ErrorCode.REFUSED_STREAM.code(), "");
            this.owner.enqueueAbortLocked(streamRuntime, ErrorCode.REFUSED_STREAM.code(), abortPayload, false);
            enqueuedAbort = true;
            this.owner.maybeCompactStreamLocked(streamRuntime);
        }
    }

    int pendingAcceptedCountLocked() {
        return this.acceptBidi.size() + this.acceptUni.size();
    }

    long pendingAcceptedBytesLocked() {
        return SessionRuntime.saturatingAdd(this.acceptBidiBytes, this.acceptUniBytes);
    }

    long visibleAcceptRefusedLocked() {
        return this.visibleAcceptRefused;
    }

    void releaseQueuedBytesLocked(StreamRuntime streamRuntime, long releasedBytes) {
        if (streamRuntime == null || releasedBytes <= 0) {
            return;
        }
        if (streamRuntime.bidirectional()) {
            this.acceptBidiBytes = Math.max(0L, this.acceptBidiBytes - releasedBytes);
        } else {
            this.acceptUniBytes = Math.max(0L, this.acceptUniBytes - releasedBytes);
        }
    }

    void clearPendingLocked() {
        StreamRuntime streamRuntime;
        while ((streamRuntime = this.acceptBidi.pollFirst()) != null) {
            streamRuntime.setAcceptQueuedLocked(false);
        }
        while ((streamRuntime = this.acceptUni.pollFirst()) != null) {
            streamRuntime.setAcceptQueuedLocked(false);
        }
        this.acceptBidi = new ArrayDeque<>();
        this.acceptUni = new ArrayDeque<>();
        this.acceptBidiBytes = 0L;
        this.acceptUniBytes = 0L;
    }

    private void finishAcceptedRemovalLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.acceptQueued()) {
            return;
        }
        this.releaseQueuedBytesLocked(streamRuntime, streamRuntime.readBufferSizeLocked());
        streamRuntime.setAcceptQueuedLocked(false);
        this.owner.maybeCompactStreamLocked(streamRuntime);
    }

    private Deque<StreamRuntime> acceptQueueLocked(boolean bidirectional) {
        return bidirectional ? this.acceptBidi : this.acceptUni;
    }

    private StreamRuntime pollAcceptedTailLocked(boolean bidirectional) {
        StreamRuntime streamRuntime = this.acceptQueueLocked(bidirectional).pollLast();
        this.finishAcceptedRemovalLocked(streamRuntime);
        return streamRuntime;
    }

    private StreamRuntime pollNewestAcceptedLocked() {
        StreamRuntime newestBidi = this.acceptBidi.peekLast();
        StreamRuntime newestUni = this.acceptUni.peekLast();
        if (newestBidi == null) {
            return this.pollAcceptedTailLocked(false);
        }
        if (newestUni == null) {
            return this.pollAcceptedTailLocked(true);
        }
        return Long.compareUnsigned(newestBidi.visibilitySequence(), newestUni.visibilitySequence()) > 0
                ? this.pollAcceptedTailLocked(true)
                : this.pollAcceptedTailLocked(false);
    }

    interface Owner {
        long retainedOpenInfoBytesLocked();

        long retainedOpenInfoBudgetLocked();

        long trackedSessionMemoryLocked();

        long sessionMemoryHardCapLocked();

        int visibleAcceptBacklogHardCapLocked();

        long visibleAcceptBacklogBytesHardCapLocked();

        byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException;

        void enqueueAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException;

        void enqueueAbortLocked(StreamRuntime streamRuntime,
                                long code,
                                byte[] payload,
                                boolean notifyWaiters) throws IOException;

        void maybeCompactStreamLocked(StreamRuntime streamRuntime);

        void notifyAcceptWaitersLocked();

        void notifyWriterWaitersLocked();
    }
}
