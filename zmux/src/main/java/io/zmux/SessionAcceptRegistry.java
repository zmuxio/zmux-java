package io.zmux;


import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionAcceptRegistry {
    private static final int RELEASE_EMPTY_ACCEPT_QUEUE_MIN_SIZE = 1024;

    private final Owner owner;
    private Deque<StreamRuntime> acceptBidi = new ArrayDeque<>();
    private Deque<StreamRuntime> acceptUni = new ArrayDeque<>();
    private long acceptBidiBytes;
    private long acceptUniBytes;
    private long nextVisibilitySequence;
    private long visibleAcceptRefused;
    private int acceptBidiPeakSize;
    private int acceptUniPeakSize;

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
        Deque<StreamRuntime> queue = this.acceptQueueLocked(streamRuntime.bidirectional());
        queue.addLast(streamRuntime);
        this.recordAcceptQueuePeakLocked(streamRuntime.bidirectional(), queue.size());
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
            this.releaseAcceptQueueStorageIfEmptyLocked(streamRuntime.bidirectional());
            return true;
        }
        if (deque.peekLast() == streamRuntime) {
            deque.pollLast();
            this.finishAcceptedRemovalLocked(streamRuntime);
            this.releaseAcceptQueueStorageIfEmptyLocked(streamRuntime.bidirectional());
            return true;
        }
        Iterator<StreamRuntime> iterator = deque.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() != streamRuntime) {
                continue;
            }
            iterator.remove();
            this.finishAcceptedRemovalLocked(streamRuntime);
            this.releaseAcceptQueueStorageIfEmptyLocked(streamRuntime.bidirectional());
            return true;
        }
        this.finishAcceptedRemovalLocked(streamRuntime);
        return false;
    }

    StreamRuntime pollAcceptedHeadLocked(boolean bidirectional) {
        Deque<StreamRuntime> deque = this.acceptQueueLocked(bidirectional);
        StreamRuntime streamRuntime = deque.pollFirst();
        if (streamRuntime != null) {
            streamRuntime.markAcceptedLocked();
        }
        this.finishAcceptedRemovalLocked(streamRuntime);
        this.releaseAcceptQueueStorageIfEmptyLocked(bidirectional);
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
        this.acceptBidiPeakSize = 0;
        this.acceptUniPeakSize = 0;
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
        Deque<StreamRuntime> deque = this.acceptQueueLocked(bidirectional);
        StreamRuntime streamRuntime = deque.pollLast();
        this.finishAcceptedRemovalLocked(streamRuntime);
        this.releaseAcceptQueueStorageIfEmptyLocked(bidirectional);
        return streamRuntime;
    }

    private void recordAcceptQueuePeakLocked(boolean bidirectional, int size) {
        if (bidirectional) {
            this.acceptBidiPeakSize = Math.max(this.acceptBidiPeakSize, size);
        } else {
            this.acceptUniPeakSize = Math.max(this.acceptUniPeakSize, size);
        }
    }

    private void releaseAcceptQueueStorageIfEmptyLocked(boolean bidirectional) {
        Deque<StreamRuntime> deque = this.acceptQueueLocked(bidirectional);
        if (!deque.isEmpty()) {
            return;
        }
        int peakSize = bidirectional ? this.acceptBidiPeakSize : this.acceptUniPeakSize;
        if (bidirectional) {
            if (peakSize >= RELEASE_EMPTY_ACCEPT_QUEUE_MIN_SIZE) {
                this.acceptBidi = new ArrayDeque<>();
            }
            this.acceptBidiPeakSize = 0;
        } else {
            if (peakSize >= RELEASE_EMPTY_ACCEPT_QUEUE_MIN_SIZE) {
                this.acceptUni = new ArrayDeque<>();
            }
            this.acceptUniPeakSize = 0;
        }
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
