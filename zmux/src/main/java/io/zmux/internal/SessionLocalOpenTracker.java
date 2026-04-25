package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("resource")
final class SessionLocalOpenTracker {
    private final Owner owner;
    private Deque<StreamRuntime> provisionalBidi = new ArrayDeque<>();
    private Deque<StreamRuntime> provisionalUni = new ArrayDeque<>();
    private Deque<StreamRuntime> unseenLocalBidi = new ArrayDeque<>();
    private Deque<StreamRuntime> unseenLocalUni = new ArrayDeque<>();

    SessionLocalOpenTracker(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static boolean removeKnownEndpointLocked(Deque<StreamRuntime> deque,
                                                     StreamRuntime streamRuntime,
                                                     boolean head) {
        if (deque == null || streamRuntime == null || deque.isEmpty()) {
            return false;
        }
        StreamRuntime removed = head ? deque.pollFirst() : deque.pollLast();
        if (removed == streamRuntime) {
            streamRuntime.setProvisionalTrackedLocked(false);
            return true;
        }
        if (removed != null) {
            if (head) {
                deque.addFirst(removed);
            } else {
                deque.addLast(removed);
            }
        }
        return false;
    }

    private static IOException normalizeProvisionalFailure(IOException error, boolean peerRefused) {
        if (error == null || peerRefused) {
            return error;
        }
        if (error instanceof OpenExpiredException) {
            return new ApplicationError(
                    ErrorCode.CANCELLED.code(),
                    OpenExpiredException.MESSAGE,
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.ABORT
            );
        }
        return error;
    }

    private static int provisionalAvailableCount(long firstProvisionalStreamId, long maxPeerAcceptedStreamId) {
        if (firstProvisionalStreamId == 0L || firstProvisionalStreamId > maxPeerAcceptedStreamId) {
            return 0;
        }
        long distance = maxPeerAcceptedStreamId - firstProvisionalStreamId;
        long available = distance / 4L + 1L;
        return available >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) available;
    }

    int provisionalCountLocked(boolean bidirectional) {
        return this.provisionalQueueLocked(bidirectional).size();
    }

    int totalProvisionalCountLocked() {
        return this.provisionalBidi.size() + this.provisionalUni.size();
    }

    boolean hasProvisionalsLocked() {
        return !this.provisionalBidi.isEmpty() || !this.provisionalUni.isEmpty();
    }

    StreamRuntime provisionalHeadLocked(boolean bidirectional) {
        return this.provisionalQueueLocked(bidirectional).peekFirst();
    }

    void appendProvisionalLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || streamRuntime.provisionalTracked()) {
            return;
        }
        streamRuntime.setProvisionalCreatedAtNanosLocked(System.nanoTime());
        streamRuntime.setProvisionalTrackedLocked(true);
        this.provisionalQueueLocked(streamRuntime.bidirectional()).addLast(streamRuntime);
        this.owner.notifyLockWaiters();
    }

    void removeProvisionalLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.provisionalTracked()) {
            return;
        }
        this.provisionalQueueLocked(streamRuntime.bidirectional()).remove(streamRuntime);
        streamRuntime.setProvisionalTrackedLocked(false);
    }

    void appendUnseenLocalLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null
                || !streamRuntime.awaitingPeerVisibilityLocked()
                || streamRuntime.unseenLocalTracked()) {
            return;
        }
        this.unseenLocalQueueLocked(streamRuntime.bidirectional()).addLast(streamRuntime);
        streamRuntime.setUnseenLocalTrackedLocked(true);
    }

    void removeUnseenLocalLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.unseenLocalTracked()) {
            return;
        }
        this.unseenLocalQueueLocked(streamRuntime.bidirectional()).remove(streamRuntime);
        streamRuntime.setUnseenLocalTrackedLocked(false);
    }

    void reapExpiredProvisionalsLocked(boolean bidirectional, long nowNanos, long provisionalOpenMaxAgeNanos) {
        Deque<StreamRuntime> deque = this.provisionalQueueLocked(bidirectional);
        StreamRuntime streamRuntime;
        boolean changed = false;
        while ((streamRuntime = deque.peekFirst()) != null
                && this.provisionalExpired(streamRuntime, nowNanos, provisionalOpenMaxAgeNanos)) {
            this.owner.recordProvisionalOpenExpiredLocked();
            this.failProvisionalHeadLocked(deque, streamRuntime, new OpenExpiredException(), false);
            changed = true;
        }
        if (changed) {
            this.owner.notifyLockWaiters();
        }
    }

    void failProvisionalLocked(StreamRuntime streamRuntime, IOException error, boolean peerRefused) {
        this.failProvisionalLocked(streamRuntime, error, peerRefused, true);
    }

    private void failProvisionalLocked(StreamRuntime streamRuntime,
                                       IOException error,
                                       boolean peerRefused,
                                       boolean notify) {
        if (streamRuntime == null) {
            return;
        }
        this.removeProvisionalLocked(streamRuntime);
        this.finishProvisionalFailureLocked(streamRuntime, error, peerRefused);
        if (notify) {
            this.owner.notifyLockWaiters();
        }
    }

    private void failProvisionalHeadLocked(Deque<StreamRuntime> deque,
                                           StreamRuntime streamRuntime,
                                           IOException error,
                                           boolean peerRefused) {
        if (!removeKnownEndpointLocked(deque, streamRuntime, true)) {
            this.removeProvisionalLocked(streamRuntime);
        }
        this.finishProvisionalFailureLocked(streamRuntime, error, peerRefused);
    }

    private void failProvisionalTailLocked(Deque<StreamRuntime> deque,
                                           StreamRuntime streamRuntime,
                                           IOException error,
                                           boolean peerRefused) {
        if (!removeKnownEndpointLocked(deque, streamRuntime, false)) {
            this.removeProvisionalLocked(streamRuntime);
        }
        this.finishProvisionalFailureLocked(streamRuntime, error, peerRefused);
    }

    private void finishProvisionalFailureLocked(StreamRuntime streamRuntime,
                                                IOException error,
                                                boolean peerRefused) {
        IOException failure = normalizeProvisionalFailure(error, peerRefused);
        streamRuntime.setProvisionalCreatedAtNanosLocked(0L);
        this.owner.onStreamOpenInfoUpdatedLocked(streamRuntime.openInfoLengthLocked(), 0);
        this.owner.releaseStreamPeerReasonBudgetLocked(streamRuntime);
        if (peerRefused) {
            streamRuntime.abortFromPeerLocked(ErrorCode.REFUSED_STREAM.code(), "", 0L);
        } else {
            streamRuntime.failLocallyLocked(failure);
        }
    }

    void failProvisionalLocalAbortLocked(StreamRuntime streamRuntime, long code, String reason) {
        this.failProvisionalLocked(
                streamRuntime,
                new ApplicationError(
                        code,
                        reason,
                        ZmuxErrorScope.STREAM,
                        ZmuxErrorSource.LOCAL,
                        ZmuxErrorDirection.BOTH,
                        ZmuxTerminationKind.ABORT
                ),
                false
        );
    }

    long projectedLocalOpenId(long firstCandidateId, int provisionalAhead) {
        if (provisionalAhead <= 0 || firstCandidateId > 0x3FFFFFFFFFFFFFFFL) {
            return firstCandidateId;
        }
        long remainingStride = (0x3FFFFFFFFFFFFFFFL - firstCandidateId) / 4L;
        if ((long) provisionalAhead > remainingStride) {
            return 0x4000000000000000L;
        }
        return firstCandidateId + (long) provisionalAhead * 4L;
    }

    void reclaimUnseenLocalStreamsLocked(long peerGoAwayBidi, long peerGoAwayUni) {
        this.reclaimUnseenLocalStreamsLocked(this.unseenLocalBidi, peerGoAwayBidi, peerGoAwayBidi, peerGoAwayUni);
        this.reclaimUnseenLocalStreamsLocked(this.unseenLocalUni, peerGoAwayUni, peerGoAwayBidi, peerGoAwayUni);
    }

    void reclaimProvisionalsLocked(long nextLocalBidi,
                                   long nextLocalUni,
                                   long peerGoAwayBidi,
                                   long peerGoAwayUni) {
        boolean changed = this.reclaimProvisionalsLocked(this.provisionalBidi, nextLocalBidi, peerGoAwayBidi);
        changed |= this.reclaimProvisionalsLocked(this.provisionalUni, nextLocalUni, peerGoAwayUni);
        if (changed) {
            this.owner.notifyLockWaiters();
        }
    }

    void reclaimGracefulCloseLocalStreamsLocked() {
        this.reclaimGracefulCloseLocalStreamsLocked(this.unseenLocalBidi);
        this.reclaimGracefulCloseLocalStreamsLocked(this.unseenLocalUni);
        this.rejectAllProvisionalsLocked(this.provisionalBidi);
        this.rejectAllProvisionalsLocked(this.provisionalUni);
        this.owner.notifyLockWaiters();
    }

    void rejectGracefulCloseProvisionalsLocked() {
        this.rejectAllProvisionalsLocked(this.provisionalBidi);
        this.rejectAllProvisionalsLocked(this.provisionalUni);
        this.owner.notifyLockWaiters();
    }

    void forEachProvisionalLocked(Consumer<StreamRuntime> consumer) {
        for (StreamRuntime streamRuntime : this.provisionalBidi) {
            consumer.accept(streamRuntime);
        }
        for (StreamRuntime streamRuntime : this.provisionalUni) {
            consumer.accept(streamRuntime);
        }
    }

    void clear() {
        this.provisionalBidi = new ArrayDeque<>();
        this.provisionalUni = new ArrayDeque<>();
        this.unseenLocalBidi = new ArrayDeque<>();
        this.unseenLocalUni = new ArrayDeque<>();
    }

    private boolean provisionalExpired(StreamRuntime streamRuntime, long nowNanos, long provisionalOpenMaxAgeNanos) {
        long createdAtNanos = streamRuntime.provisionalCreatedAtNanos();
        return createdAtNanos != 0L
                && provisionalOpenMaxAgeNanos > 0L
                && RuntimeFlow.elapsedExceeds(nowNanos, createdAtNanos, provisionalOpenMaxAgeNanos);
    }

    private void reclaimUnseenLocalStreamsLocked(Deque<StreamRuntime> deque,
                                                 long watermark,
                                                 long peerGoAwayBidi,
                                                 long peerGoAwayUni) {
        StreamRuntime streamRuntime;
        while ((streamRuntime = deque.peekLast()) != null && streamRuntime.streamIdInternal() > watermark) {
            deque.removeLast();
            streamRuntime.setUnseenLocalTrackedLocked(false);
            if (!streamRuntime.shouldReclaimUnseenLocalLocked(peerGoAwayBidi, peerGoAwayUni)) {
                continue;
            }
            streamRuntime.abortFromPeerLocked(ErrorCode.REFUSED_STREAM.code(), "", 0L);
            this.owner.maybeCompactStreamLocked(streamRuntime);
        }
    }

    private boolean reclaimProvisionalsLocked(Deque<StreamRuntime> deque, long nextLocalId, long peerWatermark) {
        int available = provisionalAvailableCount(nextLocalId, peerWatermark);
        boolean changed = false;
        while (deque.size() > available) {
            StreamRuntime streamRuntime = deque.peekLast();
            if (streamRuntime == null) {
                return changed;
            }
            this.failProvisionalTailLocked(
                    deque,
                    streamRuntime,
                    new io.zmux.ZmuxException(
                            ErrorCode.REFUSED_STREAM.code(),
                            "open",
                            "peer GOAWAY forbids new streams",
                            ZmuxErrorScope.SESSION,
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.BOTH,
                            ZmuxTerminationKind.GRACEFUL
                    ),
                    true
            );
            changed = true;
        }
        return changed;
    }

    private void reclaimGracefulCloseLocalStreamsLocked(Deque<StreamRuntime> deque) {
        Iterator<StreamRuntime> iterator = deque.iterator();
        while (iterator.hasNext()) {
            StreamRuntime streamRuntime = iterator.next();
            if (streamRuntime == null || streamRuntime.openedOnWire()) {
                continue;
            }
            iterator.remove();
            streamRuntime.setUnseenLocalTrackedLocked(false);
            streamRuntime.abortFromLocalLocked(ErrorCode.REFUSED_STREAM.code(), "");
            this.owner.maybeCompactStreamLocked(streamRuntime);
        }
    }

    private void rejectAllProvisionalsLocked(Deque<StreamRuntime> deque) {
        StreamRuntime streamRuntime;
        while ((streamRuntime = deque.peekLast()) != null) {
            this.failProvisionalTailLocked(
                    deque,
                    streamRuntime,
                    new ApplicationError(
                            ErrorCode.REFUSED_STREAM.code(),
                            "",
                            ZmuxErrorScope.STREAM,
                            ZmuxErrorSource.LOCAL,
                            ZmuxErrorDirection.BOTH,
                            ZmuxTerminationKind.SESSION_TERMINATION
                    ),
                    false
            );
        }
    }

    private Deque<StreamRuntime> provisionalQueueLocked(boolean bidirectional) {
        return bidirectional ? this.provisionalBidi : this.provisionalUni;
    }

    private Deque<StreamRuntime> unseenLocalQueueLocked(boolean bidirectional) {
        return bidirectional ? this.unseenLocalBidi : this.unseenLocalUni;
    }

    interface Owner {
        void onStreamOpenInfoUpdatedLocked(int previousLength, int nextLength);

        void releaseStreamPeerReasonBudgetLocked(StreamRuntime streamRuntime);

        void maybeCompactStreamLocked(StreamRuntime streamRuntime);

        void recordProvisionalOpenExpiredLocked();

        void notifyLockWaiters();
    }
}
