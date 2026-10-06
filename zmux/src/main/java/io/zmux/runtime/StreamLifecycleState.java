package io.zmux.runtime;

final class StreamLifecycleState {
    private boolean applicationVisible;
    private boolean idAssigned;
    private boolean acceptQueued;
    private boolean accepted;
    private boolean openedEventSent;
    private boolean acceptedEventSent;
    private boolean churnCounted;
    private boolean activeCounted;
    private boolean provisionalTracked;
    private boolean unseenLocalTracked;
    private long provisionalCreatedAtNanos;
    // Commit-turn waits in progress, the start of the current waiting stretch, and the total of completed
    // stretches. Waiting behind an earlier same-class opener is not idle provisional time, so it does not age
    // the stream, even when that opener is later abandoned.
    private int provisionalCommitWaiters;
    private long provisionalCommitWaitStartedAtNanos;
    private long provisionalCommitWaitedNanos;
    private long streamId;
    private long visibilitySequence;

    long streamId(boolean openedOnWire) {
        return openedOnWire ? streamId : 0L;
    }

    long streamIdInternal() {
        return streamId;
    }

    void assignStreamId(long streamId) {
        this.streamId = streamId;
        this.idAssigned = true;
    }

    boolean applicationVisible() {
        return applicationVisible;
    }

    void setApplicationVisible(boolean value) {
        applicationVisible = value;
    }

    boolean idAssigned() {
        return idAssigned;
    }

    boolean unseenLocalTracked() {
        return unseenLocalTracked;
    }

    void setUnseenLocalTracked(boolean value) {
        unseenLocalTracked = value;
    }

    boolean provisionalTracked() {
        return provisionalTracked;
    }

    void setProvisionalTracked(boolean value) {
        provisionalTracked = value;
    }

    long provisionalCreatedAtNanos() {
        return provisionalCreatedAtNanos;
    }

    void setProvisionalCreatedAtNanos(long value) {
        provisionalCreatedAtNanos = value;
    }

    /** Creation time shifted past completed commit-turn waits; 0 when not provisional. */
    long provisionalAgeOriginNanos() {
        if (provisionalCreatedAtNanos == 0L) {
            return 0L;
        }
        return provisionalCreatedAtNanos + provisionalCommitWaitedNanos;
    }

    boolean provisionalCommitWaiting() {
        return provisionalCommitWaiters > 0;
    }

    void beginProvisionalCommitWait(long nowNanos) {
        if (provisionalCommitWaiters++ == 0) {
            provisionalCommitWaitStartedAtNanos = nowNanos;
        }
    }

    void endProvisionalCommitWait(long nowNanos) {
        if (provisionalCommitWaiters == 0 || --provisionalCommitWaiters > 0) {
            return;
        }
        provisionalCommitWaitedNanos += Math.max(0L, nowNanos - provisionalCommitWaitStartedAtNanos);
        provisionalCommitWaitStartedAtNanos = 0L;
    }

    boolean acceptQueued() {
        return acceptQueued;
    }

    void setAcceptQueued(boolean value) {
        acceptQueued = value;
    }

    boolean accepted() {
        return accepted;
    }

    void markAccepted() {
        accepted = true;
    }

    boolean churnCounted() {
        return churnCounted;
    }

    void markChurnCounted() {
        churnCounted = true;
    }

    boolean activeCounted() {
        return activeCounted;
    }

    void markActiveCounted() {
        activeCounted = true;
    }

    void clearActiveCounted() {
        activeCounted = false;
    }

    long visibilitySequence() {
        return visibilitySequence;
    }

    void setVisibilitySequence(long value) {
        visibilitySequence = value;
    }

    boolean openedEventSent() {
        return openedEventSent;
    }

    void markOpenedEventSent() {
        openedEventSent = true;
    }

    boolean acceptedEventSent() {
        return acceptedEventSent;
    }

    void markAcceptedEventSent() {
        acceptedEventSent = true;
    }

    void clearPendingTracking() {
        provisionalTracked = false;
        provisionalCreatedAtNanos = 0L;
        unseenLocalTracked = false;
    }
}
