package io.zmux.internal;

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
