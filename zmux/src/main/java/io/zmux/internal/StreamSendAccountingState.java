package io.zmux.internal;

final class StreamSendAccountingState {
    private boolean localSendStarted;
    private long peerSendLimit;
    private long reservedSendBytes;
    private long queuedDataBytes;
    private long blockedAt = -1L;
    private long sentBytes;
    private boolean blockedQueued;

    boolean localSendStarted() {
        return localSendStarted;
    }

    void markLocalSendStarted() {
        localSendStarted = true;
    }

    long peerSendLimit() {
        return peerSendLimit;
    }

    void initializePeerSendLimit(long value) {
        peerSendLimit = Math.max(0L, value);
    }

    void raisePeerSendLimit(long value) {
        peerSendLimit = Math.max(peerSendLimit, value);
        clearBlocked();
    }

    long reservedSendBytes() {
        return reservedSendBytes;
    }

    void reserveSendBytes(int value) {
        if (value <= 0) {
            return;
        }
        reservedSendBytes = SessionRuntime.saturatingAdd(reservedSendBytes, value);
    }

    void releaseReservedSendBytes(int value) {
        if (value <= 0) {
            return;
        }
        reservedSendBytes = Math.max(0L, reservedSendBytes - value);
    }

    long queuedDataBytes() {
        return queuedDataBytes;
    }

    void reserveQueuedDataBytes(int value) {
        if (value <= 0) {
            return;
        }
        queuedDataBytes = SessionRuntime.saturatingAdd(queuedDataBytes, value);
    }

    void releaseQueuedDataBytes(int value) {
        if (value <= 0) {
            return;
        }
        queuedDataBytes = Math.max(0L, queuedDataBytes - value);
    }

    boolean blockedQueued() {
        return blockedQueued;
    }

    long blockedAt() {
        return blockedAt;
    }

    void markBlockedQueued(long value) {
        blockedQueued = true;
        blockedAt = value;
    }

    void clearBlocked() {
        blockedQueued = false;
        blockedAt = -1L;
    }

    long sentBytes() {
        return sentBytes;
    }

    void commitReservedSendBytes(int value) {
        if (value < 0) {
            return;
        }
        reservedSendBytes = Math.max(0L, reservedSendBytes - value);
        sentBytes = SessionRuntime.saturatingAdd(sentBytes, value);
        clearBlocked();
    }

    void clearPendingBufferedState() {
        reservedSendBytes = 0L;
        queuedDataBytes = 0L;
        clearBlocked();
    }
}
