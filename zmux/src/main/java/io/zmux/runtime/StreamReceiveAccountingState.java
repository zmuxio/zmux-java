package io.zmux.runtime;

final class StreamReceiveAccountingState {
    private long recvPending;
    private long lateDataReceived;
    private long lateDataOutstandingCredit;

    long recvPending() {
        return recvPending;
    }

    void addRecvPending(long value) {
        if (value <= 0L) {
            return;
        }
        recvPending = SessionRuntime.saturatingAdd(recvPending, value);
    }

    void clearRecvPending() {
        recvPending = 0L;
    }

    long lateDataReceived() {
        return lateDataReceived;
    }

    void recordLateDataReceived(int value) {
        if (value <= 0) {
            return;
        }
        lateDataReceived = SessionRuntime.saturatingAdd(lateDataReceived, value);
    }

    long lateDataOutstandingCredit() {
        return lateDataOutstandingCredit;
    }

    void captureLateDataOutstandingCredit(long value) {
        lateDataOutstandingCredit = Math.max(lateDataOutstandingCredit, value);
    }
}
