package io.zmux.internal;

final class StreamReceiveWindowState {
    private long recvAdvertisedLimit;
    private long initialReceiveWindow;
    private long recvReceivedBytes;

    long recvAdvertisedLimit() {
        return recvAdvertisedLimit;
    }

    long initialReceiveWindow() {
        return initialReceiveWindow;
    }

    long recvReceivedBytes() {
        return recvReceivedBytes;
    }

    void initialize(long recvAdvertisedLimit) {
        long normalized = Math.max(0L, recvAdvertisedLimit);
        this.recvAdvertisedLimit = normalized;
        this.initialReceiveWindow = normalized;
    }

    void recordReceivedBytes(int length) {
        if (length <= 0) {
            return;
        }
        recvReceivedBytes = SessionRuntime.saturatingAdd(recvReceivedBytes, length);
    }

    void raiseRecvAdvertisedLimit(long value) {
        recvAdvertisedLimit = Math.max(recvAdvertisedLimit, value);
    }
}
