package io.zmux.runtime;

final class StreamReceiveWindowState {
    private long recvAdvertisedLimit;
    private long initialReceiveWindow;
    private long recvReceivedBytes;
    private boolean creditGrantedSinceBlocked;

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
        if (value > recvAdvertisedLimit) {
            recvAdvertisedLimit = value;
            creditGrantedSinceBlocked = true;
        }
    }

    boolean takeCreditGrantedSinceBlocked() {
        boolean granted = creditGrantedSinceBlocked;
        creditGrantedSinceBlocked = false;
        return granted;
    }
}
