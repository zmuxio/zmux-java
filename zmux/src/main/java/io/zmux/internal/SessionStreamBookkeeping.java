package io.zmux.internal;

final class SessionStreamBookkeeping {
    private long acceptedStreams;
    private long activeLocalBidi;
    private long activeLocalUni;
    private long activePeerBidi;
    private long activePeerUni;
    private long gracefulCloseBlockingStreams;

    void recordAcceptedStreamLocked() {
        this.acceptedStreams = SessionRuntime.saturatingAdd(this.acceptedStreams, 1L);
    }

    long acceptedStreamsLocked() {
        return this.acceptedStreams;
    }

    long activeLocalCountLocked(boolean bidirectional) {
        return bidirectional ? this.activeLocalBidi : this.activeLocalUni;
    }

    long activePeerCountLocked(boolean bidirectional) {
        return bidirectional ? this.activePeerBidi : this.activePeerUni;
    }

    void onLocalOpenedLocked(boolean bidirectional) {
        if (bidirectional) {
            this.activeLocalBidi = SessionRuntime.saturatingAdd(this.activeLocalBidi, 1L);
        } else {
            this.activeLocalUni = SessionRuntime.saturatingAdd(this.activeLocalUni, 1L);
        }
    }

    void onPeerOpenedLocked(boolean bidirectional) {
        if (bidirectional) {
            this.activePeerBidi = SessionRuntime.saturatingAdd(this.activePeerBidi, 1L);
        } else {
            this.activePeerUni = SessionRuntime.saturatingAdd(this.activePeerUni, 1L);
        }
    }

    void onStreamFullyClosedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return;
        }
        if (streamRuntime.openedLocally()) {
            if (streamRuntime.bidirectional()) {
                this.activeLocalBidi = Math.max(0L, this.activeLocalBidi - 1L);
            } else {
                this.activeLocalUni = Math.max(0L, this.activeLocalUni - 1L);
            }
            return;
        }
        if (streamRuntime.bidirectional()) {
            this.activePeerBidi = Math.max(0L, this.activePeerBidi - 1L);
        } else {
            this.activePeerUni = Math.max(0L, this.activePeerUni - 1L);
        }
    }

    boolean hasGracefulCloseBlockingStreamsLocked() {
        return this.gracefulCloseBlockingStreams > 0L;
    }

    long gracefulCloseBlockingStreamsLocked() {
        return this.gracefulCloseBlockingStreams;
    }

    boolean onStreamGracefulCloseBlockingChangedLocked(boolean previous, boolean current) {
        if (previous == current) {
            return false;
        }
        if (current) {
            this.gracefulCloseBlockingStreams = SessionRuntime.saturatingAdd(this.gracefulCloseBlockingStreams, 1L);
        } else {
            this.gracefulCloseBlockingStreams = Math.max(0L, this.gracefulCloseBlockingStreams - 1L);
        }
        return true;
    }

    void registerGracefulCloseBlockingLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.blocksGracefulSessionCloseLocked()) {
            return;
        }
        this.gracefulCloseBlockingStreams = SessionRuntime.saturatingAdd(this.gracefulCloseBlockingStreams, 1L);
    }

    void unregisterGracefulCloseBlockingLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.blocksGracefulSessionCloseLocked()) {
            return;
        }
        this.gracefulCloseBlockingStreams = Math.max(0L, this.gracefulCloseBlockingStreams - 1L);
    }

    void clear() {
        this.acceptedStreams = 0L;
        this.activeLocalBidi = 0L;
        this.activeLocalUni = 0L;
        this.activePeerBidi = 0L;
        this.activePeerUni = 0L;
        this.gracefulCloseBlockingStreams = 0L;
    }
}
