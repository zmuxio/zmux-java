package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class SessionReaderCoordinator {
    private final Owner owner;
    private final SessionReceiveWindowUpdater receiveWindowUpdater;
    private final SessionLateDataHandler lateDataHandler;

    SessionReaderCoordinator(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.receiveWindowUpdater = new SessionReceiveWindowUpdater(owner);
        this.lateDataHandler = new SessionLateDataHandler(owner);
    }

    private static IOException transportReadFailure(IOException error) {
        if (ZmuxErrors.details(error) != null) {
            return error;
        }
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "read",
                "zmux: transport read failed",
                error,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private boolean ignorePeerNonCloseFrame(FrameType frameType) {
        synchronized (this.owner.lock()) {
            return this.owner.ignorePeerNonCloseFrameLocked(frameType);
        }
    }

    private boolean ignorePeerCloseFrame() {
        synchronized (this.owner.lock()) {
            return this.owner.ignorePeerCloseFrameLocked();
        }
    }

    private boolean shouldIgnoreInboundFrameLocked(FrameType frameType) {
        return frameType == FrameType.CLOSE
                ? this.owner.ignorePeerCloseFrameLocked()
                : this.owner.ignorePeerNonCloseFrameLocked(frameType);
    }

    private boolean shouldContinueReadLoop() {
        synchronized (this.owner.lock()) {
            return !this.owner.ignorePeerCloseFrameLocked()
                    || !this.owner.ignorePeerNonCloseFrameLocked(FrameType.DATA);
        }
    }

    private FrameCodec.ErrorPayload parseErrorPayloadForPeerNonCloseFrame(FrameCodec.Frame frame) throws IOException {
        if (this.ignorePeerNonCloseFrame(frame.type())) {
            return null;
        }
        try {
            return FrameCodec.parseErrorPayload(frame.payload());
        } catch (IOException error) {
            if (this.ignorePeerNonCloseFrame(frame.type())) {
                return null;
            }
            throw error;
        }
    }

    private Varint62.Decoded parseVarintForPeerNonCloseFrame(FrameCodec.Frame frame) throws IOException {
        if (this.ignorePeerNonCloseFrame(frame.type())) {
            return null;
        }
        try {
            return Varint62.decode(frame.payload(), 0);
        } catch (IOException error) {
            if (this.ignorePeerNonCloseFrame(frame.type())) {
                return null;
            }
            throw error;
        }
    }

    private FrameCodec.DataPayload parseDataPayloadForPeerNonCloseFrame(FrameCodec.Frame frame) throws IOException {
        if (this.ignorePeerNonCloseFrame(frame.type())) {
            return null;
        }
        try {
            return FrameCodec.parseDataPayloadView(frame.payload(), frame.flags());
        } catch (IOException error) {
            if (this.ignorePeerNonCloseFrame(frame.type())) {
                return null;
            }
            throw error;
        }
    }

    void run() {
        try {
            while (this.shouldContinueReadLoop()) {
                FrameEnvelopeCodec.InboundFrame frame = FrameEnvelopeCodec.readInboundFrame(
                        this.owner.input(),
                        this.owner.limits(),
                        this.owner.inboundPayloadPool()
                );
                try {
                    boolean ignored;
                    synchronized (this.owner.lock()) {
                        ignored = this.shouldIgnoreInboundFrameLocked(frame.type());
                        if (!ignored) {
                            this.owner.incrementReceivedFramesLocked();
                            long nowNanos = System.nanoTime();
                            this.owner.recordInboundBudgetsLocked(frame);
                            this.owner.noteInboundFrameLocked(nowNanos);
                            this.owner.reapExpiredHiddenControlStateLocked(nowNanos);
                        }
                    }
                    if (ignored) {
                        continue;
                    }
                    this.handleFrame(frame);
                    this.owner.emitPendingEvents();
                } finally {
                    frame.release();
                }
            }
        } catch (IOException error) {
            synchronized (this.owner.lock()) {
                if (this.owner.state() == SessionState.CLOSING
                        || this.owner.state() == SessionState.CLOSED
                        || this.owner.state() == SessionState.FAILED) {
                    this.owner.finishSessionLocked(
                            null,
                            this.owner.state() == SessionState.FAILED ? SessionState.FAILED : SessionState.CLOSED
                    );
                    return;
                }
            }
            this.owner.failSession(SessionReaderCoordinator.transportReadFailure(error));
        } finally {
            this.owner.emitPendingEvents();
        }
    }

    void handleDataFrame(FrameCodec.Frame frame) throws IOException {
        this.handleDataFrame(frame, null);
    }

    void handleStopSendingFrame(FrameCodec.Frame frame) throws IOException {
        FrameCodec.ErrorPayload errorPayload = this.parseErrorPayloadForPeerNonCloseFrame(frame);
        if (errorPayload == null) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            if (this.owner.hasTerminalMarkerLocked(frame.streamId())) {
                return;
            }

            StreamRuntime streamRuntime = this.requireLiveStreamLocked(frame.streamId(), "handle STOP_SENDING");
            if (streamRuntime == null) {
                return;
            }
            if (!streamRuntime.localSend()) {
                this.abortStreamStateLocked(streamRuntime);
                return;
            }
            this.owner.markPeerVisibleLocked(streamRuntime);
            if (streamRuntime.shouldIgnorePeerStopSendingLocked()) {
                this.owner.recordNoOpControlLocked("handle STOP_SENDING");
                this.owner.maybeCompactStreamLocked(streamRuntime);
                this.owner.notifyLockWaiters();
                return;
            }

            String retainedReason =
                    this.owner.retainPeerReasonLocked(streamRuntime.sendStopReasonBytesLocked(), errorPayload.reason());
            boolean sendWasOpen = streamRuntime.stopSendingFromPeerLocked(
                    errorPayload.code(),
                    retainedReason,
                    SessionRuntime.utf8EncodedLength(retainedReason)
            );
            this.owner.clearNoOpControlLocked();
            if (sendWasOpen && !this.owner.tryGracefulStopSendingLocked(streamRuntime)) {
                byte[] resetPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.CANCELLED.code(), "");
                streamRuntime.concludeStopSendingWithResetLocked();
                this.owner.enqueueResetLocked(streamRuntime, ErrorCode.CANCELLED.code(), resetPayload);
            }
            this.owner.maybeCompactStreamLocked(streamRuntime);
            this.owner.notifyLockWaiters();
        }
    }

    void handleGoAwayFrame(FrameCodec.Frame frame) throws IOException {
        if (this.ignorePeerNonCloseFrame(frame.type())) {
            return;
        }
        FrameCodec.GoAwayPayload goAwayPayload;
        try {
            goAwayPayload = FrameCodec.parseGoAwayPayload(frame.payload());
        } catch (IOException error) {
            if (this.ignorePeerNonCloseFrame(frame.type())) {
                return;
            }
            throw error;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            long previousBidi = this.owner.peerGoAwayBidi();
            long previousUni = this.owner.peerGoAwayUni();
            this.validateIncomingGoAwayWatermarkLocked(goAwayPayload.lastAcceptedBidi(), true);
            this.validateIncomingGoAwayWatermarkLocked(goAwayPayload.lastAcceptedUni(), false);
            if (goAwayPayload.lastAcceptedBidi() > previousBidi
                    || goAwayPayload.lastAcceptedUni() > previousUni) {
                throw this.owner.sessionError(
                        ErrorCode.PROTOCOL,
                        "handle GOAWAY",
                        "GOAWAY watermarks must be non-increasing",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }

            long oldBytes = this.owner.peerGoAwayError() == null
                    ? 0L
                    : SessionRuntime.utf8EncodedLength(this.owner.peerGoAwayError().reason());
            String retainedReason = this.owner.retainPeerReasonLocked(oldBytes, goAwayPayload.reason());
            this.owner.setPeerGoAwayError(new ApplicationError(
                    goAwayPayload.code(),
                    retainedReason,
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.GRACEFUL
            ));
            boolean changed = goAwayPayload.lastAcceptedBidi() < previousBidi
                    || goAwayPayload.lastAcceptedUni() < previousUni;
            if (!changed) {
                this.owner.recordNoOpControlLocked("handle GOAWAY");
                this.owner.notifyLockWaiters();
                return;
            }
            this.owner.setPeerGoAwayBidi(goAwayPayload.lastAcceptedBidi());
            this.owner.setPeerGoAwayUni(goAwayPayload.lastAcceptedUni());
            this.owner.clearNoOpControlLocked();
            if (this.owner.state() == SessionState.READY) {
                this.owner.setState(SessionState.DRAINING);
            }
            this.owner.reclaimUnseenLocalStreamsLocked();
            this.owner.reclaimProvisionalsLocked();
            this.owner.notifyLockWaiters();
        }
    }

    void onReadDiscardLocked(StreamRuntime streamRuntime, boolean acceptQueuedStream) {
        if (this.receiveWindowUpdater.maybeReplenishReceiveLocked(acceptQueuedStream ? streamRuntime : null, false)) {
            this.owner.notifyWriterWaiters();
        }
    }

    void retryReceiveReplenishLocked() {
        this.receiveWindowUpdater.retryReceiveReplenishLocked();
    }

    void onReadBufferAddedLocked(StreamRuntime streamRuntime, int length, int storageBytes) {
        if (streamRuntime == null || length <= 0) {
            return;
        }
        this.owner.setBufferedReceiveBytes(SessionRuntime.saturatingAdd(this.owner.bufferedReceiveBytes(), length));
        this.owner.setBufferedReceiveStorageBytes(SessionRuntime.saturatingAdd(
                this.owner.bufferedReceiveStorageBytes(),
                Math.max(length, storageBytes)
        ));
        if (!streamRuntime.acceptQueued()) {
            return;
        }
        this.owner.addAcceptQueuedBytesLocked(streamRuntime, length);
    }

    void onReadBufferReleasedLocked(StreamRuntime streamRuntime, long length, long releasedStorageBytes) {
        if (length <= 0) {
            return;
        }
        long previousTracked = releasedStorageBytes > 0 ? this.owner.trackedSessionMemoryLocked() : -1L;
        this.owner.setBufferedReceiveBytes(Math.max(0L, this.owner.bufferedReceiveBytes() - length));
        this.owner.setBufferedReceiveStorageBytes(Math.max(
                0L,
                this.owner.bufferedReceiveStorageBytes() - Math.max(0L, releasedStorageBytes)
        ));
        this.owner.setRecvSessionPending(SessionRuntime.saturatingAdd(this.owner.recvSessionPending(), length));
        if (streamRuntime == null) {
            if (previousTracked >= 0L && this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
                this.owner.notifyStreamWriteWaiters();
            }
            return;
        }
        if (streamRuntime.acceptQueued()) {
            this.owner.releaseAcceptQueuedBytesLocked(streamRuntime, length);
        }
        if (streamRuntime.localReceive() && !streamRuntime.recvStoppedOrTerminal()) {
            streamRuntime.addRecvPendingLocked(length);
        }
        if (previousTracked >= 0L && this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyStreamWriteWaiters();
        }
    }

    void handleResetFrame(FrameCodec.Frame frame) throws IOException {
        FrameCodec.ErrorPayload errorPayload = this.parseErrorPayloadForPeerNonCloseFrame(frame);
        if (errorPayload == null) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            if (this.owner.hasTerminalMarkerLocked(frame.streamId())) {
                return;
            }

            StreamRuntime streamRuntime = this.requireLiveStreamLocked(frame.streamId(), "handle RESET");
            if (streamRuntime == null) {
                return;
            }
            if (!streamRuntime.localReceive()) {
                this.abortStreamStateLocked(streamRuntime);
                return;
            }
            this.owner.markPeerVisibleLocked(streamRuntime);
            if (streamRuntime.shouldIgnorePeerResetLocked()) {
                this.owner.recordNoOpControlLocked("handle RESET");
                this.owner.maybeCompactStreamLocked(streamRuntime);
                this.owner.notifyLockWaiters();
                return;
            }

            String retainedReason =
                    this.owner.retainPeerReasonLocked(streamRuntime.recvResetReasonBytesLocked(), errorPayload.reason());
            this.owner.noteResetReasonLocked(errorPayload.code());
            streamRuntime.resetFromPeerLocked(
                    errorPayload.code(),
                    retainedReason,
                    SessionRuntime.utf8EncodedLength(retainedReason)
            );
            this.owner.clearNoOpControlLocked();
            this.owner.recordVisibleTerminalChurnLocked(streamRuntime);
            this.onReadDiscardLocked(streamRuntime, false);
            this.owner.maybeCompactStreamLocked(streamRuntime);
            this.owner.notifyLockWaiters();
        }
    }

    void handleAbortFrame(FrameCodec.Frame frame) throws IOException {
        FrameCodec.ErrorPayload errorPayload = this.parseErrorPayloadForPeerNonCloseFrame(frame);
        if (errorPayload == null) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            StreamRuntime streamRuntime = this.owner.liveStreamLocked(frame.streamId());
            if (streamRuntime == null) {
                if (this.owner.hasTerminalMarkerLocked(frame.streamId())) {
                    return;
                }
                this.validatePeerOpeningStreamIdLocked(frame.streamId(), "handle ABORT");
                if (this.peerOpenRefusedByLocalGoAwayLocked(frame.streamId())) {
                    this.owner.refusePeerOpeningStreamLocked(frame.streamId(), false, true);
                    return;
                }
                this.validatePeerStreamSequenceLocked(frame.streamId());
                if (!this.peerStreamWithinLimitLocked(SessionRuntime.streamIsBidi(frame.streamId()))) {
                    this.owner.refusePeerOpeningStreamLocked(frame.streamId(), true, true);
                    return;
                }
                this.owner.recordAcceptedPeerStreamLocked(frame.streamId());
                long nowNanos = System.nanoTime();
                this.owner.recordHiddenAbortChurnLocked(nowNanos);
                String retainedReason = this.owner.retainPeerReasonLocked(0L, errorPayload.reason());
                this.owner.noteAbortReasonLocked(errorPayload.code());
                this.owner.retainHiddenAbortTombstoneLocked(
                        frame.streamId(),
                        errorPayload.code(),
                        retainedReason,
                        nowNanos
                );
                this.owner.releasePeerReasonBytesLocked(SessionRuntime.utf8EncodedLength(retainedReason));
                return;
            }

            if (streamRuntime.shouldIgnorePeerAbortLocked()) {
                this.owner.recordNoOpControlLocked("handle ABORT");
                this.owner.maybeCompactStreamLocked(streamRuntime);
                this.owner.notifyLockWaiters();
                return;
            }
            this.owner.markPeerVisibleLocked(streamRuntime);

            String retainedReason =
                    this.owner.retainPeerReasonLocked(streamRuntime.recvAbortReasonBytesLocked(), errorPayload.reason());
            this.owner.noteAbortReasonLocked(errorPayload.code());
            streamRuntime.abortFromPeerLocked(
                    errorPayload.code(),
                    retainedReason,
                    SessionRuntime.utf8EncodedLength(retainedReason)
            );
            this.owner.clearNoOpControlLocked();
            this.owner.recordVisibleTerminalChurnLocked(streamRuntime);
            this.onReadDiscardLocked(streamRuntime, false);
            this.owner.maybeCompactStreamLocked(streamRuntime);
            this.owner.notifyLockWaiters();
        }
    }

    void handleExtFrame(FrameCodec.Frame frame) throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
        }

        Varint62.Decoded subtype;
        try {
            subtype = Varint62.decode(frame.payload(), 0);
        } catch (IOException error) {
            synchronized (this.owner.lock()) {
                if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                    return;
                }
            }
            throw error;
        }
        if (subtype.value() != Protocol.EXT_PRIORITY_UPDATE) {
            return;
        }

        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            if (!Protocol.supportsPriorityUpdate(this.owner.capabilities())) {
                return;
            }
        }

        ExtPriorityUpdateParse parse;
        try {
            parse = this.parsePriorityUpdateFrame(frame.payload());
        } catch (IOException error) {
            synchronized (this.owner.lock()) {
                if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                    return;
                }
            }
            throw error;
        }
        if (!parse.priorityUpdate()) {
            return;
        }

        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            if (parse.dropped()) {
                this.owner.recordDroppedPriorityUpdateLocked();
                return;
            }
            StreamRuntime streamRuntime = this.owner.liveStreamLocked(frame.streamId());
            if (streamRuntime == null) {
                return;
            }
            boolean terminalForPriorityUpdate = streamRuntime.effectivelyFullyTerminalLocked();
            if (terminalForPriorityUpdate || streamRuntime.awaitingPeerVisibilityLocked()) {
                if (terminalForPriorityUpdate) {
                    this.owner.recordNoOpPriorityUpdateLocked();
                }
                return;
            }

            this.owner.markPeerVisibleLocked(streamRuntime);
            FrameCodec.ParsedPriorityUpdate parsedPriorityUpdate = parse.update();
            Long previousGroup = streamRuntime.groupLocked();
            boolean changed = streamRuntime.applyPriorityUpdateLocked(parsedPriorityUpdate);
            if (changed) {
                if (parsedPriorityUpdate.hasGroup()) {
                    this.owner.recordGroupRebucketLocked(streamRuntime, previousGroup);
                }
                this.owner.clearNoOpPriorityUpdateLocked();
            } else {
                this.owner.recordNoOpPriorityUpdateLocked();
            }
            if (changed) {
                this.owner.notifyWriterWaiters();
            }
        }
    }

    StreamRuntime createPeerOpenedStreamLocked(long streamId) {
        synchronized (this.owner.lock()) {
            return this.owner.createPeerOpenedStreamLocked(streamId);
        }
    }

    private void handleFrame(FrameEnvelopeCodec.InboundFrame frame) throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
        }
        if (frame.type() == FrameType.DATA) {
            this.handleDataFrame(frame.asFrame(), frame);
            return;
        }
        this.handleFrame(frame.asFrame());
    }

    private void handleFrame(FrameCodec.Frame frame) throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
        }
        switch (frame.type()) {
            case DATA:
                this.handleDataFrame(frame);
                break;
            case MAX_DATA:
                this.handleMaxDataFrame(frame);
                break;
            case STOP_SENDING:
                this.handleStopSendingFrame(frame);
                break;
            case RESET:
                this.handleResetFrame(frame);
                break;
            case ABORT:
                this.handleAbortFrame(frame);
                break;
            case PING:
                this.handlePingFrame(frame);
                break;
            case PONG:
                this.handlePongFrame(frame);
                break;
            case BLOCKED:
                this.handleBlockedFrame(frame);
                break;
            case GOAWAY:
                this.handleGoAwayFrame(frame);
                break;
            case CLOSE:
                this.handleCloseFrame(frame);
                break;
            case EXT:
                this.handleExtFrame(frame);
                break;
            default:
                break;
        }
    }

    private void handleDataFrame(FrameCodec.Frame frame, FrameEnvelopeCodec.InboundFrame retainedFrame) throws IOException {
        if (this.ignorePeerNonCloseFrame(frame.type())) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            StreamRuntime streamRuntime = this.owner.liveStreamLocked(frame.streamId());
            boolean existingStream = streamRuntime != null;
            boolean openingMetadata = (frame.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0;
            SessionTerminalBookkeeping.TerminalDataDisposition terminalDisposition =
                    streamRuntime == null ? this.owner.terminalDataDispositionForLocked(frame.streamId()) : null;
            FrameCodec.DataPayload dataPayload = null;
            if (streamRuntime == null && terminalDisposition != null) {
                dataPayload = this.parseDataPayloadForPeerNonCloseFrame(frame);
                if (dataPayload == null) {
                    return;
                }
                this.lateDataHandler.handleTerminalDataFrameLocked(frame, dataPayload.appDataLength(), terminalDisposition);
                return;
            }

            if (streamRuntime == null) {
                this.validatePeerOpeningStreamIdLocked(frame.streamId(), "handle DATA");
                if (this.peerOpenRefusedByLocalGoAwayLocked(frame.streamId())) {
                    this.owner.refusePeerOpeningStreamLocked(frame.streamId(), false, false);
                    return;
                }
                if (openingMetadata && !Protocol.supportsOpenMetadata(this.owner.capabilities())) {
                    throw this.owner.sessionError(
                            ErrorCode.PROTOCOL,
                            "handle DATA",
                            "OPEN_METADATA requires negotiated capability",
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.READ
                    );
                }
                this.validatePeerStreamSequenceLocked(frame.streamId());
                if (!this.peerStreamWithinLimitLocked(SessionRuntime.streamIsBidi(frame.streamId()))) {
                    this.owner.refusePeerOpeningStreamLocked(frame.streamId(), true, false);
                    return;
                }
                dataPayload = this.parseDataPayloadForPeerNonCloseFrame(frame);
                if (dataPayload == null) {
                    return;
                }
                streamRuntime = this.owner.createPeerOpenedStreamLocked(frame.streamId());
                this.owner.recordAcceptedPeerStreamLocked(frame.streamId());
            }

            StreamRuntime.PeerDataAction peerDataAction =
                    streamRuntime.peerDataActionLocked((frame.flags() & Protocol.FRAME_FLAG_FIN) != 0);
            if (peerDataAction != StreamRuntime.PeerDataAction.ACCEPT) {
                if (openingMetadata) {
                    throw this.owner.sessionError(
                            ErrorCode.PROTOCOL,
                            "handle DATA",
                            "OPEN_METADATA is only valid on the opening DATA frame",
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.READ
                    );
                }
                switch (peerDataAction) {
                    case IGNORE:
                    case IGNORE_AND_FIN:
                        FrameCodec.DataPayload ignoredDataPayload = this.parseDataPayloadForPeerNonCloseFrame(frame);
                        if (ignoredDataPayload == null) {
                            return;
                        }
                        int ignoredDataLength = ignoredDataPayload.appDataLength();
                        this.lateDataHandler.discardLatePeerDataLocked(streamRuntime, ignoredDataLength);
                        if (peerDataAction == StreamRuntime.PeerDataAction.IGNORE_AND_FIN) {
                            streamRuntime.finishReceiveLocked();
                        }
                        this.owner.maybeCompactStreamLocked(streamRuntime);
                        this.owner.notifyLockWaiters();
                        return;
                    case ABORT_STREAM_STATE:
                        this.abortStreamStateLocked(streamRuntime);
                        return;
                    case ABORT_STREAM_CLOSED:
                        byte[] abortPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.STREAM_CLOSED.code(), "");
                        streamRuntime.abortFromLocalLocked(ErrorCode.STREAM_CLOSED.code(), "");
                        this.owner.enqueueReadLoopAbortLocked(streamRuntime, ErrorCode.STREAM_CLOSED.code(), abortPayload);
                        this.owner.maybeCompactStreamLocked(streamRuntime);
                        this.owner.notifyLockWaiters();
                        return;
                    default:
                        break;
                }
            }

            if (dataPayload == null) {
                dataPayload = this.parseDataPayloadForPeerNonCloseFrame(frame);
                if (dataPayload == null) {
                    return;
                }
            }
            if (existingStream && openingMetadata) {
                throw this.owner.sessionError(
                        ErrorCode.PROTOCOL,
                        "handle DATA",
                        "OPEN_METADATA is only valid on the opening DATA frame",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }

            int dataLength = dataPayload.appDataLength();
            long nextSessionReceived = 0L;
            if (dataLength > 0) {
                if (RuntimeFlow.receiveWindowExceeded(
                        this.owner.recvSessionReceivedBytes(),
                        this.owner.recvSessionAdvertised(),
                        dataLength
                )) {
                    throw this.owner.sessionError(
                            ErrorCode.FLOW_CONTROL,
                            "handle DATA",
                            "session max_data exceeded",
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.READ
                    );
                }
                nextSessionReceived = RuntimeFlow.saturatingAdd(this.owner.recvSessionReceivedBytes(), dataLength);
                if (RuntimeFlow.receiveWindowExceeded(
                        streamRuntime.recvReceivedBytes(),
                        streamRuntime.recvAdvertisedLimit(),
                        dataLength
                )) {
                    byte[] abortPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.FLOW_CONTROL.code(), "");
                    if (!streamRuntime.openedLocally()) {
                        this.owner.markPeerVisibleLocked(streamRuntime);
                    }
                    streamRuntime.abortFromLocalLocked(ErrorCode.FLOW_CONTROL.code(), "");
                    this.owner.recordLocalAbortTerminalChurnLocked(streamRuntime);
                    this.owner.enqueueReadLoopAbortLocked(streamRuntime, ErrorCode.FLOW_CONTROL.code(), abortPayload);
                    this.owner.maybeCompactStreamLocked(streamRuntime);
                    return;
                }
            }

            this.owner.markPeerVisibleLocked(streamRuntime);
            if (dataLength > 0) {
                this.owner.setRecvSessionReceivedBytes(nextSessionReceived);
                this.owner.addReceivedDataBytes(dataLength);
                int storageBytes = retainedFrame == null ? frame.payload().length : retainedFrame.storageBytes();
                Runnable payloadRelease = retainedFrame == null ? null : retainedFrame.detachPayloadRelease();
                streamRuntime.receiveDataLocked(
                        dataPayload.appDataBytes(),
                        dataPayload.appDataOffset(),
                        dataPayload.appDataLength(),
                        storageBytes,
                        payloadRelease
                );
            }

            if (dataPayload.hasMetadata() && dataPayload.metadataValid()) {
                streamRuntime.applyOpenMetadataLocked(dataPayload.priority(), dataPayload.group(), dataPayload.openInfo());
            }
            if ((frame.flags() & Protocol.FRAME_FLAG_FIN) != 0) {
                streamRuntime.finishReceiveLocked();
            }
            boolean acceptedQueued = false;
            if (!streamRuntime.applicationVisible() && this.shouldQueueAcceptedStreamLocked(streamRuntime, existingStream)) {
                this.owner.enqueueAcceptedLocked(streamRuntime);
                acceptedQueued = true;
            }

            if (existingStream && dataLength == 0 && !dataPayload.hasMetadata()
                    && (frame.flags() & Protocol.FRAME_FLAG_FIN) == 0) {
                this.owner.recordNoOpZeroDataLocked();
            } else if (dataLength > 0 || dataPayload.hasMetadata()
                    || (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0) {
                this.owner.clearNoOpZeroDataLocked();
            }

            this.owner.enforceVisibleAcceptBacklogLocked();
            IOException memoryError = this.owner.sessionMemoryCapErrorLocked("handle DATA");
            if (memoryError != null) {
                throw memoryError;
            }
            this.owner.maybeCompactStreamLocked(streamRuntime);
            if (acceptedQueued) {
                this.owner.notifyAcceptWaiters();
            } else if (dataLength > 0 || (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0) {
                this.owner.notifyStreamReadWaiters();
            }
        }
    }

    private boolean shouldQueueAcceptedStreamLocked(StreamRuntime streamRuntime, boolean existingStream) {
        return streamRuntime != null && !existingStream;
    }

    private void handleMaxDataFrame(FrameCodec.Frame frame) throws IOException {
        Varint62.Decoded decoded = this.parseVarintForPeerNonCloseFrame(frame);
        if (decoded == null) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            boolean updated = false;
            if (frame.streamId() == 0L) {
                if (decoded.value() > this.owner.sessionSendLimit()) {
                    this.owner.setSessionSendLimit(decoded.value());
                    this.owner.clearBlockedFrameLocked(0L);
                    updated = true;
                }
            } else {
                StreamRuntime streamRuntime = this.owner.liveStreamLocked(frame.streamId());
                if (streamRuntime == null) {
                    if (this.owner.hasTerminalMarkerLocked(frame.streamId())) {
                        return;
                    }
                    streamRuntime = this.requireLiveStreamLocked(frame.streamId(), "handle MAX_DATA");
                }
                if (streamRuntime == null) {
                    return;
                }
                if (streamRuntime.shouldIgnorePeerMaxDataLocked()) {
                    this.owner.recordNoOpMaxDataLocked();
                    this.owner.maybeCompactStreamLocked(streamRuntime);
                    return;
                }
                if (!streamRuntime.localSend()) {
                    this.abortStreamStateLocked(streamRuntime);
                    return;
                }
                this.owner.markPeerVisibleLocked(streamRuntime);
                if (decoded.value() > streamRuntime.peerSendLimit()) {
                    streamRuntime.raisePeerSendLimitLocked(decoded.value());
                    this.owner.clearBlockedFrameLocked(streamRuntime.streamIdInternal());
                    updated = true;
                }
            }

            if (updated) {
                this.owner.clearNoOpMaxDataLocked();
                this.owner.notifyStreamWriteWaiters();
                this.owner.notifyWriterWaiters();
            } else {
                this.owner.recordNoOpMaxDataLocked();
            }
        }
    }

    private void handlePingFrame(FrameCodec.Frame frame) throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
        }
        if (frame.payload().length < 8) {
            synchronized (this.owner.lock()) {
                if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                    return;
                }
            }
            throw FrameCodec.error(
                    ErrorCode.FRAME_SIZE,
                    "handle PING",
                    "PING payload too short: " + frame.payload().length + " bytes"
            );
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            this.owner.recordInboundPingFloodLocked();
            this.owner.enqueuePongLocked(frame.payload());
            this.owner.notifyWriterWaiters();
        }
    }

    private void handlePongFrame(FrameCodec.Frame frame) throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
        }
        if (frame.payload().length < 8) {
            synchronized (this.owner.lock()) {
                if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                    return;
                }
            }
            throw FrameCodec.error(
                    ErrorCode.FRAME_SIZE,
                    "handle PONG",
                    "PONG payload too short: " + frame.payload().length + " bytes"
            );
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            long nowNanos = System.nanoTime();
            if (!this.owner.handlePongLocked(frame.payload(), nowNanos)) {
                this.owner.recordNoOpControlLocked("handle PONG");
                return;
            }
            this.owner.clearNoOpControlLocked();
        }
    }

    private void handleBlockedFrame(FrameCodec.Frame frame) throws IOException {
        if (this.parseVarintForPeerNonCloseFrame(frame) == null) {
            return;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerNonCloseFrameLocked(frame.type())) {
                return;
            }
            if (frame.streamId() != 0L) {
                if (this.owner.hasTerminalMarkerLocked(frame.streamId())) {
                    return;
                }

                StreamRuntime streamRuntime = this.requireLiveStreamLocked(frame.streamId(), "handle BLOCKED");
                if (streamRuntime == null) {
                    return;
                }
                if (streamRuntime.shouldIgnorePeerBlockedLocked()) {
                    this.owner.recordNoOpBlockedLocked();
                    this.owner.maybeCompactStreamLocked(streamRuntime);
                    this.owner.notifyLockWaiters();
                    return;
                }
                if (!streamRuntime.localReceive()) {
                    this.abortStreamStateLocked(streamRuntime);
                    return;
                }
                this.owner.markPeerVisibleLocked(streamRuntime);
                if (this.owner.recvSessionPending() != 0L || streamRuntime.recvPendingLocked() != 0L) {
                    this.owner.clearNoOpBlockedLocked();
                } else {
                    this.owner.recordNoOpBlockedLocked();
                }
                boolean windowUpdateQueued = this.receiveWindowUpdater.maybeReplenishReceiveLocked(streamRuntime, true);
                boolean windowUpdateFlushed = this.owner.flushPendingWindowUpdatesLocked(streamRuntime.streamIdInternal());
                if (windowUpdateQueued || windowUpdateFlushed) {
                    this.owner.notifyWriterWaiters();
                }
            } else {
                if (this.owner.recvSessionPending() != 0L) {
                    this.owner.clearNoOpBlockedLocked();
                } else {
                    this.owner.recordNoOpBlockedLocked();
                }
                boolean windowUpdateQueued = this.receiveWindowUpdater.maybeReplenishSessionLocked(true);
                boolean windowUpdateFlushed = this.owner.flushPendingWindowUpdatesLocked(null);
                if (windowUpdateQueued || windowUpdateFlushed) {
                    this.owner.notifyWriterWaiters();
                }
            }
        }
    }

    private void handleCloseFrame(FrameCodec.Frame frame) throws IOException {
        if (this.ignorePeerCloseFrame()) {
            return;
        }
        FrameCodec.ErrorPayload errorPayload;
        try {
            errorPayload = FrameCodec.parseErrorPayload(frame.payload());
        } catch (IOException error) {
            if (this.ignorePeerCloseFrame()) {
                return;
            }
            throw error;
        }
        synchronized (this.owner.lock()) {
            if (this.owner.ignorePeerCloseFrameLocked()) {
                return;
            }
            long oldBytes = this.owner.peerCloseError() == null
                    ? 0L
                    : SessionRuntime.utf8EncodedLength(this.owner.peerCloseError().reason());
            String retainedReason = this.owner.retainPeerReasonLocked(oldBytes, errorPayload.reason());
            ApplicationError peerCloseError = new ApplicationError(
                    errorPayload.code(),
                    retainedReason,
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.SESSION_TERMINATION
            );
            this.owner.setPeerCloseError(peerCloseError);
            this.owner.finishSessionLocked(peerCloseError, this.owner.terminalStateForSessionError(peerCloseError));
        }
    }

    private ExtPriorityUpdateParse parsePriorityUpdateFrame(byte[] payload) throws IOException {
        Varint62.Decoded subtype = Varint62.decode(payload, 0);
        if (subtype.value() != Protocol.EXT_PRIORITY_UPDATE) {
            return ExtPriorityUpdateParse.ignoredResult();
        }
        FrameCodec.ParsedPriorityUpdate parsedPriorityUpdate = FrameCodec.parsePriorityUpdatePayload(payload);
        if (!parsedPriorityUpdate.valid()) {
            return ExtPriorityUpdateParse.droppedResult();
        }
        return ExtPriorityUpdateParse.acceptedResult(parsedPriorityUpdate);
    }

    private StreamRuntime requireLiveStreamLocked(long streamId, String operation) throws IOException {
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime == null) {
            if (this.owner.hasTerminalMarkerLocked(streamId)) {
                return null;
            }
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    operation,
                    "stream not opened",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
        return streamRuntime;
    }

    private void abortStreamStateLocked(StreamRuntime streamRuntime) throws IOException {
        byte[] abortPayload = this.owner.buildControlErrorPayloadLocked(ErrorCode.STREAM_STATE.code(), "");
        streamRuntime.abortFromLocalLocked(ErrorCode.STREAM_STATE.code(), "");
        this.owner.enqueueReadLoopAbortLocked(streamRuntime, ErrorCode.STREAM_STATE.code(), abortPayload);
        this.owner.maybeCompactStreamLocked(streamRuntime);
        this.owner.notifyLockWaiters();
    }

    private void validateIncomingGoAwayWatermarkLocked(long streamId, boolean bidirectional) throws IOException {
        if (streamId == 0L) {
            return;
        }
        if (SessionRuntime.streamIsBidi(streamId) != bidirectional) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    "handle GOAWAY",
                    "GOAWAY watermark has wrong direction",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
        if (!SessionRuntime.streamIsLocal(this.owner.localRole(), streamId)) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    "handle GOAWAY",
                    "GOAWAY watermark targets non-local stream id",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    private void validatePeerOpeningStreamIdLocked(long streamId, String operation) throws IOException {
        if (streamId == 0L || SessionRuntime.streamIsLocal(this.owner.localRole(), streamId)) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    operation,
                    "invalid opening stream id",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    private boolean peerOpenRefusedByLocalGoAwayLocked(long streamId) {
        return SessionRuntime.streamIsBidi(streamId)
                ? streamId > this.owner.localGoAwayBidi()
                : streamId > this.owner.localGoAwayUni();
    }

    private boolean peerStreamWithinLimitLocked(boolean bidirectional) {
        return this.owner.peerStreamWithinLimitLocked(bidirectional);
    }

    private void validatePeerStreamSequenceLocked(long streamId) throws IOException {
        boolean bidirectional = SessionRuntime.streamIsBidi(streamId);
        long expected = bidirectional ? this.owner.nextPeerBidi() : this.owner.nextPeerUni();
        if (streamId != expected) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    "handle peer stream",
                    "peer stream id gap or reuse",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
        if (bidirectional) {
            this.owner.setNextPeerBidi(this.owner.nextPeerBidi() + 4L);
        } else {
            this.owner.setNextPeerUni(this.owner.nextPeerUni() + 4L);
        }
    }

    interface Owner {
        FrameCodec.Decoder input();

        Limits limits();

        InboundPayloadPool inboundPayloadPool();

        Object lock();

        SessionState state();

        void setState(SessionState state);

        void incrementReceivedFramesLocked();

        void recordInboundBudgetsLocked(FrameEnvelopeCodec.InboundFrame frame) throws IOException;

        void recordInboundPingFloodLocked() throws IOException;

        void noteInboundFrameLocked(long nowNanos);

        void reapExpiredHiddenControlStateLocked(long nowNanos);

        void emitPendingEvents();

        void finishSessionLocked(IOException error, SessionState sessionState);

        void failSession(IOException error);

        boolean ignorePeerNonCloseFrameLocked(FrameType frameType);

        boolean ignorePeerCloseFrameLocked();

        long capabilities();

        Role localRole();

        long localGoAwayBidi();

        long localGoAwayUni();

        StreamRuntime liveStreamLocked(long streamId);

        Iterable<StreamRuntime> liveStreamsLocked();

        boolean hasTerminalMarkerLocked(long streamId);

        SessionTerminalBookkeeping.TerminalDataDisposition terminalDataDispositionForLocked(long streamId);

        void refusePeerOpeningStreamLocked(long streamId, boolean recordTombstone, boolean hidden) throws IOException;

        StreamRuntime createPeerOpenedStreamLocked(long streamId);

        void recordAcceptedPeerStreamLocked(long streamId);

        boolean peerStreamWithinLimitLocked(boolean bidirectional);

        void reclaimUnseenLocalStreamsLocked();

        void reclaimProvisionalsLocked();

        void markPeerVisibleLocked(StreamRuntime streamRuntime) throws IOException;

        void maybeCompactStreamLocked(StreamRuntime streamRuntime);

        byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException;

        long controlPayloadLimitLocked();

        void enqueueReadLoopAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException;

        void enqueueResetLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException;

        void enqueueControlLocked(FrameCodec.Frame frame) throws IOException;

        void enqueuePongLocked(byte[] payload) throws IOException;

        boolean handlePongLocked(byte[] payload, long nowNanos);

        void recordNoOpZeroDataLocked() throws IOException;

        void clearNoOpZeroDataLocked();

        void recordNoOpMaxDataLocked() throws IOException;

        void clearNoOpMaxDataLocked();

        void recordNoOpControlLocked(String description) throws IOException;

        void clearNoOpControlLocked();

        void recordNoOpBlockedLocked() throws IOException;

        void clearNoOpBlockedLocked();

        void recordDroppedPriorityUpdateLocked();

        void recordNoOpPriorityUpdateLocked() throws IOException;

        void clearNoOpPriorityUpdateLocked();

        void recordGroupRebucketLocked(StreamRuntime streamRuntime, Long previousGroup) throws IOException;

        void noteResetReasonLocked(long code);

        void noteAbortReasonLocked(long code);

        void retainHiddenAbortTombstoneLocked(long streamId, long code, String reason, long nowNanos);

        void recordHiddenAbortChurnLocked(long nowNanos) throws IOException;

        void recordVisibleTerminalChurnLocked(StreamRuntime streamRuntime) throws IOException;

        void recordLocalAbortTerminalChurnLocked(StreamRuntime streamRuntime) throws IOException;

        void enforceVisibleAcceptBacklogLocked() throws IOException;

        IOException sessionMemoryCapErrorLocked(String operation);

        void enqueueAcceptedLocked(StreamRuntime streamRuntime);

        boolean tryGracefulStopSendingLocked(StreamRuntime streamRuntime) throws IOException;

        String retainPeerReasonLocked(long oldBytes, String reason);

        void releasePeerReasonBytesLocked(long bytes);

        void addAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long queuedBytes);

        void releaseAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long releasedBytes);

        long sessionWindowTargetLocked();

        long sessionEmergencyThresholdLocked();

        long sessionReplenishMinPendingLocked(long target);

        boolean sessionStandingGrowthAllowedLocked();

        long streamWindowTargetLocked(StreamRuntime streamRuntime);

        long streamEmergencyThresholdLocked(long target);

        long streamReplenishMinPendingLocked(long target);

        boolean streamStandingGrowthAllowedLocked(StreamRuntime streamRuntime);

        boolean queueSessionMaxDataLocked(long desiredOffset);

        boolean queueStreamMaxDataLocked(long streamId, long desiredOffset);

        boolean flushPendingWindowUpdatesLocked(Long preferredStreamId) throws IOException;

        void noteLateDataDiscardLocked(int length, LateDataCause cause);

        void onHiddenUnreadBytesDiscardedLocked(long bytes);

        long aggregateLateDataCap();

        long lateDataPerStreamCap(StreamRuntime streamRuntime);

        SessionState terminalStateForSessionError(IOException error);

        IOException sessionError(ErrorCode code,
                                 String operation,
                                 String message,
                                 ZmuxErrorSource source,
                                 ZmuxErrorDirection direction);

        long recvSessionReceivedBytes();

        void setRecvSessionReceivedBytes(long value);

        long recvSessionAdvertised();

        void setRecvSessionAdvertised(long value);

        long recvSessionPending();

        void setRecvSessionPending(long value);

        boolean receiveReplenishRetryLocked();

        void setReceiveReplenishRetryLocked(boolean value);

        void addReceivedDataBytes(long value);

        long sessionSendLimit();

        void setSessionSendLimit(long value);

        void clearBlockedFrameLocked(long streamId);

        long peerGoAwayBidi();

        void setPeerGoAwayBidi(long value);

        long peerGoAwayUni();

        void setPeerGoAwayUni(long value);

        ApplicationError peerGoAwayError();

        void setPeerGoAwayError(ApplicationError error);

        ApplicationError peerCloseError();

        void setPeerCloseError(ApplicationError error);

        long nextPeerBidi();

        void setNextPeerBidi(long value);

        long nextPeerUni();

        void setNextPeerUni(long value);

        long bufferedReceiveBytes();

        void setBufferedReceiveBytes(long value);

        long bufferedReceiveStorageBytes();

        void setBufferedReceiveStorageBytes(long value);

        long aggregateLateDataReceived();

        void setAggregateLateDataReceived(long value);

        long trackedSessionMemoryLocked();

        boolean sessionMemoryWakeNeededLocked(long previousTracked);

        void notifyAcceptWaiters();

        void notifyStreamWaiters();

        void notifyStreamReadWaiters();

        void notifyStreamWriteWaiters();

        void notifyWriterWaiters();

        void notifyLockWaiters();
    }

    private static final class ExtPriorityUpdateParse {
        private static final ExtPriorityUpdateParse IGNORED = new ExtPriorityUpdateParse(false, false, null);
        private static final ExtPriorityUpdateParse DROPPED = new ExtPriorityUpdateParse(true, true, null);

        private final boolean priorityUpdate;
        private final boolean dropped;
        private final FrameCodec.ParsedPriorityUpdate update;

        private ExtPriorityUpdateParse(boolean priorityUpdate, boolean dropped, FrameCodec.ParsedPriorityUpdate update) {
            this.priorityUpdate = priorityUpdate;
            this.dropped = dropped;
            this.update = update;
        }

        private static ExtPriorityUpdateParse ignoredResult() {
            return IGNORED;
        }

        private static ExtPriorityUpdateParse droppedResult() {
            return DROPPED;
        }

        private static ExtPriorityUpdateParse acceptedResult(FrameCodec.ParsedPriorityUpdate update) {
            return new ExtPriorityUpdateParse(true, false, update);
        }

        private boolean priorityUpdate() {
            return priorityUpdate;
        }

        private boolean dropped() {
            return dropped;
        }

        private FrameCodec.ParsedPriorityUpdate update() {
            return update;
        }
    }
}
