package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorSource;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;

import java.io.IOException;
import java.util.Objects;

final class SessionLateDataHandler {
    private final SessionReaderCoordinator.Owner owner;

    SessionLateDataHandler(SessionReaderCoordinator.Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    void handleTerminalDataFrameLocked(FrameCodec.Frame frame,
                                       int appDataLength,
                                       SessionTerminalBookkeeping.TerminalDataDisposition disposition)
            throws IOException {
        if ((frame.flags() & 0x20) != 0) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    "handle DATA",
                    "OPEN_METADATA is only valid on the opening DATA frame",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
        if (appDataLength > 0) {
            if (disposition.action() == SessionTerminalBookkeeping.LateDataAction.IGNORE) {
                this.discardTerminalLatePeerDataLocked(frame.streamId(), appDataLength, disposition.cause());
            } else {
                // DATA rejected with ABORT (after an observed FIN, or on a missing receive half) is not late tail
                // data: it only goes through the session discard-and-release path.
                this.discardRejectedPeerDataLocked(appDataLength);
            }
        }
        switch (disposition.action()) {
            case ABORT_CLOSED:
                this.owner.enqueueControlLocked(new FrameCodec.Frame(
                        FrameType.ABORT,
                        0,
                        frame.streamId(),
                        FrameCodec.buildErrorPayload(
                                ErrorCode.STREAM_CLOSED.code(),
                                "",
                                this.owner.controlPayloadLimitLocked()
                        )
                ));
                this.owner.notifyWriterWaiters();
                break;
            case ABORT_STATE:
                this.owner.enqueueControlLocked(new FrameCodec.Frame(
                        FrameType.ABORT,
                        0,
                        frame.streamId(),
                        FrameCodec.buildErrorPayload(
                                ErrorCode.STREAM_STATE.code(),
                                "",
                                this.owner.controlPayloadLimitLocked()
                        )
                ));
                this.owner.notifyWriterWaiters();
                break;
            case IGNORE:
                break;
            default:
                throw new IllegalStateException("unexpected late-data action: " + disposition.action());
        }
    }

    void discardLatePeerDataLocked(StreamRuntime streamRuntime, int length) throws IOException {
        if (length <= 0) {
            return;
        }
        this.discardPeerDataForSessionLocked(length);
        if (streamRuntime == null) {
            return;
        }
        this.owner.noteLateDataDiscardLocked(length, streamRuntime.lateDataCauseLocked());
        if (!streamRuntime.applicationVisible()) {
            this.owner.onHiddenUnreadBytesDiscardedLocked(length);
        }
        streamRuntime.recordLateDataReceivedLocked(length);
        streamRuntime.clearRecvPendingLocked();
        this.owner.addRetainedLateDataLocked(length);
        this.throwIfLateDataCapExceededLocked(
                streamRuntime.lateDataReceivedLocked() > this.owner.lateDataPerStreamCap(streamRuntime)
        );
    }

    void discardRejectedPeerDataLocked(int length) throws IOException {
        if (length <= 0) {
            return;
        }
        this.discardPeerDataForSessionLocked(length);
    }

    private void discardTerminalLatePeerDataLocked(long streamId, int length, LateDataCause cause) throws IOException {
        if (length <= 0) {
            return;
        }
        this.discardPeerDataForSessionLocked(length);
        this.owner.noteLateDataDiscardLocked(length, cause);
        this.throwIfLateDataCapExceededLocked(this.owner.recordTerminalLateDataLocked(streamId, length));
    }

    private void discardPeerDataForSessionLocked(int length) throws IOException {
        if (RuntimeFlow.receiveWindowExceeded(
                this.owner.recvSessionReceivedBytes(),
                this.owner.recvSessionAdvertised(),
                length
        )) {
            throw this.owner.sessionError(
                    ErrorCode.FLOW_CONTROL,
                    "handle DATA",
                    "session max_data exceeded",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
        long received = RuntimeFlow.saturatingAdd(this.owner.recvSessionReceivedBytes(), length);
        this.owner.setRecvSessionReceivedBytes(received);
        this.owner.addReceivedDataBytes(length);
        long desired = SessionRuntime.clampVarint62(
                SessionRuntime.saturatingAdd(this.owner.recvSessionAdvertised(), length)
        );
        if (this.owner.queueSessionMaxDataLocked(desired)) {
            this.owner.setRecvSessionAdvertised(desired);
            this.owner.notifyWriterWaiters();
        } else {
            this.owner.setRecvSessionPending(SessionRuntime.saturatingAdd(this.owner.recvSessionPending(), length));
            this.owner.setReceiveReplenishRetryLocked(true);
        }
    }

    private void throwIfLateDataCapExceededLocked(boolean perStreamCapExceeded) throws IOException {
        // The per-direction allowance already covers all stream credit outstanding when the local stop or abort
        // committed, so only a peer that ignored flow control can exceed it. The session-wide aggregate only tracks
        // retained late-tail accounting: late bytes are always discarded with session credit released, so exceeding
        // it is never a reason to fail the session.
        if (perStreamCapExceeded) {
            throw this.owner.sessionError(
                    ErrorCode.PROTOCOL,
                    "handle DATA",
                    "late-data cap exceeded",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }
}
