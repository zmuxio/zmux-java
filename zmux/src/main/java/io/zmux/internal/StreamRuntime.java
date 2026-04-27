package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.Objects;

final class StreamRuntime implements ZmuxNativeStream {
    static final byte[] EMPTY_BYTES = new byte[0];

    private final SessionRuntime session;
    private final boolean openedLocally;
    private final boolean bidirectional;
    private final StreamMetadataState metadataState;
    private final StreamLifecycleState lifecycleState;
    private final StreamSendAccountingState sendAccountingState;
    private final StreamReceiveWindowState receiveWindowState;
    private final StreamReceiveAccountingState receiveAccountingState;
    private final StreamAdvisoryState advisoryState;
    private final StreamTerminalState terminalState;
    private final StreamHalfState halfState;
    private final StreamReadCoordinator readCoordinator;
    private final StreamWriteCoordinator writeCoordinator;
    private final StreamCloseCoordinator closeCoordinator;
    private final StreamMetadataCoordinator metadataCoordinator;
    private final StreamAdvisoryCoordinator advisoryCoordinator;
    private final StreamTerminalCoordinator terminalCoordinator;
    private final ByteArrayQueue readBuffer = new ByteArrayQueue();
    private final ZmuxNativeSendStream sendView;
    private final ZmuxNativeRecvStream recvView;

    private boolean localSend;
    private boolean localReceive;
    private long readDeadlineNanos;
    private long writeDeadlineNanos;
    private boolean gracefulCloseBlocking;

    StreamRuntime(SessionRuntime session, boolean openedLocally, boolean bidirectional, OpenOptions openOptions) {
        this.session = Objects.requireNonNull(session, "session");
        this.openedLocally = openedLocally;
        this.bidirectional = bidirectional;
        OpenOptions effectiveOpenOptions = openOptions == null ? OpenOptions.empty() : openOptions;
        this.metadataState = new StreamMetadataState(effectiveOpenOptions);
        this.lifecycleState = new StreamLifecycleState();
        this.sendAccountingState = new StreamSendAccountingState();
        this.receiveWindowState = new StreamReceiveWindowState();
        this.receiveAccountingState = new StreamReceiveAccountingState();
        this.advisoryState = new StreamAdvisoryState();
        this.terminalState = new StreamTerminalState();
        this.localSend = openedLocally || bidirectional;
        this.localReceive = bidirectional || !openedLocally;
        this.halfState = new StreamHalfState(this.localSend, this.localReceive);
        this.readCoordinator = new StreamReadCoordinator(this);
        this.writeCoordinator = new StreamWriteCoordinator(this);
        this.closeCoordinator = new StreamCloseCoordinator(this);
        this.metadataCoordinator = new StreamMetadataCoordinator(this);
        this.advisoryCoordinator = new StreamAdvisoryCoordinator(this);
        this.terminalCoordinator = new StreamTerminalCoordinator(this);
        this.sendView = localSend && !localReceive ? new NativeSendStreamView(this) : null;
        this.recvView = localReceive && !localSend ? new NativeRecvStreamView(this) : null;
        this.gracefulCloseBlocking = computeGracefulCloseBlockingLocked();
    }

    static long remainingDeadlineNanosLocked(long deadlineNanos) {
        return TimeoutBudget.remainingNanosUntil(deadlineNanos);
    }

    static ZmuxException streamLocalError(ErrorCode code,
                                          String operation,
                                          String message,
                                          ZmuxErrorDirection direction) {
        return new ZmuxException(
                code.code(),
                operation,
                message,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                direction == null ? ZmuxErrorDirection.BOTH : direction,
                ZmuxTerminationKind.UNKNOWN
        );
    }

    @Override
    public int read(byte[] dst, int offset, int length) throws IOException {
        return this.readCoordinator.read(dst, offset, length);
    }

    @Override
    public void write(byte[] src, int offset, int length) throws IOException {
        this.writeCoordinator.write(src, offset, length, false);
    }

    @Override
    public int writeFinal(byte[] src, int offset, int length) throws IOException {
        this.writeCoordinator.write(src, offset, length, true);
        return length;
    }

    @Override
    public int writevFinal(byte[]... parts) throws IOException {
        return this.writeCoordinator.writev(parts, true);
    }

    @Override
    public void setDeadline(Instant deadline) throws IOException {
        long resolved = SessionRuntime.deadlineNanos(deadline);
        synchronized (session.lock()) {
            if (session.shouldFailSessionOperationsLocked()) {
                throw session.sessionOperationErrorLocked(localReceive ? "read" : "write", session.currentErrorLocked());
            }
            boolean readChanged = false;
            boolean writeChanged = false;
            if (localReceive) {
                if (readDeadlineNanos != resolved) {
                    readDeadlineNanos = resolved;
                    readChanged = true;
                }
            }
            if (localSend) {
                if (writeDeadlineNanos != resolved) {
                    writeDeadlineNanos = resolved;
                    writeChanged = true;
                }
            }
            if (readChanged && writeChanged) {
                notifyStreamWaitersOnlyLocked();
            } else if (readChanged) {
                notifyReadWaitersLocked();
            } else if (writeChanged) {
                notifyWriteWaitersLocked();
            }
        }
    }

    @Override
    public void setReadDeadline(Instant deadline) throws IOException {
        synchronized (session.lock()) {
            if (session.shouldFailSessionOperationsLocked()) {
                throw session.sessionOperationErrorLocked("read", session.currentErrorLocked());
            }
            if (!localReceive) {
                throw new StreamNotReadableException();
            }
            long resolved = SessionRuntime.deadlineNanos(deadline);
            if (readDeadlineNanos != resolved) {
                readDeadlineNanos = resolved;
                notifyReadWaitersLocked();
            }
        }
    }

    @Override
    public void setWriteDeadline(Instant deadline) throws IOException {
        synchronized (session.lock()) {
            if (session.shouldFailSessionOperationsLocked()) {
                throw session.sessionOperationErrorLocked("write", session.currentErrorLocked());
            }
            if (!localSend) {
                throw new StreamNotWritableException();
            }
            long resolved = SessionRuntime.deadlineNanos(deadline);
            if (writeDeadlineNanos != resolved) {
                writeDeadlineNanos = resolved;
                notifyWriteWaitersLocked();
            }
        }
    }

    @Override
    public long streamId() {
        synchronized (session.lock()) {
            return lifecycleState.streamId(metadataState.openedOnWire());
        }
    }

    long streamIdInternal() {
        return lifecycleState.streamIdInternal();
    }

    Object lockInternal() {
        return session.lock();
    }

    SessionRuntime sessionInternal() {
        return session;
    }

    StreamMetadataState metadataStateInternal() {
        return metadataState;
    }

    StreamLifecycleState lifecycleStateInternal() {
        return lifecycleState;
    }

    StreamTerminalState terminalStateInternal() {
        return terminalState;
    }

    StreamHalfState halfStateInternal() {
        return halfState;
    }

    StreamAdvisoryState advisoryStateInternal() {
        return advisoryState;
    }

    StreamSendAccountingState sendAccountingStateInternal() {
        return sendAccountingState;
    }

    StreamReceiveAccountingState receiveAccountingStateInternal() {
        return receiveAccountingState;
    }

    ByteArrayQueue readBufferInternal() {
        return readBuffer;
    }

    long readDeadlineNanosInternal() {
        return readDeadlineNanos;
    }

    boolean gracefulCloseBlockingInternal() {
        return gracefulCloseBlocking;
    }

    void setGracefulCloseBlockingInternal(boolean value) {
        gracefulCloseBlocking = value;
    }

    void noteWritePayloadProgressLocked(int length) {
        if (length <= 0) {
            return;
        }
        long nowNanos = System.nanoTime();
        session.noteStreamProgressLocked(nowNanos);
        session.noteApplicationProgressLocked(nowNanos);
    }

    void noteReadPayloadProgressLocked(int length) {
        if (length <= 0) {
            return;
        }
        long nowNanos = System.nanoTime();
        session.noteStreamProgressLocked(nowNanos);
        session.noteApplicationProgressLocked(nowNanos);
    }

    void onReadBufferReleasedLocked(int readBytes, long releasedStorageBytes) {
        session.onReadBufferReleasedLocked(this, readBytes, releasedStorageBytes);
    }

    void onReadDiscardLocked(boolean acceptQueuedStream) {
        session.onReadDiscardLocked(this, acceptQueuedStream);
    }

    void notifyLockWaitersLocked() {
        session.notifyStreamWaitersLocked();
        session.notifyWriterWaitersLocked();
    }

    void notifyStreamWaitersOnlyLocked() {
        session.notifyStreamWaitersLocked();
    }

    void notifyReadWaitersLocked() {
        session.notifyStreamReadWaitersLocked();
    }

    void notifyWriteWaitersLocked() {
        session.notifyStreamWriteWaitersLocked();
    }

    void waitOnLock() throws InterruptedException {
        session.waitOnLock(SessionRuntime.LockWaitKind.READ_STREAM);
    }

    void waitOnLockNanos(long waitNanos) throws InterruptedException {
        session.waitOnLockNanos(waitNanos, SessionRuntime.LockWaitKind.READ_STREAM);
    }

    void emitPendingEvents() {
        session.emitPendingEvents();
    }

    ZmuxNativeSendStream sendView() {
        if (sendView == null) {
            throw new IllegalStateException("zmux: stream does not expose a send-only view");
        }
        return sendView;
    }

    ZmuxNativeRecvStream recvView() {
        if (recvView == null) {
            throw new IllegalStateException("zmux: stream does not expose a recv-only view");
        }
        return recvView;
    }

    @Override
    public byte[] openInfo() {
        return metadataState.openInfo();
    }

    @Override
    public StreamMetadata metadata() {
        synchronized (session.lock()) {
            return metadataState.metadata();
        }
    }

    @Override
    public void updateMetadata(MetadataUpdate update) throws IOException {
        this.metadataCoordinator.updateMetadata(update);
    }

    @Override
    public void closeRead() throws IOException {
        cancelRead(ErrorCode.CANCELLED.code());
    }

    @Override
    public void cancelRead(long code) throws IOException {
        this.readCoordinator.cancelRead(code);
    }

    @Override
    public void closeWrite() throws IOException {
        this.writeCoordinator.closeWrite();
    }

    @Override
    public void cancelWrite(long code) throws IOException {
        this.writeCoordinator.cancelWrite(code);
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        this.writeCoordinator.closeWithError(code, reason);
    }

    @Override
    public boolean openedLocally() {
        return openedLocally;
    }

    @Override
    public boolean bidirectional() {
        return bidirectional;
    }

    @Override
    public boolean readClosed() {
        synchronized (session.lock()) {
            return halfState.readClosed(localReceive);
        }
    }

    @Override
    public boolean writeClosed() {
        synchronized (session.lock()) {
            return halfState.writeClosed();
        }
    }

    @Override
    public SocketAddress localAddress() {
        SocketAddress address = session.connection().localAddress();
        if (address != null) {
            return address;
        }
        return lifecycleState.idAssigned()
                ? ZmuxSocketAddress.localStream(lifecycleState.streamIdInternal())
                : ZmuxSocketAddress.localPending();
    }

    @Override
    public SocketAddress remoteAddress() {
        SocketAddress address = session.connection().remoteAddress();
        if (address != null) {
            return address;
        }
        return lifecycleState.idAssigned()
                ? ZmuxSocketAddress.remoteStream(lifecycleState.streamIdInternal())
                : ZmuxSocketAddress.remotePending();
    }

    @Override
    public void close() throws IOException {
        this.closeCoordinator.close();
    }

    boolean localSend() {
        return localSend;
    }

    boolean localReceive() {
        return localReceive;
    }

    boolean applicationVisible() {
        return lifecycleState.applicationVisible();
    }

    void setApplicationVisibleLocked(boolean value) {
        lifecycleState.setApplicationVisible(value);
    }

    boolean idAssigned() {
        return lifecycleState.idAssigned();
    }

    boolean openedOnWire() {
        return metadataState.openedOnWire();
    }

    boolean peerVisible() {
        return metadataState.peerVisible();
    }

    boolean unseenLocalTracked() {
        return lifecycleState.unseenLocalTracked();
    }

    boolean provisionalTracked() {
        return lifecycleState.provisionalTracked();
    }

    boolean acceptQueued() {
        return lifecycleState.acceptQueued();
    }

    boolean acceptedLocked() {
        return lifecycleState.accepted();
    }

    boolean churnCountedLocked() {
        return lifecycleState.churnCounted();
    }

    long peerSendLimit() {
        return sendAccountingState.peerSendLimit();
    }

    long recvAdvertisedLimit() {
        return receiveWindowState.recvAdvertisedLimit();
    }

    long initialReceiveWindow() {
        return receiveWindowState.initialReceiveWindow();
    }

    long reservedSendBytes() {
        return sendAccountingState.reservedSendBytes();
    }

    long sentBytes() {
        return sendAccountingState.sentBytes();
    }

    long recvReceivedBytes() {
        return receiveWindowState.recvReceivedBytes();
    }

    long recvPendingLocked() {
        return receiveAccountingState.recvPending();
    }

    void initializeLocalOpenedLocked(long streamId, long peerSendLimit, long recvAdvertisedLimit) {
        lifecycleState.assignStreamId(streamId);
        this.sendAccountingState.initializePeerSendLimit(peerSendLimit);
        this.receiveWindowState.initialize(recvAdvertisedLimit);
    }

    void initializePeerOpenedLocked(long streamId,
                                    boolean localSend,
                                    boolean localReceive,
                                    long peerSendLimit,
                                    long recvAdvertisedLimit) {
        lifecycleState.assignStreamId(streamId);
        this.metadataState.markOpenedOnWire();
        this.localSend = localSend;
        this.localReceive = localReceive;
        this.halfState.initialize(localSend, localReceive);
        this.sendAccountingState.initializePeerSendLimit(peerSendLimit);
        this.receiveWindowState.initialize(recvAdvertisedLimit);
        this.gracefulCloseBlocking = computeGracefulCloseBlockingLocked();
    }

    void markOpenedOnWireLocked() {
        metadataState.markOpenedOnWire();
    }

    boolean shouldMarkPeerVisibleLocked() {
        return metadataState.shouldMarkPeerVisible(openedLocally, lifecycleState.idAssigned());
    }

    LocalOpenPhase localOpenPhaseLocked() {
        return metadataState.localOpenPhase(openedLocally);
    }

    void markPeerVisibleLocked() {
        metadataState.markPeerVisible();
    }

    boolean peerVisibleLocked() {
        return metadataState.peerVisible();
    }

    boolean openingFramePendingLocked() {
        return metadataState.openingFramePending();
    }

    void markLocalSendStartedLocked() {
        if (localSend) {
            sendAccountingState.markLocalSendStarted();
        }
    }

    long visibilitySequence() {
        return lifecycleState.visibilitySequence();
    }

    void setVisibilitySequenceLocked(long value) {
        lifecycleState.setVisibilitySequence(value);
    }

    boolean openedEventSentLocked() {
        return lifecycleState.openedEventSent();
    }

    void markOpenedEventSentLocked() {
        lifecycleState.markOpenedEventSent();
    }

    boolean acceptedEventSentLocked() {
        return lifecycleState.acceptedEventSent();
    }

    void markAcceptedEventSentLocked() {
        lifecycleState.markAcceptedEventSent();
    }

    ZmuxStreamInfo eventStreamSnapshotLocked() {
        return new ImmutableStreamInfoSnapshot(
                lifecycleState.streamIdInternal(),
                metadataState.metadata(),
                session.connection().localAddress(),
                session.connection().remoteAddress()
        );
    }

    void setAcceptQueuedLocked(boolean value) {
        lifecycleState.setAcceptQueued(value);
    }

    void markChurnCountedLocked() {
        lifecycleState.markChurnCounted();
    }

    void markAcceptedLocked() {
        lifecycleState.markAccepted();
    }

    void markOpeningFramePendingLocked() {
        metadataState.markOpeningFramePending();
    }

    void clearOpeningFramePendingLocked() {
        metadataState.clearOpeningFramePending();
    }

    void setProvisionalTrackedLocked(boolean value) {
        lifecycleState.setProvisionalTracked(value);
    }

    void setProvisionalCreatedAtNanosLocked(long value) {
        lifecycleState.setProvisionalCreatedAtNanos(value);
    }

    long provisionalCreatedAtNanos() {
        return lifecycleState.provisionalCreatedAtNanos();
    }

    void setUnseenLocalTrackedLocked(boolean value) {
        lifecycleState.setUnseenLocalTracked(value);
    }

    void markFinQueuedLocked() {
        if (halfState.markFinQueuedIfOpen() == StreamHalfState.SendState.OPEN) {
            refreshGracefulCloseBlockingLocked();
        }
    }

    void reserveSendBytesLocked(int value) {
        sendAccountingState.reserveSendBytes(value);
        refreshGracefulCloseBlockingLocked();
    }

    void releaseReservedSendBytesLocked(int value) {
        sendAccountingState.releaseReservedSendBytes(value);
        refreshGracefulCloseBlockingLocked();
    }

    long queuedDataBytesLocked() {
        return sendAccountingState.queuedDataBytes();
    }

    boolean blockedQueuedLocked() {
        return sendAccountingState.blockedQueued();
    }

    long blockedAtLocked() {
        return sendAccountingState.blockedAt();
    }

    void markBlockedQueuedLocked(long value) {
        sendAccountingState.markBlockedQueued(value);
    }

    void clearBlockedLocked() {
        sendAccountingState.clearBlocked();
    }

    void reserveQueuedDataBytesLocked(int value) {
        sendAccountingState.reserveQueuedDataBytes(value);
        refreshGracefulCloseBlockingLocked();
    }

    void releaseQueuedDataBytesLocked(int value) {
        sendAccountingState.releaseQueuedDataBytes(value);
        refreshGracefulCloseBlockingLocked();
    }

    void commitReservedSendBytesLocked(int value) {
        sendAccountingState.commitReservedSendBytes(value);
        refreshGracefulCloseBlockingLocked();
    }

    boolean shouldEmitQueuedDataLocked() {
        return shouldEmitQueuedDataLocked(false);
    }

    boolean shouldEmitQueuedDataLocked(boolean preserveAfterSendClose) {
        return halfState.shouldEmitQueuedData(preserveAfterSendClose);
    }

    void prepareLocalControlOpenerLocked(boolean replaceQueuedPayload, boolean preserveAfterSendClose) throws IOException {
        this.advisoryCoordinator.prepareLocalControlOpenerLocked(replaceQueuedPayload, preserveAfterSendClose);
    }

    void onFrameWrittenLocked(FrameCodec.Frame frame, boolean openingFrame) {
        this.advisoryCoordinator.onFrameWrittenLocked(frame, openingFrame);
    }

    void receiveDataLocked(byte[] data) {
        receiveDataLocked(data, 0, data.length);
    }

    void receiveDataLocked(byte[] data, int offset, int length) {
        receiveDataLocked(data, offset, length, data.length, null);
    }

    void receiveDataLocked(byte[] data,
                           int offset,
                           int length,
                           int storageBytes,
                           Runnable releaseAction) {
        if (length == 0) {
            if (releaseAction != null) {
                releaseAction.run();
            }
            return;
        }
        readBuffer.addRetained(data, offset, length, storageBytes, releaseAction);
        receiveWindowState.recordReceivedBytes(length);
        session.onReadBufferAddedLocked(this, length, storageBytes);
    }

    void finishReceiveLocked() {
        if (halfState.recvOpen() || halfState.recvStopSent()) {
            halfState.finishReceiveIfActive();
            refreshGracefulCloseBlockingLocked();
        }
    }

    boolean stopSendingFromPeerLocked(long code, String reason, long reasonBytes) {
        return this.terminalCoordinator.stopSendingFromPeerLocked(code, reason, reasonBytes);
    }

    void concludeStopSendingWithResetLocked() {
        this.terminalCoordinator.concludeStopSendingWithResetLocked();
    }

    boolean ignoreLateNonOpeningControlLocked() {
        return fullyTerminalLocked();
    }

    boolean shouldIgnorePeerMaxDataLocked() {
        return ignoreLateNonOpeningControlLocked();
    }

    boolean shouldIgnorePeerBlockedLocked() {
        return ignoreLateNonOpeningControlLocked();
    }

    boolean shouldIgnorePeerStopSendingLocked() {
        return halfState.shouldIgnorePeerStopSending(fullyTerminalLocked());
    }

    void resetFromPeerLocked(long code, String reason, long reasonBytes) {
        this.terminalCoordinator.resetFromPeerLocked(code, reason, reasonBytes);
    }

    boolean shouldIgnorePeerResetLocked() {
        if (ignoreLateNonOpeningControlLocked()) {
            return true;
        }
        return halfState.recvFin() || halfState.recvAbortive();
    }

    void abortFromPeerLocked(long code, String reason, long reasonBytes) {
        this.terminalCoordinator.abortFromPeerLocked(code, reason, reasonBytes);
    }

    boolean shouldIgnorePeerAbortLocked() {
        if (ignoreLateNonOpeningControlLocked()) {
            return true;
        }
        return halfState.sendAborted() || halfState.recvAborted();
    }

    void abortFromLocalLocked(long code, String reason) {
        this.terminalCoordinator.abortFromLocalLocked(code, reason);
    }

    void failLocallyLocked(IOException error) {
        this.terminalCoordinator.failLocallyLocked(error);
    }

    void closeForSessionLocked(ApplicationError error) {
        this.terminalCoordinator.closeForSessionLocked(error);
    }

    void clearSessionPendingStateLocked() {
        this.terminalCoordinator.clearSessionPendingStateLocked();
    }

    void applyOpenMetadataLocked(long priority, Long group, byte[] openInfo) {
        metadataState.applyOpenMetadataLocked(session, this, priority, group, openInfo);
    }

    boolean applyPriorityUpdateLocked(FrameCodec.ParsedPriorityUpdate update) {
        return metadataState.applyPriorityUpdateLocked(session, this, update);
    }

    int openInfoLengthLocked() {
        return metadataState.openInfoLength();
    }

    Long groupLocked() {
        return metadataState.group();
    }

    long priorityLocked() {
        return metadataState.priority();
    }

    void clearRetainedOpenInfoLocked() {
        metadataState.clearRetainedOpenInfoLocked(session, this);
    }

    long retainedPeerReasonBytesLocked() {
        return advisoryState.retainedPeerReasonBytes();
    }

    long sendStopReasonBytesLocked() {
        return advisoryState.sendStopReasonBytes();
    }

    boolean recvAbortiveLocked() {
        return halfState.recvAbortive();
    }

    LateDataCause lateDataCauseLocked() {
        if (halfState.readStopSent()) {
            return LateDataCause.CLOSE_READ;
        }
        if (halfState.recvReset()) {
            return LateDataCause.RESET;
        }
        if (halfState.recvAborted()) {
            return LateDataCause.ABORT;
        }
        return LateDataCause.NONE;
    }

    boolean needsLocalOpenerLocked() {
        return localOpenPhaseLocked().needsLocalOpener();
    }

    boolean shouldEmitOpenerFrameLocked() {
        return localOpenPhaseLocked().shouldEmitOpenerFrame();
    }

    IOException localGracefulCloseRefusedErrorLocked() {
        if (!openedLocally || peerVisibleLocked() || !session.localGracefulCloseRefusesProvisionalsLocked()) {
            return null;
        }
        IOException localError = terminalState.localError();
        if (localError != null) {
            return localError;
        }
        return new ApplicationError(
                ErrorCode.REFUSED_STREAM.code(),
                "",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    boolean canTakePendingPriorityUpdateLocked() {
        return metadataState.canTakePendingPriorityUpdate(openedLocally);
    }

    boolean sendCommittedLocked() {
        if (openedLocally) {
            return metadataState.openedOnWire();
        }
        return sendAccountingState.localSendStarted()
                || sendAccountingState.sentBytes() > 0L
                || sendAccountingState.reservedSendBytes() > 0L
                || halfState.sendFinQueued()
                || halfState.sendFin();
    }

    void armStopSendingGracefulDrainLocked(long deadlineNanos) {
        this.advisoryCoordinator.armStopSendingGracefulDrainLocked(deadlineNanos);
    }

    void clearStopSendingGracefulDrainLocked() {
        this.advisoryCoordinator.clearStopSendingGracefulDrainLocked();
    }

    long stopSendingGracefulDeadlineNanosLocked() {
        return this.advisoryCoordinator.stopSendingGracefulDeadlineNanosLocked();
    }

    boolean stopSendingGracefulExpiredLocked(long nowNanos) {
        return this.advisoryCoordinator.stopSendingGracefulExpiredLocked(nowNanos);
    }

    long recvResetReasonBytesLocked() {
        return this.advisoryCoordinator.recvResetReasonBytesLocked();
    }

    long recvAbortReasonBytesLocked() {
        return this.advisoryCoordinator.recvAbortReasonBytesLocked();
    }

    void clearRetainedPeerReasonBytesLocked() {
        this.advisoryCoordinator.clearRetainedPeerReasonBytesLocked();
    }

    void queuePeerStopGracefulFinishLocked() throws IOException {
        this.advisoryCoordinator.queuePeerStopGracefulFinishLocked();
    }

    void stagePriorityUpdateLocked(Long priority, Long group, byte[] payload) {
        this.advisoryCoordinator.stagePriorityUpdateLocked(priority, group, payload);
    }

    void clearWriteAdvisoryLocked() {
        this.advisoryCoordinator.clearWriteAdvisoryLocked();
    }

    boolean hasPendingPriorityUpdateLocked() {
        return this.advisoryCoordinator.hasPendingPriorityUpdateLocked();
    }

    boolean priorityUpdateQueuedLocked() {
        return this.advisoryCoordinator.priorityUpdateQueuedLocked();
    }

    Long pendingPriorityUpdatePriorityLocked() {
        return this.advisoryCoordinator.pendingPriorityUpdatePriorityLocked();
    }

    Long pendingPriorityUpdateGroupLocked() {
        return this.advisoryCoordinator.pendingPriorityUpdateGroupLocked();
    }

    byte[] pendingPriorityUpdatePayloadLocked() {
        return this.advisoryCoordinator.pendingPriorityUpdatePayloadLocked();
    }

    void markPriorityUpdateQueuedLocked() {
        this.advisoryCoordinator.markPriorityUpdateQueuedLocked();
    }

    void clearPriorityUpdateQueuedLocked() {
        this.advisoryCoordinator.clearPriorityUpdateQueuedLocked();
    }

    void clearPendingPriorityUpdateLocked() {
        this.advisoryCoordinator.clearPendingPriorityUpdateLocked();
    }

    boolean schedulingGroupTrackedLocked() {
        return this.advisoryCoordinator.schedulingGroupTrackedLocked();
    }

    long trackedSchedulingGroupLocked() {
        return this.advisoryCoordinator.trackedSchedulingGroupLocked();
    }

    void markSchedulingGroupTrackedLocked(long bucket) {
        this.advisoryCoordinator.markSchedulingGroupTrackedLocked(bucket);
    }

    void clearSchedulingGroupTrackedLocked() {
        this.advisoryCoordinator.clearSchedulingGroupTrackedLocked();
    }

    int writeBurstLimitLocked() {
        return WritePolicy.writeBurstLimit(metadataState.priority(), session.peerSettings().schedulerHints());
    }

    long txFragmentCapLocked(long prefixLen) {
        long baseCap = WritePolicy.fragmentCap(
                session.peerSettings().maxFramePayload(),
                prefixLen,
                metadataState.priority(),
                session.peerSettings().schedulerHints()
        );
        return WritePolicy.rateLimitedFragmentCap(
                baseCap,
                session.sendRateEstimateLocked(),
                metadataState.priority(),
                session.peerSettings().schedulerHints()
        );
    }

    void raisePeerSendLimitLocked(long value) {
        sendAccountingState.raisePeerSendLimit(value);
    }

    void raiseRecvAdvertisedLimitLocked(long value) {
        receiveWindowState.raiseRecvAdvertisedLimit(value);
    }

    void addRecvPendingLocked(long value) {
        receiveAccountingState.addRecvPending(value);
    }

    void clearRecvPendingLocked() {
        receiveAccountingState.clearRecvPending();
    }

    long lateDataReceivedLocked() {
        return receiveAccountingState.lateDataReceived();
    }

    void recordLateDataReceivedLocked(int value) {
        receiveAccountingState.recordLateDataReceived(value);
    }

    boolean recvStoppedOrTerminal() {
        return halfState.recvStoppedOrTerminal();
    }

    boolean shouldFlushPendingStreamMaxDataLocked() {
        return lifecycleState.idAssigned()
                && localReceive
                && !halfState.readStopSent()
                && !halfState.recvTerminal()
                && !awaitingPeerVisibilityLocked();
    }

    boolean shouldRetainPendingStreamMaxDataLocked() {
        return lifecycleState.idAssigned()
                && localReceive
                && !halfState.readStopSent()
                && !halfState.recvTerminal()
                && awaitingPeerVisibilityLocked();
    }

    boolean shouldFlushPendingStreamBlockedLocked() {
        return lifecycleState.idAssigned()
                && localSend
                && halfState.effectiveSendOpen()
                && !awaitingPeerVisibilityLocked();
    }

    boolean shouldRetainPendingStreamBlockedLocked() {
        return lifecycleState.idAssigned()
                && localSend
                && halfState.effectiveSendOpen()
                && awaitingPeerVisibilityLocked();
    }

    boolean awaitingPeerVisibilityLocked() {
        return this.advisoryCoordinator.awaitingPeerVisibilityLocked();
    }

    boolean blocksGracefulSessionCloseLocked() {
        return this.advisoryCoordinator.blocksGracefulSessionCloseLocked();
    }

    private boolean computeGracefulCloseBlockingLocked() {
        return this.advisoryCoordinator.computeGracefulCloseBlockingLocked();
    }

    void refreshGracefulCloseBlockingLocked() {
        this.advisoryCoordinator.refreshGracefulCloseBlockingLocked();
    }

    IOException operationErrorLocked() {
        return terminalState.operationError(halfState);
    }

    boolean shouldReclaimUnseenLocalLocked(long peerGoAwayBidi, long peerGoAwayUni) {
        if (!awaitingPeerVisibilityLocked()) {
            return false;
        }
        return bidirectional
                ? lifecycleState.streamIdInternal() > peerGoAwayBidi
                : lifecycleState.streamIdInternal() > peerGoAwayUni;
    }

    PeerDataAction peerDataActionLocked(boolean fin) {
        return halfState.peerDataAction(localReceive, fullyTerminalLocked(), fin);
    }

    boolean fullyTerminalLocked() {
        return halfState.fullyTerminal();
    }

    boolean effectivelyFullyTerminalLocked() {
        return halfState.effectivelyFullyTerminal();
    }

    boolean readBufferEmptyLocked() {
        return readBuffer.isEmpty();
    }

    long readBufferSizeLocked() {
        return readBuffer.sizeLong();
    }

    boolean receiveGracefulLocked() {
        return halfState.receiveGraceful();
    }

    long terminalCodeLocked() {
        return terminalState.terminalCode();
    }

    boolean localAbortLocked() {
        return terminalState.localAbort();
    }

    String terminalReasonLocked() {
        return terminalState.terminalReason();
    }

    long remainingWriteDeadlineNanosLocked() {
        return remainingDeadlineNanosLocked(writeDeadlineNanos);
    }

    void setWriteDeadlineNanos(long deadlineNanos) {
        synchronized (session.lock()) {
            if (!localSend) {
                throw new IllegalStateException("zmux: stream does not expose a send side");
            }
            long resolved = Math.max(0L, deadlineNanos);
            if (writeDeadlineNanos != resolved) {
                writeDeadlineNanos = resolved;
                notifyWriteWaitersLocked();
            }
        }
    }

    void ensureWritableLocked() throws IOException {
        this.writeCoordinator.ensureWritableLocked();
    }

    long initialPeerSendLimitForPendingOpenLocked() {
        if (lifecycleState.idAssigned()) {
            return sendAccountingState.peerSendLimit();
        }
        if (!openedLocally || !localSend) {
            return sendAccountingState.peerSendLimit();
        }
        return bidirectional
                ? session.peerSettings().initialMaxStreamDataBidiPeerOpened()
                : session.peerSettings().initialMaxStreamDataUni();
    }

    void discardReadBufferLocked() {
        ByteArrayQueue.DiscardResult discardResult = readBuffer.discardAllDetailed();
        long discarded = discardResult.bytes();
        if (discarded > 0 && !applicationVisible()) {
            session.onHiddenUnreadBytesDiscardedLocked(discarded);
        }
        session.onReadBufferReleasedLocked(this, discarded, discardResult.releasedStorageBytes());
    }

    void commitLocalReadStopLocked(long code) {
        halfState.markLocalReadStop();
        discardReadBufferLocked();
        terminalState.recordLocalReadStop(code);
        session.onReadDiscardLocked(this, false);
    }

    void notifySendTerminalTransitionLocked(StreamHalfState.SendState previousSendState) {
        if (!StreamHalfState.isSendTerminal(previousSendState) && halfState.sendTerminal()) {
            clearStopSendingGracefulDrainLocked();
            session.onStreamSendTerminalLocked(this);
        }
    }

    boolean sendTerminalLocked() {
        return halfState.sendTerminal();
    }

    boolean finQueuedLocked() {
        return halfState.sendFinQueued();
    }

    enum PeerDataAction {
        ACCEPT,
        IGNORE,
        IGNORE_AND_FIN,
        ABORT_STREAM_STATE,
        ABORT_STREAM_CLOSED
    }
}
