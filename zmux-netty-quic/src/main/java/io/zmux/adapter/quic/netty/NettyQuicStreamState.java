package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.DuplexChannelConfig;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.*;
import io.zmux.internal.TimeoutBudget;
import io.zmux.internal.Varint62;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class NettyQuicStreamState {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final NettyQuicSession session;
    private final boolean locallyCreated;
    private final boolean readAllowed;
    private final boolean writeAllowed;
    private final boolean bidirectional;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readChanged = lock.newCondition();
    private final Condition writeChanged = lock.newCondition();
    private final ChannelDuplexHandler handler = new StreamHandler();
    private final ReadHalfState readHalf = new ReadHalfState();
    private final WriteHalfState writeHalf = new WriteHalfState();
    private final byte[] preludeLengthScratch = new byte[8];

    private QuicStreamChannel channel;
    private IOException sessionError;
    private long readDeadlineNanos;
    private long writeDeadlineNanos;
    private int readWaiters;
    private int writeWaiters;
    private long bufferedInboundBytes;
    private boolean inboundAutoReadPaused;
    private ByteBuf inboundHead;
    private ArrayDeque<ByteBuf> inboundOverflow;

    private long priority;
    private boolean prioritySet;
    private long group;
    private boolean groupEncoded;
    private byte[] openInfo = EMPTY_BYTES;
    private StreamMetadata metadataSnapshot = StreamMetadata.empty();

    private byte[] prelude;
    private boolean preludeFrozen;
    private boolean preludeSent;
    private boolean preludeSubmitting;
    private boolean peerVisible;

    private NettyQuicStreamState(NettyQuicSession session,
                                 boolean locallyCreated,
                                 boolean readAllowed,
                                 boolean writeAllowed,
                                 boolean bidirectional) {
        this.session = Objects.requireNonNull(session, "session");
        this.locallyCreated = locallyCreated;
        this.readAllowed = readAllowed;
        this.writeAllowed = writeAllowed;
        this.bidirectional = bidirectional;
        if (!readAllowed) {
            readHalf.closeLocal(NettyQuicSupport.readClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
        }
        if (!writeAllowed) {
            writeHalf.closeLocal(NettyQuicSupport.writeClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
        }
    }

    static NettyQuicStreamState localBidi(NettyQuicSession session, OpenOptions options) {
        NettyQuicStreamState state = new NettyQuicStreamState(session, true, true, true, true);
        state.applyOpenOptions(options);
        return state;
    }

    static NettyQuicStreamState localSend(NettyQuicSession session, OpenOptions options) {
        NettyQuicStreamState state = new NettyQuicStreamState(session, true, false, true, false);
        state.applyOpenOptions(options);
        return state;
    }

    static NettyQuicStreamState acceptedBidi(NettyQuicSession session, QuicStreamChannel channel) {
        NettyQuicStreamState state = new NettyQuicStreamState(session, false, true, true, true);
        state.channel = Objects.requireNonNull(channel, "channel");
        return state;
    }

    static NettyQuicStreamState acceptedRecv(NettyQuicSession session, QuicStreamChannel channel) {
        NettyQuicStreamState state = new NettyQuicStreamState(session, false, true, false, false);
        state.channel = Objects.requireNonNull(channel, "channel");
        return state;
    }

    private static byte[] normalizeOpenInfo(byte[] openInfo) {
        return openInfo == null || openInfo.length == 0 ? EMPTY_BYTES : openInfo;
    }

    private static void releaseBuffer(ByteBuf buffer) {
        if (buffer != null && buffer.refCnt() > 0) {
            buffer.release();
        }
    }

    private static void setChannelAutoRead(QuicStreamChannel target, boolean enabled) {
        if (target == null) {
            return;
        }
        Runnable update = () -> {
            if (target.isOpen()) {
                target.config().setAutoRead(enabled);
            }
        };
        try {
            if (target.eventLoop().inEventLoop()) {
                update.run();
            } else {
                target.eventLoop().execute(update);
            }
        } catch (RuntimeException ignored) {
            // Auto-read is best-effort backpressure; stream/session close paths must not fail on it.
        }
    }

    private static long remainingDeadlineNanosLocked(long deadlineNanos) {
        return TimeoutBudget.remainingNanosUntil(deadlineNanos);
    }

    private static int checkedWritevTotalLength(byte[][] parts) throws IOException {
        int totalLength = 0;
        for (int i = 0; i < parts.length; ++i) {
            byte[] part = Objects.requireNonNull(parts[i], "parts[" + i + "]");
            if (part.length > Integer.MAX_VALUE - totalLength) {
                throw new ZmuxException(
                        ErrorCode.FRAME_SIZE.code(),
                        "writevFinal",
                        "multipart write exceeds maximum supported size",
                        ZmuxErrorScope.STREAM,
                        ZmuxErrorSource.LOCAL,
                        ZmuxErrorDirection.WRITE,
                        ZmuxTerminationKind.UNKNOWN
                );
            }
            totalLength += part.length;
        }
        return totalLength;
    }

    ChannelDuplexHandler newHandler() {
        return handler;
    }

    void attachChannel(QuicStreamChannel channel) {
        QuicStreamChannel attached = Objects.requireNonNull(channel, "channel");
        boolean autoRead;
        lock.lock();
        try {
            this.channel = attached;
            autoRead = shouldAutoReadLocked()
                    && bufferedInboundBytes < NettyQuicSupport.STREAM_INBOUND_AUTO_READ_HIGH_WATERMARK;
            inboundAutoReadPaused = !autoRead;
        } finally {
            lock.unlock();
        }
        if (bidirectional) {
            DuplexChannelConfig duplex = attached.config();
            duplex.setAllowHalfClosure(true);
        }
        attached.config().setAutoRead(autoRead);
        attached.config().setOption(QuicChannelOption.READ_FRAMES, false);
    }

    void validatePendingPrelude() throws IOException {
        lock.lock();
        try {
            if (hasPeerVisibleOpenMetadataLocked()) {
                ensurePreludeBuiltLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    void maybeSendOpenPreludeOnOpen() throws IOException {
        lock.lock();
        try {
            if (!hasPeerVisibleOpenMetadataLocked()) {
                return;
            }
        } finally {
            lock.unlock();
        }
        ensureOpenPrelude();
    }

    void prepareAcceptedPrelude(Duration timeout) throws IOException {
        readExactly(preludeLengthScratch, 0, 1, timeout, "read stream prelude length");
        int prefixLength = 1 << ((preludeLengthScratch[0] & 0xff) >>> 6);
        if (prefixLength > 1) {
            readExactly(preludeLengthScratch, 1, prefixLength - 1, timeout, "read stream prelude length");
        }
        Varint62.Decoded decoded = Varint62.decode(preludeLengthScratch, 0);
        long metadataLength = decoded.value();
        if (metadataLength == 0L) {
            session.noteControlProgress();
            return;
        }
        if (metadataLength + decoded.length() > NettyQuicSupport.STREAM_PRELUDE_MAX_PAYLOAD) {
            throw protocolPreludeError("stream prelude exceeds adapter payload cap");
        }
        byte[] metadataBytes = readExactly((int) metadataLength, timeout, "read stream metadata");
        session.noteControlProgress();
        NettyQuicPrelude.DecodedMetadata metadata = NettyQuicPrelude.decodeMetadata(metadataBytes);
        if (metadata.emptyMetadata()) {
            return;
        }
        lock.lock();
        try {
            replaceMetadataLocked(
                    metadata.priority(),
                    metadata.prioritySet(),
                    metadata.group(),
                    metadata.groupEncoded(),
                    metadata.openInfo()
            );
        } finally {
            lock.unlock();
        }
    }

    boolean bidirectional() {
        return bidirectional;
    }

    boolean openedLocally() {
        return locallyCreated;
    }

    long streamId() {
        QuicStreamChannel current = channel;
        return current == null ? 0L : current.streamId();
    }

    boolean readClosed() {
        lock.lock();
        try {
            return !readAllowed
                    || readHalf.localClosed()
                    || readHalf.remoteTerminated()
                    || readHalf.remoteError() != null
                    || currentSessionError() != null;
        } finally {
            lock.unlock();
        }
    }

    boolean writeClosed() {
        lock.lock();
        try {
            return !writeAllowed
                    || writeHalf.localClosed()
                    || currentSessionError() != null;
        } finally {
            lock.unlock();
        }
    }

    byte[] openInfo() {
        lock.lock();
        try {
            return metadataSnapshot.openInfo();
        } finally {
            lock.unlock();
        }
    }

    StreamMetadata metadata() {
        lock.lock();
        try {
            return metadataSnapshot;
        } finally {
            lock.unlock();
        }
    }

    SocketAddress localAddress() {
        QuicStreamChannel current = channel;
        SocketAddress address = current == null ? null : current.parent().localSocketAddress();
        if (address != null) {
            return address;
        }
        return current == null ? ZmuxSocketAddress.localPending() : ZmuxSocketAddress.localStream(current.streamId());
    }

    SocketAddress remoteAddress() {
        QuicStreamChannel current = channel;
        SocketAddress address = current == null ? null : current.parent().remoteSocketAddress();
        if (address != null) {
            return address;
        }
        return current == null ? ZmuxSocketAddress.remotePending() : ZmuxSocketAddress.remoteStream(current.streamId());
    }

    int read(byte[] dst, int offset, int length) throws IOException {
        Objects.requireNonNull(dst, "dst");
        RangeChecks.checkFromIndexSize(offset, length, dst.length);
        if (length == 0) {
            return 0;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "read");
        lock.lock();
        try {
            ensureReadableLocked();
            for (; ; ) {
                int copied = drainInboundLocked(dst, offset, length);
                if (copied > 0) {
                    session.noteDataRead(copied);
                    return copied;
                }
                IOException currentSessionError = currentSessionError();
                if (currentSessionError != null) {
                    throw currentSessionError;
                }
                if (readHalf.remoteError() != null) {
                    throw readHalf.remoteError();
                }
                if (readHalf.remoteTerminated()) {
                    return -1;
                }
                long remainingNanos = remainingDeadlineNanosLocked(readDeadlineNanos);
                if (remainingNanos < 0L) {
                    throw new ReadTimeoutException();
                }
                if (remainingNanos == 0L) {
                    awaitReadChangeInterruptibly();
                    ensureReadableLocked();
                } else {
                    if (!awaitReadChangeNanosInterruptibly(remainingNanos)) {
                        ensureReadableLocked();
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void write(byte[] src, int offset, int length) throws IOException {
        Objects.requireNonNull(src, "src");
        RangeChecks.checkFromIndexSize(offset, length, src.length);
        if (length == 0) {
            return;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "write");
        ensureOpenPrelude();
        lock.lock();
        try {
            ensureWritableLocked();
        } finally {
            lock.unlock();
        }
        writeBytes(src, offset, length);
        session.noteDataWrite(length);
    }

    int writeFinal(byte[] src, int offset, int length) throws IOException {
        Objects.requireNonNull(src, "src");
        RangeChecks.checkFromIndexSize(offset, length, src.length);
        if (length == 0) {
            closeWrite();
            return 0;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "writeFinal");
        writeFinalBytes(src, offset, length);
        session.noteDataWrite(length);
        closeWrite();
        return length;
    }

    int writevFinal(byte[]... parts) throws IOException {
        Objects.requireNonNull(parts, "parts");

        int totalLength = checkedWritevTotalLength(parts);
        if (totalLength == 0) {
            closeWrite();
            return 0;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "writevFinal");

        writeFinalParts(parts, totalLength);
        session.noteDataWrite(totalLength);
        closeWrite();
        return totalLength;
    }

    private void writeFinalBytes(byte[] src, int offset, int length) throws IOException {
        byte[] pendingPrelude = claimOpenPrelude();
        boolean commitsLocalOpen = pendingPrelude != null;
        if (pendingPrelude == null) {
            ensureWritableForDataWrite();
            pendingPrelude = EMPTY_BYTES;
        }

        if (pendingPrelude.length > Integer.MAX_VALUE - length) {
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw new ZmuxException(
                    ErrorCode.FRAME_SIZE.code(),
                    "writeFinal",
                    "final write plus stream prelude exceeds maximum supported size",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    ZmuxTerminationKind.UNKNOWN
            );
        }

        ByteBuf buffer = null;
        try {
            int bufferLength = pendingPrelude.length + length;
            buffer = channel.alloc().ioBuffer(bufferLength, bufferLength);
            if (pendingPrelude.length > 0) {
                buffer.writeBytes(pendingPrelude);
            }
            buffer.writeBytes(src, offset, length);
        } catch (RuntimeException failure) {
            if (buffer != null && buffer.refCnt() > 0) {
                buffer.release();
            }
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw failure;
        }
        ChannelFuture future;
        long startedAtNanos = System.nanoTime();
        try {
            future = submitWrite(buffer);
        } catch (IOException | RuntimeException failure) {
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw failure;
        }
        if (commitsLocalOpen) {
            completeOpenPreludeSubmission();
            session.noteControlProgress();
        }
        awaitWriteFuture(future, pendingPrelude.length + length, startedAtNanos);
    }

    private void writeFinalParts(byte[][] parts, int totalLength) throws IOException {
        byte[] pendingPrelude = claimOpenPrelude();
        boolean commitsLocalOpen = pendingPrelude != null;
        if (pendingPrelude == null) {
            ensureWritableForDataWrite();
            pendingPrelude = EMPTY_BYTES;
        }

        if (pendingPrelude.length > Integer.MAX_VALUE - totalLength) {
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw new ZmuxException(
                    ErrorCode.FRAME_SIZE.code(),
                    "writevFinal",
                    "multipart write plus stream prelude exceeds maximum supported size",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    ZmuxTerminationKind.UNKNOWN
            );
        }

        ByteBuf buffer = null;
        try {
            int bufferLength = pendingPrelude.length + totalLength;
            buffer = channel.alloc().ioBuffer(bufferLength, bufferLength);
            if (pendingPrelude.length > 0) {
                buffer.writeBytes(pendingPrelude);
            }
            for (byte[] part : parts) {
                if (part.length > 0) {
                    buffer.writeBytes(part);
                }
            }
        } catch (RuntimeException failure) {
            if (buffer != null && buffer.refCnt() > 0) {
                buffer.release();
            }
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw failure;
        }
        ChannelFuture future;
        long startedAtNanos = System.nanoTime();
        try {
            future = submitWrite(buffer);
        } catch (IOException | RuntimeException failure) {
            if (commitsLocalOpen) {
                abortOpenPreludeSubmission();
            }
            throw failure;
        }
        if (commitsLocalOpen) {
            completeOpenPreludeSubmission();
            session.noteControlProgress();
        }
        awaitWriteFuture(future, pendingPrelude.length + totalLength, startedAtNanos);
    }

    void setDeadline(Instant deadline) throws IOException {
        long resolved = NettyQuicSupport.deadlineNanos(deadline);
        lock.lock();
        try {
            ensureSessionOpenForDeadlineLocked(readAllowed ? "read" : "write");
            boolean readDeadlineChanged = false;
            boolean writeDeadlineChanged = false;
            if (readAllowed && readDeadlineNanos != resolved) {
                readDeadlineNanos = resolved;
                readDeadlineChanged = true;
            }
            if (writeAllowed && writeDeadlineNanos != resolved) {
                writeDeadlineNanos = resolved;
                writeDeadlineChanged = true;
            }
            if (readDeadlineChanged && writeDeadlineChanged) {
                signalStateChangedLocked();
            } else if (readDeadlineChanged) {
                signalReadChangedLocked();
            } else if (writeDeadlineChanged) {
                signalWriteChangedLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    void setReadDeadline(Instant deadline) throws IOException {
        long resolved = NettyQuicSupport.deadlineNanos(deadline);
        lock.lock();
        try {
            ensureSessionOpenForDeadlineLocked("read");
            if (readDeadlineNanos != resolved) {
                readDeadlineNanos = resolved;
                signalReadChangedLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    void setWriteDeadline(Instant deadline) throws IOException {
        long resolved = NettyQuicSupport.deadlineNanos(deadline);
        lock.lock();
        try {
            ensureSessionOpenForDeadlineLocked("write");
            if (writeDeadlineNanos != resolved) {
                writeDeadlineNanos = resolved;
                signalWriteChangedLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    void setWriteDeadlineNanos(long deadlineNanos) {
        lock.lock();
        try {
            long resolved = Math.max(0L, deadlineNanos);
            if (writeDeadlineNanos != resolved) {
                writeDeadlineNanos = resolved;
                signalWriteChangedLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    void updateMetadata(MetadataUpdate update) throws IOException {
        if (update == null || update.isEmpty()) {
            throw NettyQuicSupport.emptyMetadataUpdateError();
        }
        PendingMetadataSnapshot snapshot;
        lock.lock();
        try {
            ensureWritableLocked();
            if (!locallyCreated || peerVisible || preludeSent || preludeFrozen) {
                throw new PriorityUpdateUnavailableException();
            }
            snapshot = pendingMetadataSnapshotLocked();
            if (update.priority() != null) {
                priority = update.priority();
                prioritySet = true;
            }
            if (update.group() != null) {
                group = update.group();
                groupEncoded = true;
            }
            refreshMetadataSnapshotLocked();
        } finally {
            lock.unlock();
        }
        try {
            ensureOpenPreludeSubmitted();
        } catch (IOException error) {
            lock.lock();
            try {
                if (!preludeSent && !peerVisible) {
                    restorePendingMetadataSnapshotLocked(snapshot);
                }
            } finally {
                lock.unlock();
            }
            throw error;
        }
    }

    void closeRead() throws IOException {
        cancelRead(ErrorCode.CANCELLED.code());
    }

    void cancelRead(long code) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "cancelRead");
        if (!readAllowed) {
            throw NettyQuicSupport.readClosedError();
        }
        int quicCode;
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "read",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.READ
            );
        } finally {
            lock.unlock();
        }
        ensureOpenPreludeForReadStop();
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            readHalf.closeLocal(NettyQuicSupport.readClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED));
            releaseInboundLocked();
            signalReadChangedLocked();
        } finally {
            lock.unlock();
        }
        dispatchControlFuture(channel.shutdownInput(quicCode));
    }

    void closeWrite() throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeWrite");
        if (!writeAllowed) {
            throw NettyQuicSupport.writeClosedError();
        }
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
        } finally {
            lock.unlock();
        }
        ensureOpenPrelude();
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            writeHalf.closeLocal(NettyQuicSupport.writeClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
        handleWriteSideControlFuture(channel.shutdownOutput(), System.nanoTime());
    }

    void cancelWrite(long code) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "cancelWrite");
        if (!writeAllowed) {
            throw NettyQuicSupport.writeClosedError();
        }
        int quicCode;
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.WRITE
            );
        } finally {
            lock.unlock();
        }
        ensureOpenPrelude();
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                "",
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            writeHalf.closeLocal(error);
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
        handleWriteSideControlFuture(channel.shutdownOutput(quicCode), System.nanoTime());
        session.noteResetReason(code);
    }

    void closeWithError(long code, String reason) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeWithError");
        int quicCode;
        lock.lock();
        try {
            if (allLocallyClosedLocked()) {
                throw preferredLocalTerminalErrorLocked();
            }
            ensureSessionOpenForControlLocked();
            quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "close",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH
            );
        } finally {
            lock.unlock();
        }
        if (locallyCreated && writeAllowed) {
            ensureOpenPrelude();
        }
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                reason,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        );
        lock.lock();
        try {
            ensureSessionOpenForControlLocked();
            boolean changed = false;
            if (readAllowed) {
                if (!readHalf.localClosed()) {
                    readHalf.closeLocal(error);
                    releaseInboundLocked();
                    changed = true;
                }
            }
            if (writeAllowed) {
                if (!writeHalf.localClosed()) {
                    writeHalf.closeLocal(error);
                    changed = true;
                }
            }
            if (!changed) {
                throw preferredLocalTerminalErrorLocked();
            }
            signalStateChangedLocked();
        } finally {
            lock.unlock();
        }
        handleWriteSideControlFuture(channel.shutdown(quicCode), System.nanoTime());
        session.noteAbortReason(code);
    }

    void closeWriteWithError(long code, String reason) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeWriteWithError");
        int quicCode;
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.WRITE
            );
        } finally {
            lock.unlock();
        }
        ensureOpenPrelude();
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                reason,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            writeHalf.closeLocal(error);
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
        handleWriteSideControlFuture(channel.shutdownOutput(quicCode), System.nanoTime());
        session.noteResetReason(code);
    }

    void closeReadWithError(long code, String reason) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeReadWithError");
        int quicCode;
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "read",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.READ
            );
        } finally {
            lock.unlock();
        }
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                reason,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.STOPPED
        );
        ensureOpenPreludeForReadStop();
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            readHalf.closeLocal(error);
            releaseInboundLocked();
            signalReadChangedLocked();
        } finally {
            lock.unlock();
        }
        dispatchControlFuture(channel.shutdownInput(quicCode));
    }

    void onSessionClosed(IOException error) {
        lock.lock();
        try {
            sessionError = error;
            pauseInboundAutoReadLocked();
            signalStateChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    void closeRaw() {
        QuicStreamChannel current = channel;
        if (current != null) {
            current.close();
        }
    }

    void rejectAcceptedPrelude() {
        rejectAcceptedPrelude(ErrorCode.PROTOCOL.code());
    }

    void rejectAcceptedPrelude(long code) {
        session.noteAbortReason(code);
        QuicStreamChannel current = channel;
        if (current == null) {
            return;
        }
        try {
            int quicCode = NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "close",
                    ZmuxErrorScope.STREAM,
                    writeAllowed ? ZmuxErrorDirection.BOTH : ZmuxErrorDirection.READ
            );
            if (writeAllowed) {
                dispatchControlFuture(current.shutdown(quicCode));
            } else {
                dispatchControlFuture(current.shutdownInput(quicCode));
            }
        } catch (IOException ignored) {
            // The stream is already being rejected locally.
        } finally {
            current.close();
        }
    }

    private void applyOpenOptions(OpenOptions options) {
        OpenOptions normalized = options == null ? OpenOptions.empty() : options;
        replaceMetadataLocked(
                normalized.initialPriority() == null ? 0L : normalized.initialPriority(),
                normalized.initialPriority() != null,
                normalized.initialGroup(),
                normalized.initialGroup() != null,
                normalized.openInfo()
        );
    }

    private boolean hasPeerVisibleOpenMetadataLocked() {
        return prioritySet || groupEncoded || openInfo.length > 0;
    }

    private void replaceMetadataLocked(long priority,
                                       boolean prioritySet,
                                       Long group,
                                       boolean groupEncoded,
                                       byte[] openInfo) {
        this.priority = prioritySet ? priority : 0L;
        this.prioritySet = prioritySet;
        this.group = groupEncoded && group != null ? group : 0L;
        this.groupEncoded = groupEncoded && group != null;
        this.openInfo = normalizeOpenInfo(openInfo);
        refreshMetadataSnapshotLocked();
    }

    private void refreshMetadataSnapshotLocked() {
        Long groupValue = groupEncoded ? group : null;
        if (!prioritySet && groupValue == null && openInfo.length == 0) {
            metadataSnapshot = StreamMetadata.empty();
            return;
        }
        metadataSnapshot = new StreamMetadata(prioritySet ? priority : 0L, groupValue, openInfo);
    }

    private PendingMetadataSnapshot pendingMetadataSnapshotLocked() {
        return new PendingMetadataSnapshot(
                priority,
                prioritySet,
                group,
                groupEncoded,
                metadataSnapshot,
                prelude,
                preludeFrozen
        );
    }

    private void restorePendingMetadataSnapshotLocked(PendingMetadataSnapshot snapshot) {
        priority = snapshot.priority();
        prioritySet = snapshot.prioritySet();
        group = snapshot.group();
        groupEncoded = snapshot.groupEncoded();
        metadataSnapshot = snapshot.metadataSnapshot();
        prelude = snapshot.prelude();
        preludeFrozen = snapshot.preludeFrozen();
    }

    private void ensurePreludeBuiltLocked() throws IOException {
        if (preludeFrozen) {
            return;
        }
        prelude = NettyQuicPrelude.encode(prioritySet ? priority : null, groupEncoded ? group : null, openInfo);
        preludeFrozen = true;
    }

    private void ensureOpenPrelude() throws IOException {
        ensureOpenPrelude(true);
    }

    private void ensureOpenPreludeSubmitted() throws IOException {
        ensureOpenPrelude(false);
    }

    private void ensureOpenPreludeForReadStop() throws IOException {
        if (locallyCreated && writeAllowed) {
            ensureOpenPreludeSubmitted();
        }
    }

    private void ensureOpenPrelude(boolean waitForCompletion) throws IOException {
        byte[] pending = claimOpenPrelude();
        if (pending == null) {
            return;
        }
        ByteBuf buffer = null;
        ChannelFuture future;
        long startedAtNanos = System.nanoTime();
        try {
            buffer = channel.alloc().ioBuffer(pending.length, pending.length);
            buffer.writeBytes(pending);
            future = submitWrite(buffer);
        } catch (IOException | RuntimeException failure) {
            releaseBuffer(buffer);
            abortOpenPreludeSubmission();
            throw failure;
        }
        completeOpenPreludeSubmission();
        session.noteControlProgress();
        if (waitForCompletion) {
            awaitWriteFuture(future, pending.length, startedAtNanos);
        } else {
            submitAsyncWriteFuture(future, pending.length, startedAtNanos);
        }
    }

    private void writeBytes(byte[] src, int offset, int length) throws IOException {
        writeInternal(src, offset, length);
    }

    private void ensureWritableForDataWrite() throws IOException {
        lock.lock();
        try {
            ensureWritableLocked();
        } finally {
            lock.unlock();
        }
    }

    private void writeInternal(byte[] src, int offset, int length) throws IOException {
        writeInternal(newWriteBuffer(src, offset, length));
    }

    private ByteBuf newWriteBuffer(byte[] src, int offset, int length) {
        ByteBuf buffer = null;
        try {
            buffer = channel.alloc().ioBuffer(length, length);
            buffer.writeBytes(src, offset, length);
            return buffer;
        } catch (RuntimeException failure) {
            releaseBuffer(buffer);
            throw failure;
        }
    }

    private void writeInternal(ByteBuf buffer) throws IOException {
        int bytes = Math.max(0, buffer.readableBytes());
        long startedAtNanos = System.nanoTime();
        ChannelFuture future = submitWrite(buffer);
        awaitWriteFuture(future, bytes, startedAtNanos);
    }

    private ChannelFuture submitWrite(ByteBuf buffer) throws IOException {
        try {
            return channel.writeAndFlush(buffer);
        } catch (RuntimeException failure) {
            releaseBuffer(buffer);
            throw NettyQuicSupport.translateWriteThrowable(failure);
        }
    }

    private byte[] claimOpenPrelude() throws IOException {
        lock.lock();
        try {
            while (preludeSubmitting && !preludeSent) {
                ensureWritableLocked();
                long remainingNanos = remainingDeadlineNanosLocked(writeDeadlineNanos);
                if (remainingNanos < 0L) {
                    throw new WriteTimeoutException();
                }
                if (remainingNanos == 0L) {
                    awaitWriteChangeInterruptibly();
                } else {
                    awaitWriteChangeNanosInterruptibly(remainingNanos);
                }
            }
            if (!locallyCreated || !writeAllowed || preludeSent) {
                return null;
            }
            ensureWritableLocked();
            ensurePreludeBuiltLocked();
            preludeSubmitting = true;
            return prelude;
        } finally {
            lock.unlock();
        }
    }

    private void completeOpenPreludeSubmission() {
        lock.lock();
        try {
            preludeSubmitting = false;
            preludeSent = true;
            peerVisible = true;
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    private void abortOpenPreludeSubmission() {
        lock.lock();
        try {
            preludeSubmitting = false;
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    private void awaitWriteFuture(ChannelFuture future, int bytes, long startedAtNanos) throws IOException {
        future.addListener(ignored -> {
            lock.lock();
            try {
                signalWriteChangedLocked();
            } finally {
                lock.unlock();
            }
        });
        while (!future.isDone()) {
            lock.lock();
            try {
                ensureWritableLocked();
                long remainingNanos = remainingDeadlineNanosLocked(writeDeadlineNanos);
                if (remainingNanos < 0L) {
                    throw new WriteTimeoutException();
                }
                if (remainingNanos == 0L) {
                    awaitWriteChangeInterruptibly();
                } else {
                    awaitWriteChangeNanosInterruptibly(remainingNanos);
                }
            } finally {
                lock.unlock();
            }
        }
        long completedAtNanos = System.nanoTime();
        session.noteBlockedWrite(NettyQuicSupport.elapsedNanos(completedAtNanos, startedAtNanos));
        if (future.isSuccess()) {
            session.noteFlush(bytes, completedAtNanos);
            return;
        }
        lock.lock();
        try {
            IOException currentSessionError = currentSessionError();
            if (currentSessionError != null) {
                throw currentSessionError;
            }
        } finally {
            lock.unlock();
        }
        IOException translated = NettyQuicSupport.translateWriteThrowable(future.cause());
        lock.lock();
        try {
            if (translated instanceof ApplicationError
                    || translated instanceof WriteClosedException
                    || future.cause() instanceof io.netty.channel.socket.ChannelOutputShutdownException) {
                writeHalf.closeLocal(translated);
            }
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
        throw translated;
    }

    private void submitAsyncWriteFuture(ChannelFuture future, int bytes, long startedAtNanos) {
        if (future == null) {
            return;
        }
        future.addListener(ignored -> {
            long completedAtNanos = System.nanoTime();
            session.noteBlockedWrite(NettyQuicSupport.elapsedNanos(completedAtNanos, startedAtNanos));
            if (future.isSuccess()) {
                session.noteFlush(bytes, completedAtNanos);
                session.noteControlProgress();
            } else {
                IOException translated = NettyQuicSupport.translateWriteThrowable(future.cause());
                session.noteObservedStreamReason(translated);
                lock.lock();
                try {
                    if (translated instanceof ApplicationError
                            || translated instanceof WriteClosedException
                            || future.cause() instanceof io.netty.channel.socket.ChannelOutputShutdownException) {
                        writeHalf.closeLocal(translated);
                    }
                    signalWriteChangedLocked();
                } finally {
                    lock.unlock();
                }
            }
        });
    }

    private void awaitWriteSideFuture(ChannelFuture future, long startedAtNanos) throws IOException {
        future.addListener(ignored -> {
            lock.lock();
            try {
                signalWriteChangedLocked();
            } finally {
                lock.unlock();
            }
        });
        while (!future.isDone()) {
            lock.lock();
            try {
                IOException currentSessionError = currentSessionError();
                if (currentSessionError != null) {
                    throw currentSessionError;
                }
                long remainingNanos = remainingDeadlineNanosLocked(writeDeadlineNanos);
                if (remainingNanos < 0L) {
                    throw new WriteTimeoutException();
                }
                if (remainingNanos == 0L) {
                    awaitWriteChangeInterruptibly();
                } else {
                    awaitWriteChangeNanosInterruptibly(remainingNanos);
                }
            } finally {
                lock.unlock();
            }
        }
        long completedAtNanos = System.nanoTime();
        session.noteBlockedWrite(NettyQuicSupport.elapsedNanos(completedAtNanos, startedAtNanos));
        if (!future.isSuccess()) {
            lock.lock();
            try {
                IOException currentSessionError = currentSessionError();
                if (currentSessionError != null) {
                    throw currentSessionError;
                }
            } finally {
                lock.unlock();
            }
            throw NettyQuicSupport.translateWriteThrowable(future.cause());
        }
        session.noteTransportWriteProgress(completedAtNanos);
    }

    private void handleWriteSideControlFuture(ChannelFuture future, long startedAtNanos) throws IOException {
        if (shouldAwaitWriteSideControlFuture()) {
            awaitWriteSideFuture(future, startedAtNanos);
            return;
        }
        submitWriteSideControlFuture(future, startedAtNanos);
    }

    private boolean shouldAwaitWriteSideControlFuture() {
        lock.lock();
        try {
            return writeDeadlineNanos > 0L;
        } finally {
            lock.unlock();
        }
    }

    private void submitWriteSideControlFuture(ChannelFuture future, long startedAtNanos) {
        if (future == null) {
            return;
        }
        future.addListener(ignored -> {
            long completedAtNanos = System.nanoTime();
            session.noteBlockedWrite(NettyQuicSupport.elapsedNanos(completedAtNanos, startedAtNanos));
            if (future.isSuccess()) {
                session.noteTransportWriteProgress(completedAtNanos);
                session.noteControlProgress();
            }
            lock.lock();
            try {
                signalWriteChangedLocked();
            } finally {
                lock.unlock();
            }
        });
    }

    private byte[] readExactly(int length, Duration timeout, String operation) throws IOException {
        byte[] bytes = length == 0 ? EMPTY_BYTES : new byte[length];
        readExactly(bytes, 0, length, timeout, operation);
        return bytes;
    }

    private void readExactly(byte[] bytes, int offset, int length, Duration timeout, String operation) throws IOException {
        RangeChecks.checkFromIndexSize(offset, length, bytes.length);
        if (length == 0) {
            return;
        }
        int copiedTotal = 0;
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        lock.lock();
        try {
            while (copiedTotal < length) {
                int copied = drainInboundLocked(bytes, offset + copiedTotal, length - copiedTotal);
                if (copied > 0) {
                    copiedTotal += copied;
                    continue;
                }
                IOException currentSessionError = currentSessionError();
                if (currentSessionError != null) {
                    throw currentSessionError;
                }
                if (readHalf.remoteError() != null) {
                    throw protocolPreludeError(operation + ": " + ZmuxErrors.reason(readHalf.remoteError()), readHalf.remoteError());
                }
                if (readHalf.localClosed()) {
                    throw readHalf.localErrorOrDefault();
                }
                if (readHalf.remoteTerminated()) {
                    throw protocolPreludeError(operation + ": unexpected EOF", new EOFException(operation));
                }
                if (budget.bounded()) {
                    long remaining = budget.remainingNanos();
                    if (remaining <= 0L) {
                        throw new ReadTimeoutException();
                    }
                    awaitReadChangeNanosInterruptibly(remaining);
                } else {
                    awaitReadChangeInterruptibly();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private IOException protocolPreludeError(String message) {
        return protocolPreludeError(message, null);
    }

    private IOException protocolPreludeError(String message, Throwable cause) {
        return NettyQuicSupport.streamApplicationError(
                ErrorCode.PROTOCOL.code(),
                "quicmux: " + message,
                cause,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        );
    }

    private int drainInboundLocked(byte[] dst, int offset, int length) {
        int copied = 0;
        while (copied < length && inboundHead != null) {
            ByteBuf head = inboundHead;
            int chunk = Math.min(length - copied, head.readableBytes());
            head.readBytes(dst, offset + copied, chunk);
            copied += chunk;
            consumeInboundBytesLocked(chunk);
            if (!head.isReadable()) {
                head.release();
                inboundHead = pollInboundOverflowLocked();
            }
        }
        maybeResumeInboundAutoReadLocked();
        return copied;
    }

    private void ensureReadableLocked() throws IOException {
        if (readHalf.localClosed()) {
            throw localReadErrorOrDefault();
        }
        IOException currentSessionError = currentSessionError();
        if (currentSessionError != null && inboundHead == null) {
            throw currentSessionError;
        }
    }

    private void ensureWritableLocked() throws IOException {
        if (writeHalf.localClosed()) {
            throw localWriteErrorOrDefault();
        }
        IOException currentSessionError = currentSessionError();
        if (currentSessionError != null) {
            throw currentSessionError;
        }
    }

    private void ensureSessionOpenForDeadlineLocked(String operation) throws IOException {
        IOException currentSessionError = currentSessionError();
        if (currentSessionError != null) {
            throw NettyQuicSupport.sessionOperationError(operation, currentSessionError);
        }
    }

    private void ensureSessionOpenForControlLocked() throws IOException {
        IOException currentSessionError = currentSessionError();
        if (currentSessionError != null) {
            throw currentSessionError;
        }
    }

    private IOException currentSessionError() {
        return sessionError == null ? session.streamSessionError() : sessionError;
    }

    private IOException localReadErrorOrDefault() {
        return readHalf.localErrorOrDefault();
    }

    private IOException localWriteErrorOrDefault() {
        return writeHalf.localErrorOrDefault();
    }

    private IOException preferredLocalTerminalErrorLocked() {
        if (writeAllowed && writeHalf.localClosed()) {
            return writeHalf.localErrorOrDefault();
        }
        if (readAllowed && readHalf.localClosed()) {
            return readHalf.localErrorOrDefault();
        }
        IOException currentSessionError = currentSessionError();
        return currentSessionError != null ? currentSessionError : NettyQuicSupport.sessionClosedError();
    }

    private boolean allLocallyClosedLocked() {
        return (!writeAllowed || writeHalf.localClosed()) && (!readAllowed || readHalf.localClosed());
    }

    private IOException lateReadDiscardErrorLocked() {
        if (readHalf.localClosed()) {
            return readHalf.localErrorOrDefault();
        }
        return readHalf.remoteError();
    }

    long discardUnreadInboundBytes() {
        lock.lock();
        try {
            return releaseInboundLocked();
        } finally {
            lock.unlock();
        }
    }

    private long releaseInboundLocked() {
        long released = bufferedInboundBytes;
        if (inboundHead != null) {
            inboundHead.release();
            inboundHead = null;
        }
        if (inboundOverflow != null) {
            while (!inboundOverflow.isEmpty()) {
                inboundOverflow.removeFirst().release();
            }
            inboundOverflow = null;
        }
        bufferedInboundBytes = 0L;
        pauseInboundAutoReadLocked();
        return released;
    }

    private void enqueueInboundLocked(ByteBuf byteBuf, int readableBytes) {
        if (inboundHead == null) {
            inboundHead = byteBuf;
        } else {
            if (inboundOverflow == null) {
                inboundOverflow = new ArrayDeque<>(4);
            }
            inboundOverflow.addLast(byteBuf);
        }
        bufferedInboundBytes = NettyQuicSupport.saturatingAdd(
                bufferedInboundBytes,
                Math.max(0L, readableBytes)
        );
        if (bufferedInboundBytes >= NettyQuicSupport.STREAM_INBOUND_AUTO_READ_HIGH_WATERMARK) {
            pauseInboundAutoReadLocked();
        }
    }

    private void consumeInboundBytesLocked(long bytes) {
        if (bytes <= 0L || bufferedInboundBytes == 0L) {
            return;
        }
        bufferedInboundBytes = bytes >= bufferedInboundBytes ? 0L : bufferedInboundBytes - bytes;
    }

    private void maybeResumeInboundAutoReadLocked() {
        if (inboundAutoReadPaused
                && bufferedInboundBytes <= NettyQuicSupport.STREAM_INBOUND_AUTO_READ_LOW_WATERMARK
                && shouldAutoReadLocked()) {
            inboundAutoReadPaused = false;
            setChannelAutoRead(channel, true);
        }
    }

    private void pauseInboundAutoReadLocked() {
        if (!inboundAutoReadPaused) {
            inboundAutoReadPaused = true;
            setChannelAutoRead(channel, false);
        }
    }

    private boolean shouldAutoReadLocked() {
        return readAllowed
                && !readHalf.localClosed()
                && readHalf.remoteError() == null
                && !readHalf.remoteTerminated()
                && currentSessionError() == null;
    }

    private ByteBuf pollInboundOverflowLocked() {
        return inboundOverflow == null || inboundOverflow.isEmpty() ? null : inboundOverflow.removeFirst();
    }

    private void awaitReadChangeInterruptibly() throws InterruptedIOException {
        readWaiters++;
        try {
            readChanged.await();
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            readWaiters--;
        }
    }

    private boolean awaitReadChangeNanosInterruptibly(long nanos) throws InterruptedIOException {
        readWaiters++;
        try {
            return readChanged.awaitNanos(nanos) <= 0L;
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            readWaiters--;
        }
    }

    private void awaitWriteChangeInterruptibly() throws InterruptedIOException {
        writeWaiters++;
        try {
            writeChanged.await();
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            writeWaiters--;
        }
    }

    private boolean awaitWriteChangeNanosInterruptibly(long nanos) throws InterruptedIOException {
        writeWaiters++;
        try {
            return writeChanged.awaitNanos(nanos) <= 0L;
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            writeWaiters--;
        }
    }

    private void signalStateChangedLocked() {
        signalReadChangedLocked();
        signalWriteChangedLocked();
    }

    private void signalReadChangedLocked() {
        if (readWaiters > 0) {
            readChanged.signalAll();
        }
    }

    private void signalWriteChangedLocked() {
        if (writeWaiters > 0) {
            writeChanged.signalAll();
        }
    }

    private void dispatchControlFuture(ChannelFuture future) {
        if (future == null) {
            return;
        }
        future.addListener(ignored -> {
            if (future.isSuccess()) {
                session.noteControlProgress();
            }
        });
        NettyQuicSupport.dispatchChannelFuture(future);
    }

    long bufferedInboundBytes() {
        lock.lock();
        try {
            return bufferedInboundBytes;
        } finally {
            lock.unlock();
        }
    }

    int retainedOpenInfoBytes() {
        lock.lock();
        try {
            return openInfo.length;
        } finally {
            lock.unlock();
        }
    }

    long trackedMemoryBytes() {
        lock.lock();
        try {
            long total = openInfo.length;
            if (prelude != null && !preludeSent) {
                total = NettyQuicSupport.saturatingAdd(total, prelude.length);
            }
            total = NettyQuicSupport.saturatingAdd(total, bufferedInboundBytes);
            return total;
        } finally {
            lock.unlock();
        }
    }

    private static final class ReadHalfState {
        private IOException localError;
        private IOException remoteError;
        private boolean localClosed;
        private boolean remoteFinished;
        private boolean transportClosed;

        void closeLocal(IOException error) {
            localClosed = true;
            localError = error;
        }

        boolean localClosed() {
            return localClosed;
        }

        IOException localErrorOrDefault() {
            return localError == null ? NettyQuicSupport.readClosedError() : localError;
        }

        void failRemote(IOException error) {
            remoteError = error;
        }

        IOException remoteError() {
            return remoteError;
        }

        void markRemoteFinished() {
            remoteFinished = true;
        }

        void markTransportClosed() {
            transportClosed = true;
        }

        boolean remoteTerminated() {
            return remoteFinished || transportClosed;
        }
    }

    private static final class WriteHalfState {
        private IOException localError;
        private boolean localClosed;

        void closeLocal(IOException error) {
            localClosed = true;
            localError = error;
        }

        boolean localClosed() {
            return localClosed;
        }

        IOException localErrorOrDefault() {
            return localError == null ? NettyQuicSupport.writeClosedError() : localError;
        }
    }

    private static final class PendingMetadataSnapshot {
        private final long priority;
        private final boolean prioritySet;
        private final Long group;
        private final boolean groupEncoded;
        private final StreamMetadata metadataSnapshot;
        private final byte[] prelude;
        private final boolean preludeFrozen;

        private PendingMetadataSnapshot(long priority,
                                        boolean prioritySet,
                                        Long group,
                                        boolean groupEncoded,
                                        StreamMetadata metadataSnapshot,
                                        byte[] prelude,
                                        boolean preludeFrozen) {
            this.priority = priority;
            this.prioritySet = prioritySet;
            this.group = group;
            this.groupEncoded = groupEncoded;
            this.metadataSnapshot = metadataSnapshot;
            this.prelude = prelude;
            this.preludeFrozen = preludeFrozen;
        }

        private long priority() {
            return priority;
        }

        private boolean prioritySet() {
            return prioritySet;
        }

        private Long group() {
            return group;
        }

        private boolean groupEncoded() {
            return groupEncoded;
        }

        private StreamMetadata metadataSnapshot() {
            return metadataSnapshot;
        }

        private byte[] prelude() {
            return prelude;
        }

        private boolean preludeFrozen() {
            return preludeFrozen;
        }
    }

    private final class StreamHandler extends ChannelDuplexHandler {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf byteBuf = (ByteBuf) msg;
                int readableBytes = Math.max(0, byteBuf.readableBytes());
                lock.lock();
                try {
                    session.noteInboundFrame(readableBytes);
                    IOException lateDiscardError = lateReadDiscardErrorLocked();
                    if (lateDiscardError != null || currentSessionError() != null) {
                        if (lateDiscardError != null) {
                            session.noteLateReadDiscard(lateDiscardError, readableBytes);
                        }
                        byteBuf.release();
                        pauseInboundAutoReadLocked();
                    } else {
                        enqueueInboundLocked(byteBuf, readableBytes);
                    }
                    signalReadChangedLocked();
                } finally {
                    lock.unlock();
                }
                return;
            }
            io.netty.util.ReferenceCountUtil.release(msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == ChannelInputShutdownReadComplete.INSTANCE) {
                lock.lock();
                try {
                    readHalf.markRemoteFinished();
                    pauseInboundAutoReadLocked();
                    signalReadChangedLocked();
                } finally {
                    lock.unlock();
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            IOException translated = NettyQuicSupport.translateReadThrowable(cause);
            session.noteObservedStreamReason(translated);
            lock.lock();
            try {
                readHalf.failRemote(translated);
                pauseInboundAutoReadLocked();
                signalReadChangedLocked();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            lock.lock();
            try {
                readHalf.markTransportClosed();
                pauseInboundAutoReadLocked();
                signalStateChangedLocked();
            } finally {
                lock.unlock();
            }
            session.unregister(NettyQuicStreamState.this);
            super.channelInactive(ctx);
        }
    }
}
