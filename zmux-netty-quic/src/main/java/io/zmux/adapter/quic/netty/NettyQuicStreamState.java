package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.DuplexChannelConfig;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.*;
import io.zmux.internal.StreamIoSupport;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("resource")
final class NettyQuicStreamState {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int MAX_BUFFERED_WRITE_BYTES = 1 << 20;
    private static final int MAX_WRITEV_FINAL_COALESCE_BYTES = 64 << 10;

    private final NettyQuicSession session;
    private final boolean locallyCreated;
    private final boolean readAllowed;
    private final boolean writeAllowed;
    private final boolean bidirectional;
    private final ReentrantLock lock = new ReentrantLock();
    private final ReentrantLock writeIoLock = new ReentrantLock();
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
    private long readSignalVersion;
    private long writeSignalVersion;
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
            if (NettyQuicSupport.inEventLoop(target)) {
                update.run();
            } else {
                NettyQuicSupport.executeOnEventLoop(target, update);
            }
        } catch (RuntimeException ignored) {
            // Auto-read is best-effort backpressure; stream/session close paths must not fail on it.
        }
    }

    private static long remainingDeadlineNanosLocked(long deadlineNanos) {
        return TimeoutBudget.remainingNanosUntil(deadlineNanos);
    }

    private static long deadlineNanos(long waitNanos) {
        long now = System.nanoTime();
        return now > Long.MAX_VALUE - waitNanos ? Long.MAX_VALUE : now + waitNanos;
    }

    private static long remainingNanos(long deadlineNanos) {
        long now = System.nanoTime();
        long remaining = deadlineNanos - now;
        if (remaining <= 0L && deadlineNanos > now) {
            return Long.MAX_VALUE;
        }
        return remaining;
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

    boolean activeForStats() {
        lock.lock();
        try {
            IOException currentSessionError = currentSessionError();
            boolean readActive = readAllowed
                    && !readHalf.localClosed()
                    && readHalf.remoteError() == null
                    && currentSessionError == null
                    && (bufferedInboundBytes > 0L || !readHalf.remoteTerminated());
            boolean writeActive = writeAllowed
                    && !writeHalf.localClosed()
                    && currentSessionError == null;
            return readActive || writeActive;
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
        writeIoLock.lock();
        try {
            writeBytes(src, offset, length);
        } finally {
            writeIoLock.unlock();
        }
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
        writeIoLock.lock();
        try {
            writeFinalBytes(src, offset, length);
            session.noteDataWrite(length);
            closeWrite();
        } finally {
            writeIoLock.unlock();
        }
        return length;
    }

    int writevFinal(byte[]... parts) throws IOException {
        Objects.requireNonNull(parts, "parts");

        int totalLength = StreamIoSupport.checkedWritevTotalLength(parts, "writevFinal");
        if (totalLength == 0) {
            closeWrite();
            return 0;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "writevFinal");

        writeIoLock.lock();
        try {
            writeFinalParts(parts, totalLength);
            session.noteDataWrite(totalLength);
            closeWrite();
        } finally {
            writeIoLock.unlock();
        }
        return totalLength;
    }

    CompletionStage<Void> writeAsync(byte[] src, int offset, int length) {
        Objects.requireNonNull(src, "src");
        RangeChecks.checkFromIndexSize(offset, length, src.length);
        if (length == 0) {
            return NettyQuicSupport.completedVoid();
        }
        ByteBuf buffer;
        try {
            buffer = newWriteBuffer(src, offset, length);
        } catch (Throwable failure) {
            return NettyQuicSupport.failedFuture(failure);
        }
        return NettyQuicSupport.completionStageFromChannelFuture(writeNettyAsync(buffer));
    }

    CompletionStage<Void> writeFinalAsync(byte[] src, int offset, int length) {
        Objects.requireNonNull(src, "src");
        RangeChecks.checkFromIndexSize(offset, length, src.length);
        if (length == 0) {
            return NettyQuicSupport.completionStageFromChannelFuture(closeWriteNettyAsync());
        }
        ByteBuf buffer;
        try {
            buffer = newWriteBuffer(src, offset, length);
        } catch (Throwable failure) {
            return NettyQuicSupport.failedFuture(failure);
        }
        return NettyQuicSupport.completionStageFromChannelFuture(writeFinalNettyAsync(buffer));
    }

    CompletionStage<Void> closeWriteAsync() {
        return submitBlockingAsync(this::closeWrite);
    }

    CompletionStage<Void> cancelWriteAsync(long code) {
        return submitBlockingAsync(() -> cancelWrite(code));
    }

    CompletionStage<Void> closeReadAsync() {
        return submitBlockingAsync(this::closeRead);
    }

    CompletionStage<Void> cancelReadAsync(long code) {
        return submitBlockingAsync(() -> cancelRead(code));
    }

    CompletionStage<Void> closeWithErrorAsync(long code, String reason) {
        return submitBlockingAsync(() -> closeWithError(code, reason));
    }

    CompletionStage<Void> submitWriteCloseWithErrorAsync(long code, String reason) {
        return submitBlockingAsync(() -> closeWriteWithError(code, reason));
    }

    CompletionStage<Void> submitReadCloseWithErrorAsync(long code, String reason) {
        return submitBlockingAsync(() -> closeReadWithError(code, reason));
    }

    CompletionStage<Void> closeAsync(boolean closeWrite, boolean closeRead) {
        return submitBlockingAsync(() -> {
            IOException first = null;
            if (closeWrite) {
                try {
                    closeWrite();
                } catch (IOException error) {
                    if (!NettyQuicSupport.isBenignCloseError(error)) {
                        first = error;
                    }
                }
            }
            if (closeRead) {
                try {
                    closeRead();
                } catch (IOException error) {
                    if (first == null && !NettyQuicSupport.isBenignCloseError(error)) {
                        first = error;
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        });
    }

    ChannelFuture writeNettyAsync(ByteBuf data) {
        Objects.requireNonNull(data, "data");
        if (!data.isReadable()) {
            releaseBuffer(data);
            return channel.newSucceededFuture();
        }
        writeIoLock.lock();
        try {
            ensureOpenPreludeSubmitted();
            ensureWritableForDataWrite();
            int bytes = Math.max(0, data.readableBytes());
            long startedAtNanos = System.nanoTime();
            ChannelFuture future = submitWrite(data);
            submitAsyncWriteFuture(future, bytes, startedAtNanos);
            return future;
        } catch (Throwable failure) {
            releaseBuffer(data);
            return channel.newFailedFuture(failure);
        } finally {
            writeIoLock.unlock();
        }
    }

    ChannelFuture writeFinalNettyAsync(ByteBuf data) {
        Objects.requireNonNull(data, "data");
        if (!data.isReadable()) {
            releaseBuffer(data);
            return closeWriteNettyAsync();
        }
        writeIoLock.lock();
        try {
            ensureOpenPreludeSubmitted();
            ensureWritableForDataWrite();
            int bytes = Math.max(0, data.readableBytes());
            long startedAtNanos = System.nanoTime();
            ChannelPromise result = channel.newPromise();
            ChannelFuture writeFuture = submitWrite(data);
            submitAsyncWriteFuture(writeFuture, bytes, startedAtNanos);
            writeFuture.addListener(ignored -> {
                if (writeFuture.isSuccess()) {
                    ChannelFuture closeFuture = closeWriteNettyAsync();
                    closeFuture.addListener(closeIgnored -> {
                        if (closeFuture.isSuccess()) {
                            result.setSuccess();
                        } else {
                            result.setFailure(closeFuture.cause());
                        }
                    });
                } else {
                    result.setFailure(writeFuture.cause());
                }
            });
            return result;
        } catch (Throwable failure) {
            releaseBuffer(data);
            return channel.newFailedFuture(failure);
        } finally {
            writeIoLock.unlock();
        }
    }

    private ChannelFuture closeWriteNettyAsync() {
        try {
            lock.lock();
            try {
                closeLocalWriteGracefullyLocked();
            } finally {
                lock.unlock();
            }
            ChannelFuture future = channel.shutdownOutput();
            submitWriteSideControlFuture(future, System.nanoTime());
            return future;
        } catch (Throwable failure) {
            return channel.newFailedFuture(failure);
        }
    }

    QuicStreamChannel unsafeQuicStreamChannel() {
        return channel;
    }

    private CompletionStage<Void> submitBlockingAsync(IoRunnable action) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        NettyQuicSupport.executeAsyncTask(() -> {
            try {
                action.run();
                future.complete(null);
            } catch (Throwable failure) {
                future.completeExceptionally(failure);
            }
        });
        return future;
    }

    private void writeFinalBytes(byte[] src, int offset, int length) throws IOException {
        byte[] pendingPrelude = claimOpenPrelude();
        boolean commitsLocalOpen = pendingPrelude != null;
        if (pendingPrelude == null) {
            ensureWritableForDataWrite();
            pendingPrelude = EMPTY_BYTES;
        }

        if (pendingPrelude.length <= Integer.MAX_VALUE - length) {
            int bufferedLength = pendingPrelude.length + length;
            if (bufferedLength <= MAX_BUFFERED_WRITE_BYTES) {
                ByteBuf buffer = null;
                try {
                    buffer = channel.alloc().ioBuffer(bufferedLength, bufferedLength);
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
                submitBufferedWrite(buffer, commitsLocalOpen, bufferedLength);
                return;
            }
        }
        submitOpenPrelude(pendingPrelude, commitsLocalOpen);
        writeInternal(src, offset, length);
    }

    private void writeFinalParts(byte[][] parts, int totalLength) throws IOException {
        byte[] pendingPrelude = claimOpenPrelude();
        boolean commitsLocalOpen = pendingPrelude != null;
        if (pendingPrelude == null) {
            ensureWritableForDataWrite();
            pendingPrelude = EMPTY_BYTES;
        }

        if (pendingPrelude.length <= Integer.MAX_VALUE - totalLength) {
            int bufferedLength = pendingPrelude.length + totalLength;
            if (bufferedLength <= MAX_WRITEV_FINAL_COALESCE_BYTES) {
                ByteBuf buffer = null;
                try {
                    buffer = channel.alloc().ioBuffer(bufferedLength, bufferedLength);
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
                submitBufferedWrite(buffer, commitsLocalOpen, bufferedLength);
                return;
            }
        }
        submitOpenPrelude(pendingPrelude, commitsLocalOpen);
        writeInternal(parts, totalLength);
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
        NettyQuicSupport.ensureOffEventLoop(channel, "closeRead");
        if (!readAllowed) {
            throw NettyQuicSupport.readClosedError();
        }
        int quicCode = requireReadControlCode(ErrorCode.CANCELLED.code());
        ensureOpenPreludeForReadStop();
        closeLocalRead(NettyQuicSupport.readClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED), true);
        dispatchControlFuture(channel.shutdownInput(quicCode));
    }

    void cancelRead(long code) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "cancelRead");
        if (!readAllowed) {
            throw NettyQuicSupport.readClosedError();
        }
        closeReadWithStop(code, "");
    }

    void closeWrite() throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeWrite");
        if (!writeAllowed) {
            throw NettyQuicSupport.writeClosedError();
        }
        writeIoLock.lock();
        try {
            lock.lock();
            try {
                if (writeHalf.localClosed()) {
                    throw localWriteErrorOrDefault();
                }
                ensureSessionOpenForControlLocked();
            } finally {
                lock.unlock();
            }
            ensureOpenPrelude(true);
            lock.lock();
            try {
                closeLocalWriteGracefullyLocked();
            } finally {
                lock.unlock();
            }
            handleWriteSideControlFuture(channel.shutdownOutput(), System.nanoTime());
        } finally {
            writeIoLock.unlock();
        }
    }

    void cancelWrite(long code) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "cancelWrite");
        if (!writeAllowed) {
            throw NettyQuicSupport.writeClosedError();
        }
        closeWriteWithReset(code, "");
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
        closeWriteWithReset(code, reason);
    }

    void closeReadWithError(long code, String reason) throws IOException {
        NettyQuicSupport.ensureOffEventLoop(channel, "closeReadWithError");
        closeReadWithStop(code, reason);
    }

    void onSessionClosed(IOException error) {
        lock.lock();
        try {
            sessionError = error;
            releaseInboundLocked();
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
        closeAcceptedPrelude(code);
    }

    void discardAcceptedPrelude(long code) {
        closeAcceptedPrelude(code);
    }

    private void closeAcceptedPrelude(long code) {
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
        } catch (IOException | RuntimeException ignored) {
            // The stream is already being discarded locally.
        } finally {
            try {
                current.close();
            } catch (RuntimeException ignored) {
                // The parent event loop may already be shutting down.
            }
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
        byte[] pendingPrelude = claimOpenPrelude();
        if (pendingPrelude == null) {
            ensureWritableForDataWrite();
            writeInternal(src, offset, length);
            return;
        }
        if (pendingPrelude.length <= Integer.MAX_VALUE - length) {
            int bufferLength = pendingPrelude.length + length;
            if (bufferLength <= MAX_BUFFERED_WRITE_BYTES) {
                ByteBuf buffer = null;
                try {
                    buffer = channel.alloc().ioBuffer(bufferLength, bufferLength);
                    if (pendingPrelude.length > 0) {
                        buffer.writeBytes(pendingPrelude);
                    }
                    buffer.writeBytes(src, offset, length);
                } catch (RuntimeException failure) {
                    releaseBuffer(buffer);
                    abortOpenPreludeSubmission();
                    throw failure;
                }
                submitBufferedWrite(buffer, true, bufferLength);
                return;
            }
        }
        submitOpenPrelude(pendingPrelude, true);
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
        int cursor = offset;
        int remaining = length;
        while (remaining > 0) {
            int chunk = Math.min(remaining, MAX_BUFFERED_WRITE_BYTES);
            writeInternal(newWriteBuffer(src, cursor, chunk));
            cursor += chunk;
            remaining -= chunk;
        }
    }

    private void writeInternal(byte[][] parts, int totalLength) throws IOException {
        int index = 0;
        int offset = 0;
        int remaining = totalLength;
        while (remaining > 0) {
            int chunk = Math.min(remaining, MAX_BUFFERED_WRITE_BYTES);
            ByteBuf buffer = null;
            try {
                buffer = channel.alloc().ioBuffer(chunk, chunk);
                int chunkRemaining = chunk;
                while (chunkRemaining > 0) {
                    byte[] part = parts[index];
                    if (offset >= part.length) {
                        ++index;
                        offset = 0;
                        continue;
                    }
                    int take = Math.min(part.length - offset, chunkRemaining);
                    buffer.writeBytes(part, offset, take);
                    offset += take;
                    chunkRemaining -= take;
                    if (offset == part.length) {
                        ++index;
                        offset = 0;
                    }
                }
            } catch (RuntimeException failure) {
                releaseBuffer(buffer);
                throw failure;
            }
            writeInternal(buffer);
            remaining -= chunk;
        }
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

    private void submitBufferedWrite(ByteBuf buffer, boolean commitsLocalOpen, int bytes) throws IOException {
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
        awaitWriteFuture(future, bytes, startedAtNanos);
    }

    private void submitOpenPrelude(byte[] pendingPrelude, boolean commitsLocalOpen) throws IOException {
        if (!commitsLocalOpen) {
            return;
        }
        if (pendingPrelude.length == 0) {
            completeOpenPreludeSubmission();
            session.noteControlProgress();
            return;
        }
        ByteBuf buffer = null;
        try {
            buffer = channel.alloc().ioBuffer(pendingPrelude.length, pendingPrelude.length);
            buffer.writeBytes(pendingPrelude);
        } catch (RuntimeException failure) {
            releaseBuffer(buffer);
            abortOpenPreludeSubmission();
            throw failure;
        }
        submitBufferedWrite(buffer, true, pendingPrelude.length);
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
            prelude = null;
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
        markLocalWriteFailure(translated, future.cause());
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
                markLocalWriteFailure(translated, future.cause());
            }
        });
    }

    private void markLocalWriteFailure(IOException translated, Throwable cause) {
        lock.lock();
        try {
            if (translated instanceof ApplicationError
                    || translated instanceof WriteClosedException
                    || cause instanceof io.netty.channel.socket.ChannelOutputShutdownException) {
                writeHalf.closeLocal(translated);
            }
            signalWriteChangedLocked();
        } finally {
            lock.unlock();
        }
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

    private void closeLocalWriteGracefullyLocked() throws IOException {
        if (writeHalf.localClosed()) {
            throw localWriteErrorOrDefault();
        }
        ensureSessionOpenForControlLocked();
        writeHalf.closeLocal(NettyQuicSupport.writeClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
        signalWriteChangedLocked();
    }

    private void closeReadWithStop(long code, String reason) throws IOException {
        int quicCode = requireReadControlCode(code);
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                reason,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.STOPPED
        );
        ensureOpenPreludeForReadStop();
        closeLocalRead(error, true);
        dispatchControlFuture(channel.shutdownInput(quicCode));
    }

    private void closeWriteWithReset(long code, String reason) throws IOException {
        int quicCode = requireWriteControlCode(code, "write", ZmuxErrorDirection.WRITE);
        ensureOpenPrelude();
        ApplicationError error = NettyQuicSupport.streamApplicationError(
                code,
                reason,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );
        closeLocalWrite(error);
        handleWriteSideControlFuture(channel.shutdownOutput(quicCode), System.nanoTime());
        session.noteResetReason(code);
    }

    private int requireReadControlCode(long code) throws IOException {
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            return NettyQuicSupport.requireQuicApplicationCode(
                    code,
                    "read",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.READ
            );
        } finally {
            lock.unlock();
        }
    }

    private int requireWriteControlCode(long code, String operation, ZmuxErrorDirection direction) throws IOException {
        lock.lock();
        try {
            if (writeHalf.localClosed()) {
                throw localWriteErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            return NettyQuicSupport.requireQuicApplicationCode(code, operation, ZmuxErrorScope.STREAM, direction);
        } finally {
            lock.unlock();
        }
    }

    private void closeLocalRead(IOException error, boolean releaseInbound) throws IOException {
        lock.lock();
        try {
            if (readHalf.localClosed()) {
                throw localReadErrorOrDefault();
            }
            ensureSessionOpenForControlLocked();
            readHalf.closeLocal(error);
            if (releaseInbound) {
                releaseInboundLocked();
            }
            signalReadChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    private void closeLocalWrite(IOException error) throws IOException {
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
        if (inboundOverflow == null) {
            return null;
        }
        ByteBuf next = inboundOverflow.pollFirst();
        if (inboundOverflow.isEmpty()) {
            inboundOverflow = null;
        }
        return next;
    }

    private void awaitReadChangeInterruptibly() throws InterruptedIOException {
        awaitChangeInterruptibly(true);
    }

    private boolean awaitReadChangeNanosInterruptibly(long nanos) throws InterruptedIOException {
        return awaitChangeNanosInterruptibly(true, nanos);
    }

    private void awaitWriteChangeInterruptibly() throws InterruptedIOException {
        awaitChangeInterruptibly(false);
    }

    private boolean awaitWriteChangeNanosInterruptibly(long nanos) throws InterruptedIOException {
        return awaitChangeNanosInterruptibly(false, nanos);
    }

    private void awaitChangeInterruptibly(boolean readSide) throws InterruptedIOException {
        Condition changed = readSide ? readChanged : writeChanged;
        long observedSignalVersion = signalVersion(readSide);
        incrementSignalWaiters(readSide);
        try {
            while (signalVersion(readSide) == observedSignalVersion) {
                changed.await();
            }
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            decrementSignalWaiters(readSide);
        }
    }

    private boolean awaitChangeNanosInterruptibly(boolean readSide, long nanos) throws InterruptedIOException {
        Condition changed = readSide ? readChanged : writeChanged;
        long observedSignalVersion = signalVersion(readSide);
        long deadlineNanos = deadlineNanos(nanos);
        incrementSignalWaiters(readSide);
        try {
            while (signalVersion(readSide) == observedSignalVersion) {
                long remainingNanos = remainingNanos(deadlineNanos);
                if (remainingNanos <= 0L) {
                    return true;
                }
                long remainingAfterWait = changed.awaitNanos(remainingNanos);
                if (remainingAfterWait <= 0L && signalVersion(readSide) == observedSignalVersion) {
                    return true;
                }
            }
            return false;
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interruptedIo(
                    "waiting for stream state change",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } finally {
            decrementSignalWaiters(readSide);
        }
    }

    private long signalVersion(boolean readSide) {
        return readSide ? readSignalVersion : writeSignalVersion;
    }

    private void incrementSignalWaiters(boolean readSide) {
        if (readSide) {
            readWaiters++;
        } else {
            writeWaiters++;
        }
    }

    private void decrementSignalWaiters(boolean readSide) {
        if (readSide) {
            readWaiters--;
        } else {
            writeWaiters--;
        }
    }

    private void signalStateChangedLocked() {
        signalReadChangedLocked();
        signalWriteChangedLocked();
    }

    private void signalReadChangedLocked() {
        readSignalVersion++;
        if (readWaiters > 0) {
            readChanged.signalAll();
        }
    }

    private void signalWriteChangedLocked() {
        writeSignalVersion++;
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

    private interface IoRunnable {
        void run() throws IOException;
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
