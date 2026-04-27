package io.zmux.adapter.quic.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.quic.*;
import io.netty.util.concurrent.Future;
import io.zmux.*;
import io.zmux.internal.TimeoutBudget;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("resource")
final class NettyQuicSession implements ZmuxSession {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final AtomicLong HANDLER_SEQUENCE = new AtomicLong();

    private final QuicChannel channel;
    private final Duration acceptedPreludeReadTimeout;
    private final Semaphore prepareSlots;
    private final int pendingPrepareCapacity;
    private final ReentrantLock prepareLock = new ReentrantLock();
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Condition lifecycleChanged = lifecycleLock.newCondition();
    private final NettyQuicSupport.AcceptQueue<NettyQuicBidiStream> bidiAcceptQueue =
            new NettyQuicSupport.AcceptQueue<>(NettyQuicSupport.ACCEPT_RESULT_QUEUE_CAPACITY);
    private final NettyQuicSupport.AcceptQueue<NettyQuicRecvStream> uniAcceptQueue =
            new NettyQuicSupport.AcceptQueue<>(NettyQuicSupport.ACCEPT_RESULT_QUEUE_CAPACITY);
    private final Set<NettyQuicStreamState> activeStreams = ConcurrentHashMap.newKeySet();
    private final Set<NettyQuicStreamState> preparingStreams = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicLong acceptedStreams = new AtomicLong();
    private final AtomicLong sentDataBytes = new AtomicLong();
    private final AtomicLong receivedDataBytes = new AtomicLong();
    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong blockedWriteTotalNanos = new AtomicLong();
    private final AtomicLong hiddenRefusedStreams = new AtomicLong();
    private final AtomicLong hiddenReapedStreams = new AtomicLong();
    private final AtomicLong hiddenUnreadBytesDiscarded = new AtomicLong();
    private final AtomicLong keepaliveTimeouts = new AtomicLong();
    private final AtomicLong lateDataAfterCloseReadBytes = new AtomicLong();
    private final AtomicLong lateDataAfterResetBytes = new AtomicLong();
    private final AtomicLong lateDataAfterAbortBytes = new AtomicLong();
    private final Object resetReasonCountsLock = new Object();
    private final Object abortReasonCountsLock = new Object();
    private final ConcurrentHashMap<Long, AtomicLong> resetReasonCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicLong> abortReasonCounts = new ConcurrentHashMap<>();
    private final AtomicLong resetReasonOverflowCount = new AtomicLong();
    private final AtomicLong abortReasonOverflowCount = new AtomicLong();
    private final ParentHandler parentHandler = new ParentHandler();
    private final String handlerName = "zmux-netty-quic-" + saturatingIncrement(HANDLER_SEQUENCE);
    private final long timeOriginNanos;
    private final Instant timeOriginInstant;
    private ArrayDeque<NettyQuicStreamState> pendingPrepare = new ArrayDeque<>();
    private volatile IOException closeError;
    private volatile QuicConnectionCloseEvent closeEvent;
    private volatile long lastInboundFrameAtNanos;
    private volatile long lastControlProgressAtNanos;
    private volatile long lastTransportWriteAtNanos;
    private volatile long lastStreamProgressAtNanos;
    private int lifecycleWaiters;
    private volatile long lastApplicationProgressAtNanos;
    private volatile long lastFlushAtNanos;
    private volatile int lastFlushFrames;
    private volatile long lastFlushBytes;
    private volatile long lastOpenLatencyNanos;

    NettyQuicSession(QuicChannel channel, NettyQuicSessionOptions options) {
        this.channel = Objects.requireNonNull(channel, "channel");
        NettyQuicSessionOptions normalized = options == null ? NettyQuicSessionOptions.defaults() : options;
        this.acceptedPreludeReadTimeout = normalized.normalizedAcceptedPreludeReadTimeout();
        int maxConcurrent = normalized.normalizedAcceptedPreludeMaxConcurrent();
        this.prepareSlots = new Semaphore(maxConcurrent);
        this.pendingPrepareCapacity = NettyQuicSupport.acceptedPreludePendingCapacity(maxConcurrent);
        this.timeOriginNanos = System.nanoTime();
        this.timeOriginInstant = Instant.now();
        installParentHandler();
        this.channel.closeFuture().addListener(future -> onSessionClosed(defaultSessionCloseError(future.cause())));
        if (!channel.isOpen()) {
            onSessionClosed(defaultSessionCloseError(channel.closeFuture().cause()));
        }
    }

    static ZmuxSession closedSession() {
        return NettyQuicSupport.CLOSED_SESSION;
    }

    private static SessionStats.QueueStats reducedQueueStats(long queuedDataBytes) {
        return new SessionStats.QueueStats(
                0,
                0,
                0,
                queuedDataBytes,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L
        );
    }

    private static SessionStats.ProvisionalStats reducedProvisionalStats() {
        return SessionStats.ProvisionalStats.empty();
    }

    private static SessionStats.ActiveStreamStats activeStreamStats(Iterable<NettyQuicStreamState> states) {
        long localBidi = 0L;
        long localUni = 0L;
        long peerBidi = 0L;
        long peerUni = 0L;
        for (NettyQuicStreamState state : states) {
            if (state == null || !state.activeForStats()) {
                continue;
            }
            if (state.openedLocally()) {
                if (state.bidirectional()) {
                    localBidi = NettyQuicSupport.saturatingAdd(localBidi, 1L);
                } else {
                    localUni = NettyQuicSupport.saturatingAdd(localUni, 1L);
                }
            } else if (state.bidirectional()) {
                peerBidi = NettyQuicSupport.saturatingAdd(peerBidi, 1L);
            } else {
                peerUni = NettyQuicSupport.saturatingAdd(peerUni, 1L);
            }
        }
        return new SessionStats.ActiveStreamStats(localBidi, localUni, peerBidi, peerUni);
    }

    private static SessionStats.PressureStats reducedPressureStats(long trackedMemoryBytes,
                                                                   long trackedRetainedStateBytes,
                                                                   long hiddenRetainedCount,
                                                                   long hiddenRetainedBytes,
                                                                   long acceptBacklogCount,
                                                                   long acceptBacklogRetainedBytes,
                                                                   long bufferedInboundBytes,
                                                                   long recvSessionReceivedBytes) {
        return new SessionStats.PressureStats(
                trackedMemoryBytes,
                trackedRetainedStateBytes,
                new SessionStats.RetainedStateBreakdownStats(
                        new SessionStats.RetainedBucketStats(hiddenRetainedCount, hiddenRetainedBytes),
                        new SessionStats.RetainedBucketStats(acceptBacklogCount, acceptBacklogRetainedBytes),
                        SessionStats.RetainedBucketStats.empty(),
                        SessionStats.RetainedBucketStats.empty(),
                        SessionStats.RetainedBucketStats.empty()
                ),
                0L,
                0L,
                false,
                bufferedInboundBytes,
                bufferedInboundBytes,
                0L,
                recvSessionReceivedBytes,
                bufferedInboundBytes,
                0L
        );
    }

    private static long saturatingAdd(AtomicLong counter, long delta) {
        if (delta <= 0L) {
            return counter.get();
        }
        while (true) {
            long current = counter.get();
            long next = current > Long.MAX_VALUE - delta ? Long.MAX_VALUE : current + delta;
            if (counter.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    private static long saturatingIncrement(AtomicLong counter) {
        return saturatingAdd(counter, 1L);
    }

    private static int hashSetCapacity(int estimatedSize) {
        if (estimatedSize <= 2) {
            return 4;
        }
        return estimatedSize > (Integer.MAX_VALUE / 2) ? Integer.MAX_VALUE : estimatedSize * 2;
    }

    private static void noteReasonLocked(ConcurrentHashMap<Long, AtomicLong> counters,
                                         AtomicLong overflow,
                                         long code) {
        AtomicLong counter = counters.get(code);
        if (counter == null) {
            if (counters.size() >= SessionStats.ReasonStats.MAX_TRACKED_CODES) {
                saturatingIncrement(overflow);
                return;
            }
            counter = new AtomicLong();
            counters.put(code, counter);
        }
        saturatingIncrement(counter);
    }

    private static Map<Long, Long> copyReasonCounts(ConcurrentHashMap<Long, AtomicLong> counters) {
        if (counters.isEmpty()) {
            return Collections.emptyMap();
        }
        Long singleCode = null;
        long singleCount = 0L;
        HashMap<Long, Long> snapshot = null;
        for (Map.Entry<Long, AtomicLong> entry : counters.entrySet()) {
            long value = entry.getValue().get();
            if (value > 0L) {
                if (snapshot != null) {
                    snapshot.put(entry.getKey(), value);
                } else if (singleCode == null) {
                    singleCode = entry.getKey();
                    singleCount = value;
                } else {
                    snapshot = new HashMap<>(counters.size());
                    snapshot.put(singleCode, singleCount);
                    snapshot.put(entry.getKey(), value);
                }
            }
        }
        if (snapshot != null) {
            return Collections.unmodifiableMap(snapshot);
        }
        return singleCode == null
                ? Collections.emptyMap()
                : Collections.singletonMap(singleCode, singleCount);
    }

    private static StateMembership snapshotHiddenStates(
            List<NettyQuicStreamState> pendingPrepareSnapshot,
            List<NettyQuicStreamState> preparingSnapshot) {
        int estimatedSize = pendingPrepareSnapshot.size() + preparingSnapshot.size();
        if (estimatedSize == 0) {
            return StateMembership.EMPTY;
        }
        StateMembership.Builder states = new StateMembership.Builder(estimatedSize);
        appendUniqueStates(states, pendingPrepareSnapshot);
        appendUniqueStates(states, preparingSnapshot);
        return states.build();
    }

    private static AcceptBacklogSnapshot snapshotAcceptBacklog(
            List<NettyQuicBidiStream> bidiAcceptSnapshot,
            List<NettyQuicRecvStream> uniAcceptSnapshot) {
        int estimatedSize = bidiAcceptSnapshot.size() + uniAcceptSnapshot.size();
        if (estimatedSize == 0) {
            return AcceptBacklogSnapshot.EMPTY;
        }
        StateMembership.Builder states = new StateMembership.Builder(estimatedSize);
        long bufferedInboundBytes = 0L;
        bufferedInboundBytes = appendQueuedStreamStates(states, bidiAcceptSnapshot, bufferedInboundBytes);
        bufferedInboundBytes = appendQueuedStreamStates(states, uniAcceptSnapshot, bufferedInboundBytes);
        StateMembership membership = states.build();
        return membership.isEmpty()
                ? AcceptBacklogSnapshot.EMPTY
                : new AcceptBacklogSnapshot(membership, bufferedInboundBytes);
    }

    private static void appendUniqueStates(StateMembership.Builder target, Iterable<NettyQuicStreamState> source) {
        for (NettyQuicStreamState state : source) {
            if (state != null) {
                target.add(state);
            }
        }
    }

    private static long appendQueuedStreamStates(StateMembership.Builder target,
                                                 Iterable<? extends AbstractNettyQuicStream> source,
                                                 long bufferedInboundBytes) {
        for (AbstractNettyQuicStream stream : source) {
            if (stream != null && stream.state != null) {
                target.add(stream.state);
                bufferedInboundBytes = NettyQuicSupport.saturatingAdd(
                        bufferedInboundBytes,
                        Math.max(0L, stream.state.bufferedInboundBytes())
                );
            }
        }
        return bufferedInboundBytes;
    }

    private static boolean isKeepaliveTimeout(IOException error) {
        if (error instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) error;
            return applicationError.scope() == io.zmux.ZmuxErrorScope.SESSION
                    && applicationError.terminationKind() == ZmuxTerminationKind.TIMEOUT;
        }
        return false;
    }

    @Override
    public ZmuxStream acceptStream() throws IOException, InterruptedException {
        return acceptStream(null);
    }

    @Override
    public ZmuxStream acceptStream(Duration timeout) throws IOException, InterruptedException {
        NettyQuicSupport.ensureOffEventLoop(channel, "acceptStream");
        return acceptQueuedStream(bidiAcceptQueue, timeout);
    }

    @Override
    public ZmuxRecvStream acceptUniStream() throws IOException, InterruptedException {
        return acceptUniStream(null);
    }

    @Override
    public ZmuxRecvStream acceptUniStream(Duration timeout) throws IOException, InterruptedException {
        NettyQuicSupport.ensureOffEventLoop(channel, "acceptUniStream");
        return acceptQueuedStream(uniAcceptQueue, timeout);
    }

    @Override
    public ZmuxStream openStream() throws IOException, InterruptedException {
        return openBidiStream(OpenOptions.empty(), TimeoutBudget.unbounded());
    }

    @Override
    public ZmuxStream openStream(OpenOptions options) throws IOException, InterruptedException {
        return openBidiStream(options, TimeoutBudget.unbounded());
    }

    @Override
    public ZmuxStream openStreamWithTimeout(Duration timeout) throws IOException, InterruptedException {
        return openStreamWithTimeout(OpenOptions.empty(), timeout);
    }

    @Override
    public ZmuxStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return openBidiStream(options, TimeoutBudget.fromTimeout(timeout));
    }

    @Override
    public ZmuxSendStream openUniStream() throws IOException, InterruptedException {
        return openUniSendStream(OpenOptions.empty(), TimeoutBudget.unbounded());
    }

    @Override
    public ZmuxSendStream openUniStream(OpenOptions options) throws IOException, InterruptedException {
        return openUniSendStream(options, TimeoutBudget.unbounded());
    }

    @Override
    public ZmuxSendStream openUniStreamWithTimeout(Duration timeout) throws IOException, InterruptedException {
        return openUniStreamWithTimeout(OpenOptions.empty(), timeout);
    }

    @Override
    public ZmuxSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return openUniSendStream(options, TimeoutBudget.fromTimeout(timeout));
    }

    @Override
    public ZmuxStream openAndSend(byte[] data) throws IOException, InterruptedException {
        return openAndSend(OpenOptions.empty(), data);
    }

    @Override
    public ZmuxStream openAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException {
        ZmuxStream stream = openBidiStream(options, TimeoutBudget.unbounded());
        if (data != null && data.length > 0) {
            stream.write(data);
        }
        return stream;
    }

    @Override
    public ZmuxStream openAndSendWithTimeout(Duration timeout, byte[] data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    public ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        NettyQuicBidiStream stream = openBidiStream(options, budget);
        if (data == null || data.length == 0) {
            return stream;
        }
        if (budget.bounded()) {
            stream.state.setWriteDeadlineNanos(budget.deadlineNanos());
        }
        try {
            stream.write(data);
            return stream;
        } finally {
            if (budget.bounded()) {
                stream.state.setWriteDeadlineNanos(0L);
            }
        }
    }

    @Override
    public ZmuxSendStream openUniAndSend(byte[] data) throws IOException, InterruptedException {
        return openUniAndSend(OpenOptions.empty(), data);
    }

    @Override
    public ZmuxSendStream openUniAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException {
        ZmuxSendStream stream = openUniSendStream(options, TimeoutBudget.unbounded());
        stream.writeFinal(data == null ? EMPTY_BYTES : data);
        return stream;
    }

    @Override
    public ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    public ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        NettyQuicSendStream stream = openUniSendStream(options, budget);
        if (budget.bounded()) {
            stream.state.setWriteDeadlineNanos(budget.deadlineNanos());
        }
        try {
            stream.writeFinal(data == null ? EMPTY_BYTES : data);
            return stream;
        } finally {
            if (budget.bounded()) {
                stream.state.setWriteDeadlineNanos(0L);
            }
        }
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        if (closed.get()) {
            return;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "closeWithError");
        int quicCode = NettyQuicSupport.requireQuicApplicationCode(
                code,
                "close",
                io.zmux.ZmuxErrorScope.SESSION,
                io.zmux.ZmuxErrorDirection.BOTH
        );
        beginClosing(NettyQuicSupport.sessionApplicationError(
                code,
                reason == null ? "" : reason,
                ZmuxErrorSource.LOCAL,
                ZmuxTerminationKind.SESSION_TERMINATION
        ));
        NettyQuicSupport.awaitChannelFuture(
                channel.close(true, quicCode, Unpooled.wrappedBuffer(NettyQuicSupport.encodeReason(reason)))
        );
        noteControlProgress();
    }

    @Override
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        NettyQuicSupport.ensureOffEventLoop(channel, "awaitTermination");
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        try {
            if (!budget.bounded()) {
                channel.closeFuture().await();
                return true;
            }
            long timeoutNanos = budget.remainingNanos();
            if (timeoutNanos <= 0L) {
                return channel.closeFuture().isDone();
            }
            return channel.closeFuture().await(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interrupted(
                    "zmux-netty-quic: interrupted while waiting for session termination",
                    "wait",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        }
    }

    @Override
    public Optional<IOException> terminationCause() {
        if (!isClosed()) {
            return Optional.empty();
        }
        IOException error = closeError;
        if (error instanceof ApplicationError && ((ApplicationError) error).code() == 0L) {
            return Optional.empty();
        }
        if (error instanceof io.zmux.SessionClosedException) {
            return Optional.empty();
        }
        return Optional.ofNullable(error);
    }

    @Override
    public boolean isClosed() {
        return closed.get() || channel.closeFuture().isDone();
    }

    @Override
    public SessionState state() {
        if (isClosed()) {
            return closedState();
        }
        if (closeEvent != null) {
            return SessionState.DRAINING;
        }
        if (closing.get()) {
            return SessionState.CLOSING;
        }
        return SessionState.READY;
    }

    @Override
    public SessionStats stats() {
        SessionState currentState = state();
        List<NettyQuicStreamState> preparingSnapshot = snapshotPreparingStates();
        List<NettyQuicBidiStream> bidiAcceptSnapshot = bidiAcceptQueue.snapshot();
        List<NettyQuicRecvStream> uniAcceptSnapshot = uniAcceptQueue.snapshot();
        List<NettyQuicStreamState> pendingPrepareSnapshot = snapshotPendingPrepareStates();

        AcceptBacklogSnapshot acceptBacklogSnapshot = snapshotAcceptBacklog(bidiAcceptSnapshot, uniAcceptSnapshot);
        StateMembership hiddenStates = snapshotHiddenStates(pendingPrepareSnapshot, preparingSnapshot);
        StatsAccumulator stats = new StatsAccumulator(
                acceptBacklogSnapshot.states(),
                hiddenStates,
                activeStreams.size() + preparingSnapshot.size() + pendingPrepareSnapshot.size()
        );
        stats.addAll(activeStreams);
        stats.addAll(preparingSnapshot);
        stats.addAll(pendingPrepareSnapshot);
        long trackedRetainedStateBytes = stats.trackedRetainedStateBytes();
        long recvSessionReceivedBytes = NettyQuicSupport.saturatingAdd(
                receivedDataBytes.get(),
                stats.totalBufferedInboundBytes()
        );

        long acceptBacklogCount = bidiAcceptSnapshot.size() + (long) uniAcceptSnapshot.size();
        long acceptBacklogBytes = acceptBacklogSnapshot.bufferedInboundBytes();
        SessionStats.AcceptBacklogStats acceptBacklog = reducedAcceptBacklogStats(acceptBacklogCount, acceptBacklogBytes);
        SessionStats.QueueStats queues = reducedQueueStats(stats.totalBufferedInboundBytes());
        SessionStats.PressureStats pressure = reducedPressureStats(
                stats.trackedMemoryBytes(),
                trackedRetainedStateBytes,
                stats.hiddenRetainedCount(),
                stats.hiddenRetainedBytes(),
                acceptBacklogCount,
                stats.acceptBacklogRetainedBytes(),
                stats.totalBufferedInboundBytes(),
                recvSessionReceivedBytes
        );
        SessionStats.ProgressStats progress = reducedProgressStats();
        SessionStats.FlushStats flush = new SessionStats.FlushStats(
                flushCount.get(),
                instantForNanos(lastFlushAtNanos),
                lastFlushFrames,
                lastFlushBytes
        );
        SessionStats.KeepaliveStats keepalive = reducedTransportKeepaliveStats();
        SessionStats.ActiveStreamStats activeStreamStats = activeStreamStats(activeStreams);
        return new SessionStats(
                currentState,
                0L,
                0L,
                sentDataBytes.get(),
                receivedDataBytes.get(),
                stats.trackedStateCount(),
                acceptedStreams.get(),
                activeStreamStats,
                acceptBacklog,
                stats.retainedOpenInfoBytes(),
                0L,
                0L,
                0L,
                keepalive,
                progress,
                flush,
                blockedWriteTotalNanos.get(),
                lastOpenLatencyNanos,
                queues,
                reducedProvisionalStats(),
                reducedHiddenStateStats(stats.hiddenRetainedCount()),
                new SessionStats.ReasonStats(
                        copyReasonCounts(resetReasonCounts),
                        resetReasonOverflowCount.get(),
                        copyReasonCounts(abortReasonCounts),
                        abortReasonOverflowCount.get()
                ),
                reducedDiagnosticStats(),
                pressure
        );
    }

    private SessionStats.AcceptBacklogStats reducedAcceptBacklogStats(long count, long bytes) {
        int countLimit = bidiAcceptQueue.capacity() + uniAcceptQueue.capacity();
        return new SessionStats.AcceptBacklogStats(
                count,
                bytes,
                countLimit,
                0L,
                count >= countLimit,
                false,
                0L
        );
    }

    private SessionStats.ProgressStats reducedProgressStats() {
        return new SessionStats.ProgressStats(
                instantForNanos(lastInboundFrameAtNanos),
                instantForNanos(lastControlProgressAtNanos),
                instantForNanos(lastTransportWriteAtNanos),
                instantForNanos(lastStreamProgressAtNanos),
                instantForNanos(lastApplicationProgressAtNanos),
                null,
                null
        );
    }

    private SessionStats.HiddenStateStats reducedHiddenStateStats(long hiddenRetainedCount) {
        return new SessionStats.HiddenStateStats(
                (int) Math.min(Integer.MAX_VALUE, hiddenRetainedCount),
                0,
                0,
                false,
                false,
                0,
                0,
                hiddenRefusedStreams.get(),
                hiddenReapedStreams.get(),
                hiddenUnreadBytesDiscarded.get()
        );
    }

    private SessionStats.DiagnosticStats reducedDiagnosticStats() {
        return new SessionStats.DiagnosticStats(
                0L,
                0L,
                lateDataAfterCloseReadBytes.get(),
                lateDataAfterResetBytes.get(),
                lateDataAfterAbortBytes.get(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                keepaliveTimeouts.get(),
                0L,
                0L,
                0L,
                0L,
                0
        );
    }

    @Override
    public void close() throws IOException {
        if (closed.get()) {
            return;
        }
        NettyQuicSupport.ensureOffEventLoop(channel, "close");
        beginClosing(NettyQuicSupport.sessionClosedError(ZmuxErrorSource.LOCAL));
        NettyQuicSupport.awaitChannelFuture(channel.close(true, 0, Unpooled.EMPTY_BUFFER));
        noteControlProgress();
    }

    void unregister(NettyQuicStreamState state) {
        activeStreams.remove(state);
    }

    void noteInboundFrame(int bytes) {
        if (bytes < 0) {
            return;
        }
        lastInboundFrameAtNanos = System.nanoTime();
    }

    void noteDataRead(int bytes) {
        if (bytes <= 0) {
            return;
        }
        long nowNanos = System.nanoTime();
        saturatingAdd(receivedDataBytes, bytes);
        lastStreamProgressAtNanos = nowNanos;
        lastApplicationProgressAtNanos = nowNanos;
    }

    void noteDataWrite(int bytes) {
        if (bytes <= 0) {
            return;
        }
        long nowNanos = System.nanoTime();
        saturatingAdd(sentDataBytes, bytes);
        lastStreamProgressAtNanos = nowNanos;
        lastApplicationProgressAtNanos = nowNanos;
    }

    void noteFlush(int bytes, long completedAtNanos) {
        if (bytes < 0) {
            return;
        }
        saturatingIncrement(flushCount);
        lastTransportWriteAtNanos = completedAtNanos;
        lastFlushAtNanos = completedAtNanos;
        lastFlushFrames = 1;
        lastFlushBytes = bytes;
    }

    void noteBlockedWrite(long blockedNanos) {
        if (blockedNanos <= 0L) {
            return;
        }
        saturatingAdd(blockedWriteTotalNanos, blockedNanos);
    }

    void noteTransportWriteProgress(long completedAtNanos) {
        if (completedAtNanos <= 0L) {
            return;
        }
        lastTransportWriteAtNanos = completedAtNanos;
    }

    void noteResetReason(long code) {
        synchronized (this.resetReasonCountsLock) {
            noteReasonLocked(resetReasonCounts, resetReasonOverflowCount, code);
        }
    }

    void noteAbortReason(long code) {
        synchronized (this.abortReasonCountsLock) {
            noteReasonLocked(abortReasonCounts, abortReasonOverflowCount, code);
        }
    }

    void noteObservedStreamReason(IOException error) {
        if (!(error instanceof ApplicationError)) {
            return;
        }
        ApplicationError applicationError = (ApplicationError) error;
        switch (applicationError.terminationKind()) {
            case RESET:
                noteResetReason(applicationError.code());
                break;
            case ABORT:
                noteAbortReason(applicationError.code());
                break;
            default:
                break;
        }
    }

    void noteHiddenStreamRefused() {
        saturatingAdd(hiddenRefusedStreams, 1L);
    }

    void noteControlProgress() {
        lastControlProgressAtNanos = System.nanoTime();
    }

    void noteLateReadDiscard(IOException error, int bytes) {
        if (bytes <= 0 || error == null) {
            return;
        }
        switch (ZmuxErrors.terminationKind(error)) {
            case STOPPED:
                saturatingAdd(lateDataAfterCloseReadBytes, bytes);
                break;
            case RESET:
                saturatingAdd(lateDataAfterResetBytes, bytes);
                break;
            case ABORT:
                saturatingAdd(lateDataAfterAbortBytes, bytes);
                break;
            default:
                break;
        }
    }

    void noteHiddenStreamRefused(long unreadBytesDiscarded) {
        noteHiddenStreamRefused();
        noteHiddenUnreadBytesDiscarded(unreadBytesDiscarded);
    }

    void noteHiddenStreamReaped(long unreadBytesDiscarded) {
        saturatingAdd(hiddenReapedStreams, 1L);
        noteHiddenUnreadBytesDiscarded(unreadBytesDiscarded);
    }

    private void noteHiddenUnreadBytesDiscarded(long unreadBytesDiscarded) {
        if (unreadBytesDiscarded <= 0L) {
            return;
        }
        saturatingAdd(hiddenUnreadBytesDiscarded, unreadBytesDiscarded);
    }

    private void noteOpenLatency(long startedAtNanos, long completedAtNanos) {
        lastOpenLatencyNanos = NettyQuicSupport.positiveElapsedNanos(completedAtNanos, startedAtNanos);
        lastStreamProgressAtNanos = completedAtNanos;
    }

    private Instant instantForNanos(long eventNanos) {
        if (eventNanos == 0L) {
            return null;
        }
        long deltaNanos = eventNanos - timeOriginNanos;
        try {
            return timeOriginInstant.plusNanos(deltaNanos);
        } catch (ArithmeticException ignored) {
            return deltaNanos < 0L ? Instant.MIN : Instant.MAX;
        }
    }

    private List<NettyQuicStreamState> snapshotPreparingStates() {
        Iterator<NettyQuicStreamState> iterator = preparingStreams.iterator();
        if (!iterator.hasNext()) {
            return Collections.emptyList();
        }
        NettyQuicStreamState first = iterator.next();
        if (!iterator.hasNext()) {
            return Collections.singletonList(first);
        }
        ArrayList<NettyQuicStreamState> snapshot = new ArrayList<>(preparingStreams.size());
        snapshot.add(first);
        do {
            snapshot.add(iterator.next());
        } while (iterator.hasNext());
        return snapshot;
    }

    private List<NettyQuicStreamState> snapshotPendingPrepareStates() {
        return NettyQuicSupport.snapshotDeque(prepareLock, pendingPrepare);
    }

    private SessionStats.KeepaliveStats reducedTransportKeepaliveStats() {
        QuicTransportParameters peerTransportParameters = channel.peerTransportParameters();
        long timeoutNanos = 0L;
        if (peerTransportParameters != null) {
            long timeoutMillis = Math.max(0L, peerTransportParameters.maxIdleTimeout());
            timeoutNanos = timeoutMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE
                    : timeoutMillis * 1_000_000L;
        }
        return new SessionStats.KeepaliveStats(
                timeoutNanos > 0L,
                0L,
                0L,
                timeoutNanos,
                false,
                channel.isTimedOut(),
                0L,
                estimateSendRateBytesPerSecond()
        );
    }

    private long estimateSendRateBytesPerSecond() {
        long bytes = sentDataBytes.get();
        long lastTransportWrite = lastTransportWriteAtNanos;
        if (bytes <= 0L || lastTransportWrite == 0L) {
            return 0L;
        }
        long elapsedNanos = NettyQuicSupport.elapsedNanos(lastTransportWrite, timeOriginNanos);
        if (elapsedNanos <= 0L) {
            return 0L;
        }
        long rate = NettyQuicSupport.saturatingMulDivFloor(bytes, 1_000_000_000L, elapsedNanos);
        return Math.max(1L, rate);
    }

    private void installParentHandler() {
        if (NettyQuicSupport.inEventLoop(channel)) {
            channel.pipeline().addFirst(handlerName, parentHandler);
            return;
        }
        NettyQuicSupport.awaitFutureUninterruptibly(
                NettyQuicSupport.submitOnEventLoop(channel, () -> {
                    channel.pipeline().addFirst(handlerName, parentHandler);
                    return null;
                })
        );
    }

    private void onSessionClosed(IOException error) {
        IOException observed = error == null ? sessionUnavailableError() : error;
        boolean committedCloseError = beginClosing(observed);
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        noteControlProgress();
        if (committedCloseError && isKeepaliveTimeout(observed)) {
            saturatingAdd(keepaliveTimeouts, 1L);
        }
        bidiAcceptQueue.close();
        uniAcceptQueue.close();
        IOException sessionError = sessionUnavailableError();
        for (NettyQuicStreamState state : activeStreams) {
            state.onSessionClosed(sessionError);
        }
        NettyQuicSupport.executeOnEventLoop(channel, () -> {
            if (channel.pipeline().context(handlerName) != null) {
                channel.pipeline().remove(handlerName);
            }
        });
    }

    private void ensureSessionOpen() throws IOException {
        if (!acceptingNewStreams()) {
            throw sessionUnavailableError();
        }
    }

    private IOException sessionUnavailableError() {
        IOException error = closeError;
        if (error != null) {
            return error;
        }
        QuicConnectionCloseEvent event = closeEvent;
        if (event != null) {
            return NettyQuicSupport.connectionCloseError(event, null);
        }
        if (closing.get()) {
            return NettyQuicSupport.sessionClosedError(ZmuxErrorSource.LOCAL);
        }
        return NettyQuicSupport.sessionClosedError();
    }

    IOException streamSessionError() {
        return acceptingNewStreams() ? null : sessionUnavailableError();
    }

    private boolean acceptingNewStreams() {
        return !closed.get() && !closing.get() && closeEvent == null;
    }

    private SessionState closedState() {
        IOException error = closeError;
        if (error instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) error;
            return applicationError.code() == 0L && applicationError.reason().isEmpty()
                    ? SessionState.CLOSED
                    : SessionState.FAILED;
        }
        if (error instanceof io.zmux.SessionClosedException) {
            return SessionState.CLOSED;
        }
        if (error != null) {
            return SessionState.FAILED;
        }
        QuicConnectionCloseEvent event = closeEvent;
        if (event != null) {
            return NettyQuicSupport.isGracefulApplicationClose(event) ? SessionState.CLOSED : SessionState.FAILED;
        }
        Throwable cause = channel.closeFuture().cause();
        if (cause != null) {
            IOException translated = NettyQuicSupport.translateThrowable(cause);
            if (translated instanceof ApplicationError) {
                ApplicationError applicationError = (ApplicationError) translated;
                return applicationError.code() == 0L && applicationError.reason().isEmpty()
                        ? SessionState.CLOSED
                        : SessionState.FAILED;
            }
            if (!(translated instanceof io.zmux.SessionClosedException)) {
                return SessionState.FAILED;
            }
        }
        return SessionState.CLOSED;
    }

    private IOException defaultSessionCloseError(Throwable cause) {
        if (closeEvent != null) {
            return NettyQuicSupport.connectionCloseError(closeEvent, cause);
        }
        if (cause != null) {
            return NettyQuicSupport.translateThrowable(cause);
        }
        return NettyQuicSupport.sessionClosedError();
    }

    private void scheduleAccepted(NettyQuicStreamState state) {
        prepareLock.lock();
        try {
            if (!acceptingNewStreams()) {
                noteHiddenStreamReaped(state.discardUnreadInboundBytes());
                state.onSessionClosed(sessionUnavailableError());
                state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
                return;
            }
            drainPendingPreparationsLocked();
            if (pendingPrepare.size() >= pendingPrepareCapacity) {
                refuseHiddenAcceptedStream(state);
                return;
            }
            pendingPrepare.addLast(state);
            drainPendingPreparationsLocked();
        } finally {
            prepareLock.unlock();
        }
    }

    private void refuseHiddenAcceptedStream(NettyQuicStreamState state) {
        if (state == null) {
            return;
        }
        noteHiddenStreamRefused(state.discardUnreadInboundBytes());
        state.rejectAcceptedPrelude(ErrorCode.REFUSED_STREAM.code());
    }

    private void drainPendingPreparationsLocked() {
        boolean drainedAny = false;
        while (!pendingPrepare.isEmpty() && prepareSlots.tryAcquire()) {
            NettyQuicStreamState state = pendingPrepare.removeFirst();
            drainedAny = true;
            preparingStreams.add(state);
            try {
                NettyQuicSupport.executeAcceptedPreludeTask(() -> prepareAcceptedStream(state));
            } catch (RejectedExecutionException ignored) {
                prepareSlots.release();
                cleanupRejectedAcceptedPreparation(state, ErrorCode.REFUSED_STREAM.code());
            }
        }
        if (drainedAny && pendingPrepare.isEmpty()) {
            pendingPrepare = new ArrayDeque<>();
        }
    }

    private void prepareAcceptedStream(NettyQuicStreamState state) {
        try {
            state.prepareAcceptedPrelude(acceptedPreludeReadTimeout);
            publishPreparedAcceptedStream(state);
        } catch (ReadTimeoutException timeout) {
            cleanupRejectedAcceptedPreparation(state, ErrorCode.PROTOCOL.code());
        } catch (InterruptedException | java.io.InterruptedIOException interrupted) {
            Thread.currentThread().interrupt();
            cleanupInterruptedAcceptedPreparation(state);
        } catch (IOException ignored) {
            cleanupRejectedAcceptedPreparation(state, ErrorCode.PROTOCOL.code());
        } finally {
            preparingStreams.remove(state);
            prepareSlots.release();
            prepareLock.lock();
            try {
                drainPendingPreparationsLocked();
            } finally {
                prepareLock.unlock();
            }
        }
    }

    private void cleanupRejectedAcceptedPreparation(NettyQuicStreamState state, long code) {
        if (state == null) {
            return;
        }
        boolean wasPreparing = preparingStreams.remove(state);
        boolean wasActive = activeStreams.remove(state);
        long unreadBytes = state.discardUnreadInboundBytes();
        if (!acceptingNewStreams()) {
            if (wasPreparing || wasActive) {
                noteHiddenStreamReaped(unreadBytes);
            }
            state.onSessionClosed(sessionUnavailableError());
            state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
            return;
        }
        noteHiddenStreamRefused(unreadBytes);
        state.rejectAcceptedPrelude(code);
    }

    private void cleanupInterruptedAcceptedPreparation(NettyQuicStreamState state) {
        if (state == null) {
            return;
        }
        boolean wasPreparing = preparingStreams.remove(state);
        boolean wasActive = activeStreams.remove(state);
        long unreadBytes = state.discardUnreadInboundBytes();
        if (wasPreparing || wasActive) {
            noteHiddenStreamReaped(unreadBytes);
        }
        state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
    }

    boolean publishPreparedAcceptedStream(NettyQuicStreamState state) throws InterruptedException {
        if (state == null || !acceptingNewStreams()) {
            if (state != null) {
                noteHiddenStreamReaped(state.discardUnreadInboundBytes());
                state.onSessionClosed(sessionUnavailableError());
                state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
            }
            return false;
        }
        activeStreams.add(state);
        if (!acceptingNewStreams()) {
            activeStreams.remove(state);
            noteHiddenStreamReaped(state.discardUnreadInboundBytes());
            state.onSessionClosed(sessionUnavailableError());
            state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
            return false;
        }
        boolean published = state.bidirectional()
                ? bidiAcceptQueue.put(new NettyQuicBidiStream(state))
                : uniAcceptQueue.put(new NettyQuicRecvStream(state));
        if (published) {
            return true;
        }
        activeStreams.remove(state);
        noteHiddenStreamReaped(state.discardUnreadInboundBytes());
        state.onSessionClosed(sessionUnavailableError());
        state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
        return false;
    }

    private <T> T openPreparedLocalStream(NettyQuicStreamState state,
                                          T stream,
                                          QuicStreamType streamType,
                                          TimeoutBudget budget) throws IOException, InterruptedException {
        long startedAtNanos = System.nanoTime();
        activeStreams.add(state);
        Future<QuicStreamChannel> future = null;
        try {
            future = channel.createStream(streamType, state.newHandler());
            state.attachChannel(awaitStreamFuture(future, budget));
            state.maybeSendOpenPreludeOnOpen();
            noteOpenLatency(startedAtNanos, System.nanoTime());
            return stream;
        } catch (InterruptedException interrupted) {
            cleanupFailedLocalOpen(state, future);
            throw NettyQuicSupport.interrupted(
                    "zmux-netty-quic: interrupted while opening stream",
                    "open",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        } catch (IOException | Error failure) {
            cleanupFailedLocalOpen(state, future);
            throw failure;
        } catch (RuntimeException failure) {
            cleanupFailedLocalOpen(state, future);
            throw NettyQuicSupport.translateOpenFailure(failure);
        }
    }

    private void cleanupFailedLocalOpen(NettyQuicStreamState state, Future<QuicStreamChannel> future) {
        if (future != null) {
            future.cancel(true);
        }
        activeStreams.remove(state);
        state.closeRaw();
    }

    private NettyQuicBidiStream openBidiStream(OpenOptions options, TimeoutBudget budget)
            throws IOException, InterruptedException {
        NettyQuicSupport.ensureOffEventLoop(channel, budget.bounded() ? "openStreamWithTimeout" : "openStream");
        ensureSessionOpen();
        if (budget.expired()) {
            throw NettyQuicSupport.openTimedOut();
        }
        NettyQuicStreamState state = NettyQuicStreamState.localBidi(this, NettyQuicSupport.normalizeOptions(options));
        state.validatePendingPrelude();
        return openPreparedLocalStream(state, new NettyQuicBidiStream(state), QuicStreamType.BIDIRECTIONAL, budget);
    }

    private NettyQuicSendStream openUniSendStream(OpenOptions options, TimeoutBudget budget)
            throws IOException, InterruptedException {
        NettyQuicSupport.ensureOffEventLoop(channel, budget.bounded() ? "openUniStreamWithTimeout" : "openUniStream");
        ensureSessionOpen();
        if (budget.expired()) {
            throw NettyQuicSupport.openTimedOut();
        }
        NettyQuicStreamState state = NettyQuicStreamState.localSend(this, NettyQuicSupport.normalizeOptions(options));
        state.validatePendingPrelude();
        return openPreparedLocalStream(state, new NettyQuicSendStream(state), QuicStreamType.UNIDIRECTIONAL, budget);
    }

    private QuicStreamChannel awaitStreamFuture(Future<QuicStreamChannel> future, TimeoutBudget budget)
            throws IOException, InterruptedException {
        future.addListener(ignored -> signalLifecycleChanged());
        while (!future.isDone()) {
            awaitOpenProgressOrClose(future, budget);
        }
        if (!acceptingNewStreams()) {
            throw sessionUnavailableError();
        }
        if (!future.isSuccess()) {
            throw NettyQuicSupport.translateOpenFailure(future.cause());
        }
        return future.getNow();
    }

    private void awaitOpenProgressOrClose(Future<?> future, TimeoutBudget budget) throws IOException, InterruptedException {
        lifecycleLock.lockInterruptibly();
        try {
            if (!acceptingNewStreams()) {
                throw sessionUnavailableError();
            }
            if (future != null && future.isDone()) {
                return;
            }
            if (!budget.bounded()) {
                awaitLifecycleChanged();
            } else {
                long remainingNanos = budget.remainingNanos();
                if (remainingNanos <= 0L) {
                    throw NettyQuicSupport.openTimedOut();
                }
                if (!awaitLifecycleChanged(remainingNanos)) {
                    throw NettyQuicSupport.openTimedOut();
                }
            }
            if (!acceptingNewStreams()) {
                throw sessionUnavailableError();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void awaitLifecycleChanged() throws InterruptedException {
        lifecycleWaiters++;
        try {
            lifecycleChanged.await();
        } finally {
            lifecycleWaiters--;
        }
    }

    private boolean awaitLifecycleChanged(long remainingNanos) throws InterruptedException {
        lifecycleWaiters++;
        try {
            return lifecycleChanged.await(remainingNanos, TimeUnit.NANOSECONDS);
        } finally {
            lifecycleWaiters--;
        }
    }

    private boolean beginClosing(IOException error) {
        boolean committedCloseError = commitCloseError(error);
        boolean firstCloseSignal = closing.compareAndSet(false, true);
        signalLifecycleChanged();
        if (!firstCloseSignal) {
            return committedCloseError;
        }
        IOException closingError = sessionUnavailableError();
        bidiAcceptQueue.close(this::discardAcceptedBidi);
        uniAcceptQueue.close(this::discardAcceptedUni);

        prepareLock.lock();
        try {
            while (!pendingPrepare.isEmpty()) {
                NettyQuicStreamState state = pendingPrepare.removeFirst();
                noteHiddenStreamReaped(state.discardUnreadInboundBytes());
                state.onSessionClosed(closingError);
                state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
            }
            pendingPrepare = new ArrayDeque<>();
        } finally {
            prepareLock.unlock();
        }

        for (NettyQuicStreamState state : preparingStreams) {
            if (preparingStreams.remove(state)) {
                noteHiddenStreamReaped(state.discardUnreadInboundBytes());
            }
            state.onSessionClosed(closingError);
            state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
        }
        ArrayList<NettyQuicStreamState> activeSnapshot = new ArrayList<>(activeStreams);
        activeStreams.clear();
        for (NettyQuicStreamState state : activeSnapshot) {
            state.onSessionClosed(closingError);
        }
        return committedCloseError;
    }

    private boolean commitCloseError(IOException error) {
        if (error == null) {
            return false;
        }
        synchronized (this) {
            if (closeError != null) {
                return false;
            }
            closeError = error;
            return true;
        }
    }

    private void signalLifecycleChanged() {
        lifecycleLock.lock();
        try {
            if (lifecycleWaiters > 0) {
                lifecycleChanged.signalAll();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void discardAcceptedBidi(NettyQuicBidiStream stream) {
        if (stream == null) {
            return;
        }
        NettyQuicStreamState state = stream.state;
        activeStreams.remove(state);
        state.discardUnreadInboundBytes();
        state.onSessionClosed(sessionUnavailableError());
        state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
    }

    private void discardAcceptedUni(NettyQuicRecvStream stream) {
        if (stream == null) {
            return;
        }
        NettyQuicStreamState state = stream.state;
        activeStreams.remove(state);
        state.discardUnreadInboundBytes();
        state.onSessionClosed(sessionUnavailableError());
        state.discardAcceptedPrelude(ErrorCode.CANCELLED.code());
    }

    private <T> T acceptQueuedStream(NettyQuicSupport.AcceptQueue<T> queue, Duration timeout)
            throws IOException, InterruptedException {
        T stream;
        try {
            stream = queue.take(timeout, this::sessionUnavailableError);
        } catch (InterruptedException interrupted) {
            throw NettyQuicSupport.interrupted(
                    "zmux-netty-quic: interrupted while accepting stream",
                    "accept",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        }
        if (stream == null) {
            throw NettyQuicSupport.acceptTimedOut();
        }
        saturatingIncrement(acceptedStreams);
        return stream;
    }

    private static final class AcceptBacklogSnapshot {
        private static final AcceptBacklogSnapshot EMPTY = new AcceptBacklogSnapshot(StateMembership.EMPTY, 0L);

        private final StateMembership states;
        private final long bufferedInboundBytes;

        private AcceptBacklogSnapshot(StateMembership states, long bufferedInboundBytes) {
            this.states = states;
            this.bufferedInboundBytes = bufferedInboundBytes;
        }

        private StateMembership states() {
            return states;
        }

        private long bufferedInboundBytes() {
            return bufferedInboundBytes;
        }
    }

    private static final class StateMembership {
        private static final StateMembership EMPTY = new StateMembership(null, null);

        private final NettyQuicStreamState single;
        private final HashSet<NettyQuicStreamState> many;

        private StateMembership(NettyQuicStreamState single, HashSet<NettyQuicStreamState> many) {
            this.single = single;
            this.many = many;
        }

        private boolean isEmpty() {
            return single == null && many == null;
        }

        private boolean contains(NettyQuicStreamState state) {
            if (many != null) {
                return many.contains(state);
            }
            return state != null && single != null && (state == single || single.equals(state));
        }

        private static final class Builder {
            private final int estimatedSize;
            private NettyQuicStreamState single;
            private HashSet<NettyQuicStreamState> many;

            private Builder(int estimatedSize) {
                this.estimatedSize = Math.max(0, estimatedSize);
            }

            private void add(NettyQuicStreamState state) {
                if (state == null) {
                    return;
                }
                if (many != null) {
                    many.add(state);
                    return;
                }
                if (single == null) {
                    single = state;
                    return;
                }
                if (single == state || single.equals(state)) {
                    return;
                }
                many = new HashSet<>(hashSetCapacity(estimatedSize));
                many.add(single);
                single = null;
                many.add(state);
            }

            private StateMembership build() {
                if (many != null) {
                    return many.isEmpty() ? EMPTY : new StateMembership(null, many);
                }
                return single == null ? EMPTY : new StateMembership(single, null);
            }
        }
    }

    private static final class StatsAccumulator {
        private final StateMembership acceptBacklogStates;
        private final StateMembership hiddenStates;
        private final int estimatedTrackedStates;
        private NettyQuicStreamState singleSeen;
        private HashSet<NettyQuicStreamState> seen;
        private long trackedStateCount;
        private long trackedMemoryBytes;
        private long retainedOpenInfoBytes;
        private long totalBufferedInboundBytes;
        private long acceptBacklogRetainedBytes;
        private long hiddenRetainedBytes;
        private long hiddenRetainedCount;

        private StatsAccumulator(StateMembership acceptBacklogStates,
                                 StateMembership hiddenStates,
                                 int estimatedTrackedStates) {
            this.acceptBacklogStates = acceptBacklogStates;
            this.hiddenStates = hiddenStates;
            this.estimatedTrackedStates = Math.max(0, estimatedTrackedStates);
        }

        private void addAll(Iterable<NettyQuicStreamState> states) {
            for (NettyQuicStreamState state : states) {
                add(state);
            }
        }

        private void add(NettyQuicStreamState state) {
            if (state == null || !this.markSeen(state)) {
                return;
            }
            long trackedStateBytes = Math.max(0L, state.trackedMemoryBytes());
            long bufferedInboundBytes = Math.max(0L, state.bufferedInboundBytes());
            long retainedStateBytes = Math.max(0L, trackedStateBytes - bufferedInboundBytes);

            trackedStateCount = NettyQuicSupport.saturatingAdd(trackedStateCount, 1L);
            trackedMemoryBytes = NettyQuicSupport.saturatingAdd(trackedMemoryBytes, trackedStateBytes);
            retainedOpenInfoBytes = NettyQuicSupport.saturatingAdd(
                    retainedOpenInfoBytes,
                    Math.max(0L, state.retainedOpenInfoBytes())
            );
            totalBufferedInboundBytes = NettyQuicSupport.saturatingAdd(totalBufferedInboundBytes, bufferedInboundBytes);
            if (acceptBacklogStates.contains(state)) {
                acceptBacklogRetainedBytes = NettyQuicSupport.saturatingAdd(
                        acceptBacklogRetainedBytes,
                        retainedStateBytes
                );
                return;
            }
            if (hiddenStates.contains(state)) {
                hiddenRetainedBytes = NettyQuicSupport.saturatingAdd(hiddenRetainedBytes, retainedStateBytes);
                hiddenRetainedCount = NettyQuicSupport.saturatingAdd(hiddenRetainedCount, 1L);
            }
        }

        private boolean markSeen(NettyQuicStreamState state) {
            if (seen != null) {
                return seen.add(state);
            }
            if (singleSeen == null) {
                singleSeen = state;
                return true;
            }
            if (singleSeen == state) {
                return false;
            }
            seen = new HashSet<>(hashSetCapacity(estimatedTrackedStates));
            seen.add(singleSeen);
            singleSeen = null;
            return seen.add(state);
        }

        private long trackedStateCount() {
            return trackedStateCount;
        }

        private long trackedMemoryBytes() {
            return trackedMemoryBytes;
        }

        private long retainedOpenInfoBytes() {
            return retainedOpenInfoBytes;
        }

        private long totalBufferedInboundBytes() {
            return totalBufferedInboundBytes;
        }

        private long acceptBacklogRetainedBytes() {
            return acceptBacklogRetainedBytes;
        }

        private long hiddenRetainedBytes() {
            return hiddenRetainedBytes;
        }

        private long hiddenRetainedCount() {
            return hiddenRetainedCount;
        }

        private long trackedRetainedStateBytes() {
            return Math.max(0L, trackedMemoryBytes - totalBufferedInboundBytes);
        }
    }

    private final class ParentHandler extends ChannelDuplexHandler {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof QuicStreamChannel) {
                QuicStreamChannel stream = (QuicStreamChannel) msg;
                NettyQuicStreamState state = stream.type() == QuicStreamType.BIDIRECTIONAL
                        ? NettyQuicStreamState.acceptedBidi(NettyQuicSession.this, stream)
                        : NettyQuicStreamState.acceptedRecv(NettyQuicSession.this, stream);
                stream.pipeline().addLast(state.newHandler());
                state.attachChannel(stream);
                NettyQuicSupport.registerOnEventLoop(ctx.channel(), stream).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        scheduleAccepted(state);
                    } else {
                        state.closeRaw();
                    }
                });
                return;
            }
            super.channelRead(ctx, msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof QuicConnectionCloseEvent) {
                QuicConnectionCloseEvent event = (QuicConnectionCloseEvent) evt;
                closeEvent = event;
                noteControlProgress();
                beginClosing(NettyQuicSupport.connectionCloseError(event, null));
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            beginClosing(NettyQuicSupport.translateThrowable(cause));
            ctx.close();
        }
    }
}
