package io.zmux.adapter.quic.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.*;
import io.netty.util.concurrent.Future;
import io.zmux.*;
import io.zmux.internal.TimeoutBudget;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

@SuppressWarnings("resource")
final class NettyQuicSupport {
    static final byte[] EMPTY_BYTES = new byte[0];
    static final long OPEN_CAPABILITIES =
            io.zmux.Protocol.CAPABILITY_OPEN_METADATA
                    | io.zmux.Protocol.CAPABILITY_PRIORITY_HINTS
                    | io.zmux.Protocol.CAPABILITY_STREAM_GROUPS;
    static final int ACCEPT_RESULT_QUEUE_CAPACITY = 32;
    static final int ACCEPT_PRELUDE_PENDING_MIN_CAPACITY = ACCEPT_RESULT_QUEUE_CAPACITY * 2;
    static final int ACCEPT_PRELUDE_PENDING_MAX_CAPACITY = 4096;
    static final int STREAM_PRELUDE_MAX_PAYLOAD = 16 << 10;
    static final long STREAM_INBOUND_AUTO_READ_HIGH_WATERMARK = 1L << 20;
    static final long STREAM_INBOUND_AUTO_READ_LOW_WATERMARK =
            STREAM_INBOUND_AUTO_READ_HIGH_WATERMARK >>> 1;
    static final byte[] EMPTY_STREAM_PRELUDE = new byte[]{0};
    static final ZmuxSession CLOSED_SESSION = Zmux.closedSession();
    private static final int ACCEPTED_PRELUDE_WORKER_MAX_CAP = NettyQuic.MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT;
    private static final int ACCEPTED_PRELUDE_WORKER_QUEUE_CAPACITY_CAP = ACCEPT_PRELUDE_PENDING_MAX_CAPACITY * 16;
    private static final int MAX_ERROR_UNWRAP_DEPTH = 64;
    private static final AtomicLong PRELUDE_WORKER_SEQUENCE = new AtomicLong();
    private static final int ACCEPTED_PRELUDE_WORKER_MAX = positiveIntegerProperty(
            "io.zmux.netty.acceptedPreludeWorkers",
            defaultAcceptedPreludeWorkerMax(),
            ACCEPTED_PRELUDE_WORKER_MAX_CAP
    );
    private static final int ACCEPTED_PRELUDE_WORKER_QUEUE_CAPACITY = positiveIntegerProperty(
            "io.zmux.netty.acceptedPreludeWorkerQueueCapacity",
            4096,
            ACCEPTED_PRELUDE_WORKER_QUEUE_CAPACITY_CAP
    );
    private static final ThreadPoolExecutor ACCEPTED_PRELUDE_EXECUTOR = new ThreadPoolExecutor(
            ACCEPTED_PRELUDE_WORKER_MAX,
            ACCEPTED_PRELUDE_WORKER_MAX,
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(ACCEPTED_PRELUDE_WORKER_QUEUE_CAPACITY),
            runnable -> {
                Thread thread = new Thread(
                        runnable,
                        "zmux-netty-quic-prelude-" + saturatingIncrement(PRELUDE_WORKER_SEQUENCE)
                );
                thread.setDaemon(true);
                return thread;
            }
    );

    static {
        ACCEPTED_PRELUDE_EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private NettyQuicSupport() {
    }

    static Duration normalizeAcceptedPreludeReadTimeout(Duration timeout) {
        return NettyQuicSessionOptions.normalizeAcceptedPreludeReadTimeout(timeout);
    }

    static int normalizeAcceptedPreludeMaxConcurrent(int maxConcurrent) {
        return NettyQuicSessionOptions.normalizeAcceptedPreludeMaxConcurrent(maxConcurrent);
    }

    static int acceptedPreludePendingCapacity(int maxConcurrent) {
        long scaled = Math.max(
                ACCEPT_PRELUDE_PENDING_MIN_CAPACITY,
                Math.max(1L, maxConcurrent) * 4L
        );
        return (int) Math.min(scaled, ACCEPT_PRELUDE_PENDING_MAX_CAPACITY);
    }

    static OpenOptions normalizeOptions(OpenOptions options) {
        return options == null ? OpenOptions.empty() : options;
    }

    static byte[] encodeReason(String reason) {
        if (reason == null || reason.isEmpty()) {
            return EMPTY_BYTES;
        }
        return reason.getBytes(StandardCharsets.UTF_8);
    }

    static String decodeReason(byte[] reasonBytes) {
        if (reasonBytes == null || reasonBytes.length == 0) {
            return "";
        }
        return new String(reasonBytes, StandardCharsets.UTF_8);
    }

    static ApplicationError applicationCloseError(QuicConnectionCloseEvent event) {
        if (event == null || !event.isApplicationClose()) {
            return null;
        }
        long code = Integer.toUnsignedLong(event.error());
        String reason = decodeReason(safeCloseReason(event));
        if (code == 0 && reason.isEmpty()) {
            return null;
        }
        return sessionApplicationError(code, reason, ZmuxErrorSource.REMOTE, ZmuxTerminationKind.SESSION_TERMINATION);
    }

    static IOException connectionCloseError(QuicConnectionCloseEvent event, Throwable cause) {
        ApplicationError applicationError = applicationCloseError(event);
        if (applicationError != null) {
            return applicationError;
        }
        if (event != null) {
            return transportCloseError(event, cause);
        }
        if (cause != null) {
            return translateThrowable(cause);
        }
        return sessionClosedError(ZmuxErrorSource.TRANSPORT);
    }

    static long durationToPositiveNanos(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            return 0L;
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    static long deadlineNanos(Instant deadline) {
        if (deadline == null) {
            return 0L;
        }
        Instant nowInstant = Instant.now();
        long nowNanos = System.nanoTime();
        if (!deadline.isAfter(nowInstant)) {
            return nowNanos;
        }
        long deltaNanos = durationToPositiveNanos(Duration.between(nowInstant, deadline));
        if (deltaNanos <= 0L) {
            return nowNanos;
        }
        return saturatingAdd(nowNanos, deltaNanos);
    }

    static long deadlineFromTimeout(Duration timeout) {
        if (timeout == null) {
            return Long.MAX_VALUE;
        }
        long waitNanos = durationToPositiveNanos(timeout);
        if (waitNanos <= 0L) {
            return System.nanoTime();
        }
        return saturatingAdd(System.nanoTime(), waitNanos);
    }

    static long saturatingAdd(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    static long saturatingIncrement(AtomicLong counter) {
        while (true) {
            long current = counter.get();
            if (current == Long.MAX_VALUE) {
                return current;
            }
            long next = current + 1L;
            if (counter.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    static long elapsedNanos(long laterNanos, long earlierNanos) {
        long elapsed = laterNanos - earlierNanos;
        if (elapsed >= 0L) {
            return elapsed;
        }
        return laterNanos >= earlierNanos ? Long.MAX_VALUE : 0L;
    }

    static long positiveElapsedNanos(long laterNanos, long earlierNanos) {
        long elapsed = elapsedNanos(laterNanos, earlierNanos);
        return elapsed <= 0L ? 1L : elapsed;
    }

    static long saturatingMulDivFloor(long value, long multiplier, long divisor) {
        return io.zmux.internal.RuntimeFlow.saturatingMulDivFloor(value, multiplier, divisor);
    }

    static ApplicationError sessionApplicationError(long code,
                                                    String reason,
                                                    ZmuxErrorSource source,
                                                    ZmuxTerminationKind terminationKind) {
        return new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.SESSION,
                source,
                ZmuxErrorDirection.BOTH,
                terminationKind
        );
    }

    static ApplicationError streamApplicationError(long code,
                                                   String reason,
                                                   ZmuxErrorSource source,
                                                   ZmuxErrorDirection direction,
                                                   ZmuxTerminationKind terminationKind) {
        return streamApplicationError(code, reason, null, source, direction, terminationKind);
    }

    static ApplicationError streamApplicationError(long code,
                                                   String reason,
                                                   Throwable cause,
                                                   ZmuxErrorSource source,
                                                   ZmuxErrorDirection direction,
                                                   ZmuxTerminationKind terminationKind) {
        ApplicationError error = new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.STREAM,
                source,
                direction,
                terminationKind
        );
        if (cause != null) {
            error.initCause(cause);
        }
        return error;
    }

    static int requireQuicApplicationCode(long code,
                                          String operation,
                                          ZmuxErrorScope scope,
                                          ZmuxErrorDirection direction) throws IOException {
        if (code < 0 || code > 0xffff_ffffL) {
            throw new AdapterUnsupportedException(
                    "zmux: Netty QUIC adapter supports only 32-bit application error codes",
                    operation,
                    scope,
                    direction
            );
        }
        return (int) code;
    }

    static IOException translateThrowable(Throwable cause) {
        return translateThrowable(cause, new IdentityHashMap<>(), 0);
    }

    private static IOException translateThrowable(Throwable cause,
                                                  IdentityHashMap<Throwable, Boolean> seen,
                                                  int depth) {
        if (cause == null) {
            return sessionClosedError(ZmuxErrorSource.TRANSPORT);
        }
        if (depth > MAX_ERROR_UNWRAP_DEPTH || seen.put(cause, Boolean.TRUE) != null) {
            return transportRuntimeFailure(cause);
        }
        if (cause instanceof io.netty.channel.socket.ChannelOutputShutdownException) {
            io.netty.channel.socket.ChannelOutputShutdownException shutdown =
                    (io.netty.channel.socket.ChannelOutputShutdownException) cause;
            return new WriteClosedException(ZmuxErrorSource.REMOTE, ZmuxTerminationKind.STOPPED, shutdown);
        }
        if (cause instanceof QuicStreamResetException) {
            QuicStreamResetException reset = (QuicStreamResetException) cause;
            long code = reset.applicationProtocolCode();
            return code >= 0
                    ? streamApplicationError(
                    code,
                    "",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.WRITE,
                    ZmuxTerminationKind.STOPPED
            )
                    : writeClosedError(ZmuxErrorSource.REMOTE, ZmuxTerminationKind.STOPPED);
        }
        if (cause instanceof QuicClosedChannelException) {
            QuicClosedChannelException closed = (QuicClosedChannelException) cause;
            return connectionCloseError(closed.event(), closed);
        }
        if (cause instanceof QuicTimeoutClosedChannelException) {
            return sessionApplicationError(
                    ErrorCode.IDLE_TIMEOUT.code(),
                    "",
                    ZmuxErrorSource.TRANSPORT,
                    ZmuxTerminationKind.TIMEOUT
            );
        }
        if (cause instanceof ClosedChannelException) {
            ClosedChannelException closedChannel = (ClosedChannelException) cause;
            return sessionClosedError(ZmuxErrorSource.TRANSPORT, closedChannel);
        }
        if (cause instanceof QuicException) {
            QuicException quic = (QuicException) cause;
            QuicTransportError transportError = quic.error();
            if (transportError == QuicTransportError.STREAM_LIMIT_ERROR) {
                return new OpenLimitedException(quic, ZmuxErrorSource.TRANSPORT);
            }
            if (transportError != null) {
                return transportError(transportError, quic);
            }
        }
        if (cause instanceof CancellationException) {
            CancellationException cancelled = (CancellationException) cause;
            IOException nested = nestedIOException(cancelled.getCause());
            return nested == null
                    ? sessionClosedError(ZmuxErrorSource.TRANSPORT, cancelled)
                    : translateThrowable(nested, seen, depth + 1);
        }
        if (cause instanceof IOException) {
            IOException ioException = (IOException) cause;
            IOException nested = nestedIOException(ioException.getCause());
            if (nested != null) {
                return translateThrowable(nested, seen, depth + 1);
            }
            if (ZmuxErrors.details(ioException) != null) {
                return ioException;
            }
            return transportRuntimeFailure(ioException);
        }
        IOException nested = nestedIOException(cause.getCause());
        if (nested != null) {
            return translateThrowable(nested, seen, depth + 1);
        }
        return transportRuntimeFailure(cause);
    }

    private static IOException transportRuntimeFailure(Throwable cause) {
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "netty quic",
                cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage(),
                cause,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    static IOException translateReadThrowable(Throwable cause) {
        if (cause instanceof QuicStreamResetException) {
            QuicStreamResetException reset = (QuicStreamResetException) cause;
            long code = reset.applicationProtocolCode();
            return code >= 0
                    ? streamApplicationError(
                    code,
                    "",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ,
                    ZmuxTerminationKind.RESET
            )
                    : readClosedError(ZmuxErrorSource.REMOTE, ZmuxTerminationKind.RESET);
        }
        return translateThrowable(cause);
    }

    static IOException translateWriteThrowable(Throwable cause) {
        return translateThrowable(cause);
    }

    static IOException translateOpenFailure(Throwable cause) {
        IOException translated = translateThrowable(cause);
        if (cause instanceof QuicException) {
            QuicException quic = (QuicException) cause;
            QuicTransportError transportError = quic.error();
            if (transportError == QuicTransportError.STREAM_LIMIT_ERROR) {
                return translated instanceof OpenLimitedException
                        ? translated
                        : new OpenLimitedException(translated, ZmuxErrorSource.TRANSPORT);
            }
        }
        return translated;
    }

    static AcceptTimeoutException acceptTimedOut() {
        return new AcceptTimeoutException();
    }

    static OpenTimeoutException openTimedOut() {
        return new OpenTimeoutException();
    }

    static IOException sessionClosedError() {
        return sessionClosedError(ZmuxErrorSource.LOCAL);
    }

    static IOException sessionClosedError(ZmuxErrorSource source) {
        return new SessionClosedException(source);
    }

    static IOException sessionClosedError(ZmuxErrorSource source, Throwable cause) {
        return new SessionClosedException(source, cause);
    }

    static IOException sessionOperationError(String operation, IOException error) {
        if (error == null || operation == null || operation.isEmpty()) {
            return error;
        }
        ZmuxErrorSource source = ZmuxErrors.source(error);
        ZmuxTerminationKind terminationKind = ZmuxErrors.terminationKind(error);
        if (terminationKind == ZmuxTerminationKind.UNKNOWN) {
            terminationKind = ZmuxTerminationKind.SESSION_TERMINATION;
        }
        if (error instanceof SessionClosedException) {
            return new SessionClosedException(source, error, operation);
        }
        if (error instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) error;
            return new ApplicationError(
                    applicationError.code(),
                    applicationError.reason(),
                    ZmuxErrorScope.SESSION,
                    source,
                    ZmuxErrorDirection.BOTH,
                    terminationKind,
                    operation
            );
        }
        return new ZmuxException(
                ZmuxErrors.code(error, ErrorCode.INTERNAL.code()),
                operation,
                ZmuxErrors.reason(error),
                error,
                ZmuxErrorScope.SESSION,
                source,
                ZmuxErrorDirection.BOTH,
                terminationKind
        );
    }

    static IOException readClosedError() {
        return readClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED);
    }

    static IOException readClosedError(ZmuxErrorSource source, ZmuxTerminationKind terminationKind) {
        return new ReadClosedException(source, terminationKind);
    }

    static IOException writeClosedError() {
        return writeClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
    }

    static IOException writeClosedError(ZmuxErrorSource source, ZmuxTerminationKind terminationKind) {
        return new WriteClosedException(source, terminationKind);
    }

    static IOException emptyMetadataUpdateError() {
        return new EmptyMetadataUpdateException();
    }

    static ZmuxInterruptedIOException interruptedIo(String operation,
                                                    ZmuxErrorScope scope,
                                                    ZmuxErrorDirection direction,
                                                    InterruptedException cause) {
        Thread.currentThread().interrupt();
        return new ZmuxInterruptedIOException(
                "zmux-netty-quic: interrupted while " + operation,
                operation,
                scope,
                ZmuxErrorSource.LOCAL,
                direction,
                cause
        );
    }

    static ZmuxInterruptedException interrupted(String operation,
                                                ZmuxErrorScope scope,
                                                ZmuxErrorDirection direction,
                                                InterruptedException cause) {
        return interrupted("zmux-netty-quic: interrupted while " + operation, operation, scope, direction, cause);
    }

    static ZmuxInterruptedException interrupted(String message,
                                                String operation,
                                                ZmuxErrorScope scope,
                                                ZmuxErrorDirection direction,
                                                InterruptedException cause) {
        return new ZmuxInterruptedException(
                message,
                operation,
                scope,
                ZmuxErrorSource.LOCAL,
                direction,
                cause
        );
    }

    static ChannelFuture awaitFutureUninterruptibly(ChannelFuture future) {
        awaitFutureCompletionUninterruptibly(future);
        if (!future.isSuccess()) {
            throw uncheckedNettyFailure(future.cause());
        }
        return future;
    }

    static ChannelFuture awaitChannelFuture(ChannelFuture future) throws IOException {
        while (!future.isDone()) {
            try {
                future.await();
            } catch (InterruptedException interrupted) {
                throw interruptedIo("waiting for channel operation", ZmuxErrorScope.SESSION, ZmuxErrorDirection.BOTH, interrupted);
            }
        }
        if (!future.isSuccess()) {
            throw translateThrowable(future.cause());
        }
        return future;
    }

    static void dispatchChannelFuture(ChannelFuture future) {
        if (future == null) {
            return;
        }
        future.addListener(ignored -> {
        });
    }

    static <T> T awaitFutureUninterruptibly(Future<T> future) {
        awaitFutureCompletionUninterruptibly(future);
        if (!future.isSuccess()) {
            throw uncheckedNettyFailure(future.cause());
        }
        return future.getNow();
    }

    static <T> T awaitFuture(Future<T> future) throws IOException {
        while (!future.isDone()) {
            try {
                future.await();
            } catch (InterruptedException interrupted) {
                throw interruptedIo("waiting for future completion", ZmuxErrorScope.SESSION, ZmuxErrorDirection.BOTH, interrupted);
            }
        }
        if (!future.isSuccess()) {
            throw translateThrowable(future.cause());
        }
        return future.getNow();
    }

    static boolean inEventLoop(Channel channel) {
        return channel != null && channel.eventLoop().inEventLoop();
    }

    static void executeOnEventLoop(Channel channel, Runnable action) {
        if (channel != null) {
            channel.eventLoop().execute(action);
        }
    }

    static <T> Future<T> submitOnEventLoop(Channel channel, java.util.concurrent.Callable<T> action) {
        return channel.eventLoop().submit(action);
    }

    static ChannelFuture registerOnEventLoop(Channel parent, Channel child) {
        return parent.eventLoop().register(child);
    }

    static void ensureOffEventLoop(Channel channel, String operation) {
        if (inEventLoop(channel)) {
            throw new IllegalStateException("zmux-netty-quic: " + operation + " must not block the Netty event loop");
        }
    }

    private static UncheckedIOException uncheckedNettyFailure(Throwable cause) {
        return new UncheckedIOException("zmux-netty-quic: Netty operation failed", translateThrowable(cause));
    }

    private static byte[] safeCloseReason(QuicConnectionCloseEvent event) {
        try {
            return event.reason();
        } catch (NullPointerException ignored) {
            return null;
        }
    }

    static boolean isGracefulApplicationClose(QuicConnectionCloseEvent event) {
        if (event == null || !event.isApplicationClose()) {
            return false;
        }
        byte[] reason = safeCloseReason(event);
        return Integer.toUnsignedLong(event.error()) == 0L && (reason == null || reason.length == 0);
    }

    private static IOException transportCloseError(QuicConnectionCloseEvent event, Throwable cause) {
        if (event == null) {
            return sessionClosedError(ZmuxErrorSource.REMOTE, cause);
        }
        long code = Integer.toUnsignedLong(event.error());
        String reason = decodeReason(safeCloseReason(event));
        if (code == QuicTransportError.NO_ERROR.code() && reason.isEmpty()) {
            return sessionClosedError(ZmuxErrorSource.REMOTE, cause);
        }
        return new ZmuxException(
                code,
                "netty quic",
                reason.isEmpty() ? transportErrorLabel(code, event.isTlsError()) : reason,
                cause,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private static IOException transportError(QuicTransportError error, Throwable cause) {
        long code = error.code();
        return new ZmuxException(
                code,
                "netty quic",
                transportErrorLabel(code, error.isCryptoError()),
                cause,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private static String transportErrorLabel(long code, boolean cryptoError) {
        if (cryptoError) {
            return "CRYPTO_ERROR(" + code + ")";
        }
        try {
            return QuicTransportError.valueOf(code).name();
        } catch (IllegalArgumentException ignored) {
            return "TRANSPORT_ERROR(" + code + ")";
        }
    }

    private static IOException nestedIOException(Throwable cause) {
        Throwable current = cause;
        IdentityHashMap<Throwable, Boolean> seen = new IdentityHashMap<>();
        int depth = 0;
        while (current != null && depth <= MAX_ERROR_UNWRAP_DEPTH && seen.put(current, Boolean.TRUE) == null) {
            if (current instanceof IOException) {
                return (IOException) current;
            }
            current = current.getCause();
            depth++;
        }
        return null;
    }

    static boolean isBenignCloseError(IOException error) {
        if (error == null) {
            return true;
        }
        if (error.getCause() instanceof io.netty.channel.socket.ChannelOutputShutdownException) {
            return true;
        }
        Object details = ZmuxErrors.details(error);
        return details instanceof ApplicationError
                || details instanceof ReadClosedException
                || details instanceof WriteClosedException
                || details instanceof SessionClosedException;
    }

    static <T> List<T> snapshotDeque(ReentrantLock lock, ArrayDeque<T> queue) {
        lock.lock();
        try {
            return snapshotDequeLocked(queue);
        } finally {
            lock.unlock();
        }
    }

    static <T> List<T> snapshotDequeLocked(ArrayDeque<T> queue) {
        if (queue.isEmpty()) {
            return Collections.emptyList();
        }
        if (queue.size() == 1) {
            return Collections.singletonList(queue.getFirst());
        }
        return new ArrayList<>(queue);
    }

    static void executeAcceptedPreludeTask(Runnable task) {
        ACCEPTED_PRELUDE_EXECUTOR.execute(task);
    }

    private static int defaultAcceptedPreludeWorkerMax() {
        int processors = Runtime.getRuntime().availableProcessors();
        return Math.max(8, Math.min(64, processors * 4));
    }

    static int positiveIntegerProperty(String name, int fallback, int cap) {
        Integer value = Integer.getInteger(name);
        if (value == null || value <= 0) {
            return fallback;
        }
        return cap > 0 ? Math.min(value, cap) : value;
    }

    private static void awaitFutureCompletionUninterruptibly(Future<?> future) {
        boolean interrupted = false;
        while (!future.isDone()) {
            try {
                future.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    static final class AcceptQueue<T> {
        private final int capacity;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        private final Condition notFull = lock.newCondition();
        private final ArrayDeque<T> queue = new ArrayDeque<>();
        private boolean closed;
        private int notEmptyWaiters;
        private int notFullWaiters;

        AcceptQueue(int capacity) {
            this.capacity = capacity;
        }

        boolean put(T value) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (!closed && queue.size() >= capacity) {
                    awaitNotFull();
                }
                if (closed) {
                    return false;
                }
                queue.addLast(value);
                signalNotEmpty();
                return true;
            } finally {
                lock.unlock();
            }
        }

        T take(Duration timeout, Supplier<? extends IOException> closeErrorSupplier) throws InterruptedException, IOException {
            TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
            lock.lockInterruptibly();
            try {
                if (!budget.bounded()) {
                    while (queue.isEmpty() && !closed) {
                        awaitNotEmpty();
                    }
                } else {
                    while (queue.isEmpty() && !closed) {
                        long nanos = budget.remainingNanos();
                        if (nanos <= 0L) {
                            return null;
                        }
                        awaitNotEmpty(nanos);
                    }
                }
                if (!queue.isEmpty()) {
                    T value = queue.removeFirst();
                    signalNotFull();
                    return value;
                }
                IOException closeError = closeErrorSupplier == null ? null : closeErrorSupplier.get();
                throw closeError == null ? sessionClosedError() : closeError;
            } finally {
                lock.unlock();
            }
        }

        void close() {
            close(null);
        }

        void close(Consumer<T> onDiscard) {
            boolean hasSingleDiscarded = false;
            T singleDiscarded = null;
            ArrayList<T> discarded = null;
            lock.lock();
            try {
                closed = true;
                if (onDiscard != null && !queue.isEmpty()) {
                    singleDiscarded = queue.removeFirst();
                    hasSingleDiscarded = true;
                    if (!queue.isEmpty()) {
                        discarded = new ArrayList<>(queue.size());
                        while (!queue.isEmpty()) {
                            discarded.add(queue.removeFirst());
                        }
                    }
                }
                signalAllNotEmpty();
                signalAllNotFull();
            } finally {
                lock.unlock();
            }
            if (hasSingleDiscarded) {
                onDiscard.accept(singleDiscarded);
            }
            if (discarded != null) {
                for (T value : discarded) {
                    onDiscard.accept(value);
                }
            }
        }

        int size() {
            lock.lock();
            try {
                return queue.size();
            } finally {
                lock.unlock();
            }
        }

        int capacity() {
            return capacity;
        }

        List<T> snapshot() {
            return snapshotDeque(lock, queue);
        }

        long sumLong(java.util.function.ToLongFunction<T> extractor) {
            lock.lock();
            try {
                long total = 0L;
                for (T value : queue) {
                    total = saturatingAdd(total, Math.max(0L, extractor.applyAsLong(value)));
                }
                return total;
            } finally {
                lock.unlock();
            }
        }

        private void awaitNotEmpty() throws InterruptedException {
            notEmptyWaiters++;
            try {
                notEmpty.await();
            } finally {
                notEmptyWaiters--;
            }
        }

        private boolean awaitNotEmpty(long nanos) throws InterruptedException {
            notEmptyWaiters++;
            try {
                return notEmpty.awaitNanos(nanos) <= 0L;
            } finally {
                notEmptyWaiters--;
            }
        }

        private void awaitNotFull() throws InterruptedException {
            notFullWaiters++;
            try {
                notFull.await();
            } finally {
                notFullWaiters--;
            }
        }

        private void signalNotEmpty() {
            if (notEmptyWaiters > 0) {
                notEmpty.signal();
            }
        }

        private void signalAllNotEmpty() {
            if (notEmptyWaiters > 0) {
                notEmpty.signalAll();
            }
        }

        private void signalNotFull() {
            if (notFullWaiters > 0) {
                notFull.signal();
            }
        }

        private void signalAllNotFull() {
            if (notFullWaiters > 0) {
                notFull.signalAll();
            }
        }
    }

}
