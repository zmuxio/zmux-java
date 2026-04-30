package io.zmux;

import io.zmux.internal.StreamIoSupport;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.GatheringByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("resource")
public final class JoinedDuplexConnection implements DuplexConnection {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition inputChanged = lock.newCondition();
    private final Condition outputChanged = lock.newCondition();
    private final JoinedInputStream inputView = new JoinedInputStream();
    private final JoinedOutputStream outputView = new JoinedOutputStream();
    private final JoinedGatheringOutput gatheringOutputView = new JoinedGatheringOutput();
    private final SocketAddress fallbackLocalAddress;
    private final SocketAddress fallbackRemoteAddress;

    private InputStream inputHalf;
    private OutputStream outputHalf;
    private GatheringByteChannel gatheringOutput;
    private boolean inputPaused;
    private boolean outputPaused;
    private int activeInputOperations;
    private int activeOutputOperations;
    private int activeInputDeadlineOperations;
    private int activeOutputDeadlineOperations;
    private int inputWaiters;
    private int outputWaiters;
    private Instant readDeadline;
    private Instant writeDeadline;
    private long readDeadlineGeneration;
    private long writeDeadlineGeneration;
    private boolean closed;

    public JoinedDuplexConnection(InputStream inputHalf, OutputStream outputHalf) {
        this(inputHalf, outputHalf, null, null, null);
    }

    public JoinedDuplexConnection(ReadHalf inputHalf, WriteHalf outputHalf) {
        this(
                wrap(inputHalf),
                wrap(outputHalf),
                gatheringOutput(outputHalf),
                localAddress(inputHalf, outputHalf),
                remoteAddress(inputHalf, outputHalf)
        );
    }

    public JoinedDuplexConnection(ZmuxRecvStream inputHalf, ZmuxSendStream outputHalf) {
        this(inputHalf, (WriteHalf) outputHalf);
    }

    public JoinedDuplexConnection(InputStream inputHalf,
                                  OutputStream outputHalf,
                                  GatheringByteChannel gatheringOutput,
                                  SocketAddress localAddress,
                                  SocketAddress remoteAddress) {
        this.inputHalf = inputHalf;
        this.outputHalf = outputHalf;
        this.gatheringOutput = gatheringOutput;
        this.fallbackLocalAddress = localAddress;
        this.fallbackRemoteAddress = remoteAddress;
    }

    private static InputStream wrap(ZmuxRecvStream inputHalf) {
        return inputHalf == null ? null : new StreamInputHalf(inputHalf);
    }

    private static InputStream wrap(ReadHalf inputHalf) {
        return inputHalf == null ? null : new GenericInputHalf(inputHalf);
    }

    private static OutputStream wrap(ZmuxSendStream outputHalf) {
        return outputHalf == null ? null : new StreamOutputHalf(outputHalf);
    }

    private static OutputStream wrap(WriteHalf outputHalf) {
        return outputHalf == null ? null : new GenericOutputHalf(outputHalf);
    }

    private static GatheringByteChannel gatheringOutput(WriteHalf outputHalf) {
        return outputHalf == null ? null : outputHalf.gatheringOutput();
    }

    private static SocketAddress localAddress(ReadHalf inputHalf, WriteHalf outputHalf) {
        SocketAddress address = inputHalf == null ? null : inputHalf.localAddress();
        return address != null ? address : (outputHalf == null ? null : outputHalf.localAddress());
    }

    private static SocketAddress localAddress(ZmuxRecvStream inputHalf, ZmuxSendStream outputHalf) {
        SocketAddress address = inputHalf == null ? null : inputHalf.localAddress();
        return address != null ? address : (outputHalf == null ? null : outputHalf.localAddress());
    }

    private static SocketAddress remoteAddress(ReadHalf inputHalf, WriteHalf outputHalf) {
        SocketAddress address = inputHalf == null ? null : inputHalf.remoteAddress();
        return address != null ? address : (outputHalf == null ? null : outputHalf.remoteAddress());
    }

    private static SocketAddress remoteAddress(ZmuxRecvStream inputHalf, ZmuxSendStream outputHalf) {
        SocketAddress address = inputHalf == null ? null : inputHalf.remoteAddress();
        return address != null ? address : (outputHalf == null ? null : outputHalf.remoteAddress());
    }

    private static SocketTimeoutException pauseTimeout() {
        return new SocketTimeoutException("zmux: joined connection pause timed out");
    }

    private static Object closeIdentity(AutoCloseable closeable) {
        return closeable instanceof CloseIdentityHalf ? ((CloseIdentityHalf) closeable).closeIdentity() : closeable;
    }

    private static AutoCloseable closeTarget(AutoCloseable closeable) {
        Object identity = closeIdentity(closeable);
        return identity instanceof AutoCloseable ? (AutoCloseable) identity : closeable;
    }

    private static IOException closeOnce(AutoCloseable closeable,
                                         IdentityHashMap<Object, Boolean> closedObjects,
                                         IOException current) {
        if (closeable == null || closedObjects.put(closeIdentity(closeable), Boolean.TRUE) != null) {
            return current;
        }
        try {
            closeable.close();
            return current;
        } catch (IOException error) {
            if (ignorableCloseFailure(error)) {
                return current;
            }
            return appendCloseFailure(current, error);
        } catch (Exception error) {
            return appendCloseFailure(current, new IOException("zmux: failed to close joined connection half", error));
        }
    }

    private static boolean ignorableCloseFailure(IOException error) {
        return ZmuxErrors.readClosed(error)
                || ZmuxErrors.writeClosed(error)
                || ZmuxErrors.sessionClosed(error);
    }

    private static IOException appendCloseFailure(IOException current, IOException next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static boolean awaitUntilDeadline(Condition condition, Instant deadline) throws InterruptedException {
        if (deadline == null) {
            condition.await();
            return true;
        }
        Instant now = Instant.now();
        if (!deadline.isAfter(now)) {
            return false;
        }
        long remainingNanos;
        try {
            remainingNanos = Duration.between(now, deadline).toNanos();
        } catch (ArithmeticException overflow) {
            remainingNanos = Long.MAX_VALUE;
        }
        if (remainingNanos <= 0L) {
            return false;
        }
        return condition.await(remainingNanos, TimeUnit.NANOSECONDS);
    }

    private static SocketTimeoutException readDeadlineTimeout() {
        return new SocketTimeoutException("zmux: joined connection read deadline exceeded");
    }

    private static SocketTimeoutException writeDeadlineTimeout() {
        return new SocketTimeoutException("zmux: joined connection write deadline exceeded");
    }

    private static SocketAddress localAddress(Object half) {
        return half instanceof AddressAwareHalf ? ((AddressAwareHalf) half).localAddress() : null;
    }

    private static SocketAddress remoteAddress(Object half) {
        return half instanceof AddressAwareHalf ? ((AddressAwareHalf) half).remoteAddress() : null;
    }

    private static ReadHalf typedReadHalf(InputStream half) {
        return half instanceof TypedInputHalf ? ((TypedInputHalf) half).readHalf() : null;
    }

    private static WriteHalf typedWriteHalf(OutputStream half) {
        return half instanceof TypedOutputHalf ? ((TypedOutputHalf) half).writeHalf() : null;
    }

    @Override
    public InputStream input() {
        return inputView;
    }

    public InputStream inputHalf() {
        lock.lock();
        try {
            return inputPaused || closed ? null : inputHalf;
        } finally {
            lock.unlock();
        }
    }

    public ReadHalf readHalf() {
        lock.lock();
        try {
            return inputPaused || closed ? null : typedReadHalf(inputHalf);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public OutputStream output() {
        return outputView;
    }

    public OutputStream outputHalf() {
        lock.lock();
        try {
            return outputPaused || closed ? null : outputHalf;
        } finally {
            lock.unlock();
        }
    }

    public WriteHalf writeHalf() {
        lock.lock();
        try {
            return outputPaused || closed ? null : typedWriteHalf(outputHalf);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public GatheringByteChannel gatheringOutput() {
        lock.lock();
        try {
            return outputPaused || closed || gatheringOutput == null ? null : gatheringOutputView;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SocketAddress localAddress() {
        lock.lock();
        try {
            SocketAddress address = localAddress(inputHalf);
            if (address != null) {
                return address;
            }
            address = localAddress(outputHalf);
            if (address != null) {
                return address;
            }
            return fallbackLocalAddress != null ? fallbackLocalAddress : ZmuxSocketAddress.localPending();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SocketAddress remoteAddress() {
        lock.lock();
        try {
            SocketAddress address = remoteAddress(inputHalf);
            if (address != null) {
                return address;
            }
            address = remoteAddress(outputHalf);
            if (address != null) {
                return address;
            }
            return fallbackRemoteAddress != null ? fallbackRemoteAddress : ZmuxSocketAddress.remotePending();
        } finally {
            lock.unlock();
        }
    }

    public PausedInput pauseInput() throws IOException, InterruptedException {
        return pauseInput(null);
    }

    public PausedInput pauseInput(Duration timeout) throws IOException, InterruptedException {
        lock.lockInterruptibly();
        boolean ownsPause = false;
        try {
            PauseDeadline deadline = PauseDeadline.from(timeout);
            while (inputPaused && !closed) {
                awaitInput(deadline);
            }
            ensureOpenLocked();
            inputPaused = true;
            ownsPause = true;
            signalInputChangedLocked();
            while ((activeInputOperations > 0 || activeInputDeadlineOperations > 0) && !closed) {
                awaitInput(deadline);
            }
            ensureOpenLocked();
            InputStream current = inputHalf;
            inputHalf = null;
            signalInputChangedLocked();
            return new PausedInput(this, current);
        } catch (IOException | InterruptedException error) {
            if (ownsPause) {
                inputPaused = false;
                signalInputChangedLocked();
            }
            throw error;
        } finally {
            lock.unlock();
        }
    }

    public PausedInput pauseRead() throws IOException, InterruptedException {
        return pauseInput();
    }

    public PausedInput pauseRead(Duration timeout) throws IOException, InterruptedException {
        return pauseInput(timeout);
    }

    public PausedOutput pauseOutput() throws IOException, InterruptedException {
        return pauseOutput(null);
    }

    public PausedOutput pauseOutput(Duration timeout) throws IOException, InterruptedException {
        lock.lockInterruptibly();
        boolean ownsPause = false;
        try {
            PauseDeadline deadline = PauseDeadline.from(timeout);
            while (outputPaused && !closed) {
                awaitOutput(deadline);
            }
            ensureOpenLocked();
            outputPaused = true;
            ownsPause = true;
            signalOutputChangedLocked();
            while ((activeOutputOperations > 0 || activeOutputDeadlineOperations > 0) && !closed) {
                awaitOutput(deadline);
            }
            ensureOpenLocked();
            OutputStream current = outputHalf;
            outputHalf = null;
            GatheringByteChannel currentGathering = gatheringOutput;
            gatheringOutput = null;
            signalOutputChangedLocked();
            return new PausedOutput(this, current, currentGathering);
        } catch (IOException | InterruptedException error) {
            if (ownsPause) {
                outputPaused = false;
                signalOutputChangedLocked();
            }
            throw error;
        } finally {
            lock.unlock();
        }
    }

    public PausedOutput pauseWrite() throws IOException, InterruptedException {
        return pauseOutput();
    }

    public PausedOutput pauseWrite(Duration timeout) throws IOException, InterruptedException {
        return pauseOutput(timeout);
    }

    public void closeInput() throws IOException {
        inputView.close();
    }

    public void closeOutput() throws IOException {
        closeJoinedOutput();
    }

    public void closeRead() throws IOException {
        closeInput();
    }

    public void closeWrite() throws IOException {
        closeOutput();
    }

    public void setDeadline(Instant deadline) throws IOException {
        setReadDeadline(deadline);
        setWriteDeadline(deadline);
    }

    @Override
    public boolean supportsReadDeadline() {
        return true;
    }

    @Override
    public boolean supportsWriteDeadline() {
        return true;
    }

    @Override
    public void setReadDeadline(Instant deadline) throws IOException {
        ReadHalf half;
        lock.lock();
        try {
            ensureOpenLocked();
            readDeadline = deadline;
            readDeadlineGeneration++;
            half = typedReadHalf(inputHalf);
            if (half != null) {
                activeInputDeadlineOperations++;
            }
            signalInputChangedLocked();
        } finally {
            lock.unlock();
        }

        if (half == null) {
            return;
        }

        try {
            half.setReadDeadline(deadline);
        } finally {
            lock.lock();
            try {
                if (activeInputDeadlineOperations > 0) {
                    activeInputDeadlineOperations--;
                }
                signalInputChangedLocked();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void setWriteDeadline(Instant deadline) throws IOException {
        WriteHalf half;
        lock.lock();
        try {
            ensureOpenLocked();
            writeDeadline = deadline;
            writeDeadlineGeneration++;
            half = typedWriteHalf(outputHalf);
            if (half != null) {
                activeOutputDeadlineOperations++;
            }
            signalOutputChangedLocked();
        } finally {
            lock.unlock();
        }

        if (half == null) {
            return;
        }

        try {
            half.setWriteDeadline(deadline);
        } finally {
            lock.lock();
            try {
                if (activeOutputDeadlineOperations > 0) {
                    activeOutputDeadlineOperations--;
                }
                signalOutputChangedLocked();
            } finally {
                lock.unlock();
            }
        }
    }

    public void setReadTimeout(Duration timeout) throws IOException {
        setReadDeadline(DeadlineSupport.after(timeout));
    }

    public void setWriteTimeout(Duration timeout) throws IOException {
        setWriteDeadline(DeadlineSupport.after(timeout));
    }

    public void setTimeout(Duration timeout) throws IOException {
        setDeadline(DeadlineSupport.after(timeout));
    }

    public void clearReadDeadline() throws IOException {
        setReadDeadline(null);
    }

    public void clearWriteDeadline() throws IOException {
        setWriteDeadline(null);
    }

    public void clearDeadline() throws IOException {
        setDeadline(null);
    }

    @Override
    public void close() throws IOException {
        InputStream input;
        OutputStream output;
        GatheringByteChannel gathering;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            input = inputHalf;
            output = outputHalf;
            gathering = gatheringOutput;
            inputHalf = null;
            outputHalf = null;
            gatheringOutput = null;
            inputPaused = false;
            outputPaused = false;
            signalInputChangedLocked();
            signalOutputChangedLocked();
        } finally {
            lock.unlock();
        }

        IdentityHashMap<Object, Boolean> closedObjects = new IdentityHashMap<>(3);
        IOException error = closeOnce(closeTarget(input), closedObjects, null);
        error = closeOnce(closeTarget(output), closedObjects, error);
        error = closeOnce(gathering, closedObjects, error);
        if (error != null) {
            throw error;
        }
    }

    private InputStream enterInput() throws IOException {
        lock.lock();
        try {
            while (inputPaused && !closed) {
                awaitInputUninterruptiblyAsIo();
            }
            ensureOpenLocked();
            activeInputOperations++;
            return inputHalf;
        } finally {
            lock.unlock();
        }
    }

    private void leaveInput() {
        lock.lock();
        try {
            if (activeInputOperations > 0) {
                activeInputOperations--;
            }
            signalInputChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    private OutputStream enterOutput() throws IOException {
        lock.lock();
        try {
            while (outputPaused && !closed) {
                awaitOutputUninterruptiblyAsIo();
            }
            ensureOpenLocked();
            activeOutputOperations++;
            return outputHalf;
        } finally {
            lock.unlock();
        }
    }

    private GatheringByteChannel currentGatheringOutputLocked() {
        return gatheringOutput;
    }

    private void leaveOutput() {
        lock.lock();
        try {
            if (activeOutputOperations > 0) {
                activeOutputOperations--;
            }
            signalOutputChangedLocked();
        } finally {
            lock.unlock();
        }
    }

    private void resumeInput(PausedInput paused) throws IOException {
        while (true) {
            ReadHalf currentHalf = typedReadHalf(paused.current);
            ResumeSnapshot snapshot = snapshotResumeState(paused, true);
            if (snapshot == null) {
                return;
            }
            if (currentHalf != null) {
                currentHalf.setReadDeadline(snapshot.deadline);
            }
            if (completeResume(paused, true, currentHalf != null, snapshot.generation, new ResumeCommit() {
                @Override
                public void commit() {
                    inputHalf = paused.current;
                    inputPaused = false;
                }
            })) {
                return;
            }
        }
    }

    private void resumeOutput(PausedOutput paused) throws IOException {
        while (true) {
            WriteHalf currentHalf = typedWriteHalf(paused.current);
            ResumeSnapshot snapshot = snapshotResumeState(paused, false);
            if (snapshot == null) {
                return;
            }
            if (currentHalf != null) {
                currentHalf.setWriteDeadline(snapshot.deadline);
            }
            if (completeResume(paused, false, currentHalf != null, snapshot.generation, new ResumeCommit() {
                @Override
                public void commit() {
                    outputHalf = paused.current;
                    gatheringOutput = paused.gathering;
                    outputPaused = false;
                }
            })) {
                return;
            }
        }
    }

    private ResumeSnapshot snapshotResumeState(ResumablePause paused, boolean readSide) throws IOException {
        lock.lock();
        try {
            if (paused.resumed()) {
                return null;
            }
            if (closed) {
                paused.markResumed();
                throw new SessionClosedException(ZmuxErrorSource.LOCAL);
            }
            return readSide
                    ? new ResumeSnapshot(readDeadline, readDeadlineGeneration)
                    : new ResumeSnapshot(writeDeadline, writeDeadlineGeneration);
        } finally {
            lock.unlock();
        }
    }

    private boolean completeResume(ResumablePause paused,
                                   boolean readSide,
                                   boolean hasCurrentHalf,
                                   long generation,
                                   ResumeCommit commit) throws IOException {
        lock.lock();
        try {
            if (paused.resumed()) {
                return true;
            }
            if (closed) {
                paused.markResumed();
                throw new SessionClosedException(ZmuxErrorSource.LOCAL);
            }
            long currentGeneration = readSide ? readDeadlineGeneration : writeDeadlineGeneration;
            if (hasCurrentHalf && currentGeneration != generation) {
                return false;
            }
            commit.commit();
            paused.markResumed();
            if (readSide) {
                signalInputChangedLocked();
            } else {
                signalOutputChangedLocked();
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpenLocked() throws IOException {
        if (closed) {
            throw new SessionClosedException(ZmuxErrorSource.LOCAL);
        }
    }

    private void awaitInput(PauseDeadline deadline) throws IOException, InterruptedException {
        inputWaiters++;
        try {
            if (!deadline.await(inputChanged)) {
                throw pauseTimeout();
            }
        } finally {
            inputWaiters--;
        }
    }

    private void awaitOutput(PauseDeadline deadline) throws IOException, InterruptedException {
        outputWaiters++;
        try {
            if (!deadline.await(outputChanged)) {
                throw pauseTimeout();
            }
        } finally {
            outputWaiters--;
        }
    }

    private void awaitInputUninterruptiblyAsIo() throws IOException {
        inputWaiters++;
        try {
            if (!awaitUntilDeadline(inputChanged, readDeadline)) {
                throw readDeadlineTimeout();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            InterruptedIOException io = new InterruptedIOException("zmux: interrupted while waiting for input half resume");
            io.initCause(interrupted);
            throw io;
        } finally {
            inputWaiters--;
        }
    }

    private void awaitOutputUninterruptiblyAsIo() throws IOException {
        outputWaiters++;
        try {
            if (!awaitUntilDeadline(outputChanged, writeDeadline)) {
                throw writeDeadlineTimeout();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            InterruptedIOException io = new InterruptedIOException("zmux: interrupted while waiting for output half resume");
            io.initCause(interrupted);
            throw io;
        } finally {
            outputWaiters--;
        }
    }

    private void signalInputChangedLocked() {
        if (inputWaiters > 0) {
            inputChanged.signalAll();
        }
    }

    private void signalOutputChangedLocked() {
        if (outputWaiters > 0) {
            outputChanged.signalAll();
        }
    }

    private void closeJoinedOutput() throws IOException {
        OutputStream output;
        GatheringByteChannel gathering;
        try {
            output = enterOutput();
        } catch (SessionClosedException closedException) {
            return;
        }
        lock.lock();
        try {
            gathering = currentGatheringOutputLocked();
        } finally {
            lock.unlock();
        }
        IdentityHashMap<Object, Boolean> closedObjects = new IdentityHashMap<>(2);
        try {
            IOException error = closeOnce(output, closedObjects, null);
            error = closeOnce(gathering, closedObjects, error);
            if (error != null) {
                throw error;
            }
        } finally {
            leaveOutput();
        }
    }

    private interface ResumablePause {
        boolean resumed();

        void markResumed();
    }

    private interface ResumeCommit {
        void commit();
    }

    private interface AddressAwareHalf {
        SocketAddress localAddress();

        SocketAddress remoteAddress();
    }

    private interface TypedInputHalf {
        ReadHalf readHalf();
    }

    private interface TypedOutputHalf {
        WriteHalf writeHalf();
    }

    private interface CloseIdentityHalf {
        Object closeIdentity();
    }

    private static final class ResumeSnapshot {
        private final Instant deadline;
        private final long generation;

        private ResumeSnapshot(Instant deadline, long generation) {
            this.deadline = deadline;
            this.generation = generation;
        }
    }

    public static final class PausedInput implements ResumablePause {
        private final JoinedDuplexConnection owner;
        private final Object lock = new Object();
        private InputStream current;
        private boolean resumed;

        private PausedInput(JoinedDuplexConnection owner, InputStream current) {
            this.owner = owner;
            this.current = current;
        }

        public InputStream current() {
            synchronized (lock) {
                return current;
            }
        }

        public ReadHalf currentReadHalf() {
            synchronized (lock) {
                return typedReadHalf(current);
            }
        }

        public InputStream set(ZmuxRecvStream next) {
            return set(wrap(next));
        }

        public InputStream set(ReadHalf next) {
            return set(wrap(next));
        }

        public ReadHalf replaceReadHalf(ReadHalf next) {
            return typedReadHalf(set(next));
        }

        public InputStream set(InputStream next) {
            synchronized (lock) {
                InputStream previous = current;
                current = next;
                return previous;
            }
        }

        public void resume() throws IOException {
            synchronized (lock) {
                owner.resumeInput(this);
            }
        }

        @Override
        public boolean resumed() {
            synchronized (lock) {
                return resumed;
            }
        }

        @Override
        public void markResumed() {
            synchronized (lock) {
                resumed = true;
            }
        }
    }

    public static final class PausedOutput implements ResumablePause {
        private final JoinedDuplexConnection owner;
        private final Object lock = new Object();
        private OutputStream current;
        private GatheringByteChannel gathering;
        private boolean resumed;

        private PausedOutput(JoinedDuplexConnection owner, OutputStream current, GatheringByteChannel gathering) {
            this.owner = owner;
            this.current = current;
            this.gathering = gathering;
        }

        public OutputStream current() {
            synchronized (lock) {
                return current;
            }
        }

        public WriteHalf currentWriteHalf() {
            synchronized (lock) {
                return typedWriteHalf(current);
            }
        }

        public OutputStream set(ZmuxSendStream next) {
            return set(wrap(next));
        }

        public OutputStream set(WriteHalf next) {
            GatheringByteChannel nextGathering = JoinedDuplexConnection.gatheringOutput(next);
            synchronized (lock) {
                OutputStream previous = current;
                current = wrap(next);
                gathering = nextGathering;
                return previous;
            }
        }

        public WriteHalf replaceWriteHalf(WriteHalf next) {
            return typedWriteHalf(set(next));
        }

        public OutputStream set(OutputStream next) {
            synchronized (lock) {
                OutputStream previous = current;
                current = next;
                gathering = null;
                return previous;
            }
        }

        public GatheringByteChannel gatheringOutput() {
            synchronized (lock) {
                return gathering;
            }
        }

        public GatheringByteChannel setGatheringOutput(GatheringByteChannel next) {
            synchronized (lock) {
                GatheringByteChannel previous = gathering;
                gathering = next;
                return previous;
            }
        }

        public void resume() throws IOException {
            synchronized (lock) {
                owner.resumeOutput(this);
            }
        }

        @Override
        public boolean resumed() {
            synchronized (lock) {
                return resumed;
            }
        }

        @Override
        public void markResumed() {
            synchronized (lock) {
                resumed = true;
            }
        }
    }

    private static final class PauseDeadline {
        private final long deadlineNanos;
        private final boolean bounded;

        private PauseDeadline(long deadlineNanos, boolean bounded) {
            this.deadlineNanos = deadlineNanos;
            this.bounded = bounded;
        }

        static PauseDeadline from(Duration timeout) {
            if (timeout == null) {
                return new PauseDeadline(0L, false);
            }
            if (timeout.isNegative() || timeout.isZero()) {
                return new PauseDeadline(System.nanoTime(), true);
            }
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                nanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            long deadline = now > Long.MAX_VALUE - nanos ? Long.MAX_VALUE : now + nanos;
            return new PauseDeadline(deadline, true);
        }

        boolean await(Condition condition) throws InterruptedException {
            if (!bounded) {
                condition.await();
                return true;
            }
            long now = System.nanoTime();
            long remaining = deadlineNanos - now;
            if (remaining <= 0L && deadlineNanos > now) {
                remaining = Long.MAX_VALUE;
            }
            if (remaining <= 0L) {
                return false;
            }
            return condition.await(remaining, TimeUnit.NANOSECONDS);
        }
    }

    private static final class StreamInputHalf extends InputStream
            implements AddressAwareHalf, TypedInputHalf, CloseIdentityHalf {
        private final ZmuxRecvStream stream;
        private final byte[] singleByte = new byte[1];

        private StreamInputHalf(ZmuxRecvStream stream) {
            this.stream = stream;
        }

        @Override
        public int read() throws IOException {
            int read;
            do {
                read = stream.read(singleByte, 0, 1);
                StreamIoSupport.validateReadProgress(read, 1);
            } while (read == 0);
            return read < 0 ? -1 : singleByte[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            return StreamIoSupport.validateReadProgress(stream.read(buffer, offset, length), length);
        }

        @Override
        public void close() throws IOException {
            stream.closeRead();
        }

        @Override
        public SocketAddress localAddress() {
            return stream.localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return stream.remoteAddress();
        }

        @Override
        public ReadHalf readHalf() {
            return stream;
        }

        @Override
        public Object closeIdentity() {
            return stream;
        }
    }

    private static final class GenericInputHalf extends InputStream
            implements AddressAwareHalf, TypedInputHalf, CloseIdentityHalf {
        private final ReadHalf half;
        private final byte[] singleByte = new byte[1];

        private GenericInputHalf(ReadHalf half) {
            this.half = half;
        }

        @Override
        public int read() throws IOException {
            int read;
            do {
                read = half.read(singleByte, 0, 1);
                StreamIoSupport.validateReadProgress(read, 1);
            } while (read == 0);
            return read < 0 ? -1 : singleByte[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            return StreamIoSupport.validateReadProgress(half.read(buffer, offset, length), length);
        }

        @Override
        public void close() throws IOException {
            half.closeRead();
        }

        @Override
        public SocketAddress localAddress() {
            return half.localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return half.remoteAddress();
        }

        @Override
        public ReadHalf readHalf() {
            return half;
        }

        @Override
        public Object closeIdentity() {
            return half;
        }
    }

    private static final class StreamOutputHalf extends OutputStream
            implements AddressAwareHalf, TypedOutputHalf, CloseIdentityHalf {
        private final ZmuxSendStream stream;
        private final byte[] singleByte = new byte[1];

        private StreamOutputHalf(ZmuxSendStream stream) {
            this.stream = stream;
        }

        @Override
        public void write(int value) throws IOException {
            singleByte[0] = (byte) value;
            stream.write(singleByte, 0, 1);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return;
            }
            stream.write(buffer, offset, length);
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void close() throws IOException {
            stream.closeWrite();
        }

        @Override
        public SocketAddress localAddress() {
            return stream.localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return stream.remoteAddress();
        }

        @Override
        public WriteHalf writeHalf() {
            return stream;
        }

        @Override
        public Object closeIdentity() {
            return stream;
        }
    }

    private static final class GenericOutputHalf extends OutputStream
            implements AddressAwareHalf, TypedOutputHalf, CloseIdentityHalf {
        private final WriteHalf half;
        private final byte[] singleByte = new byte[1];

        private GenericOutputHalf(WriteHalf half) {
            this.half = half;
        }

        @Override
        public void write(int value) throws IOException {
            singleByte[0] = (byte) value;
            half.write(singleByte, 0, 1);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return;
            }
            half.write(buffer, offset, length);
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void close() throws IOException {
            half.closeWrite();
        }

        @Override
        public SocketAddress localAddress() {
            return half.localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return half.remoteAddress();
        }

        @Override
        public WriteHalf writeHalf() {
            return half;
        }

        @Override
        public Object closeIdentity() {
            return half;
        }
    }

    private final class JoinedInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            InputStream input = enterInput();
            try {
                if (input == null) {
                    throw new StreamNotReadableException();
                }
                return input.read();
            } finally {
                leaveInput();
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            RangeChecks.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) {
                return 0;
            }
            InputStream input = enterInput();
            try {
                if (input == null) {
                    throw new StreamNotReadableException();
                }
                return StreamIoSupport.validateReadProgress(input.read(buffer, offset, length), length);
            } finally {
                leaveInput();
            }
        }

        @Override
        public void close() throws IOException {
            InputStream input;
            try {
                input = enterInput();
            } catch (SessionClosedException closed) {
                return;
            }
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException error) {
                if (!ignorableCloseFailure(error)) {
                    throw error;
                }
            } finally {
                leaveInput();
            }
        }
    }

    private final class JoinedGatheringOutput implements GatheringByteChannel {
        @Override
        public int write(java.nio.ByteBuffer src) throws IOException {
            enterOutput();
            try {
                GatheringByteChannel gathering;
                lock.lock();
                try {
                    gathering = currentGatheringOutputLocked();
                } finally {
                    lock.unlock();
                }
                if (gathering == null) {
                    throw new StreamNotWritableException();
                }
                return gathering.write(src);
            } finally {
                leaveOutput();
            }
        }

        @Override
        public long write(java.nio.ByteBuffer[] srcs, int offset, int length) throws IOException {
            RangeChecks.checkFromIndexSize(offset, length, srcs.length);
            enterOutput();
            try {
                GatheringByteChannel gathering;
                lock.lock();
                try {
                    gathering = currentGatheringOutputLocked();
                } finally {
                    lock.unlock();
                }
                if (gathering == null) {
                    throw new StreamNotWritableException();
                }
                return gathering.write(srcs, offset, length);
            } finally {
                leaveOutput();
            }
        }

        @Override
        public long write(java.nio.ByteBuffer[] srcs) throws IOException {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            lock.lock();
            try {
                return !closed && !outputPaused && gatheringOutput != null && gatheringOutput.isOpen();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            closeJoinedOutput();
        }
    }

    private final class JoinedOutputStream extends OutputStream {
        @Override
        public void write(int value) throws IOException {
            OutputStream output = enterOutput();
            try {
                if (output == null) {
                    throw new StreamNotWritableException();
                }
                output.write(value);
            } finally {
                leaveOutput();
            }
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            RangeChecks.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) {
                return;
            }
            OutputStream output = enterOutput();
            try {
                if (output == null) {
                    throw new StreamNotWritableException();
                }
                output.write(buffer, offset, length);
            } finally {
                leaveOutput();
            }
        }

        @Override
        public void flush() throws IOException {
            OutputStream output = enterOutput();
            try {
                if (output != null) {
                    output.flush();
                }
            } finally {
                leaveOutput();
            }
        }

        @Override
        public void close() throws IOException {
            closeJoinedOutput();
        }
    }
}
