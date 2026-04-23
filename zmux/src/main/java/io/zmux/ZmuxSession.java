package io.zmux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public interface ZmuxSession extends AutoCloseable {
    ZmuxStream acceptStream() throws IOException, InterruptedException;

    ZmuxStream acceptStream(Duration timeout) throws IOException, InterruptedException;

    ZmuxRecvStream acceptUniStream() throws IOException, InterruptedException;

    ZmuxRecvStream acceptUniStream(Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openStream() throws IOException, InterruptedException;

    ZmuxStream openStream(OpenOptions options) throws IOException, InterruptedException;

    ZmuxStream openStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    ZmuxSendStream openUniStream() throws IOException, InterruptedException;

    ZmuxSendStream openUniStream(OpenOptions options) throws IOException, InterruptedException;

    ZmuxSendStream openUniStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    ZmuxSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openAndSend(byte[] data) throws IOException, InterruptedException;

    ZmuxStream openAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

    default ZmuxStream openAndSend(byte[] data, int offset, int length) throws IOException, InterruptedException {
        return openAndSend(OpenOptions.empty(), data, offset, length);
    }

    default ZmuxStream openAndSend(OpenOptions options, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        RangeChecks.checkFromIndexSize(offset, length, data.length);
        ZmuxStream stream = openStream(options);
        if (length == 0) {
            return stream;
        }
        stream.write(data, offset, length);
        return stream;
    }

    default ZmuxStream openAndSend(ByteBuffer data) throws IOException, InterruptedException {
        return openAndSend(OpenOptions.empty(), data);
    }

    default ZmuxStream openAndSend(OpenOptions options, ByteBuffer data) throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        ZmuxStream stream = openStream(options);
        if (!data.hasRemaining()) {
            return stream;
        }
        stream.write(data);
        return stream;
    }

    default ZmuxStream openAndSendWithTimeout(Duration timeout, byte[] data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    default ZmuxStream openAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data, offset, length);
    }

    default ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        RangeChecks.checkFromIndexSize(offset, length, data.length);
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxStream stream = openStreamWithTimeout(options, timeout);
        if (length == 0) {
            return stream;
        }
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            stream.write(data, offset, length);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    default ZmuxStream openAndSendWithTimeout(Duration timeout, ByteBuffer data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    default ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxStream stream = openStreamWithTimeout(options, timeout);
        if (!data.hasRemaining()) {
            return stream;
        }
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            stream.write(data);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    default ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxStream stream = openStreamWithTimeout(options, timeout);
        if (data == null || data.length == 0) {
            return stream;
        }
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            stream.write(data);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    ZmuxSendStream openUniAndSend(byte[] data) throws IOException, InterruptedException;

    ZmuxSendStream openUniAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

    default ZmuxSendStream openUniAndSend(byte[] data, int offset, int length) throws IOException, InterruptedException {
        return openUniAndSend(OpenOptions.empty(), data, offset, length);
    }

    default ZmuxSendStream openUniAndSend(OpenOptions options, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        RangeChecks.checkFromIndexSize(offset, length, data.length);
        ZmuxSendStream stream = openUniStream(options);
        stream.writeFinal(data, offset, length);
        return stream;
    }

    default ZmuxSendStream openUniAndSend(ByteBuffer data) throws IOException, InterruptedException {
        return openUniAndSend(OpenOptions.empty(), data);
    }

    default ZmuxSendStream openUniAndSend(OpenOptions options, ByteBuffer data) throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        ZmuxSendStream stream = openUniStream(options);
        stream.writeFinal(data);
        return stream;
    }

    default ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    default ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data, offset, length);
    }

    default ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        RangeChecks.checkFromIndexSize(offset, length, data.length);
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxSendStream stream = openUniStreamWithTimeout(options, timeout);
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            stream.writeFinal(data, offset, length);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    default ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    default ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        Objects.requireNonNull(data, "data");
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxSendStream stream = openUniStreamWithTimeout(options, timeout);
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            stream.writeFinal(data);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    default ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        Instant deadline = DeadlineSupport.after(timeout);
        ZmuxSendStream stream = openUniStreamWithTimeout(options, timeout);
        if (deadline != null) {
            stream.setWriteDeadline(deadline);
        }
        try {
            if (data == null || data.length == 0) {
                stream.closeWrite();
            } else {
                stream.writeFinal(data);
            }
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    void closeWithError(long code, String reason) throws IOException;

    boolean awaitTermination(Duration timeout) throws InterruptedException;

    default Optional<IOException> awaitTerminationCause(Duration timeout) throws IOException, InterruptedException {
        if (!awaitTermination(timeout)) {
            throw new SessionWaitTimeoutException();
        }
        return terminationCause();
    }

    default void awaitTerminationOrThrow(Duration timeout) throws IOException, InterruptedException {
        Optional<IOException> cause = awaitTerminationCause(timeout);
        if (cause.isPresent()) {
            throw cause.get();
        }
    }

    default Optional<IOException> terminationCause() {
        return Optional.empty();
    }

    boolean isClosed();

    SessionState state();

    SessionStats stats();

    @Override
    void close() throws IOException;

}
