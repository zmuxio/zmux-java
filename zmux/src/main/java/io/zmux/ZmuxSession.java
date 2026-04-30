package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public interface ZmuxSession extends Closeable {
    ZmuxStream acceptStream() throws IOException, InterruptedException;

    ZmuxStream acceptStream(Duration timeout) throws IOException, InterruptedException;

    ZmuxRecvStream acceptUniStream() throws IOException, InterruptedException;

    ZmuxRecvStream acceptUniStream(Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openStream() throws IOException, InterruptedException;

    ZmuxStream openStream(OpenOptions options) throws IOException, InterruptedException;

    default ZmuxStream openStream(Duration timeout) throws IOException, InterruptedException {
        return openStreamWithTimeout(timeout);
    }

    default ZmuxStream openStream(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return openStreamWithTimeout(options, timeout);
    }

    ZmuxStream openStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    ZmuxSendStream openUniStream() throws IOException, InterruptedException;

    ZmuxSendStream openUniStream(OpenOptions options) throws IOException, InterruptedException;

    default ZmuxSendStream openUniStream(Duration timeout) throws IOException, InterruptedException {
        return openUniStreamWithTimeout(timeout);
    }

    default ZmuxSendStream openUniStream(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return openUniStreamWithTimeout(options, timeout);
    }

    ZmuxSendStream openUniStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    ZmuxSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    ZmuxStream openAndSend(byte[] data) throws IOException, InterruptedException;

    ZmuxStream openAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

    default ZmuxStream openAndSendUtf8(String data) throws IOException, InterruptedException {
        return openAndSend(TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxStream openAndSendUtf8(OpenOptions options, String data) throws IOException, InterruptedException {
        return openAndSend(options, TextSupport.utf8Bytes(data, "data"));
    }

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

    default ZmuxStream openAndSend(Duration timeout, byte[] data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(timeout, data);
    }

    default ZmuxStream openAndSend(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(options, timeout, data);
    }

    default ZmuxStream openAndSendUtf8(Duration timeout, String data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(timeout, TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxStream openAndSendUtf8(OpenOptions options, Duration timeout, String data)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(options, timeout, TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxStream openAndSend(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(timeout, data, offset, length);
    }

    default ZmuxStream openAndSend(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(options, timeout, data, offset, length);
    }

    default ZmuxStream openAndSend(Duration timeout, ByteBuffer data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(timeout, data);
    }

    default ZmuxStream openAndSend(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(options, timeout, data);
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

    default ZmuxSendStream openUniAndSendUtf8(String data) throws IOException, InterruptedException {
        return openUniAndSend(TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxSendStream openUniAndSendUtf8(OpenOptions options, String data) throws IOException, InterruptedException {
        return openUniAndSend(options, TextSupport.utf8Bytes(data, "data"));
    }

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

    default ZmuxSendStream openUniAndSend(Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(timeout, data);
    }

    default ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(options, timeout, data);
    }

    default ZmuxSendStream openUniAndSendUtf8(Duration timeout, String data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(timeout, TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxSendStream openUniAndSendUtf8(OpenOptions options, Duration timeout, String data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(options, timeout, TextSupport.utf8Bytes(data, "data"));
    }

    default ZmuxSendStream openUniAndSend(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(timeout, data, offset, length);
    }

    default ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(options, timeout, data, offset, length);
    }

    default ZmuxSendStream openUniAndSend(Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(timeout, data);
    }

    default ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(options, timeout, data);
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
            stream.writeFinal(data == null ? DeadlineSupport.EMPTY_BYTES : data);
            return stream;
        } finally {
            if (deadline != null) {
                stream.clearWriteDeadline();
            }
        }
    }

    void closeWithError(long code, String reason) throws IOException;

    default void closeWithError(ErrorCode code, String reason) throws IOException {
        Objects.requireNonNull(code, "code");
        closeWithError(code.code(), reason);
    }

    default void closeWithError(Throwable error) throws IOException {
        if (error == null) {
            close();
            return;
        }
        closeWithError(ZmuxErrors.code(error, ErrorCode.INTERNAL.code()), ZmuxErrors.reason(error));
    }

    default boolean awaitTermination() throws InterruptedException {
        return awaitTermination(null);
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException;

    default Optional<IOException> awaitTerminationCause() throws IOException, InterruptedException {
        return awaitTerminationCause(null);
    }

    default Optional<IOException> awaitTerminationCause(Duration timeout) throws IOException, InterruptedException {
        if (!awaitTermination(timeout)) {
            throw new SessionWaitTimeoutException();
        }
        return terminationCause();
    }

    default void awaitTerminationOrThrow() throws IOException, InterruptedException {
        awaitTerminationOrThrow(null);
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
