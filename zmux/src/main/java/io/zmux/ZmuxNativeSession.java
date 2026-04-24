package io.zmux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

public interface ZmuxNativeSession extends ZmuxSession {
    @Override
    ZmuxNativeStream acceptStream() throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream acceptStream(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeRecvStream acceptUniStream() throws IOException, InterruptedException;

    @Override
    ZmuxNativeRecvStream acceptUniStream(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openStream() throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openStream(OpenOptions options) throws IOException, InterruptedException;

    @Override
    default ZmuxNativeStream openStream(Duration timeout) throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openStream(timeout);
    }

    @Override
    default ZmuxNativeStream openStream(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openStream(options, timeout);
    }

    @Override
    ZmuxNativeStream openStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStream() throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStream(OpenOptions options) throws IOException, InterruptedException;

    @Override
    default ZmuxNativeSendStream openUniStream(Duration timeout) throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniStream(timeout);
    }

    @Override
    default ZmuxNativeSendStream openUniStream(OpenOptions options, Duration timeout) throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniStream(options, timeout);
    }

    @Override
    ZmuxNativeSendStream openUniStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openAndSend(byte[] data) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

    @Override
    default ZmuxNativeStream openAndSend(byte[] data, int offset, int length) throws IOException, InterruptedException {
        return openAndSend(OpenOptions.empty(), data, offset, length);
    }

    @Override
    default ZmuxNativeStream openAndSend(OpenOptions options, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openAndSend(options, data, offset, length);
    }

    @Override
    default ZmuxNativeStream openAndSend(ByteBuffer data) throws IOException, InterruptedException {
        return openAndSend(OpenOptions.empty(), data);
    }

    @Override
    default ZmuxNativeStream openAndSend(OpenOptions options, ByteBuffer data) throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openAndSend(options, data);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(Duration timeout, byte[] data) throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openAndSendWithTimeout(options, timeout, data);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data, offset, length);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openAndSendWithTimeout(options, timeout, data, offset, length);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    default ZmuxNativeStream openAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return (ZmuxNativeStream) ZmuxSession.super.openAndSendWithTimeout(options, timeout, data);
    }

    @Override
    ZmuxNativeSendStream openUniAndSend(byte[] data) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

    @Override
    default ZmuxNativeSendStream openUniAndSend(byte[] data, int offset, int length) throws IOException, InterruptedException {
        return openUniAndSend(OpenOptions.empty(), data, offset, length);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSend(OpenOptions options, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniAndSend(options, data, offset, length);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSend(ByteBuffer data) throws IOException, InterruptedException {
        return openUniAndSend(OpenOptions.empty(), data);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSend(OpenOptions options, ByteBuffer data) throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniAndSend(options, data);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data)
            throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniAndSendWithTimeout(options, timeout, data);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data, offset, length);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length)
            throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniAndSendWithTimeout(options, timeout, data, offset, length);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return openUniAndSendWithTimeout(OpenOptions.empty(), timeout, data);
    }

    @Override
    default ZmuxNativeSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data)
            throws IOException, InterruptedException {
        return (ZmuxNativeSendStream) ZmuxSession.super.openUniAndSendWithTimeout(options, timeout, data);
    }

    default Duration ping() throws IOException, InterruptedException {
        return ping(null, null);
    }

    default Duration ping(byte[] echo) throws IOException, InterruptedException {
        return ping(echo, null);
    }

    Duration ping(byte[] echo, Duration timeout) throws IOException, InterruptedException;

    default void goAway(long lastAcceptedBidi, long lastAcceptedUni) throws IOException {
        goAway(lastAcceptedBidi, lastAcceptedUni, ErrorCode.NO_ERROR.code(), "");
    }

    default void goAwayWithError(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException {
        goAway(lastAcceptedBidi, lastAcceptedUni, code, reason);
    }

    default void goAwayWithError(long lastAcceptedBidi, long lastAcceptedUni, ErrorCode code, String reason)
            throws IOException {
        Objects.requireNonNull(code, "code");
        goAwayWithError(lastAcceptedBidi, lastAcceptedUni, code.code(), reason);
    }

    default void goAway(long lastAcceptedBidi, long lastAcceptedUni, ErrorCode code, String reason) throws IOException {
        goAwayWithError(lastAcceptedBidi, lastAcceptedUni, code, reason);
    }

    void goAway(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException;

    ApplicationError peerGoAwayError();

    ApplicationError peerCloseError();

    Preface localPreface();

    Preface peerPreface();

    Negotiated negotiated();
}
