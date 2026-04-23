package io.zmux;

import java.io.IOException;
import java.time.Duration;

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
    ZmuxNativeStream openStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStream() throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStream(OpenOptions options) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStreamWithTimeout(Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openAndSend(byte[] data) throws IOException, InterruptedException;

    @Override
    ZmuxNativeStream openAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

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
    ZmuxNativeSendStream openUniAndSend(byte[] data) throws IOException, InterruptedException;

    @Override
    ZmuxNativeSendStream openUniAndSend(OpenOptions options, byte[] data) throws IOException, InterruptedException;

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

    Duration ping(byte[] echo, Duration timeout) throws IOException, InterruptedException;

    void goAway(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException;

    ApplicationError peerGoAwayError();

    ApplicationError peerCloseError();

    Preface localPreface();

    Preface peerPreface();

    Negotiated negotiated();
}
