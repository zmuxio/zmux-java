package io.zmux;

import java.util.Objects;
import java.util.Optional;

public final class ZmuxAsync {
    private ZmuxAsync() {
    }

    public static ZmuxAsyncSession session(ZmuxSession session) {
        Objects.requireNonNull(session, "session");
        if (session instanceof ZmuxAsyncSession) {
            return (ZmuxAsyncSession) session;
        }
        throw unsupported("session");
    }

    public static boolean supportsSession(ZmuxSession session) {
        return session instanceof ZmuxAsyncSession;
    }

    public static Optional<ZmuxAsyncSession> optionalSession(ZmuxSession session) {
        return session instanceof ZmuxAsyncSession
                ? Optional.of((ZmuxAsyncSession) session)
                : Optional.empty();
    }

    public static ZmuxAsyncStream stream(ZmuxStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncStream) {
            return (ZmuxAsyncStream) stream;
        }
        throw unsupported("stream");
    }

    public static boolean supportsStream(ZmuxStream stream) {
        return stream instanceof ZmuxAsyncStream;
    }

    public static Optional<ZmuxAsyncStream> optionalStream(ZmuxStream stream) {
        return stream instanceof ZmuxAsyncStream
                ? Optional.of((ZmuxAsyncStream) stream)
                : Optional.empty();
    }

    public static ZmuxAsyncSendStream sendStream(ZmuxSendStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncSendStream) {
            return (ZmuxAsyncSendStream) stream;
        }
        throw unsupported("send stream");
    }

    public static boolean supportsSendStream(ZmuxSendStream stream) {
        return stream instanceof ZmuxAsyncSendStream;
    }

    public static Optional<ZmuxAsyncSendStream> optionalSendStream(ZmuxSendStream stream) {
        return stream instanceof ZmuxAsyncSendStream
                ? Optional.of((ZmuxAsyncSendStream) stream)
                : Optional.empty();
    }

    public static ZmuxAsyncRecvStream recvStream(ZmuxRecvStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncRecvStream) {
            return (ZmuxAsyncRecvStream) stream;
        }
        throw unsupported("receive stream");
    }

    public static boolean supportsRecvStream(ZmuxRecvStream stream) {
        return stream instanceof ZmuxAsyncRecvStream;
    }

    public static Optional<ZmuxAsyncRecvStream> optionalRecvStream(ZmuxRecvStream stream) {
        return stream instanceof ZmuxAsyncRecvStream
                ? Optional.of((ZmuxAsyncRecvStream) stream)
                : Optional.empty();
    }

    private static UnsupportedOperationException unsupported(String surface) {
        return new UnsupportedOperationException("zmux: async " + surface + " API is not supported by this implementation");
    }
}
