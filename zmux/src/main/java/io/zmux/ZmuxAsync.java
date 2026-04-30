package io.zmux;

import java.util.Objects;

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

    public static ZmuxAsyncStream stream(ZmuxStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncStream) {
            return (ZmuxAsyncStream) stream;
        }
        throw unsupported("stream");
    }

    public static ZmuxAsyncSendStream sendStream(ZmuxSendStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncSendStream) {
            return (ZmuxAsyncSendStream) stream;
        }
        throw unsupported("send stream");
    }

    public static ZmuxAsyncRecvStream recvStream(ZmuxRecvStream stream) {
        Objects.requireNonNull(stream, "stream");
        if (stream instanceof ZmuxAsyncRecvStream) {
            return (ZmuxAsyncRecvStream) stream;
        }
        throw unsupported("receive stream");
    }

    private static UnsupportedOperationException unsupported(String surface) {
        return new UnsupportedOperationException("zmux: async " + surface + " API is not supported by this implementation");
    }
}
