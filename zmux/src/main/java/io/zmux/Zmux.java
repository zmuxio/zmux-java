package io.zmux;

import io.zmux.internal.SessionRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;

public final class Zmux {
    private static final Settings ZERO_SETTINGS = new Settings(
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            SchedulerHint.UNSPECIFIED_OR_BALANCED
    );
    private static final Preface ZERO_PREFACE = new Preface(
            (byte) 0,
            Role.INITIATOR,
            0L,
            0L,
            0L,
            0L,
            ZERO_SETTINGS
    );
    private static final Negotiated ZERO_NEGOTIATED = new Negotiated(
            0L,
            0L,
            Role.INITIATOR,
            Role.INITIATOR,
            ZERO_SETTINGS
    );
    private static final ZmuxNativeSession CLOSED_SESSION = new ClosedSession();

    private Zmux() {
    }

    public static ZmuxSession closedSession() {
        return CLOSED_SESSION;
    }

    public static ZmuxNativeSession closedNativeSession() {
        return CLOSED_SESSION;
    }

    public static ZmuxSession asSession(ZmuxSession session) {
        return session == null ? closedSession() : session;
    }

    public static ZmuxNativeSession asNativeSession(ZmuxNativeSession session) {
        return session == null ? closedNativeSession() : session;
    }

    public static ZmuxNativeSession open(DuplexConnection connection) throws IOException {
        return open(connection, null);
    }

    public static ZmuxNativeSession open(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return SessionRuntime.open(connection, effectiveConfig(config));
    }

    public static ZmuxNativeSession open(Socket socket) throws IOException {
        return open(socket, null);
    }

    public static ZmuxNativeSession open(Socket socket, ZmuxConfig config) throws IOException {
        return open(ZmuxConnections.of(socket), config);
    }

    public static ZmuxNativeSession open(InputStream input, OutputStream output) throws IOException {
        return open(input, output, null);
    }

    public static ZmuxNativeSession open(InputStream input, OutputStream output, ZmuxConfig config) throws IOException {
        return open(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxNativeSession open(ByteChannel channel) throws IOException {
        return open(channel, (ZmuxConfig) null);
    }

    public static ZmuxNativeSession open(ByteChannel channel, ZmuxConfig config) throws IOException {
        return open(ZmuxConnections.of(channel), config);
    }

    public static ZmuxNativeSession open(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return open(input, output, null);
    }

    public static ZmuxNativeSession open(ReadableByteChannel input,
                                         WritableByteChannel output,
                                         ZmuxConfig config) throws IOException {
        return open(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxSession openSession(DuplexConnection connection) throws IOException {
        return openSession(connection, null);
    }

    public static ZmuxSession openSession(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return open(connection, config);
    }

    public static ZmuxSession openSession(Socket socket) throws IOException {
        return openSession(socket, null);
    }

    public static ZmuxSession openSession(Socket socket, ZmuxConfig config) throws IOException {
        return open(socket, config);
    }

    public static ZmuxSession openSession(InputStream input, OutputStream output) throws IOException {
        return openSession(input, output, null);
    }

    public static ZmuxSession openSession(InputStream input, OutputStream output, ZmuxConfig config) throws IOException {
        return open(input, output, config);
    }

    public static ZmuxSession openSession(ByteChannel channel) throws IOException {
        return openSession(channel, (ZmuxConfig) null);
    }

    public static ZmuxSession openSession(ByteChannel channel, ZmuxConfig config) throws IOException {
        return open(channel, config);
    }

    public static ZmuxSession openSession(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return openSession(input, output, null);
    }

    public static ZmuxSession openSession(ReadableByteChannel input,
                                          WritableByteChannel output,
                                          ZmuxConfig config) throws IOException {
        return open(input, output, config);
    }

    public static ZmuxNativeSession client(DuplexConnection connection) throws IOException {
        return client(connection, null);
    }

    public static ZmuxNativeSession client(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return openWithRole(connection, config, Role.INITIATOR);
    }

    public static ZmuxNativeSession client(Socket socket) throws IOException {
        return client(socket, null);
    }

    public static ZmuxNativeSession client(Socket socket, ZmuxConfig config) throws IOException {
        return client(ZmuxConnections.of(socket), config);
    }

    public static ZmuxNativeSession client(InputStream input, OutputStream output) throws IOException {
        return client(input, output, null);
    }

    public static ZmuxNativeSession client(InputStream input, OutputStream output, ZmuxConfig config) throws IOException {
        return client(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxNativeSession client(ByteChannel channel) throws IOException {
        return client(channel, (ZmuxConfig) null);
    }

    public static ZmuxNativeSession client(ByteChannel channel, ZmuxConfig config) throws IOException {
        return client(ZmuxConnections.of(channel), config);
    }

    public static ZmuxNativeSession client(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return client(input, output, null);
    }

    public static ZmuxNativeSession client(ReadableByteChannel input,
                                           WritableByteChannel output,
                                           ZmuxConfig config) throws IOException {
        return client(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxSession clientSession(DuplexConnection connection) throws IOException {
        return clientSession(connection, null);
    }

    public static ZmuxSession clientSession(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return client(connection, config);
    }

    public static ZmuxSession clientSession(Socket socket) throws IOException {
        return clientSession(socket, null);
    }

    public static ZmuxSession clientSession(Socket socket, ZmuxConfig config) throws IOException {
        return client(socket, config);
    }

    public static ZmuxSession clientSession(InputStream input, OutputStream output) throws IOException {
        return clientSession(input, output, null);
    }

    public static ZmuxSession clientSession(InputStream input,
                                            OutputStream output,
                                            ZmuxConfig config) throws IOException {
        return client(input, output, config);
    }

    public static ZmuxSession clientSession(ByteChannel channel) throws IOException {
        return clientSession(channel, (ZmuxConfig) null);
    }

    public static ZmuxSession clientSession(ByteChannel channel, ZmuxConfig config) throws IOException {
        return client(channel, config);
    }

    public static ZmuxSession clientSession(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return clientSession(input, output, null);
    }

    public static ZmuxSession clientSession(ReadableByteChannel input,
                                            WritableByteChannel output,
                                            ZmuxConfig config) throws IOException {
        return client(input, output, config);
    }

    public static ZmuxNativeSession server(DuplexConnection connection) throws IOException {
        return server(connection, null);
    }

    public static ZmuxNativeSession server(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return openWithRole(connection, config, Role.RESPONDER);
    }

    public static ZmuxNativeSession server(Socket socket) throws IOException {
        return server(socket, null);
    }

    public static ZmuxNativeSession server(Socket socket, ZmuxConfig config) throws IOException {
        return server(ZmuxConnections.of(socket), config);
    }

    public static ZmuxNativeSession server(InputStream input, OutputStream output) throws IOException {
        return server(input, output, null);
    }

    public static ZmuxNativeSession server(InputStream input, OutputStream output, ZmuxConfig config) throws IOException {
        return server(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxNativeSession server(ByteChannel channel) throws IOException {
        return server(channel, (ZmuxConfig) null);
    }

    public static ZmuxNativeSession server(ByteChannel channel, ZmuxConfig config) throws IOException {
        return server(ZmuxConnections.of(channel), config);
    }

    public static ZmuxNativeSession server(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return server(input, output, null);
    }

    public static ZmuxNativeSession server(ReadableByteChannel input,
                                           WritableByteChannel output,
                                           ZmuxConfig config) throws IOException {
        return server(ZmuxConnections.of(input, output), config);
    }

    public static ZmuxSession serverSession(DuplexConnection connection) throws IOException {
        return serverSession(connection, null);
    }

    public static ZmuxSession serverSession(DuplexConnection connection, ZmuxConfig config) throws IOException {
        return server(connection, config);
    }

    public static ZmuxSession serverSession(Socket socket) throws IOException {
        return serverSession(socket, null);
    }

    public static ZmuxSession serverSession(Socket socket, ZmuxConfig config) throws IOException {
        return server(socket, config);
    }

    public static ZmuxSession serverSession(InputStream input, OutputStream output) throws IOException {
        return serverSession(input, output, null);
    }

    public static ZmuxSession serverSession(InputStream input,
                                            OutputStream output,
                                            ZmuxConfig config) throws IOException {
        return server(input, output, config);
    }

    public static ZmuxSession serverSession(ByteChannel channel) throws IOException {
        return serverSession(channel, (ZmuxConfig) null);
    }

    public static ZmuxSession serverSession(ByteChannel channel, ZmuxConfig config) throws IOException {
        return server(channel, config);
    }

    public static ZmuxSession serverSession(ReadableByteChannel input, WritableByteChannel output) throws IOException {
        return serverSession(input, output, null);
    }

    public static ZmuxSession serverSession(ReadableByteChannel input,
                                            WritableByteChannel output,
                                            ZmuxConfig config) throws IOException {
        return server(input, output, config);
    }

    private static ZmuxConfig effectiveConfig(ZmuxConfig config) {
        return config == null ? ZmuxConfig.defaults() : config;
    }

    private static ZmuxNativeSession openWithRole(DuplexConnection connection,
                                                  ZmuxConfig config,
                                                  Role role) throws IOException {
        return open(connection, effectiveConfig(config).withRole(role));
    }

    private static final class ClosedSession implements ZmuxNativeSession {
        @Override
        public ZmuxNativeStream acceptStream() throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream acceptStream(Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeRecvStream acceptUniStream() throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeRecvStream acceptUniStream(Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openStream() throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openStream(OpenOptions options) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openStreamWithTimeout(Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniStream() throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniStream(OpenOptions options) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniStreamWithTimeout(Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openAndSend(byte[] data) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeStream openAndSend(OpenOptions options, byte[] data) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniAndSend(byte[] data) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ZmuxNativeSendStream openUniAndSend(OpenOptions options, byte[] data) throws IOException {
            throw sessionClosed();
        }

        @Override
        public Duration ping(byte[] echo, Duration timeout) throws IOException {
            throw sessionClosed();
        }

        @Override
        public void goAway(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException {
            throw sessionClosed();
        }

        @Override
        public ApplicationError peerGoAwayError() {
            return null;
        }

        @Override
        public ApplicationError peerCloseError() {
            return null;
        }

        @Override
        public Preface localPreface() {
            return ZERO_PREFACE;
        }

        @Override
        public Preface peerPreface() {
            return ZERO_PREFACE;
        }

        @Override
        public Negotiated negotiated() {
            return ZERO_NEGOTIATED;
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public boolean awaitTermination(Duration timeout) {
            return true;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public SessionState state() {
            return SessionState.INVALID;
        }

        @Override
        public SessionStats stats() {
            return SessionStats.empty(SessionState.INVALID);
        }

        @Override
        public void close() {
        }

        private static SessionClosedException sessionClosed() {
            return new SessionClosedException(ZmuxErrorSource.LOCAL);
        }
    }
}
