package io.zmux.transport;

import io.zmux.ZmuxRecvStream;
import io.zmux.ZmuxSendStream;
import io.zmux.ZmuxStream;

import javax.net.ssl.SSLSocket;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.*;
import java.time.Duration;
import java.time.Instant;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public final class ZmuxConnections {
    private ZmuxConnections() {
    }

    public static BasicDuplexConnection.Builder builder(InputStream input, OutputStream output) {
        return BasicDuplexConnection.builder(input, output);
    }

    public static DuplexConnection of(ZmuxStream stream) {
        Objects.requireNonNull(stream, "stream");
        return new JoinedDuplexConnection(stream, stream);
    }

    public static JoinedDuplexConnection join(InputStream input, OutputStream output) {
        return new JoinedDuplexConnection(input, output);
    }

    public static JoinedDuplexConnection join(InputStream input,
                                              OutputStream output,
                                              GatheringByteChannel gatheringOutput) {
        return new JoinedDuplexConnection(input, output, gatheringOutput, null, null);
    }

    public static JoinedDuplexConnection join(ReadHalf input, WriteHalf output) {
        return new JoinedDuplexConnection(input, output);
    }

    public static JoinedDuplexConnection join(InputStream input,
                                              OutputStream output,
                                              SocketAddress localAddress,
                                              SocketAddress remoteAddress) {
        return new JoinedDuplexConnection(input, output, null, localAddress, remoteAddress);
    }

    public static JoinedDuplexConnection join(InputStream input,
                                              OutputStream output,
                                              GatheringByteChannel gatheringOutput,
                                              SocketAddress localAddress,
                                              SocketAddress remoteAddress) {
        return new JoinedDuplexConnection(input, output, gatheringOutput, localAddress, remoteAddress);
    }

    public static JoinedDuplexConnection join(ZmuxRecvStream input, ZmuxSendStream output) {
        return new JoinedDuplexConnection(input, output);
    }

    public static DuplexConnection of(Socket socket) throws IOException {
        return new SocketDuplexConnection(socket);
    }

    public static DuplexConnection of(InputStream input, OutputStream output) {
        return new BasicDuplexConnection(input, output);
    }

    public static DuplexConnection of(InputStream input, OutputStream output, AutoCloseable closer) {
        return new BasicDuplexConnection(input, output, closer, null, null);
    }

    public static DuplexConnection of(InputStream input,
                                      OutputStream output,
                                      SocketAddress localAddress,
                                      SocketAddress remoteAddress) {
        return new BasicDuplexConnection(input, output, null, localAddress, remoteAddress);
    }

    public static DuplexConnection of(InputStream input,
                                      OutputStream output,
                                      AutoCloseable closer,
                                      SocketAddress localAddress,
                                      SocketAddress remoteAddress) {
        return new BasicDuplexConnection(input, output, closer, localAddress, remoteAddress);
    }

    public static DuplexConnection of(InputStream input,
                                      OutputStream output,
                                      AutoCloseable closer,
                                      SocketAddress localAddress,
                                      SocketAddress remoteAddress,
                                      GatheringByteChannel gatheringOutput) {
        return builder(input, output)
                .closer(closer)
                .addresses(localAddress, remoteAddress)
                .gatheringOutput(gatheringOutput)
                .build();
    }

    public static DuplexConnection of(ByteChannel channel) {
        Objects.requireNonNull(channel, "channel");
        return of(channel, channel);
    }

    public static DuplexConnection of(ReadableByteChannel input, WritableByteChannel output) {
        return new ChannelDuplexConnection(input, output);
    }

    private static SocketAddress localAddress(ReadableByteChannel input, WritableByteChannel output) {
        SocketAddress address = localAddress(input);
        return address == null ? localAddress(output) : address;
    }

    private static SocketAddress localAddress(Object channel) {
        if (channel instanceof NetworkChannel) {
            NetworkChannel networkChannel = (NetworkChannel) channel;
            try {
                return networkChannel.getLocalAddress();
            } catch (IOException ignored) {
                return null;
            }
        }
        return null;
    }

    private static SocketAddress remoteAddress(ReadableByteChannel input, WritableByteChannel output) {
        SocketAddress address = remoteAddress(input);
        return address == null ? remoteAddress(output) : address;
    }

    private static SocketAddress remoteAddress(Object channel) {
        if (channel instanceof SocketChannel) {
            SocketChannel socketChannel = (SocketChannel) channel;
            try {
                return socketChannel.getRemoteAddress();
            } catch (IOException ignored) {
                return null;
            }
        }
        return null;
    }

    private static IOException closeOnce(Channel channel,
                                         IdentityHashMap<Object, Boolean> closed,
                                         IOException error) {
        if (channel == null || closed.put(channel, Boolean.TRUE) != null) {
            return error;
        }
        try {
            channel.close();
            return error;
        } catch (IOException closeError) {
            if (error == null) {
                return closeError;
            }
            error.addSuppressed(closeError);
            return error;
        }
    }

    private static int socketReadTimeoutMillis(Instant deadline) {
        if (deadline == null) {
            return 0;
        }
        Instant now = Instant.now();
        if (!deadline.isAfter(now)) {
            return 1;
        }
        long millis;
        try {
            millis = Duration.between(now, deadline).toMillis();
        } catch (ArithmeticException overflow) {
            return Integer.MAX_VALUE;
        }
        if (millis <= 0L) {
            return 1;
        }
        return millis >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) millis;
    }

    private static final class SocketDuplexConnection implements DuplexConnection {
        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;
        private final GatheringByteChannel gatheringOutput;
        // Writes currently inside the socket's output stream; only tracked for TLS sockets (see close()).
        private final AtomicInteger writesInProgress;

        private SocketDuplexConnection(Socket socket) throws IOException {
            this.socket = Objects.requireNonNull(socket, "socket");
            this.input = socket.getInputStream();
            if (socket instanceof SSLSocket) {
                this.writesInProgress = new AtomicInteger();
                this.output = new WriteTrackingOutputStream(socket.getOutputStream(), this.writesInProgress);
            } else {
                this.writesInProgress = null;
                this.output = socket.getOutputStream();
            }
            this.gatheringOutput = socket.getChannel();
        }

        @Override
        public InputStream input() {
            return input;
        }

        @Override
        public OutputStream output() {
            return output;
        }

        @Override
        public GatheringByteChannel gatheringOutput() {
            return gatheringOutput;
        }

        @Override
        public SocketAddress localAddress() {
            return socket.getLocalSocketAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return socket.getRemoteSocketAddress();
        }

        @Override
        public boolean supportsReadDeadline() {
            return true;
        }

        @Override
        public void setReadDeadline(Instant deadline) throws IOException {
            socket.setSoTimeout(socketReadTimeoutMillis(deadline));
        }

        /**
         * Closes the socket. A plain socket close never waits for a blocked write (the write fails instead), but an
         * orderly TLS close sends close_notify under the record lock that an in-progress write holds, so it would
         * wait for that write, forever if the peer stopped reading. A close that races a write is being used to
         * break it (session teardown, a missed close-frame deadline, a keepalive timeout), so a TLS socket is then
         * reset instead: SO_LINGER 0 makes the close skip close_notify and abort the connection.
         */
        @Override
        public void close() throws IOException {
            if (writesInProgress != null && writesInProgress.get() > 0) {
                try {
                    socket.setSoLinger(true, 0);
                } catch (SocketException ignored) {
                    // Already closed: there is nothing left to abort.
                }
            }
            socket.close();
        }
    }

    /**
     * Counts writes in progress so {@link SocketDuplexConnection#close()} can tell when a write would block it.
     */
    private static final class WriteTrackingOutputStream extends FilterOutputStream {
        private final AtomicInteger writesInProgress;

        private WriteTrackingOutputStream(OutputStream output, AtomicInteger writesInProgress) {
            super(output);
            this.writesInProgress = writesInProgress;
        }

        @Override
        public void write(int b) throws IOException {
            writesInProgress.incrementAndGet();
            try {
                out.write(b);
            } finally {
                writesInProgress.decrementAndGet();
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            writesInProgress.incrementAndGet();
            try {
                out.write(b, off, len);
            } finally {
                writesInProgress.decrementAndGet();
            }
        }

        @Override
        public void flush() throws IOException {
            writesInProgress.incrementAndGet();
            try {
                out.flush();
            } finally {
                writesInProgress.decrementAndGet();
            }
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static final class ChannelDuplexConnection implements DuplexConnection {
        private final ReadableByteChannel inputChannel;
        private final WritableByteChannel outputChannel;
        private final InputStream input;
        private final OutputStream output;
        private final GatheringByteChannel gatheringOutput;
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;

        private ChannelDuplexConnection(ReadableByteChannel inputChannel, WritableByteChannel outputChannel) {
            this.inputChannel = Objects.requireNonNull(inputChannel, "inputChannel");
            this.outputChannel = Objects.requireNonNull(outputChannel, "outputChannel");
            this.input = Channels.newInputStream(inputChannel);
            this.output = Channels.newOutputStream(outputChannel);
            this.gatheringOutput = outputChannel instanceof GatheringByteChannel
                    ? (GatheringByteChannel) outputChannel
                    : null;
            this.localAddress = ZmuxConnections.localAddress(inputChannel, outputChannel);
            this.remoteAddress = ZmuxConnections.remoteAddress(inputChannel, outputChannel);
        }

        @Override
        public InputStream input() {
            return input;
        }

        @Override
        public OutputStream output() {
            return output;
        }

        @Override
        public GatheringByteChannel gatheringOutput() {
            return gatheringOutput;
        }

        @Override
        public SocketAddress localAddress() {
            return localAddress;
        }

        @Override
        public SocketAddress remoteAddress() {
            return remoteAddress;
        }

        @Override
        public void close() throws IOException {
            IdentityHashMap<Object, Boolean> closed = new IdentityHashMap<>(2);
            IOException error = closeOnce(inputChannel, closed, null);
            error = closeOnce(outputChannel, closed, error);
            if (error != null) {
                throw error;
            }
        }
    }
}
