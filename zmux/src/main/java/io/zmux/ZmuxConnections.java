package io.zmux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.IdentityHashMap;
import java.util.Objects;

public final class ZmuxConnections {
    private ZmuxConnections() {
    }

    public static BasicDuplexConnection.Builder builder(InputStream input, OutputStream output) {
        return BasicDuplexConnection.builder(input, output);
    }

    public static DuplexConnection of(ZmuxStream stream) {
        Objects.requireNonNull(stream, "stream");
        return builder(stream.asInputStream(), stream.asOutputStream())
                .addresses(stream.localAddress(), stream.remoteAddress())
                .build();
    }

    public static JoinedDuplexConnection join(InputStream input, OutputStream output) {
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

    private static final class SocketDuplexConnection implements DuplexConnection {
        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;
        private final GatheringByteChannel gatheringOutput;

        private SocketDuplexConnection(Socket socket) throws IOException {
            this.socket = Objects.requireNonNull(socket, "socket");
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
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
        public void close() throws IOException {
            socket.close();
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
