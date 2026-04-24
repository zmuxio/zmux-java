package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class ZmuxConnectionsTest {
    @Test
    void streamAdapterPreservesAddresses() {
        InetSocketAddress local = InetSocketAddress.createUnresolved("local.example", 1111);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("remote.example", 2222);

        DuplexConnection connection = ZmuxConnections.of(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(),
                local,
                remote
        );

        assertSame(local, connection.localAddress());
        assertSame(remote, connection.remoteAddress());
    }

    @Test
    void byteChannelAdapterExposesGatheringOutputAndClosesSharedChannelOnce() throws Exception {
        RecordingByteChannel channel = new RecordingByteChannel();
        DuplexConnection connection = ZmuxConnections.of(channel);

        assertSame(channel, connection.gatheringOutput());

        connection.output().write(new byte[]{1, 2, 3});
        assertArrayEquals(new byte[]{1, 2, 3}, channel.writtenBytes());

        connection.close();

        assertEquals(1, channel.closeCount(), "shared input/output channel should be closed once");
        assertFalse(channel.isOpen());
    }

    @Test
    void builderAndExtendedFactorySupportCustomConnectionMetadata() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("builder.local", 3333);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("builder.remote", 4444);
        RecordingByteChannel gathering = new RecordingByteChannel();
        AtomicInteger closerCalls = new AtomicInteger();

        DuplexConnection built = ZmuxConnections.builder(
                        new ByteArrayInputStream(new byte[0]),
                        new ByteArrayOutputStream()
                )
                .closer(() -> closerCalls.incrementAndGet())
                .addresses(local, remote)
                .gatheringOutput(gathering)
                .build();

        assertSame(local, built.localAddress());
        assertSame(remote, built.remoteAddress());
        assertSame(gathering, built.gatheringOutput());
        built.close();
        assertEquals(1, closerCalls.get());

        RecordingByteChannel gatheringFromFactory = new RecordingByteChannel();
        DuplexConnection viaFactory = ZmuxConnections.of(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(),
                () -> {
                },
                local,
                remote,
                gatheringFromFactory
        );

        assertSame(local, viaFactory.localAddress());
        assertSame(remote, viaFactory.remoteAddress());
        assertSame(gatheringFromFactory, viaFactory.gatheringOutput());
        viaFactory.close();
    }

    @Test
    void joinBuildsJoinedDuplexConnectionFromUniStreams() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("joined.local", 5555);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("joined.remote", 6666);
        RecordingRecvStream recv = new RecordingRecvStream(new byte[]{1, 2, 3}, local, remote);
        RecordingSendStream send = new RecordingSendStream(local, remote);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(recv, send)) {
            assertSame(local, connection.localAddress());
            assertSame(remote, connection.remoteAddress());
            assertEquals(1, connection.input().read());
            connection.output().write(new byte[]{4, 5});
        }

        assertArrayEquals(new byte[]{4, 5}, send.writtenBytes());
        assertEquals(1, recv.closeReadCalls());
        assertEquals(1, send.closeWriteCalls());
    }

    @Test
    void joinedConnectionPauseHandlesCanSwapTypedStreamHalvesAndRefreshAddresses() throws Exception {
        InetSocketAddress firstLocal = InetSocketAddress.createUnresolved("joined.local.a", 7777);
        InetSocketAddress firstRemote = InetSocketAddress.createUnresolved("joined.remote.a", 8888);
        InetSocketAddress secondLocal = InetSocketAddress.createUnresolved("joined.local.b", 9999);
        InetSocketAddress secondRemote = InetSocketAddress.createUnresolved("joined.remote.b", 10_000);

        RecordingRecvStream firstRecv = new RecordingRecvStream(new byte[]{1}, firstLocal, firstRemote);
        RecordingSendStream firstSend = new RecordingSendStream(firstLocal, firstRemote);
        RecordingRecvStream secondRecv = new RecordingRecvStream(new byte[]{2}, secondLocal, secondRemote);
        RecordingSendStream secondSend = new RecordingSendStream(secondLocal, secondRemote);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(firstRecv, firstSend)) {
            assertSame(firstLocal, connection.localAddress());
            assertSame(firstRemote, connection.remoteAddress());

            JoinedDuplexConnection.PausedInput pausedInput = connection.pauseInput();
            pausedInput.set(secondRecv);
            pausedInput.resume();
            assertSame(secondLocal, connection.localAddress());
            assertSame(secondRemote, connection.remoteAddress());
            assertEquals(2, connection.input().read());

            JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseOutput();
            pausedOutput.set(secondSend);
            pausedOutput.resume();
            assertSame(secondLocal, connection.localAddress());
            assertSame(secondRemote, connection.remoteAddress());
            connection.output().write(new byte[]{9});
        }

        assertEquals(0, firstRecv.closeReadCalls());
        assertEquals(0, firstSend.closeWriteCalls());
        assertEquals(1, secondRecv.closeReadCalls());
        assertEquals(1, secondSend.closeWriteCalls());
        assertArrayEquals(new byte[]{9}, secondSend.writtenBytes());
    }

    @Test
    void joinedConnectionExposesCurrentHalvesAndDirectionalClose() throws Exception {
        RecordingInputStream input = new RecordingInputStream(new byte[]{7});
        RecordingOutputStream output = new RecordingOutputStream();

        try (JoinedDuplexConnection connection = new JoinedDuplexConnection(input, output)) {
            assertSame(input, connection.inputHalf());
            assertSame(output, connection.outputHalf());

            JoinedDuplexConnection.PausedInput pausedInput = connection.pauseInput();
            assertNull(connection.inputHalf());
            pausedInput.resume();
            assertSame(input, connection.inputHalf());

            JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseOutput();
            assertNull(connection.outputHalf());
            pausedOutput.resume();
            assertSame(output, connection.outputHalf());

            connection.closeInput();
            assertSame(input, connection.inputHalf());
            connection.closeInput();
            assertEquals(2, input.closeCalls());

            connection.closeOutput();
            assertSame(output, connection.outputHalf());
            connection.closeOutput();
            assertEquals(2, output.closeCalls());
        }
    }

    @Test
    void joinedGatheringOutputViewFollowsReplacedWriteHalf() throws Exception {
        RecordingByteChannel firstGathering = new RecordingByteChannel();
        RecordingByteChannel secondGathering = new RecordingByteChannel();

        try (JoinedDuplexConnection connection = new JoinedDuplexConnection(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(),
                firstGathering,
                null,
                null
        )) {
            GatheringByteChannel gatheringView = connection.gatheringOutput();
            assertNotNull(gatheringView);

            JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseOutput();
            pausedOutput.set(new ByteArrayOutputStream());
            pausedOutput.setGatheringOutput(secondGathering);
            pausedOutput.resume();

            gatheringView.write(ByteBuffer.wrap(new byte[]{9, 8, 7}));
        }

        assertArrayEquals(new byte[0], firstGathering.writtenBytes());
        assertArrayEquals(new byte[]{9, 8, 7}, secondGathering.writtenBytes());
    }

    private static final class RecordingByteChannel implements ByteChannel, GatheringByteChannel {
        private final ByteArrayOutputStream written = new ByteArrayOutputStream();
        private final AtomicInteger closeCount = new AtomicInteger();
        private boolean open = true;

        @Override
        public int read(ByteBuffer dst) {
            return -1;
        }

        @Override
        public int write(ByteBuffer src) {
            int length = src.remaining();
            byte[] bytes = new byte[length];
            src.get(bytes);
            written.write(bytes, 0, bytes.length);
            return length;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            long total = 0L;
            int end = offset + length;
            for (int i = offset; i < end; i++) {
                total += write(srcs[i]);
            }
            return total;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
            open = false;
        }

        byte[] writtenBytes() {
            return written.toByteArray();
        }

        int closeCount() {
            return closeCount.get();
        }
    }

    private static final class RecordingRecvStream implements ZmuxRecvStream {
        private final ByteArrayInputStream input;
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;
        private int closeReadCalls;

        private RecordingRecvStream(byte[] data, SocketAddress localAddress, SocketAddress remoteAddress) {
            this.input = new ByteArrayInputStream(data);
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
        }

        @Override
        public int read(byte[] dst, int offset, int length) {
            return input.read(dst, offset, length);
        }

        @Override
        public void closeRead() {
            closeReadCalls++;
        }

        @Override
        public void cancelRead(long code) {
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public void setReadDeadline(java.time.Instant deadline) {
        }

        @Override
        public void close() {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return localAddress;
        }

        @Override
        public SocketAddress remoteAddress() {
            return remoteAddress;
        }

        int closeReadCalls() {
            return closeReadCalls;
        }
    }

    private static final class RecordingInputStream extends ByteArrayInputStream {
        private int closeCalls;

        private RecordingInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void close() {
            closeCalls++;
        }

        int closeCalls() {
            return closeCalls;
        }
    }

    private static final class RecordingOutputStream extends ByteArrayOutputStream {
        private int closeCalls;

        @Override
        public void close() throws IOException {
            closeCalls++;
            super.close();
        }

        int closeCalls() {
            return closeCalls;
        }
    }

    private static final class RecordingSendStream implements ZmuxSendStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;
        private int closeWriteCalls;

        private RecordingSendStream(SocketAddress localAddress, SocketAddress remoteAddress) {
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
            output.write(src, offset, length);
        }

        @Override
        public int writeFinal(byte[] src, int offset, int length) {
            output.write(src, offset, length);
            return length;
        }

        @Override
        public void updateMetadata(MetadataUpdate update) {
        }

        @Override
        public void closeWrite() {
            closeWriteCalls++;
        }

        @Override
        public void cancelWrite(long code) {
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public void setWriteDeadline(java.time.Instant deadline) {
        }

        @Override
        public void close() {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return localAddress;
        }

        @Override
        public SocketAddress remoteAddress() {
            return remoteAddress;
        }

        byte[] writtenBytes() {
            return output.toByteArray();
        }

        int closeWriteCalls() {
            return closeWriteCalls;
        }
    }
}
