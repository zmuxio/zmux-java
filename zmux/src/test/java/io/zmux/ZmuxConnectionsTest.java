package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
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
}
