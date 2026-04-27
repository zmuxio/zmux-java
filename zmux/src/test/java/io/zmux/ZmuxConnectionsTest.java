package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void bidiStreamAdapterExposesDuplexConnectionSurface() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("bidi.local", 2121);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("bidi.remote", 3434);
        RecordingBidiStream stream = new RecordingBidiStream(new byte[]{6, 7}, local, remote);

        DuplexConnection connection = ZmuxConnections.of(stream);

        assertSame(local, connection.localAddress());
        assertSame(remote, connection.remoteAddress());
        assertTrue(connection.supportsWriteDeadline(), "bidi stream adapters should preserve write-deadline support");
        assertEquals(6, connection.input().read());
        connection.output().write(new byte[]{1, 2, 3});
        connection.close();

        assertArrayEquals(new byte[]{1, 2, 3}, stream.writtenBytes());
        assertEquals(1, stream.closeReadCalls());
        assertEquals(1, stream.closeWriteCalls());
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
            assertTrue(connection.supportsWriteDeadline(), "joined stream halves should expose write-deadline support");
            assertEquals(1, connection.input().read());
            connection.output().write(new byte[]{4, 5});
        }

        assertArrayEquals(new byte[]{4, 5}, send.writtenBytes());
        assertEquals(1, recv.closeReadCalls());
        assertEquals(1, send.closeWriteCalls());
    }

    @Test
    void joinBuildsJoinedDuplexConnectionFromGenericHalves() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("joined.generic.local", 1234);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("joined.generic.remote", 5678);
        RecordingInputStream input = new RecordingInputStream(new byte[]{7, 8});
        RecordingOutputStream output = new RecordingOutputStream();
        RecordingByteChannel gathering = new RecordingByteChannel();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(input, output, gathering, local, remote)) {
            assertSame(local, connection.localAddress());
            assertSame(remote, connection.remoteAddress());
            assertNotNull(connection.gatheringOutput());
            assertEquals(7, connection.input().read());
            connection.output().write(new byte[]{1, 2});
            connection.gatheringOutput().write(ByteBuffer.wrap(new byte[]{3}));
        }

        assertArrayEquals(new byte[]{1, 2}, output.toByteArray());
        assertArrayEquals(new byte[]{3}, gathering.writtenBytes());
        assertEquals(1, input.closeCalls());
        assertEquals(1, output.closeCalls());
    }

    @Test
    void joinedConnectionNilReadHalfStillSupportsDeadlineAndClose() throws Exception {
        RecordingWriteHalf writeHalf = new RecordingWriteHalf(null, null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join((ReadHalf) null, writeHalf)) {
            assertThrows(StreamNotReadableException.class, connection.input()::read);
            assertDoesNotThrow(() -> connection.setReadDeadline(Instant.now().plusSeconds(1)));
            assertDoesNotThrow(connection::closeRead);
            assertEquals(0, writeHalf.closeWriteCalls(), "closeRead should not touch the write half");
        }
    }

    @Test
    void joinedConnectionNilWriteHalfStillSupportsDeadlineAndClose() throws Exception {
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[0], null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, (WriteHalf) null)) {
            assertThrows(StreamNotWritableException.class, () -> connection.output().write(1));
            assertDoesNotThrow(() -> connection.setWriteDeadline(Instant.now().plusSeconds(1)));
            assertDoesNotThrow(connection::closeWrite);
            assertEquals(0, readHalf.closeReadCalls(), "closeWrite should not touch the read half");
        }
    }

    @Test
    void joinBuildsJoinedDuplexConnectionFromDirectionalHalves() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("joined.half.local", 4321);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("joined.half.remote", 8765);
        RecordingByteChannel gathering = new RecordingByteChannel();
        RecordingReadHalf input = new RecordingReadHalf(new byte[]{3, 4}, local, remote);
        RecordingWriteHalf output = new RecordingWriteHalf(local, remote, gathering);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(input, output)) {
            assertSame(local, connection.localAddress());
            assertSame(remote, connection.remoteAddress());
            assertSame(input, connection.readHalf());
            assertSame(output, connection.writeHalf());
            assertNotNull(connection.gatheringOutput());
            assertEquals(3, connection.input().read());
            connection.output().write(new byte[]{1, 2});
            connection.gatheringOutput().write(ByteBuffer.wrap(new byte[]{9}));
        }

        assertEquals(1, input.closeReadCalls());
        assertEquals(1, output.closeWriteCalls());
        assertArrayEquals(new byte[]{1, 2}, output.writtenBytes());
        assertArrayEquals(new byte[]{9}, gathering.writtenBytes());
    }

    @Test
    void joinedConnectionRejectsInvalidReadProgress() throws Exception {
        int[] reads = {-2, 4};
        for (int read : reads) {
            try (JoinedDuplexConnection connection = ZmuxConnections.join(
                    new InvalidProgressReadHalf(read),
                    (WriteHalf) null
            )) {
                assertThrows(
                        IOException.class,
                        () -> connection.input().read(new byte[3]),
                        "joined read should reject invalid progress " + read
                );
            }
        }
    }

    @Test
    void joinedConnectionFallsBackToOtherHalfAddressesWhenPrimaryHalfHasNone() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("joined.write.local", 4567);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("joined.write.remote", 4568);
        RecordingReadHalf input = new RecordingReadHalf(new byte[0], null, null);
        RecordingWriteHalf output = new RecordingWriteHalf(local, remote, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(input, output)) {
            assertSame(local, connection.localAddress());
            assertSame(remote, connection.remoteAddress());
        }
    }

    @Test
    void joinedConnectionTypedHalvesFallbackToSyntheticAddressesWhenBothSidesHaveNone() throws Exception {
        RecordingReadHalf input = new RecordingReadHalf(new byte[0], null, null);
        RecordingWriteHalf output = new RecordingWriteHalf(null, null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(input, output)) {
            assertEquals(ZmuxSocketAddress.localPending(), connection.localAddress());
            assertEquals(ZmuxSocketAddress.remotePending(), connection.remoteAddress());
            assertEquals("local/stream/pending", connection.localAddress().toString());
            assertEquals("remote/stream/pending", connection.remoteAddress().toString());
        }
    }

    @Test
    void directionalHalfDefaultsExposeJavaIoConvenienceHelpers() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("half.local", 3010);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("half.remote", 4010);
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[]{1, 2, 3}, local, remote);
        RecordingWriteHalf writeHalf = new RecordingWriteHalf(local, remote, null);

        byte[] first = new byte[2];
        assertEquals(2, readHalf.read(first));
        assertArrayEquals(new byte[]{1, 2}, first);

        ByteBuffer dst = ByteBuffer.allocate(2);
        assertEquals(1, readHalf.read(dst));
        assertEquals(1, dst.position());
        assertEquals((byte) 3, dst.get(0));

        InputStream input = readHalf.asInputStream();
        assertEquals(-1, input.read());
        input.close();
        assertEquals(1, readHalf.closeReadCalls());

        readHalf.setReadTimeout(Duration.ofMillis(5));
        assertNotNull(readHalf.lastReadDeadline());
        readHalf.clearReadDeadline();
        assertEquals(1, readHalf.readDeadlineSetCalls());
        assertEquals(1, readHalf.readDeadlineClearCalls());

        writeHalf.write(new byte[]{4, 5});
        ByteBuffer src = ByteBuffer.wrap(new byte[]{6, 7});
        assertEquals(2, writeHalf.write(src));
        assertEquals(2, src.position());

        OutputStream output = writeHalf.asOutputStream();
        output.write(new byte[]{8, 9});
        output.close();
        assertEquals(1, writeHalf.closeWriteCalls());
        assertArrayEquals(new byte[]{4, 5, 6, 7, 8, 9}, writeHalf.writtenBytes());

        writeHalf.setWriteTimeout(Duration.ofMillis(5));
        assertNotNull(writeHalf.lastWriteDeadline());
        writeHalf.clearWriteDeadline();
        assertEquals(1, writeHalf.writeDeadlineSetCalls());
        assertEquals(1, writeHalf.writeDeadlineClearCalls());
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
            assertNull(connection.readHalf());
            assertNull(connection.writeHalf());

            JoinedDuplexConnection.PausedInput pausedInput = connection.pauseRead();
            assertNull(connection.inputHalf());
            assertNull(connection.readHalf());
            pausedInput.resume();
            assertSame(input, connection.inputHalf());
            assertNull(connection.readHalf());

            JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseWrite();
            assertNull(connection.outputHalf());
            assertNull(connection.writeHalf());
            pausedOutput.resume();
            assertSame(output, connection.outputHalf());
            assertNull(connection.writeHalf());

            connection.closeRead();
            assertSame(input, connection.inputHalf());
            connection.closeRead();
            assertEquals(2, input.closeCalls());

            connection.closeWrite();
            assertSame(output, connection.outputHalf());
            connection.closeWrite();
            assertEquals(2, output.closeCalls());
        }
    }

    @Test
    void joinedConnectionTypedPauseHandlesMatchGoStyleHalfSwaps() throws Exception {
        InetSocketAddress firstLocal = InetSocketAddress.createUnresolved("typed.local.a", 7101);
        InetSocketAddress firstRemote = InetSocketAddress.createUnresolved("typed.remote.a", 7102);
        InetSocketAddress secondLocal = InetSocketAddress.createUnresolved("typed.local.b", 7201);
        InetSocketAddress secondRemote = InetSocketAddress.createUnresolved("typed.remote.b", 7202);
        RecordingReadHalf firstRead = new RecordingReadHalf(new byte[]{1}, firstLocal, firstRemote);
        RecordingWriteHalf firstWrite = new RecordingWriteHalf(firstLocal, firstRemote, null);
        RecordingReadHalf secondRead = new RecordingReadHalf(new byte[]{2}, secondLocal, secondRemote);
        RecordingWriteHalf secondWrite = new RecordingWriteHalf(secondLocal, secondRemote, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(firstRead, firstWrite)) {
            JoinedDuplexConnection.PausedInput pausedInput = connection.pauseRead();
            assertSame(firstRead, pausedInput.currentReadHalf());
            assertSame(firstRead, pausedInput.replaceReadHalf(secondRead));
            pausedInput.resume();
            assertSame(secondRead, connection.readHalf());

            JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseWrite();
            assertSame(firstWrite, pausedOutput.currentWriteHalf());
            assertSame(firstWrite, pausedOutput.replaceWriteHalf(secondWrite));
            pausedOutput.resume();
            assertSame(secondWrite, connection.writeHalf());
        }
    }

    @Test
    void joinedPauseResumeAfterCloseFailsOnceThenBecomesIdempotent() throws Exception {
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[]{1}, null, null);
        RecordingWriteHalf writeHalf = new RecordingWriteHalf(null, null, null);
        JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, writeHalf);

        JoinedDuplexConnection.PausedInput pausedInput = connection.pauseRead();
        JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseWrite();
        connection.close();

        assertThrows(SessionClosedException.class, pausedInput::resume);
        assertDoesNotThrow(pausedInput::resume);

        assertThrows(SessionClosedException.class, pausedOutput::resume);
        assertDoesNotThrow(pausedOutput::resume);
    }

    @Test
    void joinedConnectionPauseReadBlocksUpperReadUntilResume() throws Exception {
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[]{42}, null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, new RecordingWriteHalf(null, null, null))) {
            JoinedDuplexConnection.PausedInput pause = connection.pauseRead();

            int[] result = {-2};
            Throwable[] failure = new Throwable[1];
            Thread reader = new Thread(() -> {
                try {
                    result[0] = connection.input().read();
                } catch (Throwable error) {
                    failure[0] = error;
                }
            });
            reader.start();

            Thread.sleep(50L);
            assertTrue(reader.isAlive(), "read should remain blocked while paused");

            pause.resume();
            reader.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(reader.isAlive(), "read should finish after resume");
            assertNull(failure[0], "blocked read should not fail");
            assertEquals(42, result[0]);
        }
    }

    @Test
    void joinedConnectionPauseWriteBlocksUpperWriteUntilResume() throws Exception {
        RecordingWriteHalf writeHalf = new RecordingWriteHalf(null, null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(new RecordingReadHalf(new byte[0], null, null), writeHalf)) {
            JoinedDuplexConnection.PausedOutput pause = connection.pauseWrite();

            Throwable[] failure = new Throwable[1];
            Thread writer = new Thread(() -> {
                try {
                    connection.output().write(new byte[]{9});
                } catch (Throwable error) {
                    failure[0] = error;
                }
            });
            writer.start();

            Thread.sleep(50L);
            assertTrue(writer.isAlive(), "write should remain blocked while paused");

            pause.resume();
            writer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(writer.isAlive(), "write should finish after resume");
            assertNull(failure[0], "blocked write should not fail");
            assertArrayEquals(new byte[]{9}, writeHalf.writtenBytes());
        }
    }

    @Test
    void joinedConnectionPauseReadWaitsForInflightReadToDrain() throws Exception {
        BlockingReadHalf readHalf = new BlockingReadHalf((byte) 5);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            int[] result = {-1};
            Throwable[] readFailure = new Throwable[1];
            Thread reader = new Thread(() -> {
                try {
                    result[0] = connection.input().read();
                } catch (Throwable error) {
                    readFailure[0] = error;
                }
            });
            reader.start();

            assertTrue(readHalf.awaitReadStarted(), "in-flight read should start");

            JoinedDuplexConnection.PausedInput[] pausedHolder = new JoinedDuplexConnection.PausedInput[1];
            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    pausedHolder[0] = connection.pauseRead();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseRead should wait for the in-flight read");

            readHalf.releaseRead();
            reader.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(reader.isAlive(), "in-flight read should finish after release");
            assertNull(readFailure[0], "in-flight read should not fail");
            assertEquals(5, result[0]);

            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseRead should finish after in-flight read drains");
            assertNull(pauseFailure[0], "pauseRead should not fail");
            assertNotNull(pausedHolder[0], "pauseRead should return a pause handle");
            assertSame(readHalf, pausedHolder[0].currentReadHalf());
            pausedHolder[0].resume();
        }
    }

    @Test
    void joinedConnectionPauseWriteWaitsForInflightWriteToDrain() throws Exception {
        BlockingWriteHalf writeHalf = new BlockingWriteHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            Throwable[] writeFailure = new Throwable[1];
            Thread writer = new Thread(() -> {
                try {
                    connection.output().write(new byte[]{7});
                } catch (Throwable error) {
                    writeFailure[0] = error;
                }
            });
            writer.start();

            assertTrue(writeHalf.awaitWriteStarted(), "in-flight write should start");

            JoinedDuplexConnection.PausedOutput[] pausedHolder = new JoinedDuplexConnection.PausedOutput[1];
            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    pausedHolder[0] = connection.pauseWrite();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseWrite should wait for the in-flight write");

            writeHalf.releaseWrite();
            writer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(writer.isAlive(), "in-flight write should finish after release");
            assertNull(writeFailure[0], "in-flight write should not fail");
            assertArrayEquals(new byte[]{7}, writeHalf.writtenBytes());

            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseWrite should finish after in-flight write drains");
            assertNull(pauseFailure[0], "pauseWrite should not fail");
            assertNotNull(pausedHolder[0], "pauseWrite should return a pause handle");
            assertSame(writeHalf, pausedHolder[0].currentWriteHalf());
            pausedHolder[0].resume();
        }
    }

    @Test
    void joinedConnectionPauseReadTimeoutDoesNotDisturbInflightRead() throws Exception {
        BlockingReadHalf readHalf = new BlockingReadHalf((byte) 6);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            int[] result = {-1};
            Throwable[] readFailure = new Throwable[1];
            Thread reader = new Thread(() -> {
                try {
                    result[0] = connection.input().read();
                } catch (Throwable error) {
                    readFailure[0] = error;
                }
            });
            reader.start();

            assertTrue(readHalf.awaitReadStarted(), "in-flight read should start");

            SocketTimeoutException timeout = assertThrows(
                    SocketTimeoutException.class,
                    () -> connection.pauseRead(Duration.ofMillis(30))
            );
            assertTrue(timeout.getMessage().contains("pause timed out"));

            readHalf.releaseRead();
            reader.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(reader.isAlive(), "in-flight read should still finish after pause timeout");
            assertNull(readFailure[0], "pause timeout must not poison the in-flight read");
            assertEquals(6, result[0]);
        }
    }

    @Test
    void joinedConnectionPauseReadInterruptDoesNotDisturbInflightRead() throws Exception {
        BlockingReadHalf readHalf = new BlockingReadHalf((byte) 10);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            int[] result = {-1};
            Throwable[] readFailure = new Throwable[1];
            Thread reader = new Thread(() -> {
                try {
                    result[0] = connection.input().read();
                } catch (Throwable error) {
                    readFailure[0] = error;
                }
            });
            reader.start();

            assertTrue(readHalf.awaitReadStarted(), "in-flight read should start");

            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    connection.pauseRead();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseRead should be waiting for the in-flight read");
            pauser.interrupt();
            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseRead should exit after interruption");
            assertInstanceOf(InterruptedException.class, pauseFailure[0]);

            readHalf.releaseRead();
            reader.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(reader.isAlive(), "in-flight read should still finish after pause interruption");
            assertNull(readFailure[0], "pause interruption must not poison the in-flight read");
            assertEquals(10, result[0]);
        }
    }

    @Test
    void joinedConnectionPauseWriteTimeoutDoesNotDisturbInflightWrite() throws Exception {
        BlockingWriteHalf writeHalf = new BlockingWriteHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            Throwable[] writeFailure = new Throwable[1];
            Thread writer = new Thread(() -> {
                try {
                    connection.output().write(new byte[]{8});
                } catch (Throwable error) {
                    writeFailure[0] = error;
                }
            });
            writer.start();

            assertTrue(writeHalf.awaitWriteStarted(), "in-flight write should start");

            SocketTimeoutException timeout = assertThrows(
                    SocketTimeoutException.class,
                    () -> connection.pauseWrite(Duration.ofMillis(30))
            );
            assertTrue(timeout.getMessage().contains("pause timed out"));

            writeHalf.releaseWrite();
            writer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(writer.isAlive(), "in-flight write should still finish after pause timeout");
            assertNull(writeFailure[0], "pause timeout must not poison the in-flight write");
            assertArrayEquals(new byte[]{8}, writeHalf.writtenBytes());
        }
    }

    @Test
    void joinedConnectionPauseWriteInterruptDoesNotDisturbInflightWrite() throws Exception {
        BlockingWriteHalf writeHalf = new BlockingWriteHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            Throwable[] writeFailure = new Throwable[1];
            Thread writer = new Thread(() -> {
                try {
                    connection.output().write(new byte[]{11});
                } catch (Throwable error) {
                    writeFailure[0] = error;
                }
            });
            writer.start();

            assertTrue(writeHalf.awaitWriteStarted(), "in-flight write should start");

            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    connection.pauseWrite();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseWrite should be waiting for the in-flight write");
            pauser.interrupt();
            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseWrite should exit after interruption");
            assertInstanceOf(InterruptedException.class, pauseFailure[0]);

            writeHalf.releaseWrite();
            writer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(writer.isAlive(), "in-flight write should still finish after pause interruption");
            assertNull(writeFailure[0], "pause interruption must not poison the in-flight write");
            assertArrayEquals(new byte[]{11}, writeHalf.writtenBytes());
        }
    }

    @Test
    void joinedConnectionPausedReadStillHonorsReadDeadline() throws Exception {
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[]{1}, null, null);

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            assertEquals(1, connection.input().read());

            JoinedDuplexConnection.PausedInput pause = connection.pauseRead();
            connection.setReadDeadline(Instant.now().plusMillis(30));

            SocketTimeoutException timeout = assertThrows(SocketTimeoutException.class, () -> connection.input().read());
            assertTrue(timeout.getMessage().contains("read deadline"));

            pause.resume();
        }
    }

    @Test
    void joinedConnectionPauseReadWaitsForInflightCloseReadToDrain() throws Exception {
        BlockingCloseReadHalf readHalf = new BlockingCloseReadHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            Throwable[] closeFailure = new Throwable[1];
            Thread closer = new Thread(() -> {
                try {
                    connection.closeRead();
                } catch (Throwable error) {
                    closeFailure[0] = error;
                }
            });
            closer.start();

            assertTrue(readHalf.awaitCloseStarted(), "closeRead should reach the underlying read half");

            JoinedDuplexConnection.PausedInput[] pausedHolder = new JoinedDuplexConnection.PausedInput[1];
            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    pausedHolder[0] = connection.pauseRead();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseRead should wait for in-flight closeRead");

            readHalf.releaseClose();
            closer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(closer.isAlive(), "closeRead should finish after underlying close releases");
            assertNull(closeFailure[0], "closeRead should not fail");

            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseRead should finish after in-flight closeRead drains");
            assertNull(pauseFailure[0], "pauseRead should not fail");
            assertNotNull(pausedHolder[0], "pauseRead should return a pause handle");
            assertSame(readHalf, pausedHolder[0].currentReadHalf());
            pausedHolder[0].resume();
        }
    }

    @Test
    void joinedConnectionPauseWriteWaitsForInflightCloseWriteToDrain() throws Exception {
        BlockingCloseWriteHalf writeHalf = new BlockingCloseWriteHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            Throwable[] closeFailure = new Throwable[1];
            Thread closer = new Thread(() -> {
                try {
                    connection.closeWrite();
                } catch (Throwable error) {
                    closeFailure[0] = error;
                }
            });
            closer.start();

            assertTrue(writeHalf.awaitCloseStarted(), "closeWrite should reach the underlying write half");

            JoinedDuplexConnection.PausedOutput[] pausedHolder = new JoinedDuplexConnection.PausedOutput[1];
            Throwable[] pauseFailure = new Throwable[1];
            Thread pauser = new Thread(() -> {
                try {
                    pausedHolder[0] = connection.pauseWrite();
                } catch (Throwable error) {
                    pauseFailure[0] = error;
                }
            });
            pauser.start();

            Thread.sleep(50L);
            assertTrue(pauser.isAlive(), "pauseWrite should wait for in-flight closeWrite");

            writeHalf.releaseClose();
            closer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(closer.isAlive(), "closeWrite should finish after underlying close releases");
            assertNull(closeFailure[0], "closeWrite should not fail");

            pauser.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(pauser.isAlive(), "pauseWrite should finish after in-flight closeWrite drains");
            assertNull(pauseFailure[0], "pauseWrite should not fail");
            assertNotNull(pausedHolder[0], "pauseWrite should return a pause handle");
            assertSame(writeHalf, pausedHolder[0].currentWriteHalf());
            pausedHolder[0].resume();
        }
    }

    @Test
    void joinedConnectionPauseReadBlocksCloseReadUntilResume() throws Exception {
        BlockingCloseReadHalf readHalf = new BlockingCloseReadHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            JoinedDuplexConnection.PausedInput pause = connection.pauseRead();
            assertSame(readHalf, pause.currentReadHalf());

            Throwable[] closeFailure = new Throwable[1];
            Thread closer = new Thread(() -> {
                try {
                    connection.closeRead();
                } catch (Throwable error) {
                    closeFailure[0] = error;
                }
            });
            closer.start();

            Thread.sleep(50L);
            assertTrue(closer.isAlive(), "closeRead should remain blocked while the read side is paused");
            assertEquals(0, readHalf.closeStartCount(), "closeRead must not reach the detached read half");

            pause.resume();
            assertTrue(readHalf.awaitCloseStarted(), "closeRead should reach the underlying read half after resume");
            readHalf.releaseClose();

            closer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(closer.isAlive(), "closeRead should finish after resume");
            assertNull(closeFailure[0], "closeRead should not fail");
        }
    }

    @Test
    void joinedConnectionPauseWriteBlocksCloseWriteUntilResume() throws Exception {
        BlockingCloseWriteHalf writeHalf = new BlockingCloseWriteHalf();

        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            JoinedDuplexConnection.PausedOutput pause = connection.pauseWrite();
            assertSame(writeHalf, pause.currentWriteHalf());

            Throwable[] closeFailure = new Throwable[1];
            Thread closer = new Thread(() -> {
                try {
                    connection.closeWrite();
                } catch (Throwable error) {
                    closeFailure[0] = error;
                }
            });
            closer.start();

            Thread.sleep(50L);
            assertTrue(closer.isAlive(), "closeWrite should remain blocked while the write side is paused");
            assertEquals(0, writeHalf.closeStartCount(), "closeWrite must not reach the detached write half");

            pause.resume();
            assertTrue(writeHalf.awaitCloseStarted(), "closeWrite should reach the underlying write half after resume");
            writeHalf.releaseClose();

            closer.join(TimeUnit.SECONDS.toMillis(1));
            assertFalse(closer.isAlive(), "closeWrite should finish after resume");
            assertNull(closeFailure[0], "closeWrite should not fail");
        }
    }

    @Test
    void joinedConnectionResumeReadReplaysDeadlineSetWhilePaused() throws Exception {
        JoinedDuplexConnection connection = ZmuxConnections.join(new RecordingReadHalf(new byte[0], null, null), null);
        JoinedDuplexConnection.PausedInput pause = connection.pauseRead();

        BlockingDeadlineReadHalf replacement = new BlockingDeadlineReadHalf();
        pause.replaceReadHalf(replacement);

        Instant firstDeadline = Instant.now().plusMillis(30);
        Instant secondDeadline = Instant.now().plusMillis(60);
        connection.setReadDeadline(firstDeadline);

        Throwable[] resumeFailure = new Throwable[1];
        Thread resume = new Thread(() -> {
            try {
                pause.resume();
            } catch (Throwable failure) {
                resumeFailure[0] = failure;
            }
        });
        resume.start();

        assertTrue(replacement.awaitFirstSet(), "resume should apply first read deadline");
        connection.setReadDeadline(secondDeadline);
        replacement.releaseSet();
        resume.join(TimeUnit.SECONDS.toMillis(1));
        assertFalse(resume.isAlive(), "pause resume should complete after deadline refresh");
        assertNull(resumeFailure[0], "pause resume should not fail");

        List<Instant> deadlines = replacement.snapshotDeadlines();
        assertEquals(2, deadlines.size());
        assertEquals(firstDeadline, deadlines.get(0));
        assertEquals(secondDeadline, deadlines.get(1));
        assertSame(replacement, connection.readHalf());
        connection.close();
    }

    @Test
    void joinedConnectionPausedReadHandleSerializesResumeAndReplace() throws Exception {
        JoinedDuplexConnection connection = ZmuxConnections.join(new RecordingReadHalf(new byte[0], null, null), null);
        JoinedDuplexConnection.PausedInput pause = connection.pauseRead();
        BlockingDeadlineReadHalf replacement = new BlockingDeadlineReadHalf();
        RecordingReadHalf next = new RecordingReadHalf(new byte[0], null, null);
        pause.replaceReadHalf(replacement);
        connection.setReadDeadline(Instant.now().plusMillis(30));

        Throwable[] resumeFailure = new Throwable[1];
        Thread resume = new Thread(() -> {
            try {
                pause.resume();
            } catch (Throwable failure) {
                resumeFailure[0] = failure;
            }
        });
        resume.start();

        assertTrue(replacement.awaitFirstSet(), "resume should begin applying the paused read half deadline");
        Throwable[] replaceFailure = new Throwable[1];
        ReadHalf[] previous = new ReadHalf[1];
        Thread replacer = new Thread(() -> {
            try {
                previous[0] = pause.replaceReadHalf(next);
            } catch (Throwable failure) {
                replaceFailure[0] = failure;
            }
        });
        replacer.start();

        Thread.sleep(50L);
        assertTrue(replacer.isAlive(), "replaceReadHalf should wait while resume is replaying the deadline");
        replacement.releaseSet();
        resume.join(TimeUnit.SECONDS.toMillis(1));
        replacer.join(TimeUnit.SECONDS.toMillis(1));

        assertFalse(resume.isAlive(), "resume should complete after deadline replay is released");
        assertFalse(replacer.isAlive(), "replaceReadHalf should complete after resume releases the pause handle");
        assertNull(resumeFailure[0], "resume should not fail");
        assertNull(replaceFailure[0], "replaceReadHalf should not fail");
        assertSame(replacement, previous[0], "replacement should observe the half that resume attached");
        assertSame(replacement, connection.readHalf(), "resume should attach the deadline-replayed half");
        connection.close();
    }

    @Test
    void joinedConnectionResumeWriteReplaysDeadlineSetWhilePaused() throws Exception {
        JoinedDuplexConnection connection = ZmuxConnections.join(null, new RecordingWriteHalf(null, null, null));
        JoinedDuplexConnection.PausedOutput pause = connection.pauseWrite();

        BlockingDeadlineWriteHalf replacement = new BlockingDeadlineWriteHalf();
        pause.replaceWriteHalf(replacement);

        Instant firstDeadline = Instant.now().plusMillis(30);
        Instant secondDeadline = Instant.now().plusMillis(60);
        connection.setWriteDeadline(firstDeadline);

        Throwable[] resumeFailure = new Throwable[1];
        Thread resume = new Thread(() -> {
            try {
                pause.resume();
            } catch (Throwable failure) {
                resumeFailure[0] = failure;
            }
        });
        resume.start();

        assertTrue(replacement.awaitFirstSet(), "resume should apply first write deadline");
        connection.setWriteDeadline(secondDeadline);
        replacement.releaseSet();
        resume.join(TimeUnit.SECONDS.toMillis(1));
        assertFalse(resume.isAlive(), "pause resume should complete after deadline refresh");
        assertNull(resumeFailure[0], "pause resume should not fail");

        List<Instant> deadlines = replacement.snapshotDeadlines();
        assertEquals(2, deadlines.size());
        assertEquals(firstDeadline, deadlines.get(0));
        assertEquals(secondDeadline, deadlines.get(1));
        assertSame(replacement, connection.writeHalf());
        connection.close();
    }

    @Test
    void joinedConnectionPausedWriteHandleSerializesResumeAndReplace() throws Exception {
        JoinedDuplexConnection connection = ZmuxConnections.join(null, new RecordingWriteHalf(null, null, null));
        JoinedDuplexConnection.PausedOutput pause = connection.pauseWrite();
        BlockingDeadlineWriteHalf replacement = new BlockingDeadlineWriteHalf();
        RecordingWriteHalf next = new RecordingWriteHalf(null, null, null);
        pause.replaceWriteHalf(replacement);
        connection.setWriteDeadline(Instant.now().plusMillis(30));

        Throwable[] resumeFailure = new Throwable[1];
        Thread resume = new Thread(() -> {
            try {
                pause.resume();
            } catch (Throwable failure) {
                resumeFailure[0] = failure;
            }
        });
        resume.start();

        assertTrue(replacement.awaitFirstSet(), "resume should begin applying the paused write half deadline");
        Throwable[] replaceFailure = new Throwable[1];
        WriteHalf[] previous = new WriteHalf[1];
        Thread replacer = new Thread(() -> {
            try {
                previous[0] = pause.replaceWriteHalf(next);
            } catch (Throwable failure) {
                replaceFailure[0] = failure;
            }
        });
        replacer.start();

        Thread.sleep(50L);
        assertTrue(replacer.isAlive(), "replaceWriteHalf should wait while resume is replaying the deadline");
        replacement.releaseSet();
        resume.join(TimeUnit.SECONDS.toMillis(1));
        replacer.join(TimeUnit.SECONDS.toMillis(1));

        assertFalse(resume.isAlive(), "resume should complete after deadline replay is released");
        assertFalse(replacer.isAlive(), "replaceWriteHalf should complete after resume releases the pause handle");
        assertNull(resumeFailure[0], "resume should not fail");
        assertNull(replaceFailure[0], "replaceWriteHalf should not fail");
        assertSame(replacement, previous[0], "replacement should observe the half that resume attached");
        assertSame(replacement, connection.writeHalf(), "resume should attach the deadline-replayed half");
        connection.close();
    }

    @Test
    void joinedConnectionPauseReadDeadlineTimesOutBlockedCloseRead() throws Exception {
        RecordingReadHalf readHalf = new RecordingReadHalf(new byte[0], null, null);
        try (JoinedDuplexConnection connection = ZmuxConnections.join(readHalf, null)) {
            JoinedDuplexConnection.PausedInput pause = connection.pauseRead();
            connection.setReadDeadline(Instant.now().plusMillis(30));

            SocketTimeoutException timeout = assertThrows(SocketTimeoutException.class, connection::closeRead);
            assertTrue(timeout.getMessage().contains("read deadline"));
            assertEquals(0, readHalf.closeReadCalls());

            pause.resume();
        }
    }

    @Test
    void joinedConnectionPauseWriteDeadlineTimesOutBlockedCloseWrite() throws Exception {
        RecordingWriteHalf writeHalf = new RecordingWriteHalf(null, null, null);
        try (JoinedDuplexConnection connection = ZmuxConnections.join(null, writeHalf)) {
            JoinedDuplexConnection.PausedOutput pause = connection.pauseWrite();
            connection.setWriteDeadline(Instant.now().plusMillis(30));

            SocketTimeoutException timeout = assertThrows(SocketTimeoutException.class, connection::closeWrite);
            assertTrue(timeout.getMessage().contains("write deadline"));
            assertEquals(0, writeHalf.closeWriteCalls());

            pause.resume();
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

    @Test
    void joinedConnectionFallsBackToSyntheticAddresses() throws Exception {
        try (JoinedDuplexConnection connection = new JoinedDuplexConnection(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        )) {
            assertEquals(ZmuxSocketAddress.localPending(), connection.localAddress());
            assertEquals(ZmuxSocketAddress.remotePending(), connection.remoteAddress());
            assertEquals("local/stream/pending", connection.localAddress().toString());
            assertEquals("remote/stream/pending", connection.remoteAddress().toString());
        }
    }

    @Test
    void joinedConnectionCloseAggregatesHalfCloseErrors() {
        IOException readError = new IOException("read-close-failure");
        IOException writeError = new IOException("write-close-failure");
        JoinedDuplexConnection connection = new JoinedDuplexConnection(
                new ThrowingInputStream(readError),
                new ThrowingOutputStream(writeError)
        );

        IOException error = assertThrows(IOException.class, connection::close);

        assertSame(readError, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(writeError, error.getSuppressed()[0]);
    }

    @Test
    void joinedConnectionCloseKeepsDetachedHalvesCallerOwned() throws Exception {
        RecordingInputStream input = new RecordingInputStream(new byte[0]);
        RecordingOutputStream output = new RecordingOutputStream();
        JoinedDuplexConnection connection = new JoinedDuplexConnection(input, output);

        JoinedDuplexConnection.PausedInput pausedInput = connection.pauseRead();
        JoinedDuplexConnection.PausedOutput pausedOutput = connection.pauseWrite();
        connection.close();

        assertEquals(0, input.closeCalls(), "detached read half should stay caller-owned");
        assertEquals(0, output.closeCalls(), "detached write half should stay caller-owned");

        pausedInput.current().close();
        pausedOutput.current().close();
        assertEquals(1, input.closeCalls());
        assertEquals(1, output.closeCalls());
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

    private static final class RecordingReadHalf implements ReadHalf {
        private final ByteArrayInputStream input;
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;
        private int closeReadCalls;
        private java.time.Instant lastReadDeadline;
        private int readDeadlineSetCalls;
        private int readDeadlineClearCalls;

        private RecordingReadHalf(byte[] data, SocketAddress localAddress, SocketAddress remoteAddress) {
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
        public void setReadDeadline(java.time.Instant deadline) {
            lastReadDeadline = deadline;
            if (deadline == null) {
                readDeadlineClearCalls++;
            } else {
                readDeadlineSetCalls++;
            }
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

        java.time.Instant lastReadDeadline() {
            return lastReadDeadline;
        }

        int readDeadlineSetCalls() {
            return readDeadlineSetCalls;
        }

        int readDeadlineClearCalls() {
            return readDeadlineClearCalls;
        }
    }

    private static final class InvalidProgressReadHalf implements ReadHalf {
        private final int read;

        private InvalidProgressReadHalf(int read) {
            this.read = read;
        }

        @Override
        public int read(byte[] dst, int offset, int length) {
            return read;
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void setReadDeadline(Instant deadline) {
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

    private static final class RecordingBidiStream implements ZmuxNativeStream {
        private final ByteArrayInputStream input;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;
        private int closeReadCalls;
        private int closeWriteCalls;

        private RecordingBidiStream(byte[] inputBytes, SocketAddress localAddress, SocketAddress remoteAddress) {
            this.input = new ByteArrayInputStream(inputBytes);
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
        }

        @Override
        public int read(byte[] dst, int offset, int length) {
            return input.read(dst, offset, length);
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
        public void closeRead() {
            closeReadCalls++;
        }

        @Override
        public void cancelRead(long code) {
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
        public void setDeadline(java.time.Instant deadline) {
        }

        @Override
        public void setReadDeadline(java.time.Instant deadline) {
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

        @Override
        public boolean openedLocally() {
            return true;
        }

        @Override
        public boolean bidirectional() {
            return true;
        }

        @Override
        public boolean readClosed() {
            return closeReadCalls > 0;
        }

        @Override
        public boolean writeClosed() {
            return closeWriteCalls > 0;
        }

        byte[] writtenBytes() {
            return output.toByteArray();
        }

        int closeReadCalls() {
            return closeReadCalls;
        }

        int closeWriteCalls() {
            return closeWriteCalls;
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

    private static final class ThrowingInputStream extends InputStream {
        private final IOException closeError;

        private ThrowingInputStream(IOException closeError) {
            this.closeError = closeError;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() throws IOException {
            throw closeError;
        }
    }

    private static final class ThrowingOutputStream extends OutputStream {
        private final IOException closeError;

        private ThrowingOutputStream(IOException closeError) {
            this.closeError = closeError;
        }

        @Override
        public void write(int b) {
        }

        @Override
        public void close() throws IOException {
            throw closeError;
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

    private static final class RecordingWriteHalf implements WriteHalf {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final SocketAddress localAddress;
        private final SocketAddress remoteAddress;
        private final GatheringByteChannel gatheringOutput;
        private int closeWriteCalls;
        private java.time.Instant lastWriteDeadline;
        private int writeDeadlineSetCalls;
        private int writeDeadlineClearCalls;

        private RecordingWriteHalf(SocketAddress localAddress,
                                   SocketAddress remoteAddress,
                                   GatheringByteChannel gatheringOutput) {
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
            this.gatheringOutput = gatheringOutput;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
            output.write(src, offset, length);
        }

        @Override
        public void closeWrite() {
            closeWriteCalls++;
        }

        @Override
        public void setWriteDeadline(java.time.Instant deadline) {
            lastWriteDeadline = deadline;
            if (deadline == null) {
                writeDeadlineClearCalls++;
            } else {
                writeDeadlineSetCalls++;
            }
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

        byte[] writtenBytes() {
            return output.toByteArray();
        }

        int closeWriteCalls() {
            return closeWriteCalls;
        }

        java.time.Instant lastWriteDeadline() {
            return lastWriteDeadline;
        }

        int writeDeadlineSetCalls() {
            return writeDeadlineSetCalls;
        }

        int writeDeadlineClearCalls() {
            return writeDeadlineClearCalls;
        }
    }

    private static final class BlockingDeadlineReadHalf implements ReadHalf {
        private final CountDownLatch setStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSet = new CountDownLatch(1);
        private final List<Instant> deadlines = new ArrayList<>();

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            synchronized (deadlines) {
                deadlines.add(deadline);
            }
            setStarted.countDown();
            try {
                releaseSet.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
        }

        boolean awaitFirstSet() throws InterruptedException {
            return setStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseSet() {
            releaseSet.countDown();
        }

        List<Instant> snapshotDeadlines() {
            synchronized (deadlines) {
                return new ArrayList<>(deadlines);
            }
        }
    }

    private static final class BlockingDeadlineWriteHalf implements WriteHalf {
        private final CountDownLatch setStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSet = new CountDownLatch(1);
        private final List<Instant> deadlines = new ArrayList<>();

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            synchronized (deadlines) {
                deadlines.add(deadline);
            }
            setStarted.countDown();
            try {
                releaseSet.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
        }

        boolean awaitFirstSet() throws InterruptedException {
            return setStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseSet() {
            releaseSet.countDown();
        }

        List<Instant> snapshotDeadlines() {
            synchronized (deadlines) {
                return new ArrayList<>(deadlines);
            }
        }
    }

    private static final class BlockingReadHalf implements ReadHalf {
        private final CountDownLatch readStarted = new CountDownLatch(1);
        private final CountDownLatch releaseRead = new CountDownLatch(1);
        private final byte value;

        private BlockingReadHalf(byte value) {
            this.value = value;
        }

        @Override
        public int read(byte[] dst, int offset, int length) throws IOException {
            readStarted.countDown();
            try {
                releaseRead.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            if (length == 0) {
                return 0;
            }
            dst[offset] = value;
            return 1;
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }

        boolean awaitReadStarted() throws InterruptedException {
            return readStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseRead() {
            releaseRead.countDown();
        }
    }

    private static final class BlockingWriteHalf implements WriteHalf {
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public void write(byte[] src, int offset, int length) throws IOException {
            writeStarted.countDown();
            try {
                releaseWrite.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            output.write(src, offset, length);
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        boolean awaitWriteStarted() throws InterruptedException {
            return writeStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseWrite() {
            releaseWrite.countDown();
        }

        byte[] writtenBytes() {
            return output.toByteArray();
        }
    }

    private static final class BlockingCloseReadHalf implements ReadHalf {
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);
        private final AtomicInteger closeStartCount = new AtomicInteger();

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void closeRead() throws IOException {
            closeStartCount.incrementAndGet();
            closeStarted.countDown();
            try {
                releaseClose.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }

        boolean awaitCloseStarted() throws InterruptedException {
            return closeStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseClose() {
            releaseClose.countDown();
        }

        int closeStartCount() {
            return closeStartCount.get();
        }
    }

    private static final class BlockingCloseWriteHalf implements WriteHalf {
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);
        private final AtomicInteger closeStartCount = new AtomicInteger();

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public void closeWrite() throws IOException {
            closeStartCount.incrementAndGet();
            closeStarted.countDown();
            try {
                releaseClose.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        boolean awaitCloseStarted() throws InterruptedException {
            return closeStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseClose() {
            releaseClose.countDown();
        }

        int closeStartCount() {
            return closeStartCount.get();
        }
    }
}
