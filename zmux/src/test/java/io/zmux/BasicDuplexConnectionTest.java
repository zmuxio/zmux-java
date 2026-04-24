package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class BasicDuplexConnectionTest {
    private static InputStream failingInput(IOException failure, AtomicBoolean closed) {
        return new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() throws IOException {
                closed.set(true);
                throw failure;
            }
        };
    }

    private static OutputStream failingOutput(IOException failure, AtomicBoolean closed) {
        return new OutputStream() {
            @Override
            public void write(int b) {
            }

            @Override
            public void close() throws IOException {
                closed.set(true);
                throw failure;
            }
        };
    }

    private static InputStream trackingInput(AtomicBoolean closed) {
        return new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() {
                closed.set(true);
            }
        };
    }

    private static OutputStream trackingOutput(AtomicBoolean closed) {
        return new OutputStream() {
            @Override
            public void write(int b) {
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
    }

    @Test
    void closeAggregatesFailuresWithSuppressedExceptions() {
        IOException closerFailure = new IOException("closer");
        IOException inputFailure = new IOException("input");
        IOException outputFailure = new IOException("output");
        AtomicBoolean closerClosed = new AtomicBoolean();
        AtomicBoolean inputClosed = new AtomicBoolean();
        AtomicBoolean outputClosed = new AtomicBoolean();

        BasicDuplexConnection connection = new BasicDuplexConnection(
                failingInput(inputFailure, inputClosed),
                failingOutput(outputFailure, outputClosed),
                () -> {
                    closerClosed.set(true);
                    throw closerFailure;
                },
                null,
                null
        );

        IOException error = assertThrows(IOException.class, connection::close);

        assertSame(closerFailure, error);
        assertEquals(2, error.getSuppressed().length);
        assertSame(inputFailure, error.getSuppressed()[0]);
        assertSame(outputFailure, error.getSuppressed()[1]);
        assertTrue(closerClosed.get(), "closer should be attempted");
        assertTrue(inputClosed.get(), "input should still be closed after closer failure");
        assertTrue(outputClosed.get(), "output should still be closed after earlier failures");
    }

    @Test
    void closeWrapsNonIoCloserFailureButStillClosesStreams() {
        RuntimeException closerFailure = new RuntimeException("boom");
        AtomicBoolean inputClosed = new AtomicBoolean();
        AtomicBoolean outputClosed = new AtomicBoolean();

        BasicDuplexConnection connection = new BasicDuplexConnection(
                trackingInput(inputClosed),
                trackingOutput(outputClosed),
                () -> {
                    throw closerFailure;
                },
                null,
                null
        );

        IOException error = assertThrows(IOException.class, connection::close);

        assertEquals("zmux: failed to close duplex connection closer", error.getMessage());
        assertSame(closerFailure, error.getCause());
        assertInstanceOf(ZmuxException.class, error);
        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(error, -1));
        assertEquals(ErrorCode.INTERNAL, ZmuxErrors.code(error));
        assertTrue(ZmuxErrors.isCode(error, ErrorCode.INTERNAL));
        assertEquals("close", ZmuxErrors.operation(error));
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error));
        assertEquals(ZmuxErrorSource.LOCAL, ZmuxErrors.source(error));
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error));
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, ZmuxErrors.terminationKind(error));
        assertTrue(inputClosed.get(), "input should still be closed after non-io closer failure");
        assertTrue(outputClosed.get(), "output should still be closed after non-io closer failure");
    }

    @Test
    void closeDoesNotDoubleCloseWhenCloserIsTheInputStream() throws Exception {
        AtomicInteger inputCloseCount = new AtomicInteger();
        AtomicBoolean outputClosed = new AtomicBoolean();
        InputStream sharedInput = new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() {
                inputCloseCount.incrementAndGet();
            }
        };

        BasicDuplexConnection connection = new BasicDuplexConnection(
                sharedInput,
                trackingOutput(outputClosed),
                sharedInput,
                null,
                null
        );

        connection.close();

        assertEquals(1, inputCloseCount.get(), "shared closer/input should only be closed once");
        assertTrue(outputClosed.get(), "output should still be closed");
    }

    @Test
    void closeClosesDistinctGatheringOutput() throws Exception {
        AtomicBoolean inputClosed = new AtomicBoolean();
        AtomicBoolean outputClosed = new AtomicBoolean();
        RecordingGatheringChannel gatheringOutput = new RecordingGatheringChannel(null);

        BasicDuplexConnection connection = new BasicDuplexConnection(
                trackingInput(inputClosed),
                trackingOutput(outputClosed),
                null,
                null,
                null,
                gatheringOutput
        );

        connection.close();

        assertTrue(inputClosed.get(), "input should be closed");
        assertTrue(outputClosed.get(), "output should be closed");
        assertEquals(1, gatheringOutput.closeCount(), "gathering output should be closed exactly once");
        assertFalse(gatheringOutput.isOpen(), "gathering output should no longer be open");
    }

    @Test
    void closeAggregatesGatheringOutputFailureAfterStreamFailures() {
        IOException outputFailure = new IOException("output");
        IOException gatheringFailure = new IOException("gathering");
        RecordingGatheringChannel gatheringOutput = new RecordingGatheringChannel(gatheringFailure);

        BasicDuplexConnection connection = new BasicDuplexConnection(
                trackingInput(new AtomicBoolean()),
                failingOutput(outputFailure, new AtomicBoolean()),
                null,
                null,
                null,
                gatheringOutput
        );

        IOException error = assertThrows(IOException.class, connection::close);

        assertSame(outputFailure, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(gatheringFailure, error.getSuppressed()[0]);
        assertEquals(1, gatheringOutput.closeCount(), "gathering output should be attempted after output failure");
    }

    @Test
    void closeDoesNotDoubleCloseWhenGatheringOutputIsTheCloser() throws Exception {
        AtomicBoolean inputClosed = new AtomicBoolean();
        AtomicBoolean outputClosed = new AtomicBoolean();
        RecordingGatheringChannel gatheringOutput = new RecordingGatheringChannel(null);

        BasicDuplexConnection connection = new BasicDuplexConnection(
                trackingInput(inputClosed),
                trackingOutput(outputClosed),
                gatheringOutput,
                null,
                null,
                gatheringOutput
        );

        connection.close();

        assertEquals(1, gatheringOutput.closeCount(), "shared closer/gathering output should only be closed once");
        assertTrue(inputClosed.get(), "input should still be closed");
        assertTrue(outputClosed.get(), "output should still be closed");
    }

    @Test
    void builderCarriesOptionalComponents() {
        AtomicBoolean inputClosed = new AtomicBoolean();
        AtomicBoolean outputClosed = new AtomicBoolean();
        RecordingGatheringChannel gatheringOutput = new RecordingGatheringChannel(null);

        BasicDuplexConnection connection = BasicDuplexConnection.builder(
                        trackingInput(inputClosed),
                        trackingOutput(outputClosed)
                )
                .closer(gatheringOutput)
                .addresses(null, null)
                .gatheringOutput(gatheringOutput)
                .build();

        assertSame(gatheringOutput, connection.gatheringOutput());
    }

    private static final class RecordingGatheringChannel implements GatheringByteChannel {
        private final IOException closeFailure;
        private final AtomicInteger closeCount = new AtomicInteger();
        private volatile boolean open = true;

        private RecordingGatheringChannel(IOException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public int write(ByteBuffer src) {
            int written = src.remaining();
            src.position(src.limit());
            return written;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            long written = 0L;
            int end = offset + length;
            for (int i = offset; i < end; i++) {
                ByteBuffer src = srcs[i];
                written += src.remaining();
                src.position(src.limit());
            }
            return written;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
            open = false;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private int closeCount() {
            return closeCount.get();
        }
    }
}
