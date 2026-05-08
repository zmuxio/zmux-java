package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class EstablishmentFailureTest {
    private static FrameCodec.Frame readFrame(BufferedInputStream input, Socket socket, int timeoutMillis) throws Exception {
        socket.setSoTimeout(timeoutMillis);
        try {
            return FrameCodec.readFrame(input, Settings.defaults().limits());
        } catch (SocketTimeoutException e) {
            throw new AssertionError("timed out waiting for frame", e);
        }
    }

    private static byte[] prefaceBytes(Role role) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.writePreface(output, new Preface(
                Protocol.PREFACE_VERSION,
                role,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        ));
        return output.toByteArray();
    }

    private static OutputStream failingOutput(String message) {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException(message);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException(message);
            }
        };
    }

    private static void assertConstructorClosesTransportOnce(String name, EstablishmentOpen open) {
        CountingDuplexConnection connection = new CountingDuplexConnection();

        IOException error = assertThrows(
                IOException.class,
                () -> open.open(connection),
                name + " should fail when the peer preface is missing"
        );

        assertEquals(1, connection.closeCount(), name + " should close the transport exactly once");
        assertEquals("read preface", ZmuxErrors.operation(error), name + " operation mismatch");
        assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error), name + " source mismatch");
    }

    @Test
    void sameRoleConflictEmitsFatalCloseDuringEstablishment() throws Exception {
        ServerSocket listener = new ServerSocket(0);
        Socket peerSocket = new Socket("127.0.0.1", listener.getLocalPort());
        Socket sessionSocket = listener.accept();
        listener.close();

        AtomicReference<Throwable> openError = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        DeadlineSocketDuplexConnection connection = new DeadlineSocketDuplexConnection(sessionSocket);

        Thread openThread = new Thread(() -> {
            try {
                Zmux.client(connection, ZmuxConfig.builder().build());
                openError.set(new AssertionError("establishment should fail on same-role conflict"));
            } catch (Throwable t) {
                openError.set(t);
            } finally {
                finished.countDown();
            }
        }, "establishment-role-conflict");
        openThread.start();

        IOException closeError = null;
        try (Socket ignoredPeerSocket = peerSocket;
             BufferedInputStream input = new BufferedInputStream(peerSocket.getInputStream());
             BufferedOutputStream output = new BufferedOutputStream(peerSocket.getOutputStream())) {
            FrameCodec.writePreface(output, new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    0L,
                    Settings.defaults()
            ));
            output.flush();

            Preface localPreface = FrameCodec.readPreface(input);
            assertEquals(Role.INITIATOR, localPreface.role(), "client should advertise initiator role");

            FrameCodec.Frame close = readFrame(input, peerSocket, 1000);
            assertEquals(FrameType.CLOSE, close.type(), "establishment failure should emit CLOSE");
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.ROLE_CONFLICT.code(), payload.code(), "same-role conflict should surface ROLE_CONFLICT");
        } catch (IOException e) {
            closeError = e;
        }

        finished.await();
        openThread.join(1000L);
        assertFalse(openThread.isAlive(), "establishment failure thread should finish");
        if (closeError != null) {
            throw closeError;
        }
        Throwable error = openError.get();
        assertNotNull(error, "establishment should fail");
        ZmuxException zmuxError = assertInstanceOf(ZmuxException.class, error);
        assertEquals(ErrorCode.ROLE_CONFLICT.code(), zmuxError.code(), "same-role conflict should propagate ROLE_CONFLICT locally");
    }

    @Test
    void prefaceWriteFailureSurfacesStructuredTransportError() throws Exception {
        String message = "synthetic preface write failure";
        BasicDuplexConnection connection = new BasicDuplexConnection(
                new ByteArrayInputStream(prefaceBytes(Role.RESPONDER)),
                failingOutput(message)
        );

        IOException error = assertThrows(
                IOException.class,
                () -> Zmux.client(connection, ZmuxConfig.builder().build())
        );

        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(error, -1L), "preface write code mismatch");
        assertEquals("write preface", ZmuxErrors.operation(error), "preface write operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error), "preface write scope mismatch");
        assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error), "preface write source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error), "preface write direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(error),
                "preface write termination mismatch"
        );
        assertEquals(message, error.getCause().getMessage(), "preface write cause should be retained");
    }

    @Test
    void prefaceReadFailureSurfacesStructuredTransportError() {
        BasicDuplexConnection connection = new BasicDuplexConnection(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        );

        IOException error = assertThrows(
                IOException.class,
                () -> Zmux.client(connection, ZmuxConfig.builder().build())
        );

        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(error, -1L), "preface read code mismatch");
        assertEquals("read preface", ZmuxErrors.operation(error), "preface read operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error), "preface read scope mismatch");
        assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error), "preface read source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error), "preface read direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(error),
                "preface read termination mismatch"
        );
    }

    @Test
    void nativeConstructorsCloseTransportOnceOnEstablishmentFailure() {
        assertAll(
                () -> assertConstructorClosesTransportOnce("Zmux.open", connection -> Zmux.open(connection, ZmuxConfig.builder().build())),
                () -> assertConstructorClosesTransportOnce("Zmux.client", connection -> Zmux.client(connection, ZmuxConfig.builder().build())),
                () -> assertConstructorClosesTransportOnce("Zmux.server", connection -> Zmux.server(connection, ZmuxConfig.builder().build()))
        );
    }

    @Test
    void stableConstructorsCloseTransportOnceOnEstablishmentFailure() {
        assertAll(
                () -> assertConstructorClosesTransportOnce("Zmux.openSession", connection -> Zmux.openSession(connection, ZmuxConfig.builder().build())),
                () -> assertConstructorClosesTransportOnce("Zmux.clientSession", connection -> Zmux.clientSession(connection, ZmuxConfig.builder().build())),
                () -> assertConstructorClosesTransportOnce("Zmux.serverSession", connection -> Zmux.serverSession(connection, ZmuxConfig.builder().build()))
        );
    }

    private interface EstablishmentOpen {
        void open(DuplexConnection connection) throws IOException;
    }

    private static final class DeadlineSocketDuplexConnection implements DuplexConnection {
        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;

        private DeadlineSocketDuplexConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
        }

        @Override
        public InputStream input() {
            return this.input;
        }

        @Override
        public OutputStream output() {
            return this.output;
        }

        @Override
        public java.net.SocketAddress localAddress() {
            return this.socket.getLocalSocketAddress();
        }

        @Override
        public java.net.SocketAddress remoteAddress() {
            return this.socket.getRemoteSocketAddress();
        }

        @Override
        public boolean supportsWriteDeadline() {
            return true;
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        @Override
        public void close() throws IOException {
            this.socket.close();
        }
    }

    private static final class CountingDuplexConnection implements DuplexConnection {
        private final AtomicInteger closeCount = new AtomicInteger();
        private final InputStream input = new ByteArrayInputStream(new byte[0]);
        private final OutputStream output = new ByteArrayOutputStream();

        @Override
        public InputStream input() {
            return input;
        }

        @Override
        public OutputStream output() {
            return output;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        int closeCount() {
            return closeCount.get();
        }
    }
}
