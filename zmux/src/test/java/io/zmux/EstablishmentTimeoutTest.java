package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.transport.DuplexConnection;
import io.zmux.transport.ZmuxConnections;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class EstablishmentTimeoutTest {
    private static final long SLOW_PEER_DELAY_MILLIS = 1_300L;

    private static byte[] initiatorPrefaceBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.writePreface(output, new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        ));
        return output.toByteArray();
    }

    /**
     * Runs a server session over a real TCP socket against a raw peer that sends its preface only after
     * {@code peerDelayMillis}. Returns the state of the established session, observed while both sockets are still
     * open (closing them first would race the session's reader into FAILED), then closes the session. On failure,
     * rethrows the establishment error after checking that the peer saw the full server preface followed by
     * CLOSE(INTERNAL).
     */
    private static SessionState serverWithSlowPeer(ZmuxConfig config, long peerDelayMillis) throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             Socket peer = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            AtomicReference<Throwable> peerError = new AtomicReference<>();
            Thread peerThread = new Thread(() -> {
                try {
                    Thread.sleep(peerDelayMillis);
                    OutputStream output = peer.getOutputStream();
                    output.write(initiatorPrefaceBytes());
                    output.flush();
                } catch (Throwable error) {
                    peerError.set(error);
                }
            }, "establishment-timeout-peer");
            peerThread.setDaemon(true);
            peerThread.start();

            BufferedInputStream peerInput = new BufferedInputStream(peer.getInputStream());
            ZmuxNativeSession session;
            try {
                session = Zmux.server(new WriteDeadlineSocketConnection(accepted), config);
            } catch (IOException establishmentError) {
                peer.setSoTimeout(2_000);
                FrameCodec.readPreface(peerInput);
                FrameCodec.Frame close = FrameCodec.readFrame(peerInput, Settings.defaults().limits());
                assertEquals(FrameType.CLOSE, close.type(), "failed establishment should send a best-effort CLOSE");
                assertEquals(
                        ErrorCode.INTERNAL.code(),
                        FrameCodec.parseErrorPayload(close.payload()).code(),
                        "establishment timeout CLOSE code"
                );
                throw establishmentError;
            }
            try {
                peerThread.join(5_000L);
                assertNull(peerError.get(), "slow peer should send its preface");
                FrameCodec.readPreface(peerInput);
                return session.state();
            } finally {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
            }
        }
    }

    @Test
    void defaultEstablishmentTimeoutIsTenSeconds() {
        assertEquals(Duration.ofSeconds(10), ZmuxConfig.DEFAULT_ESTABLISHMENT_TIMEOUT);
        assertEquals(ZmuxConfig.DEFAULT_ESTABLISHMENT_TIMEOUT, ZmuxConfig.defaults().establishmentTimeout());
        assertEquals(
                ZmuxConfig.DEFAULT_ESTABLISHMENT_TIMEOUT,
                ZmuxConfig.builder().establishmentTimeout(Duration.ZERO).build().establishmentTimeout(),
                "zero selects the default"
        );
        assertEquals(
                ZmuxConfig.DEFAULT_ESTABLISHMENT_TIMEOUT,
                ZmuxConfig.builder().establishmentTimeout(null).build().establishmentTimeout(),
                "null selects the default"
        );
        assertFalse(ZmuxConfig.defaults().disableEstablishmentTimeout());

        ZmuxConfig configured = ZmuxConfig.builder().establishmentTimeout(Duration.ofMillis(1_500)).build();
        assertEquals(Duration.ofMillis(1_500), configured.establishmentTimeout());
        assertEquals(configured, configured.toBuilder().build(), "toBuilder should round-trip the establishment timeout");

        ZmuxConfig disabled = ZmuxConfig.builder()
                .establishmentTimeout(Duration.ofSeconds(3))
                .disableEstablishmentTimeout()
                .build();
        assertTrue(disabled.disableEstablishmentTimeout());
        assertEquals(Duration.ZERO, disabled.establishmentTimeout(), "a disabled bound reports zero");
        assertEquals(disabled, disabled.toBuilder().build(), "toBuilder should round-trip the disabled bound");
        assertNotEquals(configured, disabled);

        IllegalArgumentException negative = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().establishmentTimeout(Duration.ofMillis(-1)).build()
        );
        assertEquals("zmux config establishmentTimeout must be >= 0", negative.getMessage());
    }

    @Test
    void defaultTimeoutToleratesPeerPrefaceSlowerThanOneSecond() throws Exception {
        assertEquals(
                SessionState.READY,
                serverWithSlowPeer(ZmuxConfig.defaults(), SLOW_PEER_DELAY_MILLIS),
                "a compliant but slow peer should establish"
        );
    }

    @Test
    void configuredTimeoutToleratesSlowPeerWithinBound() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder().establishmentTimeout(Duration.ofSeconds(3)).build();
        assertEquals(
                SessionState.READY,
                serverWithSlowPeer(config, SLOW_PEER_DELAY_MILLIS),
                "peer within the configured bound should establish"
        );
    }

    @Test
    void disabledTimeoutWaitsForSlowPeer() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder().disableEstablishmentTimeout().build();
        assertEquals(
                SessionState.READY,
                serverWithSlowPeer(config, SLOW_PEER_DELAY_MILLIS),
                "a disabled bound should wait for the peer preface"
        );
    }

    @Test
    void shortConfiguredTimeoutFailsWithInternalAndClose() {
        ZmuxConfig config = ZmuxConfig.builder().establishmentTimeout(Duration.ofMillis(300)).build();
        long startedAt = System.nanoTime();
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> serverWithSlowPeer(config, SLOW_PEER_DELAY_MILLIS)
        );
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "establishment timeout should be INTERNAL");
        assertTrue(
                error.getMessage().contains("peer preface read stalled during establishment"),
                "unexpected establishment failure: " + error.getMessage()
        );
        assertTrue(elapsedMillis < 1_200L, "configured bound should fire well before the peer preface, took " + elapsedMillis + "ms");
    }

    /**
     * Socket transport that also advertises (no-op) write-deadline support, so a failed establishment emits its
     * bounded fatal CLOSE (the session skips that CLOSE when it cannot bound the write).
     */
    private static final class WriteDeadlineSocketConnection implements DuplexConnection {
        private final DuplexConnection delegate;

        WriteDeadlineSocketConnection(Socket socket) throws IOException {
            this.delegate = ZmuxConnections.of(socket);
        }

        @Override
        public InputStream input() {
            return delegate.input();
        }

        @Override
        public OutputStream output() {
            return delegate.output();
        }

        @Override
        public boolean supportsReadDeadline() {
            return delegate.supportsReadDeadline();
        }

        @Override
        public void setReadDeadline(Instant deadline) throws IOException {
            delegate.setReadDeadline(deadline);
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
            delegate.close();
        }
    }
}
