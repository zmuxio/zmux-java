package io.zmux.adapter.quic.netty;

import io.zmux.Zmux;
import io.zmux.ZmuxRecvStream;
import io.zmux.ZmuxSendStream;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.zmux.adapter.quic.netty.NettyQuicTestSupport.async;
import static io.zmux.adapter.quic.netty.NettyQuicTestSupport.await;
import static io.zmux.adapter.quic.netty.NettyQuicTestSupport.readAll;
import static io.zmux.adapter.quic.netty.NettyQuicTestSupport.utf8;
import static org.junit.jupiter.api.Assertions.*;

final class SessionPolymorphismTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void sameUpperLayerCodeWorksWithNativeAndNettyQuicSessions() throws Exception {
        try (CommonPair pair = NativePair.open()) {
            exerciseCommonSessionCode(pair.client(), pair.server());
        }

        try (CommonPair pair = AdapterPair.open()) {
            exerciseCommonSessionCode(pair.client(), pair.server());
        }
    }

    private static void exerciseCommonSessionCode(ZmuxSession client, ZmuxSession server) throws Exception {
        assertNotNull(client);
        assertNotNull(server);
        assertFalse(client.isClosed());
        assertFalse(server.isClosed());
        assertEquals(client.state(), client.stats().state());
        assertEquals(server.state(), server.stats().state());

        CompletableFuture<ZmuxStream> acceptedBidiFuture = async(() -> server.acceptStream(TIMEOUT));
        try (ZmuxStream outbound = client.openStream()) {
            outbound.writeFinal(utf8("client-to-server"));
            try (ZmuxStream inbound = await(acceptedBidiFuture)) {
                assertArrayEquals(utf8("client-to-server"), readAll(inbound));

                inbound.writeFinal(utf8("server-to-client"));
                assertArrayEquals(utf8("server-to-client"), readAll(outbound));
            }
        }

        CompletableFuture<ZmuxStream> acceptedOpenAndSendFuture = async(() -> server.acceptStream(TIMEOUT));
        byte[] framed = utf8("xxopen-and-sendyy");
        try (ZmuxStream outbound = client.openAndSend(framed, 2, "open-and-send".length());
             ZmuxStream inbound = await(acceptedOpenAndSendFuture)) {
            outbound.closeWrite();
            assertArrayEquals(utf8("open-and-send"), readAll(inbound));
        }

        CompletableFuture<ZmuxRecvStream> acceptedUniFuture = async(() -> client.acceptUniStream(TIMEOUT));
        try (ZmuxSendStream outbound = server.openUniAndSend(ByteBuffer.wrap(utf8("server-uni")));
             ZmuxRecvStream inbound = await(acceptedUniFuture)) {
            assertArrayEquals(utf8("server-uni"), readAll(inbound));
        }

        client.close();
        server.close();
        assertTrue(client.awaitTermination(TIMEOUT));
        assertTrue(server.awaitTermination(TIMEOUT));
        assertTrue(client.isClosed());
        assertTrue(server.isClosed());
    }

    private interface CommonPair extends AutoCloseable {
        ZmuxSession client();

        ZmuxSession server();

        @Override
        void close() throws Exception;
    }

    private static final class NativePair implements CommonPair {
        private final ZmuxSession client;
        private final ZmuxSession server;

        private NativePair(ZmuxSession client, ZmuxSession server) {
            this.client = client;
            this.server = server;
        }

        private static NativePair open() throws Exception {
            ServerSocket listener = new ServerSocket(0);
            Socket clientSocket = null;
            Socket serverSocket = null;
            try {
                clientSocket = new Socket("127.0.0.1", listener.getLocalPort());
                serverSocket = listener.accept();
            } finally {
                listener.close();
            }

            Socket finalClientSocket = clientSocket;
            Socket finalServerSocket = serverSocket;
            AtomicReference<ZmuxSession> clientRef = new AtomicReference<>();
            AtomicReference<ZmuxSession> serverRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(2);

            Thread clientThread = new Thread(() -> {
                try {
                    clientRef.set(Zmux.clientSession(finalClientSocket));
                } catch (Throwable failure) {
                    errorRef.compareAndSet(null, failure);
                } finally {
                    established.countDown();
                }
            }, "session-polymorphism-native-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.serverSession(finalServerSocket));
                } catch (Throwable failure) {
                    errorRef.compareAndSet(null, failure);
                } finally {
                    established.countDown();
                }
            }, "session-polymorphism-native-server-open");

            clientThread.start();
            serverThread.start();
            if (!established.await(10L, TimeUnit.SECONDS)) {
                closeSuppress(clientRef.get());
                closeSuppress(serverRef.get());
                throw new AssertionError("timed out establishing native zmux sessions");
            }
            rethrow(errorRef.get());
            return new NativePair(clientRef.get(), serverRef.get());
        }

        @Override
        public ZmuxSession client() {
            return client;
        }

        @Override
        public ZmuxSession server() {
            return server;
        }

        @Override
        public void close() throws Exception {
            IOException error = closeSuppress(client);
            IOException serverError = closeSuppress(server);
            if (error != null) {
                throw error;
            }
            if (serverError != null) {
                throw serverError;
            }
        }
    }

    private static final class AdapterPair implements CommonPair {
        private final NettyQuicTestSupport.SessionPair pair;

        private AdapterPair(NettyQuicTestSupport.SessionPair pair) {
            this.pair = pair;
        }

        private static AdapterPair open() throws Exception {
            return new AdapterPair(NettyQuicTestSupport.openPair());
        }

        @Override
        public ZmuxSession client() {
            return pair.client;
        }

        @Override
        public ZmuxSession server() {
            return pair.server;
        }

        @Override
        public void close() throws Exception {
            pair.close();
        }
    }

    private static IOException closeSuppress(ZmuxSession session) {
        if (session == null) {
            return null;
        }
        try {
            session.close();
            return null;
        } catch (IOException error) {
            return error;
        }
    }

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        if (error instanceof Error) {
            throw (Error) error;
        }
        throw new RuntimeException(error);
    }
}
