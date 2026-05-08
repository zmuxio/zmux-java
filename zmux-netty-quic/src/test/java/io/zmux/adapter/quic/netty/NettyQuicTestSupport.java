package io.zmux.adapter.quic.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.zmux.ApplicationError;
import io.zmux.ZmuxRecvStream;
import io.zmux.ZmuxSendStream;
import io.zmux.ZmuxSession;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

final class NettyQuicTestSupport {
    private static final String APPLICATION_PROTOCOL = "zmux-java-test";
    private static final long MAX_DATA = 1 << 20;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    private static final long CLOSE_WAIT_SECONDS = 2L;
    private static final long QUIC_IDLE_TIMEOUT_SECONDS = 30L;
    private static final Object CERTIFICATE_LOCK = new Object();
    private static GeneratedCertificate sharedCertificate;

    private NettyQuicTestSupport() {
    }

    static ChannelHandler quietHandler() {
        return new QuietChannelHandler();
    }

    static SessionPair openPair() throws Exception {
        return openPair(NettyQuicSessionOptions.defaults(), NettyQuicSessionOptions.defaults());
    }

    static SessionPair openPair(NettyQuicSessionOptions clientOptions,
                                NettyQuicSessionOptions serverOptions) throws Exception {
        GeneratedCertificate certificate = sharedCertificate();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel serverDatagram = null;
        Channel clientDatagram = null;
        QuicChannel rawClient = null;
        QuicChannel rawServer = null;
        try {
            QuicSslContext serverSsl = QuicSslContextBuilder
                    .forServer(certificate.key(), null, certificate.cert())
                    .applicationProtocols(APPLICATION_PROTOCOL)
                    .build();
            QuicSslContext clientSsl = QuicSslContextBuilder
                    .forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(APPLICATION_PROTOCOL)
                    .build();

            CompletableFuture<QuicChannel> acceptedServer = new CompletableFuture<>();
            serverDatagram = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(new QuicServerCodecBuilder()
                            .sslContext(serverSsl)
                            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                            .handler(new AcceptedChannelRecorder(acceptedServer))
                            .streamHandler(new QuietChannelHandler())
                            .maxIdleTimeout(QUIC_IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .initialMaxData(MAX_DATA)
                            .initialMaxStreamDataBidirectionalLocal(MAX_DATA)
                            .initialMaxStreamDataBidirectionalRemote(MAX_DATA)
                            .initialMaxStreamDataUnidirectional(MAX_DATA)
                            .initialMaxStreamsBidirectional(64)
                            .initialMaxStreamsUnidirectional(64)
                            .build())
                    .bind(new InetSocketAddress("127.0.0.1", 0))
                    .syncUninterruptibly()
                    .channel();

            clientDatagram = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(new QuicClientCodecBuilder()
                            .sslContext(clientSsl)
                            .maxIdleTimeout(QUIC_IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .initialMaxData(MAX_DATA)
                            .initialMaxStreamDataBidirectionalLocal(MAX_DATA)
                            .initialMaxStreamDataBidirectionalRemote(MAX_DATA)
                            .initialMaxStreamDataUnidirectional(MAX_DATA)
                            .initialMaxStreamsBidirectional(64)
                            .initialMaxStreamsUnidirectional(64)
                            .build())
                    .bind(new InetSocketAddress("127.0.0.1", 0))
                    .syncUninterruptibly()
                    .channel();

            rawClient = NettyQuicSupport.awaitFuture(
                    QuicChannel.newBootstrap(clientDatagram)
                            .handler(new QuietChannelHandler())
                            .streamHandler(new QuietChannelHandler())
                            .remoteAddress((InetSocketAddress) serverDatagram.localAddress())
                            .connect()
            );
            rawServer = await(acceptedServer);

            return new SessionPair(
                    group,
                    certificate,
                    clientDatagram,
                    serverDatagram,
                    rawClient,
                    rawServer,
                    NettyQuic.wrapSession(rawClient, clientOptions),
                    NettyQuic.wrapSession(rawServer, serverOptions)
            );
        } catch (Throwable failure) {
            closeQuietly(rawClient);
            closeQuietly(rawServer);
            closeQuietly(clientDatagram);
            closeQuietly(serverDatagram);
            shutdownGroupQuietly(group);
            throw failure;
        }
    }

    static <T> CompletableFuture<T> async(ThrowingSupplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable failure) {
                future.completeExceptionally(failure);
            }
        }, "zmux-netty-quic-test");
        thread.setDaemon(true);
        thread.start();
        return future;
    }

    static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException execution) {
            Throwable cause = execution.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        } catch (TimeoutException timeout) {
            throw new AssertionError("timed out waiting for asynchronous result", timeout);
        }
    }

    static byte[] readAll(ZmuxRecvStream stream) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        for (; ; ) {
            int read = stream.read(buffer);
            if (read < 0) {
                return output.toByteArray();
            }
            output.write(buffer, 0, read);
        }
    }

    static byte[] readExactly(ZmuxRecvStream stream, int length) throws IOException {
        byte[] buffer = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = stream.read(buffer, offset, length - offset);
            if (read < 0) {
                throw new EOFException("expected " + length + " bytes, got " + offset);
            }
            offset += read;
        }
        return buffer;
    }

    static IOException waitForWriteFailure(ZmuxSendStream stream, byte[] payload, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                stream.write(payload);
            } catch (IOException error) {
                return error;
            }
            Thread.sleep(10L);
        }
        return null;
    }

    static ApplicationError findApplicationError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ApplicationError) {
                return (ApplicationError) current;
            }
            current = current.getCause();
        }
        return null;
    }

    static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void closeQuietly(QuicChannel channel) {
        if (channel != null) {
            channel.close().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static void closeQuietly(Channel channel) {
        if (channel != null) {
            channel.close().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static GeneratedCertificate newCertificate() throws Exception {
        SecureRandom random = new SecureRandom();
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"), random);
        KeyPair keyPair = generator.generateKeyPair();

        Instant now = Instant.now();
        Date notBefore = Date.from(now.minus(Duration.ofMinutes(1)));
        Date notAfter = Date.from(now.plus(Duration.ofDays(1)));
        BigInteger serial = new BigInteger(64, random);
        if (serial.signum() <= 0) {
            serial = BigInteger.ONE;
        }

        X500Name subject = new X500Name("CN=localhost");
        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(subject, serial, notBefore, notAfter, subject, keyPair.getPublic());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withECDSA").build(keyPair.getPrivate());
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(builder.build(signer));
        certificate.checkValidity(new Date());
        certificate.verify(keyPair.getPublic());
        return new GeneratedCertificate(keyPair.getPrivate(), certificate);
    }

    private static GeneratedCertificate sharedCertificate() throws Exception {
        GeneratedCertificate certificate = sharedCertificate;
        if (certificate != null) {
            return certificate;
        }
        synchronized (CERTIFICATE_LOCK) {
            if (sharedCertificate == null) {
                sharedCertificate = newCertificate();
            }
            return sharedCertificate;
        }
    }

    private static void shutdownGroupQuietly(EventLoopGroup group) {
        if (group == null) {
            return;
        }
        group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
                .awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
        group.terminationFuture().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    static final class SessionPair implements AutoCloseable {
        final EventLoopGroup group;
        final GeneratedCertificate certificate;
        final Channel clientDatagram;
        final Channel serverDatagram;
        final QuicChannel rawClient;
        final QuicChannel rawServer;
        final ZmuxSession client;
        final ZmuxSession server;

        SessionPair(EventLoopGroup group,
                    GeneratedCertificate certificate,
                    Channel clientDatagram,
                    Channel serverDatagram,
                    QuicChannel rawClient,
                    QuicChannel rawServer,
                    ZmuxSession client,
                    ZmuxSession server) {
            this.group = group;
            this.certificate = certificate;
            this.clientDatagram = clientDatagram;
            this.serverDatagram = serverDatagram;
            this.rawClient = rawClient;
            this.rawServer = rawServer;
            this.client = client;
            this.server = server;
        }

        private static void closeSuppress(ZmuxSession session) {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception ignored) {
            }
        }

        private static void closeSuppress(QuicChannel channel) {
            try {
                if (channel != null) {
                    channel.close().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
                }
            } catch (Exception ignored) {
            }
        }

        private static void closeSuppress(Channel channel) {
            try {
                if (channel != null) {
                    channel.close().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
                }
            } catch (Exception ignored) {
            }
        }

        @Override
        public void close() {
            closeSuppress(client);
            closeSuppress(server);
            closeSuppress(rawClient);
            closeSuppress(rawServer);
            closeSuppress(clientDatagram);
            closeSuppress(serverDatagram);
            shutdownGroupQuietly(group);
        }
    }

    private static final class GeneratedCertificate {
        private final PrivateKey key;
        private final X509Certificate cert;

        private GeneratedCertificate(PrivateKey key, X509Certificate cert) {
            this.key = key;
            this.cert = cert;
        }

        private PrivateKey key() {
            return this.key;
        }

        private X509Certificate cert() {
            return this.cert;
        }
    }

    private static final class AcceptedChannelRecorder extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<QuicChannel> accepted;

        private AcceptedChannelRecorder(CompletableFuture<QuicChannel> accepted) {
            this.accepted = accepted;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            accepted.complete((QuicChannel) ctx.channel());
            super.channelActive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            accepted.completeExceptionally(cause);
            ctx.close();
        }
    }

    private static final class QuietChannelHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
