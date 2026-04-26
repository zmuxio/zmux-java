package io.zmux.adapter.quic.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.zmux.OpenOptions;
import io.zmux.StreamMetadata;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class GoQuicInteropSmokeTest {
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(30);
    private static final String APPLICATION_PROTOCOL = "zmux-java-go-quic-interop";
    private static final long MAX_DATA = 1 << 20;
    private static final long QUIC_IDLE_TIMEOUT_SECONDS = 30L;
    private static final long CLOSE_WAIT_SECONDS = 2L;

    private static boolean commandExists(String command) {
        String checker = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win")
                ? "where"
                : "which";
        try {
            Process process = new ProcessBuilder(checker, command)
                    .redirectErrorStream(true)
                    .start();
            return process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static Path requireGoRoot() {
        assumeTrue("1".equals(System.getenv("ZMUX_INTEROP")), "set ZMUX_INTEROP=1 to run Java/Go interop smoke");
        String goRootEnv = System.getenv("ZMUX_GO_ROOT");
        assumeTrue(goRootEnv != null && !goRootEnv.trim().isEmpty(), "set ZMUX_GO_ROOT to the Go implementation root");
        Path goRoot = Paths.get(goRootEnv);
        assumeTrue(Files.isDirectory(goRoot), "Go implementation root not found: " + goRoot);
        assumeTrue(commandExists("go"), "go executable not found");
        return goRoot;
    }

    private static String goQuicMod(Path goRoot) {
        String goPath = goRoot.toAbsolutePath().toString().replace('\\', '/');
        String adapterPath = goRoot.resolve("adapter").resolve("quicmux").toAbsolutePath().toString().replace('\\', '/');
        return String.format(
                "module zmux_java_go_quic_interop_smoke\n"
                        + "\n"
                        + "go 1.25\n"
                        + "\n"
                        + "require (\n"
                        + "    github.com/quic-go/quic-go v0.59.0\n"
                        + "    github.com/zmuxio/zmux-go v1.0.8\n"
                        + "    github.com/zmuxio/zmux-go/adapter/quicmux v0.0.0\n"
                        + ")\n"
                        + "\n"
                        + "replace github.com/zmuxio/zmux-go => %s\n"
                        + "replace github.com/zmuxio/zmux-go/adapter/quicmux => %s\n",
                goPath,
                adapterPath
        );
    }

    private static String firstOutputLine(BufferedReader output) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new RuntimeException(error);
            }
        });
        return line.get(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void deleteTree(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                deleteTreeOnce(root);
                return;
            } catch (IOException ignored) {
                if (!sleepBeforeCleanupRetry(attempt)) {
                    break;
                }
            }
        }
        markDeleteOnExit(root);
    }

    private static void deleteTreeOnce(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static boolean sleepBeforeCleanupRetry(int attempt) {
        try {
            TimeUnit.MILLISECONDS.sleep(50L * (attempt + 1L));
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void markDeleteOnExit(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                path.toFile().deleteOnExit();
            }
        } catch (IOException ignored) {
            root.toFile().deleteOnExit();
        }
    }

    private static void terminateProcess(Process process) {
        if (process == null) {
            return;
        }
        try {
            if (process.waitFor(200, TimeUnit.MILLISECONDS)) {
                return;
            }
            process.destroy();
            if (process.waitFor(2, TimeUnit.SECONDS)) {
                return;
            }
            process.destroyForcibly();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static GeneratedCertificate newCertificate() throws Exception {
        SecureRandom random = new SecureRandom();
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"), random);
        KeyPair keyPair = generator.generateKeyPair();

        long now = System.currentTimeMillis();
        Date notBefore = new Date(now - Duration.ofMinutes(1).toMillis());
        Date notAfter = new Date(now + Duration.ofDays(1).toMillis());
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

    private static void shutdownGroupQuietly(EventLoopGroup group) {
        if (group == null) {
            return;
        }
        group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
                .awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
        group.terminationFuture().awaitUninterruptibly(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static void runJavaClient(String address) throws Exception {
        String[] hostPort = address.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel clientDatagram = null;
        QuicChannel rawClient = null;
        try {
            QuicSslContext clientSsl = QuicSslContextBuilder
                    .forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(APPLICATION_PROTOCOL)
                    .build();

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
                            .handler(NettyQuicTestSupport.quietHandler())
                            .streamHandler(NettyQuicTestSupport.quietHandler())
                            .remoteAddress(new InetSocketAddress(host, port))
                            .connect()
            );

            try (ZmuxSession session = NettyQuic.wrapSession(rawClient)) {
                ZmuxStream stream = session.openStream(new OpenOptions(
                        7L,
                        11L,
                        "java-open".getBytes(StandardCharsets.UTF_8)
                ));
                stream.writeFinal("java->go".getBytes(StandardCharsets.UTF_8));
                ByteArrayOutputStream response = new ByteArrayOutputStream();
                byte[] buffer = new byte[64];
                int read;
                while ((read = stream.read(buffer)) >= 0) {
                    response.write(buffer, 0, read);
                }
                assertEquals("go:java->go", response.toString(StandardCharsets.UTF_8.name()));
                session.close();
                session.awaitTerminationOrThrow(Duration.ofSeconds(5));
            }
        } finally {
            closeQuietly(rawClient);
            closeQuietly(clientDatagram);
            shutdownGroupQuietly(group);
        }
    }

    private static String goQuicServerMain() {
        return String.join("\n",
                "package main",
                "",
                "import (",
                "    \"context\"",
                "    \"crypto/rand\"",
                "    \"crypto/rsa\"",
                "    \"crypto/tls\"",
                "    \"crypto/x509\"",
                "    \"crypto/x509/pkix\"",
                "    \"encoding/pem\"",
                "    \"fmt\"",
                "    \"io\"",
                "    \"math/big\"",
                "    \"os\"",
                "    \"time\"",
                "",
                "    \"github.com/quic-go/quic-go\"",
                "    quicmux \"github.com/zmuxio/zmux-go/adapter/quicmux\"",
                ")",
                "",
                "func fatal(format string, args ...any) {",
                "    fmt.Fprintf(os.Stdout, \"ERR \"+format+\"\\n\", args...)",
                "    os.Exit(1)",
                "}",
                "",
                "func serverTLS() *tls.Config {",
                "    privateKey, err := rsa.GenerateKey(rand.Reader, 2048)",
                "    if err != nil {",
                "        fatal(\"generate key: %v\", err)",
                "    }",
                "    template := &x509.Certificate{",
                "        SerialNumber: big.NewInt(1),",
                "        Subject: pkix.Name{CommonName: \"localhost\"},",
                "        NotBefore: time.Now().Add(-time.Hour),",
                "        NotAfter: time.Now().Add(time.Hour),",
                "        KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,",
                "        ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},",
                "        BasicConstraintsValid: true,",
                "        DNSNames: []string{\"localhost\"},",
                "    }",
                "    der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)",
                "    if err != nil {",
                "        fatal(\"create cert: %v\", err)",
                "    }",
                "    certPEM := pem.EncodeToMemory(&pem.Block{Type: \"CERTIFICATE\", Bytes: der})",
                "    keyPEM := pem.EncodeToMemory(&pem.Block{Type: \"RSA PRIVATE KEY\", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})",
                "    cert, err := tls.X509KeyPair(certPEM, keyPEM)",
                "    if err != nil {",
                "        fatal(\"load key pair: %v\", err)",
                "    }",
                "    return &tls.Config{",
                "        Certificates: []tls.Certificate{cert},",
                "        NextProtos:   []string{\"" + APPLICATION_PROTOCOL + "\"},",
                "    }",
                "}",
                "",
                "func main() {",
                "    listener, err := quic.ListenAddr(\"127.0.0.1:0\", serverTLS(), nil)",
                "    if err != nil {",
                "        fatal(\"listen: %v\", err)",
                "    }",
                "    defer listener.Close()",
                "    fmt.Fprintln(os.Stdout, \"ADDR \"+listener.Addr().String())",
                "",
                "    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer cancel()",
                "    conn, err := listener.Accept(ctx)",
                "    if err != nil {",
                "        fatal(\"accept conn: %v\", err)",
                "    }",
                "    session := quicmux.WrapSession(conn)",
                "    defer session.Close()",
                "",
                "    stream, err := session.AcceptStream(ctx)",
                "    if err != nil {",
                "        fatal(\"accept stream: %v\", err)",
                "    }",
                "    if got := string(stream.OpenInfo()); got != \"java-open\" {",
                "        fatal(\"open info = %q\", got)",
                "    }",
                "    meta := stream.Metadata()",
                "    if meta.Priority != 7 || meta.Group == nil || *meta.Group != 11 {",
                "        fatal(\"metadata = priority:%d group:%v\", meta.Priority, meta.Group)",
                "    }",
                "    payload, err := io.ReadAll(stream)",
                "    if err != nil {",
                "        fatal(\"read stream: %v\", err)",
                "    }",
                "    if got := string(payload); got != \"java->go\" {",
                "        fatal(\"payload = %q\", got)",
                "    }",
                "    if _, err := stream.WriteFinal([]byte(\"go:\" + string(payload))); err != nil {",
                "        fatal(\"write final: %v\", err)",
                "    }",
                "    if err := session.Close(); err != nil {",
                "        fatal(\"close session: %v\", err)",
                "    }",
                "    waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer waitCancel()",
                "    if err := session.Wait(waitCtx); err != nil {",
                "        fatal(\"wait: %v\", err)",
                "    }",
                "}",
                "");
    }

    private static String goQuicClientMain() {
        return String.join("\n",
                "package main",
                "",
                "import (",
                "    \"context\"",
                "    \"crypto/tls\"",
                "    \"fmt\"",
                "    \"io\"",
                "    \"os\"",
                "    \"time\"",
                "",
                "    \"github.com/quic-go/quic-go\"",
                "    zmux \"github.com/zmuxio/zmux-go\"",
                "    quicmux \"github.com/zmuxio/zmux-go/adapter/quicmux\"",
                ")",
                "",
                "func fatal(format string, args ...any) {",
                "    fmt.Fprintf(os.Stdout, \"ERR \"+format+\"\\n\", args...)",
                "    os.Exit(1)",
                "}",
                "",
                "func clientTLS() *tls.Config {",
                "    return &tls.Config{",
                "        InsecureSkipVerify: true,",
                "        NextProtos:         []string{\"" + APPLICATION_PROTOCOL + "\"},",
                "    }",
                "}",
                "",
                "func main() {",
                "    if len(os.Args) != 2 {",
                "        fatal(\"usage: main <addr>\")",
                "    }",
                "    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer cancel()",
                "    conn, err := quic.DialAddr(ctx, os.Args[1], clientTLS(), nil)",
                "    if err != nil {",
                "        fatal(\"dial: %v\", err)",
                "    }",
                "    session := quicmux.WrapSession(conn)",
                "    defer session.Close()",
                "",
                "    var gate [1]byte",
                "    if _, err := os.Stdin.Read(gate[:]); err != nil {",
                "        fatal(\"read start signal: %v\", err)",
                "    }",
                "",
                "    priority := uint64(7)",
                "    group := uint64(11)",
                "    stream, err := session.OpenStreamWithOptions(ctx, zmux.OpenOptions{",
                "        InitialPriority: &priority,",
                "        InitialGroup:    &group,",
                "        OpenInfo:        []byte(\"go-open\"),",
                "    })",
                "    if err != nil {",
                "        fatal(\"open stream: %v\", err)",
                "    }",
                "    if _, err := stream.WriteFinal([]byte(\"go->java\")); err != nil {",
                "        fatal(\"write final: %v\", err)",
                "    }",
                "    response, err := io.ReadAll(stream)",
                "    if err != nil {",
                "        fatal(\"read response: %v\", err)",
                "    }",
                "    if got := string(response); got != \"java:go->java\" {",
                "        fatal(\"response = %q\", got)",
                "    }",
                "    if err := session.Close(); err != nil {",
                "        fatal(\"close session: %v\", err)",
                "    }",
                "    waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer waitCancel()",
                "    if err := session.Wait(waitCtx); err != nil {",
                "        fatal(\"wait: %v\", err)",
                "    }",
                "}",
                "");
    }

    @Test
    void javaNettyQuicClientTalksToGoQuicServerWithOpenMetadata() throws Exception {
        Path goRoot = requireGoRoot();
        Path work = Files.createTempDirectory("zmux-java-go-quic-server-");
        Files.write(work.resolve("go.mod"), goQuicMod(goRoot).getBytes(StandardCharsets.UTF_8));
        Files.write(work.resolve("main.go"), goQuicServerMain().getBytes(StandardCharsets.UTF_8));

        Process process = new ProcessBuilder("go", "run", "-mod=mod", ".")
                .directory(work.toFile())
                .redirectErrorStream(true)
                .start();
        try (BufferedReader output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String address = firstOutputLine(output);
            assertTrue(address.startsWith("ADDR "), "unexpected go helper output: " + address);
            runJavaClient(address.substring("ADDR ".length()));
            assertTrue(process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "go helper did not exit");
            String rest = output.lines().collect(Collectors.joining("\n"));
            assertEquals(0, process.exitValue(), rest);
        } finally {
            terminateProcess(process);
            deleteTree(work);
        }
    }

    @Test
    void goQuicClientTalksToJavaNettyQuicServerWithOpenMetadata() throws Exception {
        Path goRoot = requireGoRoot();
        Path work = Files.createTempDirectory("zmux-go-java-quic-client-");
        Files.write(work.resolve("go.mod"), goQuicMod(goRoot).getBytes(StandardCharsets.UTF_8));
        Files.write(work.resolve("main.go"), goQuicClientMain().getBytes(StandardCharsets.UTF_8));

        GeneratedCertificate certificate = newCertificate();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel serverDatagram = null;
        QuicChannel rawServer = null;
        try {
            QuicSslContext serverSsl = QuicSslContextBuilder
                    .forServer(certificate.key(), null, certificate.cert())
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
                            .streamHandler(NettyQuicTestSupport.quietHandler())
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

            InetSocketAddress localAddress = (InetSocketAddress) serverDatagram.localAddress();
            String address = localAddress.getAddress().getHostAddress() + ":" + localAddress.getPort();

            Process process = new ProcessBuilder("go", "run", "-mod=mod", ".", address)
                    .directory(work.toFile())
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                rawServer = NettyQuicTestSupport.await(acceptedServer);
                try (ZmuxSession session = NettyQuic.wrapSession(rawServer)) {
                    process.getOutputStream().write(1);
                    process.getOutputStream().flush();
                    process.getOutputStream().close();
                    ZmuxStream stream = session.acceptStream(Duration.ofSeconds(10));
                    assertArrayEquals("go-open".getBytes(StandardCharsets.UTF_8), stream.openInfo());
                    StreamMetadata metadata = stream.metadata();
                    assertEquals(7L, metadata.priority());
                    assertEquals(Long.valueOf(11L), metadata.group());
                    assertEquals("go->java", new String(NettyQuicTestSupport.readAll(stream), StandardCharsets.UTF_8.name()));
                    stream.writeFinal("java:go->java".getBytes(StandardCharsets.UTF_8));
                    stream.close();
                    session.close();
                    session.awaitTerminationOrThrow(Duration.ofSeconds(5));
                }

                assertTrue(process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "go helper did not exit");
                String rest = output.lines().collect(Collectors.joining("\n"));
                assertEquals(0, process.exitValue(), rest);
            } finally {
                terminateProcess(process);
            }
        } finally {
            deleteTree(work);
            closeQuietly(rawServer);
            closeQuietly(serverDatagram);
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
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            accepted.completeExceptionally(cause);
            ctx.close();
        }
    }
}
