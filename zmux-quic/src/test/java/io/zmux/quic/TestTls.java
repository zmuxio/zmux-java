package io.zmux.quic;

import io.zmux.*;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

final class TestTls {
    static QuicSslContextBuilder server;
    static QuicSslContextBuilder client;
    private TestTls() {}
    static synchronized void init() throws Exception {
        if (server != null) return;
        Path directory = Path.of("target", "tls"); Files.createDirectories(directory);
        Path storePath = directory.resolve("localhost.p12");
        if (!Files.exists(storePath)) {
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                    "-genkeypair", "-alias", "localhost", "-keyalg", "EC", "-groupname", "secp256r1",
                    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "3650",
                    "-storetype", "PKCS12", "-keystore", storePath.toString(), "-storepass", "changeit", "-keypass", "changeit")
                    .redirectErrorStream(true).start();
            if (!process.waitFor(30, TimeUnit.SECONDS)) { process.destroyForcibly().waitFor(); throw new IllegalStateException("keytool timed out"); }
            if (process.exitValue() != 0) throw new IllegalStateException(new String(process.getInputStream().readAllBytes()));
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(storePath)) { store.load(input, "changeit".toCharArray()); }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, "changeit".toCharArray());
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); trust.init(store);
        server = QuicSslContextBuilder.forServer(keys, "changeit").applicationProtocols("qmux-02").earlyData(false);
        client = QuicSslContextBuilder.forClient().trustManager(trust).endpointIdentificationAlgorithm("HTTPS")
                .applicationProtocols("qmux-02").earlyData(false);
    }
}
