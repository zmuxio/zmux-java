package io.zmux;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** A self-signed localhost certificate made with the JDK's keytool in the build directory. */
final class TestTls {
    private static final char[] PASS = "changeit".toCharArray();
    private static SSLContext server;
    private static SSLContext client;

    private TestTls() {}

    static synchronized void init() throws Exception {
        if (server != null) return;
        Path dir = Path.of("target", "tls");
        Files.createDirectories(dir);
        Path ks = dir.resolve("localhost.p12");
        if (!Files.exists(ks)) {
            Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
            Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "localhost", "-keyalg", "EC",
                    "-groupname", "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                    "-validity", "3650", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                    "-keypass", "changeit").redirectErrorStream(true).start();
            if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
                throw new IllegalStateException("keytool failed: " + new String(p.getInputStream().readAllBytes()));
            }
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(ks.toFile())) {
            store.load(in, PASS);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, PASS);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(store);
        server = SSLContext.getInstance("TLSv1.3");
        server.init(kmf.getKeyManagers(), null, null);
        client = SSLContext.getInstance("TLSv1.3");
        client.init(null, tmf.getTrustManagers(), null);
    }

    static SSLContext server() throws Exception {
        init();
        return server;
    }

    static SSLContext client() throws Exception {
        init();
        return client;
    }
}
