package io.zmux.nettytest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/** Shared localhost certificate for the HTTP adapter tests. */
public final class TestTls {
    private TestTls() {}
    private static final char[] PASS = "changeit".toCharArray();
    private static final AtomicReference<KeyStore> STORE = new AtomicReference<>();

    private static synchronized KeyStore store() throws IOException, GeneralSecurityException, InterruptedException {
        KeyStore cached = STORE.get();
        if (cached != null) return cached;
        Path dir = Path.of("target", "tls");
        Files.createDirectories(dir);
        Path ks = dir.resolve("localhost-" + ProcessHandle.current().pid() + ".p12");
        if (!Files.exists(ks)) {
            Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
            // Process is AutoCloseable only from JDK 26 on, after the release this builds for; the finally destroys it.
            //noinspection resource
            Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "localhost", "-keyalg", "EC",
                    "-groupname", "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                    "-startdate", "-1H", "-validity", "3650", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                    "-keypass", "changeit").redirectErrorStream(true).start();
            try {
                if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) throw new IOException("keytool failed");
            } finally {
                if (p.isAlive()) p.destroyForcibly();
            }
        }
        KeyStore s = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) { s.load(in, PASS); }
        STORE.set(s);
        return s;
    }

    public static KeyManagerFactory kmf() throws IOException, GeneralSecurityException, InterruptedException {
        var k = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        k.init(store(), PASS);
        return k;
    }

    public static TrustManagerFactory tmf() throws IOException, GeneralSecurityException, InterruptedException {
        var t = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        t.init(store());
        return t;
    }
}
