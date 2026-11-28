package io.zmux.nettytest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/** Shared localhost certificate for the Netty adapter tests. */
public final class TestTls {
    private TestTls() {}
    private static final char[] PASS = "changeit".toCharArray();
    private static final Object LOCK = new Object();
    private static final AtomicReference<KeyStore> STORE = new AtomicReference<>();

    private static KeyStore store() throws IOException, GeneralSecurityException, InterruptedException {
        synchronized (LOCK) { return loadStore(); }
    }

    private static KeyStore loadStore() throws IOException, GeneralSecurityException, InterruptedException {
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

    /** Exports the same certificate and key for a native interop peer. */
    public static Path[] pem() throws IOException, GeneralSecurityException, InterruptedException {
        synchronized (LOCK) {
            var store = loadStore();
            Path dir = Path.of("target", "tls");
            String name = "localhost-" + ProcessHandle.current().pid();
            var encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
            Path cert = dir.resolve(name + ".crt"), key = dir.resolve(name + ".key");
            Files.writeString(cert, "-----BEGIN CERTIFICATE-----\n" + encoder.encodeToString(store.getCertificate("localhost").getEncoded())
                    + "\n-----END CERTIFICATE-----\n");
            Files.writeString(key, "-----BEGIN PRIVATE KEY-----\n" + encoder.encodeToString(store.getKey("localhost", PASS).getEncoded())
                    + "\n-----END PRIVATE KEY-----\n");
            return new Path[] {cert, key};
        }
    }
}
