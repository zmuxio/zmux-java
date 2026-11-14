package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.*;
import io.zmux.carrier.PipeIo;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Joins over QUIC: Sealed by default, Bound through the exporter of the key log, lanes only outside Sealed. */
@Timeout(60)
class QuicSecureCarrierTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    private static ZmuxConfig config() {
        return ZmuxConfig.builder().maxPipes(4).noProbation().pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((c, m, p) -> true).build();
    }

    private static void echo(ZmuxSession c, ZmuxSession s) throws Exception {
        byte[] data = new byte[200_000];
        new java.util.Random(3).nextBytes(data);
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                ZmuxStream st = s.acceptStream();
                byte[] got = QuicTest.read(st);
                st.write(got);
                st.finish();
            } catch (Exception ignored) { /* The client's read fails. */ }
        });
        ZmuxStream st = c.openStream();
        st.write(data);
        st.finish();
        assertArrayEquals(data, QuicTest.read(st));
        t.join();
    }

    /** RFC 8446 7.5 by hand from JDK HKDF, independently of the derivation under test. */
    @Test void theExporterDerivationIsTheTls13One() throws Exception {
        byte[] secret = HexFormat.of().parseHex("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff");
        var kdf = javax.crypto.KDF.getInstance("HKDF-SHA256");
        byte[] empty = java.security.MessageDigest.getInstance("SHA-256").digest();
        var key = new javax.crypto.spec.SecretKeySpec(secret, "HKDF");
        var derived = kdf.deriveData(javax.crypto.spec.HKDFParameterSpec.expandOnly(key, label("EXPORTER-x", empty, 32), 32));
        var out = kdf.deriveData(javax.crypto.spec.HKDFParameterSpec.expandOnly(new javax.crypto.spec.SecretKeySpec(derived, "HKDF"), label("exporter", empty, 32), 32));
        assertArrayEquals(out, QuicExporter.derive(secret, "SHA-256", "EXPORTER-x", null, 32));
    }

    private static byte[] label(String l, byte[] ctx, int n) {
        byte[] full = ("tls13 " + l).getBytes(StandardCharsets.US_ASCII);
        var b = java.nio.ByteBuffer.allocate(2 + 1 + full.length + 1 + ctx.length);
        return b.putShort((short) n).put((byte) full.length).put(full).put((byte) ctx.length).put(ctx).array();
    }

    /** A prefix token bound to the connection's TLS exporter is presented and verified; another key is refused. */
    @Test void aPrefixTokenBoundToTheQuicHandshake() throws Exception {
        byte[] key = new byte[32];
        PrefixAuth auth = new PrefixAuth(key, Duration.ofSeconds(30), true, 1, 0);
        ZmuxConfig plain = ZmuxConfig.builder().build();
        try (ZmuxListener l = ZmuxListener.of(ListenerConfig.builder().session(plain).admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()::decide)).build());
             QuicServer server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), l);
             QuicClient client = new QuicClient(TestTls.client, QuicOptions.defaults())) {
            ZmuxSession c = client.connectAsync(server.address(), "localhost", ZmuxConfig.builder().carrierCredential(auth).build()).get(10, TimeUnit.SECONDS);
            ZmuxSession s = l.accept(WAIT);
            assertNotNull(s);
            echo(c, s);
            c.close();
            byte[] other = new byte[32];
            other[0] = 1;
            var refused = new PrefixAuth(other, Duration.ofSeconds(30), true, 1, 0);
            var denied = client.connectAsync(server.address(), "localhost", ZmuxConfig.builder().carrierCredential(refused).build());
            assertThrows(java.net.SocketTimeoutException.class, () -> l.accept(Duration.ofMillis(500)), "another key's token is not admitted");
            denied.cancel(true);
        }
    }

    @Test void aJoinIsSealedByDefaultAndBoundOnRequest() throws Exception {
        ZmuxConfig cfg = config();
        try (ZmuxListener l = ZmuxListener.of(ListenerConfig.builder().session(cfg).build());
             QuicServer server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), l);
             QuicClient client = new QuicClient(TestTls.client, QuicOptions.defaults())) {
            ZmuxSession c = client.connectAsync(server.address(), "localhost", cfg).get(10, TimeUnit.SECONDS);
            ZmuxSession s = l.accept(WAIT);
            assertTrue(c.adapter().isJoinReady(), "the exporter of the key log is the session's secret");
            echo(c, s);
            PipeIo p1 = client.dialAsync(server.address(), "localhost", c).get(10, TimeUnit.SECONDS);
            assertTrue(p1.isSecure());
            assertNotNull(p1.capabilities().exporter().export("EXPORTER-x", null, 32));
            ZmuxPipe sealed = c.addPipe(p1, PipeOptions.defaults());
            PipeStats st = sealed.stats().orElseThrow();
            assertEquals(RecordMode.SEALED, st.recordMode());
            echo(c, s);
            ZmuxPipe bound = c.addPipe(client.dialAsync(server.address(), "localhost", c).get(10, TimeUnit.SECONDS),
                    PipeOptions.builder().recordMode(RecordMode.BOUND).build());
            assertEquals(RecordMode.BOUND, bound.stats().orElseThrow().recordMode());
            echo(c, s);
            for (var ps : c.pipeStats()) {
                if (ps.pipeId().equals(sealed.id())) assertTrue(ps.lanesOut() <= 1, "a Sealed pipe has no lanes");
            }
            c.close();
        }
    }

    /** An Establish on a QUIC stream, through the listener's transform: Sealed hides the stream's lanes; Bound mixes the exporter. */
    @Test void anEstablishRunsOnAQuicStreamInBothModes() throws Exception {
        io.zmux.seal.StaticKey node = io.zmux.seal.StaticKey.generate();
        byte[] secret = new byte[32];
        var hp = io.zmux.seal.HandlePsk.derive(secret, "q".getBytes(), null);
        var gate = new io.zmux.seal.Gate() {
            @Override public byte[] resolve(byte[] h, java.net.SocketAddress src) { return hp.psk(); }
            @Override public io.zmux.seal.Credential credential(byte[] h, io.zmux.seal.Profile p, java.net.SocketAddress src) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, java.net.SocketAddress src) { return "q"; }
        };
        ZmuxConfig cfg = ZmuxConfig.builder().maxPipes(4).noProbation().allowRecordMode((c, m, p) -> true).build();
        var lc = ListenerConfig.builder().session(cfg).inboundTransform(io.zmux.seal.Seal.server(io.zmux.seal.ServerConfig.builder(gate, node).allowRecordMode((c, m, p) -> c.isSecure()).build())).build();
        for (RecordMode mode : java.util.List.of(RecordMode.SEALED, RecordMode.BOUND)) {
            try (ZmuxListener l = ZmuxListener.of(lc);
                 QuicServer server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), l);
                 QuicClient client = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = client.connectAsync(server.address(), "localhost", cfg, p -> io.zmux.seal.Seal.establish(p,
                        io.zmux.seal.ClientConfig.builder().allowPskOnly(true).psk(hp.handle(), hp.psk()).mode(mode).build())).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession s = l.accept(WAIT);
                assertEquals(mode, c.weakestRecordMode());
                assertEquals(mode, s.weakestRecordMode());
                echo(c, s);
                c.close();
            }
        }
    }
}
