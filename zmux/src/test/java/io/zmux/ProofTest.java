package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.carrier.PipeIo;
import io.zmux.seal.ClientConfig;
import io.zmux.seal.CredentialsFailedException;
import io.zmux.seal.KeyAlg;
import io.zmux.seal.KeyId;
import io.zmux.seal.Peer;
import io.zmux.seal.PinAnchor;
import io.zmux.seal.Principal;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import io.zmux.seal.SigningKey;
import io.zmux.seal.VerifyVerdict;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Transcript signatures of Establish over real TCP: each end proves, and PQ authentication follows. */
@org.junit.jupiter.api.Timeout(60)
class ProofTest {
    private static final SigningKey NODE_PROOF, CLIENT_PROOF;
    static {
        try {
            NODE_PROOF = SigningKey.generate(KeyAlg.ML_DSA_44);
            CLIENT_PROOF = SigningKey.generate(KeyAlg.ML_DSA_44);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static PinAnchor pinWithProof() {
        return new PinAnchor(KeyId.staticKeyId(SealTest.NODE.publicKey()), NODE_PROOF.keyId());
    }

    @Test void mutualProofsGivePqAuthentication() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).build();
            var sc = ServerConfig.builder(SealTest.gate(null), SealTest.NODE)
                    .anchor(in -> new VerifyVerdict(new Principal(7, new byte[] {7}), CLIENT_PROOF.keyId(), null)).requirePqAuth(true)
                    .signer(NODE_PROOF).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc)).build())) {
                ClientConfig cc = SealTest.client().anchor(pinWithProof()).requirePqAuth(true).signer(CLIENT_PROOF).build();
                PipeIo p = SealTest.establish(SealTest.tcp(l.localAddress()), cc);
                assertTrue(Seal.peerOf(p).orElseThrow().pqAuthenticated());
                try (var _ = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(SealTest.WAIT)) {
                    Peer client = (Peer) s.admitted().orElseThrow();
                    assertTrue(client.pqAuthenticated());
                    // The Peer carries the principal the verdict gave.
                    assertEquals(7, client.verdict().principal().type());
                    org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] {7}, client.verdict().principal().bytes());
                }
            }
        }
    }

    /** A server without a signer fails a client whose pin names a proof key; a pin alone cannot satisfy requirePqAuth; a client without a proof is refused as credentials_failed. */
    @Test void aMissingProofFailsTheVerdict() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).build();
            List<String> reasons = new CopyOnWriteArrayList<>();
            var sc = ServerConfig.builder(SealTest.gate(null), SealTest.NODE)
                    .anchor(in -> new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()), CLIENT_PROOF.keyId(), null)).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc))
                    .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var e = fails(Seal.establish(SealTest.tcp(l.localAddress()), SealTest.client().anchor(pinWithProof()).build()).toCompletableFuture());
                assertTrue(e instanceof CredentialsFailedException, e.toString());
                var pq = fails(Seal.establish(SealTest.tcp(l.localAddress()), SealTest.client().requirePqAuth(true).build()).toCompletableFuture());
                assertEquals(io.zmux.seal.SecureCarrierException.class, pq.getClass());
                // The client finishes before the server judges message 3; the server then refuses it.
                Seal.establish(SealTest.tcp(l.localAddress()), SealTest.client().build()).toCompletableFuture().get(10, TimeUnit.SECONDS)
                        .writer().abort(new java.io.IOException("done"));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!reasons.contains("credentials_failed") && System.nanoTime() < until) Thread.sleep(2);
                assertTrue(reasons.contains("credentials_failed"), reasons.toString());
            }
        }
    }

    private static Throwable fails(CompletableFuture<PipeIo> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS).writer().abort(new java.io.IOException("unexpected"));
            throw new AssertionError("the handshake succeeded");
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }
}
