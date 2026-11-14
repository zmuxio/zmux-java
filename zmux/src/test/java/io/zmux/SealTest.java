package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.seal.SecureCarrierException;
import io.zmux.seal.*;
import io.zmux.seal.Credential;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Establish through the public API, over real listeners and sessions. */
@Timeout(60)
class SealTest {
    static final byte[] SECRET = new byte[32];
    static final HandlePsk HP;
    static final StaticKey NODE;
    static final Duration WAIT = Duration.ofSeconds(10);

    static {
        try {
            HP = HandlePsk.derive(SECRET, "test".getBytes(), null);
            byte[] seed = new byte[32];
            Arrays.fill(seed, (byte) 7);
            NODE = StaticKey.fromScalar(seed);
        } catch (SecureCarrierException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static Gate gate(Credential cred) {
        return new Gate() {
            @Override public byte[] resolve(byte[] handle, SocketAddress src) throws Exception {
                if (!Arrays.equals(handle, HP.handle())) return null;
                return HP.psk();
            }
            @Override public Credential credential(byte[] handle, io.zmux.seal.Profile profile, SocketAddress src) { return cred; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { return "committed"; }
        };
    }

    static ZmuxConfig.Builder sessions(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(4).noProbation();
    }

    static ClientConfig.Builder client() {
        return ClientConfig.builder().anchor(new PinAnchor(io.zmux.seal.KeyId.of(io.zmux.seal.KeyAlg.X25519, NODE.publicKey()))).psk(HP.handle(), HP.psk());
    }

    static PipeIo tcp(InetSocketAddress at) throws Exception {
        SocketChannel ch = SocketChannel.open(at);
        return PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
    }

    static PipeIo establish(PipeIo p, ClientConfig c) throws Exception {
        return Seal.establish(p, c).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    static void echo(ZmuxSession c, ZmuxSession s) throws Exception {
        byte[] data = new byte[100_000];
        new java.util.Random(1).nextBytes(data);
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                ZmuxStream st = s.acceptStream();
                byte[] got = st.inputStream().readAllBytes();
                st.write(got);
                st.finish();
            } catch (Exception ignored) { /* The client's read fails. */ }
        });
        ZmuxStream st = c.openStream();
        st.write(data);
        st.finish();
        assertArrayEquals(data, st.inputStream().readAllBytes());
        t.join();
    }

    private static int readToEnd(SocketChannel ch) {
        ByteBuffer b = ByteBuffer.allocate(4096);
        int total = 0;
        try {
            for (int n; (n = ch.read(b)) >= 0; b.clear()) total += n;
        } catch (java.io.IOException reset) { /* An abortive end is an end. */ }
        return total;
    }

    /** A secure carrier first and only an end-to-end check configured: further pipes join on the carrier's exporter. */
    @Test void multipathOnASecureCarrierWithEndToEndOnly() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = sessions(loops).pipeAuth(PipeAuth.none().withEndToEnd()).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build());
                 var c = Zmux.open(establish(tcp(l.localAddress()), client().build()), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                ZmuxPipe joined = c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertTrue(joined.id().value() > 0);
                assertEquals(2, c.pipes().size());
                echo(c, s);
            }
        }
    }

    /** The listener's own logger takes its warnings; the session's sees none of them. */
    @Test void theListenerLogsToItsOwnLogger() throws Exception {
        List<String> listener = new CopyOnWriteArrayList<>(), session = new CopyOnWriteArrayList<>();
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).logger(recorder(session)).build();
            var lc = ListenerConfig.builder().session(cfg).logger(recorder(listener))
                    .onReject(i -> { throw new IllegalStateException("host failure"); })
                    .inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var f = Seal.establish(tcp(l.localAddress()), client().psk(new byte[16], new byte[32]).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
                for (long end = System.nanoTime() + 5_000_000_000L; listener.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
            }
        }
        assertEquals(List.of("the listener's onReject failed; the carrier is closed"), listener);
        assertEquals(List.of(), session);
    }

    private static System.Logger recorder(List<String> into) {
        return new System.Logger() {
            @Override public String getName() { return "test"; }
            @Override public boolean isLoggable(Level level) { return true; }
            @Override public void log(Level level, java.util.ResourceBundle b, String msg, Throwable t) { into.add(msg); }
            @Override public void log(Level level, java.util.ResourceBundle b, String format, Object... params) { into.add(format); }
        };
    }

    @Test void establishThenJoinOverTcpWithTheHostValue() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = sessions(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build())) {
                PipeIo p = establish(tcp(l.localAddress()), client().build());
                Peer serverPeer = Seal.peerOf(p).orElseThrow();
                assertArrayEquals(NODE.publicKey(), serverPeer.staticKey());
                assertEquals(Principal.NODE_KEY, serverPeer.verdict().principal().type());
                // The Peer's principal is the verdict's: a pin names the node by the KeyId of its key.
                assertArrayEquals(io.zmux.seal.KeyId.of(io.zmux.seal.KeyAlg.X25519, NODE.publicKey()), serverPeer.verdict().principal().bytes());
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    assertEquals(RecordMode.SEALED, c.weakestRecordMode());
                    assertEquals(RecordMode.SEALED, s.weakestRecordMode());
                    Peer peer = (Peer) s.admitted().orElseThrow();
                    assertEquals("committed", peer.value());
                    echo(c, s);
                    ZmuxPipe joined = c.addPipe(l.localAddress(), PipeOptions.defaults());
                    assertTrue(joined.id().value() > 0);
                    assertEquals(RecordMode.SEALED, joined.stats().orElseThrow().recordMode());
                    echo(c, s);
                }
            }
        }
    }

    @Test void bothProfilesAndCiphersHoldAcrossTheHandshake() throws Exception {
        for (Profile profile : List.of(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_PSK0_AES)) {
            try (EventLoops loops = EventLoops.create(1)) {
                ZmuxConfig cfg = sessions(loops).build();
                try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build());
                     var c = Zmux.open(establish(tcp(l.localAddress()), client().allowedProfiles(profile).build()), Role.CLIENT, cfg);
                     var s = l.accept(WAIT)) {
                    assertEquals(profile, ((Peer) s.admitted().orElseThrow()).profile());
                    echo(c, s);
                }
            }
        }
    }

    /** A client chooses a Profile with a pre-shared key by default; every Profile holds when the host lists it. */
    @Test void clientsDefaultToHybridAndEveryProfileHolds() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build());
                 var c = Zmux.open(establish(tcp(l.localAddress()), client().build()), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                Profile p = ((Peer) s.admitted().orElseThrow()).profile();
                assertTrue(p == Profile.XXHFS_PSK0_CHACHA || p == Profile.XXHFS_PSK0_AES, p.toString());
                echo(c, s);
            }
            var server = ServerConfig.builder(gate(null), NODE).acceptedProfiles(Profile.values()).build();
            for (Profile profile : Profile.values()) {
                var b = client().allowedProfiles(profile);
                if (!profile.isPskBased()) b.psk(null, null);
                try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(server)).build());
                     var c = Zmux.open(establish(tcp(l.localAddress()), b.build()), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    assertEquals(profile, ((Peer) s.admitted().orElseThrow()).profile());
                    echo(c, s);
                }
            }
        }
    }

    /** A server takes 1 and 3 by default, the Profiles without a pre-shared key when listed, and exactly its list. */
    @Test void serverProfilePolicy() throws Exception {
        record Case(String name, Profile client, ServerConfig server, String refused) {}
        var cases = List.of(
                new Case("no-psk-hybrid-off-by-default", Profile.XXHFS_CHACHA, ServerConfig.builder(gate(null), NODE).build(), "profile_disabled"),
                new Case("no-psk-hybrid-allowed", Profile.XXHFS_AES, ServerConfig.builder(gate(null), NODE).acceptedProfiles(Profile.XXHFS_AES).build(), null),
                new Case("psk-by-default", Profile.XXHFS_PSK0_AES, ServerConfig.builder(gate(null), NODE).build(), null),
                new Case("allowed-list", Profile.XXHFS_PSK0_AES, ServerConfig.builder(gate(null), NODE).acceptedProfiles(Profile.XXHFS_PSK0_CHACHA).build(), "profile_disabled"));
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            for (Case k : cases) {
                List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
                var lc = ListenerConfig.builder().session(cfg).onReject(i -> { rejected.add(i); return RejectInfo.Action.CLOSE; }).inboundTransform(Seal.server(k.server())).build();
                var b = client().allowedProfiles(k.client()).handshakeTimeout(Duration.ofSeconds(5));
                if (!k.client().isPskBased()) b.psk(null, null);
                try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                    var f = Seal.establish(tcp(l.localAddress()), b.build()).toCompletableFuture();
                    if (k.refused() != null) {
                        assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS), k.name());
                        for (long end = System.nanoTime() + 5_000_000_000L; rejected.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
                        assertEquals(k.refused(), rejected.getFirst().reason(), k.name());
                        assertEquals(k.client().value(), rejected.getFirst().profile(), k.name());
                    } else {
                        try (var c = Zmux.open(f.get(10, TimeUnit.SECONDS), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                            echo(c, s);
                        }
                    }
                }
            }
        }
        var f = Seal.establish(MemoryPipe.pair(Framing.BYTE_STREAM)[0], client().allowedProfiles(Profile.XXHFS_CHACHA).build()).toCompletableFuture();
        var e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertEquals(SecureCarrierException.Code.INVALID_CONFIG, ((SecureCarrierException) e.getCause()).code());
    }

    /** Gate.commit gets the Handle and the client's whole Peer: credential, verdict, application data and the PQ facts. */
    @Test void theCommitSeesTheWholePeer() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            var seen = new java.util.concurrent.atomic.AtomicReference<io.zmux.seal.CommitRequest>();
            Gate base = gate(null);
            Gate recording = new Gate() {
                @Override public byte[] resolve(byte[] h, SocketAddress src) throws Exception { return base.resolve(h, src); }
                @Override public Credential credential(byte[] h, io.zmux.seal.Profile p, SocketAddress src) { return null; }
                @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { seen.set(a); return "value"; }
            };
            Object mine = new Object();
            var sc = ServerConfig.builder(recording, NODE).anchor(in -> new io.zmux.seal.VerifyVerdict(new Principal(9, new byte[] {9}), null, mine)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc)).build())) {
                Credential cred = new Credential(5, new byte[] {1, 2});
                PipeIo p = establish(tcp(l.localAddress()), client().credential(cred)
                        .appData((m, peer) -> java.util.concurrent.CompletableFuture.completedFuture(java.util.Optional.of(new byte[] {(byte) m}))).build());
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    var a = seen.get();
                    assertArrayEquals(HP.handle(), a.handle());
                    Peer peer = a.peer();
                    assertEquals(5, peer.credential().type());
                    assertArrayEquals(new byte[] {1, 2}, peer.credential().data());
                    assertEquals(9, peer.verdict().principal().type());
                    assertSame(mine, peer.verdict().value());
                    assertEquals(List.of(1, 3), peer.appData().stream().map(io.zmux.seal.AppData::message).toList());
                    assertTrue(peer.pqConfidential());
                    assertEquals(false, peer.pqAuthenticated());
                    assertNull(peer.value());
                    // The session's host value is the same Peer with the commit's value.
                    Peer admitted = (Peer) s.admitted().orElseThrow();
                    assertEquals("value", admitted.value());
                    assertSame(mine, admitted.verdict().value());
                }
            }
        }
    }

    @Test void aPinOfAnotherKeyIdFailsBeforeAnythingOfTheClientIsWritten() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build())) {
                var f = Seal.establish(tcp(l.localAddress()), client().anchor(new PinAnchor(new byte[io.zmux.seal.KeyId.LEN])).build()).toCompletableFuture();
                var e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
                var failed = assertInstanceOf(CredentialsFailedException.class, e.getCause());
                assertEquals("credentials failed: key not pinned", failed.getMessage());
            }
        }
    }

    @Test void aServerWithoutACredentialIsJudgedOnItsKeyAlone() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            List<Credential> seen = new CopyOnWriteArrayList<>();
            Anchor recording = in -> {
                seen.add(in.credential());
                return new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()));
            };
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build())) {
                establish(tcp(l.localAddress()), client().anchor(recording).build());
                assertEquals(1, seen.size());
                assertNull(seen.get(0));
            }
            // A sender does not send type 0: the handshake ends there, with nothing of it on the wire.
            var zero = new Credential(0, new byte[] {1});
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(ServerConfig.builder(gate(zero), NODE).build())).build())) {
                var f = Seal.establish(tcp(l.localAddress()), client().anchor(recording).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
                assertEquals(0, l.stats().sessions());
            }
        }
    }

    /** A server's Anchor judges the client's credential: a rejection is a refused carrier, an unavailable verifier is not. */
    @Test void aServerAnchorRejectsOrIsUnavailable() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            Credential mine = new Credential(9, new byte[] {4, 2});
            for (String verdict : List.of("pass", "reject", "unavailable")) {
                List<String> reasons = new CopyOnWriteArrayList<>();
                List<VerifyInput> seen = new CopyOnWriteArrayList<>();
                Anchor anchor = in -> {
                    seen.add(in);
                    return switch (verdict) {
                        case "reject" -> throw new CredentialsFailedException("unknown client");
                        case "unavailable" -> throw new VerifierUnavailableException("store down", null);
                        default -> new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()));
                    };
                };
                var sc = ServerConfig.builder(gate(null), NODE).anchor(anchor).localContext("edge").build();
                var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc))
                        .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; }).build();
                try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                    var f = Seal.establish(tcp(l.localAddress()), client().credential(mine).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                    PipeIo p = f.get(10, TimeUnit.SECONDS); // the client is done after message 3
                    if (verdict.equals("pass")) {
                        try (var c = Zmux.open(p, Role.CLIENT, cfg); var srv = l.accept(WAIT)) {
                            assertEquals(Principal.NODE_KEY, ((Peer) srv.admitted().orElseThrow()).verdict().principal().type());
                        }
                    } else {
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (seen.isEmpty() && System.nanoTime() < until) Thread.sleep(2);
                        if (verdict.equals("reject")) {
                            while (reasons.isEmpty() && System.nanoTime() < until) Thread.sleep(2);
                            assertEquals(List.of("credentials_failed"), reasons);
                        }
                        assertEquals(0, l.stats().sessions());
                    }
                    assertEquals(1, seen.size());
                    assertEquals(io.zmux.Role.CLIENT, seen.get(0).role());
                    assertEquals(9, seen.get(0).credential().type());
                    assertEquals("edge", seen.get(0).localContext());
                }
                if (verdict.equals("unavailable")) assertEquals(List.of(), reasons);
            }
        }
    }

    @Test void profilesWithoutAKeyAreAcceptedOnlyWhenListed() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            List<String> reasons = new CopyOnWriteArrayList<>();
            List<Profile> asked = new CopyOnWriteArrayList<>();
            Gate gate = new Gate() {
                @Override public byte[] resolve(byte[] h, SocketAddress src) { return null; }
                @Override public Credential credential(byte[] h, Profile p, SocketAddress src) { asked.add(p); return null; }
                @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { return "ok"; }
            };
            var anonymous = ClientConfig.builder().anchor(new PinAnchor(io.zmux.seal.KeyId.of(io.zmux.seal.KeyAlg.X25519, NODE.publicKey()))).allowedProfiles(Profile.XXHFS_CHACHA)
                    .handshakeTimeout(Duration.ofSeconds(5)).build();
            var byDefault = ListenerConfig.builder().session(cfg).onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; })
                    .inboundTransform(Seal.server(ServerConfig.builder(gate, NODE).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), byDefault)) {
                var f = Seal.establish(tcp(l.localAddress()), anonymous).toCompletableFuture();
                var e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
                assertInstanceOf(NoReplyException.class, e.getCause());
                // An unknown Handle on the default Profiles.
                var g = Seal.establish(tcp(l.localAddress()), client().handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertInstanceOf(NoReplyException.class, assertThrows(ExecutionException.class, () -> g.get(10, TimeUnit.SECONDS)).getCause());
                for (long end = System.nanoTime() + 5_000_000_000L; reasons.size() < 2 && System.nanoTime() < end; ) Thread.sleep(10);
                assertEquals(List.of("profile_disabled", "unknown_handle"), reasons);
            }
            var listed = ListenerConfig.builder().session(cfg)
                    .inboundTransform(Seal.server(ServerConfig.builder(gate, NODE).acceptedProfiles(Profile.XXHFS_CHACHA).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), listed)) {
                assertEquals(Profile.XXHFS_CHACHA, Seal.peerOf(establish(tcp(l.localAddress()), anonymous)).orElseThrow().profile());
                assertEquals(List.of(Profile.XXHFS_CHACHA), asked);
            }
        }
    }

    @Test void aFailingGateIsAGateError() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            List<String> reasons = new CopyOnWriteArrayList<>();
            Gate gate = new Gate() {
                @Override public byte[] resolve(byte[] h, SocketAddress src) throws Exception { throw new Exception("down"); }
                @Override public Credential credential(byte[] h, Profile p, SocketAddress src) { return null; }
                @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { return "ok"; }
            };
            var lc = ListenerConfig.builder().session(cfg).onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; })
                    .inboundTransform(Seal.server(ServerConfig.builder(gate, NODE).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var f = Seal.establish(tcp(l.localAddress()), client().handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertInstanceOf(NoReplyException.class, assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS)).getCause());
                for (long end = System.nanoTime() + 5_000_000_000L; reasons.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
                assertEquals(List.of("gate_error"), reasons);
            }
        }
    }

    @Test void aHandleWithoutAKeyIsNamed() throws Exception {
        var f = Seal.establish(MemoryPipe.pair(Framing.BYTE_STREAM)[0], client().psk(HP.handle(), null).build()).toCompletableFuture();
        var e = assertInstanceOf(SecureCarrierException.class, assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS)).getCause());
        assertEquals("invalid_config: Handle set without a pre-shared key", e.getMessage());
    }

    @Test void refusalsGoToOnRejectAndNothingIsWritten() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).build();
            List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
            var lc = ListenerConfig.builder().session(cfg).onReject(i -> { rejected.add(i); return RejectInfo.Action.CLOSE; })
                    .inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                // A wrong key: a Hello the Noise AEAD refuses.
                byte[] other = new byte[32];
                Arrays.fill(other, (byte) 9);
                var f = Seal.establish(tcp(l.localAddress()), client().psk(HP.handle(), other).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertInstanceOf(NoReplyException.class, assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS)).getCause());
                // A bad first byte, and a plain session where an Establish is required.
                for (byte[] first : List.of(new byte[] {7, 0, 0, 0}, new byte[] {0x40, 0x10, (byte) 0xff, 0, 0, 0})) {
                    try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                        ch.write(ByteBuffer.wrap(first));
                        assertEquals(0, readToEnd(ch));
                    }
                }
                // A Trusted Hello on a server that takes only Sealed.
                var t = Seal.establish(tcp(l.localAddress()), client().mode(RecordMode.TRUSTED).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertThrows(ExecutionException.class, () -> t.get(10, TimeUnit.SECONDS));
                for (long end = System.nanoTime() + 5_000_000_000L; rejected.size() < 4 && System.nanoTime() < end; ) Thread.sleep(10);
                var reasons = new ArrayList<String>();
                for (RejectInfo i : rejected) reasons.add(i.reason());
                assertEquals(List.of("auth", "unknown_profile", "unknown_profile", "record_mode"), reasons);
                assertEquals(1, rejected.get(0).profile() == 0 ? 0 : 1, "the Hello's profile");
                assertEquals(RecordMode.TRUSTED, rejected.get(3).recordMode());
                assertEquals(0, l.stats().sessions());
            }
        }
    }

    /** A stall lasts the handshake timeout, not the first-record timeout. */
    @Test void aStalledCarrierEndsAtTheHandshakeTimeout() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).handshakeTimeout(Duration.ofMillis(1500)).build();
            var lc = ListenerConfig.builder().session(cfg).firstRecordTimeout(Duration.ofMillis(100)).onReject(i -> RejectInfo.Action.STALL)
                    .inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc); SocketChannel ch = SocketChannel.open(l.localAddress())) {
                long start = System.nanoTime();
                ch.write(ByteBuffer.wrap(new byte[] {7, 0, 0}));
                assertEquals(0, readToEnd(ch), "a stall sends nothing");
                long ms = (System.nanoTime() - start) / 1_000_000;
                assertTrue(ms >= 1300 && ms < 5000, "stalled for " + ms + " ms");
            }
        }
    }

    @Test void boundOverTlsNeedsTheDeclarationOnBothEnds() throws Exception {
        TestTls.init();
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = sessions(loops).allowRecordMode((c, m, p) -> true).build();
            var server = ServerConfig.builder(gate(null), NODE).allowRecordMode((c, m, p) -> c.isSecure() && c.isFirstPipe()).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()).withSecure()).inboundTransform(Seal.server(server)).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var tls = TlsOptions.client(TestTls.client(), "localhost");
                // Without the client's declaration nothing is sent.
                PipeIo plain = Zmux.dialTlsPipeAsync(l.localAddress(), tls, cfg).get(10, TimeUnit.SECONDS);
                var bad = assertThrows(ExecutionException.class, () -> establish0(plain, client().mode(RecordMode.BOUND).build()));
                assertEquals(SecureCarrierException.Code.INVALID_CONFIG, ((SecureCarrierException) bad.getCause()).code());
                PipeIo p = Zmux.dialTlsPipeAsync(l.localAddress(), tls.withSecure(), cfg).get(10, TimeUnit.SECONDS);
                assertTrue(p.isSecure());
                try (var c = Zmux.open(establish(p, client().mode(RecordMode.BOUND).build()), Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    assertEquals(RecordMode.BOUND, c.weakestRecordMode());
                    assertEquals(RecordMode.BOUND, s.weakestRecordMode());
                    echo(c, s);
                    ZmuxPipe j = c.addTlsPipe(l.localAddress(), tls.withSecure(), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                    assertEquals(RecordMode.BOUND, j.stats().orElseThrow().recordMode());
                    echo(c, s);
                }
            }
        }
    }

    private static PipeIo establish0(PipeIo p, ClientConfig c) throws Exception {
        return Seal.establish(p, c).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** A Bound Hello on a carrier with another exporter value is auth: a third party terminates the carrier. */
    @Test void boundThroughAPartyWithAnotherExporterIsAuth() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).allowRecordMode((c, m, p) -> true).build();
            List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
            var server = ServerConfig.builder(gate(null), NODE).allowRecordMode((c, m, p) -> true).build();
            var lc = ListenerConfig.builder().session(cfg).onReject(i -> { rejected.add(i); return RejectInfo.Action.CLOSE; }).inboundTransform(Seal.server(server)).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(pair[1].withExporter((a, b, n) -> new byte[n]).withSecure(true), JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                PipeIo near = pair[0].withExporter((a, b, n) -> { byte[] o = new byte[n]; Arrays.fill(o, (byte) 1); return o; }).withSecure(true);
                var f = Seal.establish(near, client().mode(RecordMode.BOUND).handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
                for (long end = System.nanoTime() + 5_000_000_000L; rejected.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
                assertEquals("auth", rejected.getFirst().reason());
            }
        }
    }

    /** Carrier first bytes reach the session in order, before any later byte of the carrier. */
    @Test void carrierFirstBytesComeBeforeTheCarriersBytes() throws Exception {
        EstablishFirstBytes.run();
    }

    /** The responder vectors of negative.json with this name, as their steps. */
    @SuppressWarnings("unchecked")
    static List<java.util.Map<String, Object>> negativeSteps(String name) throws Exception {
        var all = (List<Object>) io.zmux.testutil.Json.parse(java.nio.file.Files.readString(
                io.zmux.testutil.SpecFixtures.root().resolve("secure-carrier/vectors/negative.json")));
        for (Object o : all) {
            var c = (java.util.Map<String, Object>) o;
            if (name.equals(c.get("name"))) return (List<java.util.Map<String, Object>>) (List<?>) c.get("steps");
        }
        throw new AssertionError("no vector " + name);
    }

    static ListenerConfig profile4(ZmuxConfig cfg, List<RejectInfo> rejected) {
        return ListenerConfig.builder().session(cfg).onReject(i -> { rejected.add(i); return RejectInfo.Action.STALL; })
                .inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).acceptedProfiles(Profile.XXHFS_CHACHA, Profile.XXHFS_AES).build())).build();
    }

    /** A Profile 4 or 5 Hello whose e is of low order or whose ek has a coefficient out of range is refused as kex: OnReject and a stall, no byte. */
    @Test void aBadKeyExchangeInTheHelloIsARefusal() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).handshakeTimeout(Duration.ofMillis(800)).build();
            List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
            List<String> names = List.of("low-order-initiator-ephemeral-no-psk", "encapsulation-key-coefficient-first-3329-no-psk",
                    "encapsulation-key-coefficient-last-4095-no-psk", "aes-low-order-ephemeral", "aes-encapsulation-key-invalid-no-psk");
            try (var l = Zmux.listen(NetworkTest.any(), profile4(cfg, rejected))) {
                for (String n : names) {
                    try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                        long start = System.nanoTime();
                        ch.write(ByteBuffer.wrap(io.zmux.testutil.SpecFixtures.hex(negativeSteps(n).getFirst().get("wire"))));
                        assertEquals(0, readToEnd(ch), n + ": nothing is written");
                        long ms = (System.nanoTime() - start) / 1_000_000;
                        assertTrue(ms >= 600, n + ": stalled, not closed (" + ms + " ms)");
                    }
                }
                for (long end = System.nanoTime() + 5_000_000_000L; rejected.size() < names.size() && System.nanoTime() < end; ) Thread.sleep(10);
                for (RejectInfo i : rejected) assertEquals("kex", i.reason());
                assertEquals(names.size(), rejected.size());
                assertEquals(0, l.stats().sessions());
            }
        }
    }

    /** A message 3 that does not parse is a refusal (OnReject); a carrier that ends instead closes without one. */
    @Test void aBadFinishIsARefusalAndAnEndIsNot() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = sessions(loops).handshakeTimeout(Duration.ofSeconds(5)).build();
            List<RejectInfo> rejected = new CopyOnWriteArrayList<>();
            var steps = negativeSteps("encapsulation-key-coefficient-3328-valid");
            byte[] hello = io.zmux.testutil.SpecFixtures.hex(steps.get(0).get("wire"));
            byte[] finish = io.zmux.testutil.SpecFixtures.hex(steps.get(2).get("wire"));
            try (var l = Zmux.listen(NetworkTest.any(), profile4(cfg, rejected))) {
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap(hello));
                    assertTrue(ch.read(ByteBuffer.allocate(1 << 16)) > 0, "the Reply");
                    ch.write(ByteBuffer.wrap(finish));
                    for (long end = System.nanoTime() + 5_000_000_000L; rejected.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
                    assertEquals(1, rejected.size());
                    assertEquals("malformed", rejected.getFirst().reason());
                }
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap(hello));
                    assertTrue(ch.read(ByteBuffer.allocate(1 << 16)) > 0, "the Reply");
                    long start = System.nanoTime();
                    ch.shutdownOutput();
                    readToEnd(ch);
                    assertTrue(System.nanoTime() - start < 3_000_000_000L, "closed at the end, not stalled");
                }
                Thread.sleep(200);
                assertEquals(1, rejected.size(), "an end is no refusal");
            }
        }
    }

    /**
     * A record that fails authentication after the session is up ends the pipe's input: the session
     * does not wait for a carrier end that the abortive close never reports.
     */
    @Test void aForgedRecordEndsTheSessionAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(1);
             var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(sessions(loops).build())
                     .inboundTransform(Seal.server(ServerConfig.builder(gate(null), NODE).build())).build());
             var relay = java.nio.channels.ServerSocketChannel.open().bind(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0))) {
            var toServer = new java.util.concurrent.CompletableFuture<SocketChannel>();
            Thread.ofVirtual().start(() -> {
                try (SocketChannel in = relay.accept(); SocketChannel out = SocketChannel.open(l.localAddress())) {
                    toServer.complete(out);
                    Thread back = Thread.ofVirtual().start(() -> copy(out, in));
                    copy(in, out);
                    back.join();
                } catch (Exception ended) { /* The test is over. */ }
            });
            PipeIo p = establish(tcp((InetSocketAddress) relay.getLocalAddress()), client().build());
            try (var c = Zmux.open(p, Role.CLIENT, sessions(loops).build()); var s = l.accept(WAIT)) {
                echo(c, s);
                // A whole record of the stream framing whose tag cannot verify, between two of the client's.
                ByteBuffer bad = ByteBuffer.allocate(2 + 32).putShort((short) 32);
                bad.flip();
                toServer.get(WAIT.toSeconds(), TimeUnit.SECONDS).write(bad);
                assertNotNull(s.closed().get(5, TimeUnit.SECONDS));
            }
        }
    }

    private static void copy(SocketChannel from, SocketChannel to) {
        ByteBuffer b = ByteBuffer.allocate(1 << 16);
        try {
            while (from.read(b) >= 0) { b.flip(); while (b.hasRemaining()) to.write(b); b.clear(); }
        } catch (java.io.IOException ended) { /* Either side closed. */ }
    }
}
