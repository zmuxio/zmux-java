package io.zmux;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.carrier.PipeIo;
import io.zmux.seal.SecureCarrierException;
import io.zmux.seal.AppData;
import io.zmux.seal.AppDataSource;
import io.zmux.seal.Peer;
import io.zmux.seal.Principal;
import io.zmux.seal.Profile;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import io.zmux.seal.VerifyVerdict;
import io.zmux.seal.VerifyInput;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Application data of Establish messages 1, 2 and 3 over real TCP. */
class AppDataTest {
    private record Asked(int message, AppDataSource.Known peer) {}

    private static AppDataSource answering(List<Asked> asked, java.util.function.IntFunction<byte[]> data) {
        return (msg, peer) -> {
            asked.add(new Asked(msg, peer));
            return CompletableFuture.completedFuture(Optional.ofNullable(data.apply(msg)));
        };
    }

    private static void same(List<AppData> want, List<AppData> got) {
        assertEquals(want.size(), got.size(), got.toString());
        for (int i = 0; i < want.size(); i++) {
            assertEquals(want.get(i).message(), got.get(i).message());
            assertArrayEquals(want.get(i).data(), got.get(i).data());
            assertEquals(want.get(i).forwardSecret(), got.get(i).forwardSecret());
            assertEquals(want.get(i).authenticated(), got.get(i).authenticated());
        }
    }

    /** Each piece reaches the peer with its message and protection; each source sees what is known of the peer then. */
    @Test void dataOfEveryMessage() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).build();
            List<Asked> client = new CopyOnWriteArrayList<>(), server = new CopyOnWriteArrayList<>();
            List<VerifyInput> judged = new CopyOnWriteArrayList<>();
            var sc = ServerConfig.builder(SealTest.gate(null), SealTest.NODE)
                    .anchor(in -> { judged.add(in); return new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic())); }).localContext("edge")
                    .appData(answering(server, m -> "two".getBytes())).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc)).build())) {
                PipeIo p = SealTest.establish(SealTest.tcp(l.localAddress()), SealTest.client().appData(answering(client, m -> new byte[] {(byte) m})).build());
                List<AppData> fromClient = List.of(new AppData(1, new byte[] {1}, false, false), new AppData(3, new byte[] {3}, true, true));
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(SealTest.WAIT)) {
                    same(fromClient, ((Peer) s.admitted().orElseThrow()).appData());
                }
                same(List.of(new AppData(2, "two".getBytes(), true, true)), Seal.peerOf(p).orElseThrow().appData());
                // The anchor sees the data before its own verification: none of it is authenticated yet.
                same(List.of(new AppData(1, new byte[] {1}, false, false), new AppData(3, new byte[] {3}, true, false)), judged.getFirst().peerAppData());
                assertEquals(2, client.size());
                assertTrue(client.get(0).peer().profile().hasPsk());
                assertNull(client.get(0).peer().input());
                assertArrayEquals(SealTest.NODE.publicKey(), client.get(1).peer().input().peerStatic());
                same(List.of(new AppData(2, "two".getBytes(), true, false)), client.get(1).peer().input().peerAppData());
                assertTrue(client.get(1).peer().verdict() != null);
                assertEquals(1, server.size());
                assertEquals(2, server.getFirst().message());
                VerifyInput known = server.getFirst().peer().input();
                assertNull(server.getFirst().peer().verdict());
                assertArrayEquals(SealTest.HP.handle(), known.pskIdentity());
                assertNull(known.peerStatic());
                assertEquals("edge", known.localContext());
                same(fromClient.subList(0, 1), known.peerAppData());
            }
        }
    }

    /**
     * Message 1 of a Profile without a pre-shared key carries none; a failed source ends the handshake; data above the
     * receiver's bound, and data the message cannot hold, are too_large.
     */
    @Test void limits() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).build();
            List<String> reasons = new CopyOnWriteArrayList<>();
            var sc = ServerConfig.builder(SealTest.gate(null), SealTest.NODE).acceptedProfiles(Profile.XXHFS_CHACHA, Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_PSK0_AES).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc))
                    .onReject(i -> { reasons.add(i.reason()); return RejectInfo.Action.CLOSE; }).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                List<Asked> asked = new CopyOnWriteArrayList<>();
                var noPsk = io.zmux.seal.ClientConfig.builder().anchor(SealTest.client().build().anchor()).allowedProfiles(Profile.XXHFS_CHACHA)
                        .appData(answering(asked, m -> null)).build();
                PipeIo p = SealTest.establish(SealTest.tcp(l.localAddress()), noPsk);
                p.writer().abort(new java.io.IOException("done"));
                assertEquals(List.of(3), asked.stream().map(Asked::message).toList());

                var failing = SealTest.client().appData((m, peer) -> CompletableFuture.failedFuture(new IllegalStateException("no data"))).build();
                var e = assertFails(Seal.establish(SealTest.tcp(l.localAddress()), failing).toCompletableFuture());
                assertTrue(e instanceof java.io.IOException && e.getCause() instanceof IllegalStateException, e.toString());

                var over = SealTest.client().appData((m, peer) -> CompletableFuture.completedFuture(Optional.of(new byte[300]))).handshakeTimeout(java.time.Duration.ofSeconds(2)).build();
                assertFails(Seal.establish(SealTest.tcp(l.localAddress()), over).toCompletableFuture());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!reasons.contains("too_large") && System.nanoTime() < until) Thread.sleep(2);
                assertTrue(reasons.contains("too_large"), reasons.toString());

                var huge = SealTest.client().appData((m, peer) -> CompletableFuture.completedFuture(Optional.of(new byte[70000]))).build();
                var big = assertFails(Seal.establish(SealTest.tcp(l.localAddress()), huge).toCompletableFuture());
                assertEquals(SecureCarrierException.Code.TOO_LARGE, ((SecureCarrierException) big).code());
            }
        }
    }

    private static Throwable assertFails(CompletableFuture<PipeIo> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS).writer().abort(new java.io.IOException("unexpected"));
            throw new AssertionError("the handshake succeeded");
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }
}
