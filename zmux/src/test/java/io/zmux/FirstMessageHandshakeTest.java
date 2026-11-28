package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.seal.ClientConfig;
import io.zmux.seal.HandshakeLimits;
import io.zmux.seal.KeyAlg;
import io.zmux.seal.KeyId;
import io.zmux.seal.PinAnchor;
import io.zmux.seal.Profile;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import io.zmux.seal.Signer;
import io.zmux.seal.SigningKey;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class FirstMessageHandshakeTest {

    private static final byte[] EXPORTER = new byte[32];

    private static final class Exchange implements AutoCloseable {
        final PipeIo io;
        final CompletableFuture<PipeIo> server;
        final LinkedBlockingQueue<byte[]> output = new LinkedBlockingQueue<>();

        Exchange(Framing framing, ServerConfig config) {
            this(framing, config, java.util.function.UnaryOperator.identity());
        }

        Exchange(Framing framing, ServerConfig config, java.util.function.UnaryOperator<PipeIo> setup) {
            var pipe = MemoryPipe.pair(framing);
            io = pipe[0];
            io.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(Chunk chunk, Runnable consumed) {
                    try (chunk) { output.add(chunk.copy()); }
                    finally { consumed.run(); }
                }
                @Override public void onEnd() {}
                @Override public void onError(Throwable cause) {}
            });
            server = Seal.server(config).transform(setup.apply(pipe[1]), context()).toCompletableFuture();
        }

        void send(byte[] wire) { write(io, wire); }

        void refuses(String reason) throws Exception {
            var failure = assertThrows(ExecutionException.class, () -> server.get(5, TimeUnit.SECONDS));
            var rejection = assertInstanceOf(CarrierRejectedException.class, failure.getCause());
            assertEquals(reason, rejection.reason());
            assertTrue(output.isEmpty(), "refusal emitted a reply");
        }

        @Override public void close() {
            server.cancel(false);
            io.writer().close();
            io.reader().close();
        }
    }

    private static List<?> list(String file) throws IOException {
        return (List<?>) Json.parse(Files.readString(SpecFixtures.root().resolve("secure-carrier/vectors").resolve(file)));
    }

    private static Map<?, ?> vector(String file, String name) throws IOException {
        for (Object value : list(file)) {
            Map<?, ?> row = (Map<?, ?>) value;
            if (name.equals(row.get("name"))) return row;
        }
        throw new AssertionError("no such vector " + name);
    }

    private static byte[] wire(Map<?, ?> handshake, int index, Framing framing) {
        Map<?, ?> message = (Map<?, ?>) ((List<?>) handshake.get("messages")).get(index);
        return SpecFixtures.hex(message.get(framing == Framing.MESSAGES ? "wire_message" : "wire_stream"));
    }

    private static InboundTransform.Context context() {
        return new InboundTransform.Context(JoinHint.inBand(TestAddr.peer("client")), Optional.empty(),
                new Admission.Deadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(15)));
    }

    private static void write(PipeIo pipe, byte[] bytes) {
        pipe.writer().write(OutboundBatch.of(ByteBuffer.wrap(bytes)), new PipeWriter.Completion() {
            @Override public void completed() {}
            @Override public void failed(Throwable cause) {}
        });
    }

    private static ServerConfig.Builder server(Profile... profiles) {
        return ServerConfig.builder(SealTest.gate(null), SealTest.NODE).acceptedProfiles(profiles)
                .allowRecordMode((carrier, mode, peer) -> true);
    }

    private static PipeIo secure(PipeIo pipe) {
        return pipe.withCarrierSecure().withExporter((label, context, size) -> EXPORTER.clone());
    }

    @Test void anonymousScenariosRespectTheReplyBoundary() throws Exception {
        var root = Json.object(Files.readString(SpecFixtures.root().resolve("secure-carrier/vectors/first_message_cases.json")));
        var cases = (List<?>) ((Map<?, ?>) root.get("anonymous")).get("cases");
        assertEquals(10, cases.size());
        for (Object value : cases) {
            var scenario = (Map<?, ?>) value;
            if (scenario.get("handshake") instanceof String name) {
                Map<?, ?> example = vector("handshake.json", name);
                Profile profile = Profile.of(((Number) example.get("profile")).longValue());
                RecordMode mode = RecordMode.values()[((Number) example.get("mode")).intValue()];
                for (Framing framing : List.of(Framing.BYTE_STREAM, Framing.MESSAGES)) {
                    var pipe = MemoryPipe.pair(framing);
                    var responder = Seal.server(server(profile).handshakeLimits(new HandshakeLimits(2, 1)).build())
                            .transform(secure(pipe[1]), context()).toCompletableFuture();
                    var clientConfig = ClientConfig.builder().allowedProfiles(profile).mode(mode)
                            .anchor(new PinAnchor(KeyId.staticKeyId(SealTest.NODE.publicKey()))).build();
                    var client = Seal.establish(secure(pipe[0]), clientConfig).toCompletableFuture();
                    try {
                        PipeIo server = responder.get(5, TimeUnit.SECONDS);
                        assertEquals(profile, Seal.peerOf(server).orElseThrow().profile());
                        assertEquals(mode, Seal.peerOf(client.get(5, TimeUnit.SECONDS)).orElseThrow().mode());
                    } finally {
                        client.cancel(false);
                        responder.cancel(false);
                        for (PipeIo endpoint : pipe) { endpoint.writer().close(); endpoint.reader().close(); }
                    }
                }
                continue;
            }

            var anonymousHello = vector("handshake.json", "profile4-sealed-empty");
            for (Framing framing : List.of(Framing.BYTE_STREAM, Framing.MESSAGES)) {
                if ("stream".equals(scenario.get("framing")) && framing != Framing.BYTE_STREAM) continue;
                byte[] hello = wire(anonymousHello, 0, framing);
                var config = server(Profile.XXHFS_CHACHA).handshakeLimits(new HandshakeLimits(2, 1));
                String reason;
                if (Boolean.FALSE.equals(scenario.get("accept_profile"))) {
                    config.acceptedProfiles(Profile.XXHFS_PSK0_CHACHA);
                    reason = "profile_disabled";
                } else if (Boolean.FALSE.equals(scenario.get("capacity_available"))) {
                    var entered = new CompletableFuture<Void>();
                    var release = new CompletableFuture<Optional<byte[]>>();
                    var limited = config.appData((msg, peer) -> { entered.complete(null); return release; }).build();
                    try (var first = new Exchange(framing, limited); var excess = new Exchange(framing, limited)) {
                        first.send(hello);
                        entered.get(5, TimeUnit.SECONDS);
                        excess.send(hello);
                        excess.refuses("handshake_capacity");
                    } finally {
                        release.complete(Optional.empty());
                    }
                    continue;
                } else if (scenario.get("append_to_hello_hex") instanceof String extra) {
                    hello = append(hello, SpecFixtures.hex(extra));
                    reason = "trailing_bytes";
                } else if (scenario.get("truncate_noise_msg1_to") instanceof Number length) {
                    int head = framing == Framing.MESSAGES ? 2 : 4;
                    hello = Arrays.copyOf(hello, head + length.intValue());
                    resizeHello(hello, framing);
                    reason = "malformed";
                } else if (scenario.get("append_to_noise_msg1_hex") instanceof String data) {
                    hello = append(hello, SpecFixtures.hex(data));
                    resizeHello(hello, framing);
                    reason = "malformed";
                } else if (scenario.get("replace_first_kem_coefficient") instanceof Number coefficient) {
                    int offset = (framing == Framing.MESSAGES ? 2 : 4) + 32;
                    hello[offset] = (byte) coefficient.intValue();
                    hello[offset + 1] = (byte) ((hello[offset + 1] & 0xf0) | coefficient.intValue() >>> 8);
                    reason = "kex";
                } else throw new AssertionError(scenario);

                try (var exchange = new Exchange(framing, config.build())) {
                    exchange.send(hello);
                    exchange.refuses(reason);
                }
            }
        }
    }

    private static void resizeHello(byte[] hello, Framing framing) {
        if (framing == Framing.BYTE_STREAM) ByteBuffer.wrap(hello).putShort(2, (short) (hello.length - 4));
    }

    private static byte[] append(byte[] hello, byte[] extra) {
        byte[] result = Arrays.copyOf(hello, hello.length + extra.length);
        System.arraycopy(extra, 0, result, hello.length, extra.length);
        return result;
    }

    @Test void localFailuresBeforeTheReplyUseTheSameRefusalPath() throws Exception {
        var bound = vector("handshake.json", "profile5-bound-empty");
        var bounded = server(Profile.XXHFS_AES).handshakeLimits(new HandshakeLimits(2, 1)).build();
        try (var exchange = new Exchange(Framing.BYTE_STREAM, bounded,
                pipe -> pipe.withCarrierSecure().withExporter((label, context, size) -> {
                    throw new IllegalStateException("exporter unavailable");
                }))) {
            exchange.send(wire(bound, 0, Framing.BYTE_STREAM));
            exchange.refuses("binding_unavailable");
        }

        var example = vector("handshake.json", "profile1-sealed-empty");
        var brokenSigner = new Signer() {
            @Override public KeyAlg alg() { throw new IllegalStateException("signer unavailable"); }
            @Override public byte[] publicKey() { throw new AssertionError("unexpected publicKey call"); }
            @Override public byte[] signProof(byte[] message) { throw new AssertionError("an invalid signer ran"); }
        };
        var server = ServerConfig.builder(vectorGate(example), SealTest.NODE).signer(brokenSigner).build();
        try (var exchange = new Exchange(Framing.BYTE_STREAM, server)) {
            exchange.send(wire(example, 0, Framing.BYTE_STREAM));
            exchange.refuses("local_error");
        }
    }

    @Test void pskResponseVectorsEmitOnlyTheAllowedFlight() throws Exception {
        var vectors = Json.object(Files.readString(SpecFixtures.root().resolve("secure-carrier/vectors/first_message_cases.json")));
        var frames = Json.object(Files.readString(SpecFixtures.root().resolve("secure-carrier/vectors/first_message.json")));
        try (var loops = EventLoops.create(1)) {
            for (Object value : (List<?>) vectors.get("responses")) {
                var scenario = (Map<?, ?>) value;
                if (!(scenario.get("expect_server_flights") instanceof List<?> expected)) continue;
                Map<?, ?> example;
                byte[] hello;
                byte[] finish = null;
                if (scenario.get("negative") instanceof String name) {
                    example = vector("negative.json", name);
                    List<?> steps = (List<?>) example.get("steps");
                    hello = SpecFixtures.hex(((Map<?, ?>) steps.getFirst()).get("wire"));
                    if (!expected.isEmpty()) finish = SpecFixtures.hex(((Map<?, ?>) steps.getLast()).get("wire"));
                } else {
                    example = vector("handshake.json", (String) scenario.get("handshake"));
                    hello = wire(example, 0, Framing.BYTE_STREAM);
                }

                var server = ServerConfig.builder(vectorGate(example), SealTest.NODE).build();
                var config = ListenerConfig.builder().session(SealTest.sessions(loops).build()).inboundTransform(Seal.server(server));
                var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
                if (scenario.get("prefix") instanceof String tokenName) {
                    var ticket = ((List<?>) frames.get("prefix")).stream().map(Map.class::cast)
                            .filter(row -> tokenName.equals(row.get("name"))).findFirst().orElseThrow();
                    var generation = new PrefixStore.Generation(SpecFixtures.hex(ticket.get("outer")), 0, -1);
                    var slot = Duration.ofSeconds(((Number) ticket.get("slot_seconds")).longValue());
                    var store = new MemoryPrefixStore(new PrefixStore.Window(slot, 1), List.of(generation));
                    store.addTicket(new PrefixStore.Ticket(PrefixStore.Id.of(SpecFixtures.hex(ticket.get("id"))),
                            SpecFixtures.hex(ticket.get("ticket_key")), 1, 0, -1, true));
                    byte[] binding = SpecFixtures.hex(ticket.get("binding"));
                    var time = java.time.Instant.ofEpochSecond(((Number) ticket.get("unix_seconds")).longValue());
                    var verifier = new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(binding.length != 0, false),
                            store, 2, java.time.Clock.fixed(time, java.time.ZoneOffset.UTC));
                    config.admission(verifier.admission().build());
                    pipe[1] = pipe[1].withOuterSecurity(Admission.OuterSecurity.TLS_1_3)
                            .withExporter((label, context, size) -> binding.clone());
                    hello = append(SpecFixtures.hex(ticket.get("token")), hello);
                }

                var flights = new LinkedBlockingQueue<byte[]>();
                pipe[0].reader().start(new PipeReader.Sink() {
                    @Override public void onChunk(Chunk chunk, Runnable consumed) {
                        try (chunk) { flights.add(chunk.copy()); }
                        finally { consumed.run(); }
                    }
                    @Override public void onEnd() {}
                    @Override public void onError(Throwable cause) {}
                });
                try (var listener = Zmux.listener(config.build())) {
                    var handled = listener.adapter().handle(pipe[1], JoinHint.inBand(TestAddr.peer("client")));
                    write(pipe[0], hello);
                    if (!expected.isEmpty()) {
                        assertNotNull(flights.poll(5, TimeUnit.SECONDS), scenario.toString());
                        if (finish != null) write(pipe[0], finish);
                    }
                    if (finish == null) pipe[0].writer().close();
                    handled.closed().get(5, TimeUnit.SECONDS);
                    assertEquals(0, listener.stats().sessions(), scenario.toString());
                    assertTrue(flights.isEmpty(), scenario.toString());
                } finally {
                    pipe[0].writer().close();
                    pipe[0].reader().close();
                }
            }
        }
    }

    private static io.zmux.seal.Gate vectorGate(Map<?, ?> fixture) {
        byte[] expected = SpecFixtures.hex(fixture.get("handle"));
        byte[] psk = SpecFixtures.hex(fixture.get("psk"));
        return new io.zmux.seal.Gate() {
            @Override public byte[] resolve(byte[] handle, java.net.SocketAddress source) {
                return Arrays.equals(handle, expected) ? psk.clone() : null;
            }
            @Override public io.zmux.seal.Credential credential(byte[] handle, Profile profile, java.net.SocketAddress source) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest request, java.net.SocketAddress source) { return null; }
        };
    }

    private record Signing(Thread worker, CompletableFuture<Void> release) {
        void finish() { release.complete(null); }
    }

    @Test void permitsReservePskCapacityAndFollowActualSignerWork() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new HandshakeLimits(1, 1));
        var anon = vector("handshake.json", "profile4-sealed-empty");
        var psk = vector("handshake.json", "profile1-sealed-empty");
        byte[] anonymousHello = wire(anon, 0, Framing.BYTE_STREAM);
        byte[] pskHello = wire(psk, 0, Framing.BYTE_STREAM);
        var signed = new LinkedBlockingQueue<Signing>();
        var key = SigningKey.fromSeed(KeyAlg.ML_DSA_44, new byte[32]);
        var signer = new Signer() {
            @Override public KeyAlg alg() { return key.alg(); }
            @Override public byte[] publicKey() { return key.publicKey(); }
            @Override public byte[] signProof(byte[] message) throws Exception {
                var signing = new Signing(Thread.currentThread(), new CompletableFuture<>());
                signed.add(signing);
                signing.release().join();
                return key.signProof(message);
            }
        };
        var config = ServerConfig.builder(vectorGate(psk), SealTest.NODE)
                .acceptedProfiles(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_CHACHA)
                .handshakeLimits(new HandshakeLimits(2, 1)).signer(signer).build();
        try (var anonymous = new Exchange(Framing.BYTE_STREAM, config);
             var keyed = new Exchange(Framing.BYTE_STREAM, config)) {
            anonymous.send(anonymousHello);
            Signing a = signed.poll(5, TimeUnit.SECONDS);
            assertNotNull(a);
            try {
                assertTrue(anonymous.server.cancel(false));
                try (var denied = new Exchange(Framing.BYTE_STREAM, config)) {
                    denied.send(anonymousHello);
                    denied.refuses("handshake_capacity");
                }
                keyed.send(pskHello);
                Signing b = signed.poll(5, TimeUnit.SECONDS);
                assertNotNull(b, "PSK has a reserved permit");
                try {
                    try (var denied = new Exchange(Framing.BYTE_STREAM, config)) {
                        denied.send(pskHello);
                        denied.refuses("handshake_capacity");
                    }
                    b.finish();
                    assertNotNull(keyed.output.poll(5, TimeUnit.SECONDS), "message 2 is written");
                    assertFalse(keyed.server.isDone(), "message 3 is still outstanding");
                    try (var next = new Exchange(Framing.BYTE_STREAM, config)) {
                        next.send(pskHello);
                        Signing c = signed.poll(5, TimeUnit.SECONDS);
                        assertNotNull(c, "waiting for message 3 does not occupy a permit");
                        c.finish();
                        assertNotNull(next.output.poll(5, TimeUnit.SECONDS));
                    }
                } finally {
                    b.finish();
                }
            } finally {
                a.finish();
                a.worker().join(5000);
            }
            assertFalse(a.worker().isAlive());
            assertTrue(anonymous.output.isEmpty(), "a cancelled signer cannot send a reply");
            try (var next = new Exchange(Framing.BYTE_STREAM, config)) {
                next.send(anonymousHello);
                Signing d = signed.poll(5, TimeUnit.SECONDS);
                assertNotNull(d, "returning from the cancelled callback releases the anonymous permit");
                try {
                    next.send(new byte[] {0});
                } finally {
                    d.finish();
                }
                next.refuses("trailing_bytes");
            }
        }
    }
}
