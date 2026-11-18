package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.Limiter;
import io.zmux.helper.PacketStream;
import io.zmux.helper.PacketStream.Options;
import io.zmux.helper.PacketStream.SendPolicy;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class PacketStreamTest {
    static final Preamble HELLO = Preamble.builder().openInfo(new byte[] {'p'}).build();
    static ZmuxConfig config(EventLoops loops, boolean datagrams) {
        return ZmuxConfig.builder().eventLoops(loops).enableDatagrams(datagrams).build();
    }
    static ByteBuffer bytes(int n, int seed) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i * 31 + seed);
        return ByteBuffer.wrap(b);
    }
    static byte[] array(ByteBuffer b) { byte[] a = new byte[b.remaining()]; b.duplicate().get(a); return a; }

    /** EXTENSIONS.md Section 12 capsule vectors of zmux-spec/fixtures/capsules.json. */
    @Test void capsuleVectorsDecodeAndEncode() throws Exception {
        @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
        var cases = (List<Map<String, Object>>) Json.object(Files.readString(SpecFixtures.root().resolve("fixtures/capsules.json"))).get("cases");
        assertFalse(cases.isEmpty());
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false))) {
            for (var c : cases) {
                String id = (String) c.get("id");
                byte[] wire = SpecFixtures.hex(c.get("hex"));
                int max = ((Long) c.get("max_payload")).intValue();
                // Parsed JSON: the vector file's schema fixes the type.
                @SuppressWarnings("unchecked") var expected = (List<Map<String, Object>>) c.get("packets");
                Object error = c.get("error");

                ZmuxStream raw = pair.client().openStream(HELLO);
                raw.write(wire); raw.finish();
                var receiver = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)), new Options(SendPolicy.STREAM_ONLY, max));
                var got = new ArrayList<StreamDatagram>();
                PacketErrorException failure = null;
                try {
                    for (StreamDatagram p; (p = receiver.receive(Duration.ofSeconds(3))) != null; ) got.add(p);
                } catch (PacketErrorException e) { failure = e; }
                assertEquals(expected.size(), got.size(), id);
                for (int i = 0; i < got.size(); i++) {
                    assertEquals(((Number) expected.get(i).get("context")).longValue(), got.get(i).context(), id);
                    assertArrayEquals(SpecFixtures.hex(expected.get(i).get("payload")), got.get(i).payload(), id);
                }
                assertEquals(((Long) c.get("skipped")).longValue(), receiver.stats().capsulesSkipped(), id);
                if (error == null) assertNull(failure, id);
                else {
                    assertNotNull(failure, id);
                    var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(raw), id);
                    assertEquals(PacketStream.PACKET_ERROR, reset.code(), id);
                }
                receiver.close(); raw.close();

                if (Boolean.TRUE.equals(c.get("encode"))) {
                    var sender = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.STREAM_ONLY, max));
                    for (var p : expected) sender.send(((Number) p.get("context")).longValue(), ByteBuffer.wrap(SpecFixtures.hex(p.get("payload"))));
                    sender.close();
                    try (var peer = pair.server().acceptStream(Duration.ofSeconds(3))) {
                        assertEquals(HexFormat.of().formatHex(wire), HexFormat.of().formatHex(SessionTest.readAll(peer)), id);
                    }
                }
            }
        }
    }

    /** finishAsync is a half-close: the FIN follows the queued capsules and the reply is still received. */
    @Test void finishKeepsReceiving() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, true))) {
            var client = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.STREAM_ONLY, 1 << 16));
            var queued = client.sendAsync(3, bytes(40000, 1));
            client.finishAsync().get(5, TimeUnit.SECONDS);
            assertTrue(queued.get(5, TimeUnit.SECONDS));
            var server = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)), new Options(SendPolicy.STREAM_ONLY, 1 << 16));
            var got = server.receive(Duration.ofSeconds(3));
            assertEquals(3, got.context()); assertArrayEquals(array(bytes(40000, 1)), got.payload());
            assertNull(server.receive(Duration.ofSeconds(3)));
            server.send(4, bytes(10, 2)); server.close();
            var reply = client.receive(Duration.ofSeconds(3));
            assertEquals(4, reply.context()); assertArrayEquals(array(bytes(10, 2)), reply.payload());
            assertNull(client.receive(Duration.ofSeconds(3)));
            client.close();
        }
    }

    @Test void policiesChooseTheFormAtEachSend() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, true))) {
            ZmuxStream underlying = pair.client().openStream(HELLO); underlying.flush();
            var prefer = PacketStream.of(underlying);
            var peer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            int mtu = underlying.maxDatagramPayload(9);
            assertTrue(mtu > 1000 && mtu < 65535);
            assertTrue(prefer.send(9, bytes(100, 1)));
            assertTrue(prefer.send(9, bytes(mtu + 1, 2))); // Above the datagram limit: a capsule.
            var first = new ArrayList<StreamDatagram>();
            first.add(peer.receive(Duration.ofSeconds(3))); first.add(peer.receive(Duration.ofSeconds(3)));
            first.sort((x, y) -> Integer.compare(x.payload().length, y.payload().length));
            assertEquals(9, first.get(0).context()); assertArrayEquals(array(bytes(100, 1)), first.get(0).payload());
            assertArrayEquals(array(bytes(mtu + 1, 2)), first.get(1).payload());
            assertEquals(1, prefer.stats().sentAsDatagrams()); assertEquals(1, prefer.stats().sentAsCapsules());
            assertEquals(1, peer.stats().receivedAsDatagrams()); assertEquals(1, peer.stats().receivedAsCapsules());
            assertThrows(IllegalArgumentException.class, () -> prefer.send(ByteBuffer.allocate(PacketStream.DEFAULT_MAX_PACKET + 1)));
            prefer.close();
            assertNull(peer.receive(Duration.ofSeconds(3)));
            peer.close();

            var only = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.STREAM_ONLY, PacketStream.DEFAULT_MAX_PACKET));
            var onlyPeer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            for (int i = 0; i < 20; i++) only.send(i, bytes(10, i));
            for (int i = 0; i < 20; i++) assertEquals(i, onlyPeer.receive(Duration.ofSeconds(3)).context(), "capsules stay in order");
            assertEquals(20, only.stats().sentAsCapsules()); assertEquals(0, only.stats().sentAsDatagrams());
            only.close(); onlyPeer.close();

            var datagrams = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.DATAGRAM_ONLY, PacketStream.DEFAULT_MAX_PACKET));
            var datagramPeer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            assertTrue(datagrams.send(3, bytes(50, 3)));
            assertEquals(3, datagramPeer.receive(Duration.ofSeconds(3)).context());
            ByteBuffer big = bytes(mtu + 1, 4);
            assertThrows(DatagramTooLargeException.class, () -> datagrams.send(9, big));
            assertEquals(mtu + 1, big.remaining(), "a refused packet is not consumed");
            assertEquals(1, datagrams.stats().droppedTooLarge()); assertEquals(1, datagrams.stats().dropped());
            datagrams.close(); datagramPeer.close();
        }
    }

    @Test void limiterRefusalDropsWithoutACapsule() throws Exception {
        Limiter refuse = new Limiter() {
            @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
            @Override public long tryAcquire(long min, long max) { return 0; }
            @Override public void refund(long n) {}
        };
        try (EventLoops loops = EventLoops.create(2);
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(refuse, null)).build())) {
            var s = PacketStream.open(pair.client(), HELLO, Options.defaults());
            var peer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            assertFalse(s.send(ByteBuffer.wrap(new byte[] {1})));
            assertEquals(1, s.stats().droppedLimited()); assertEquals(0, s.stats().sentAsCapsules());
            s.close();
            assertNull(peer.receive(Duration.ofSeconds(3)), "nothing went as a capsule");
            peer.close();
        }
    }

    @Test void withoutDatagramsEverythingGoesAsCapsules() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false))) {
            var s = PacketStream.open(pair.client(), HELLO, Options.defaults());
            var peer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            for (int i = 0; i < 10; i++) assertTrue(s.send(i, bytes(1000, i)));
            for (int i = 0; i < 10; i++) {
                var p = peer.receive(Duration.ofSeconds(3));
                assertEquals(i, p.context()); assertArrayEquals(array(bytes(1000, i)), p.payload());
            }
            assertEquals(10, s.stats().sentAsCapsules());
            var datagramOnly = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.DATAGRAM_ONLY, 100));
            assertThrows(NotNegotiatedException.class, () -> datagramOnly.send(ByteBuffer.allocate(1)));
            datagramOnly.abort(0); s.close(); peer.close();
        }
    }

    @Test void oversizedCapsuleResetsAndStopsWithPacketError() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false))) {
            ZmuxStream raw = pair.client().openStream(HELLO); raw.flush();
            var receiver = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)), new Options(SendPolicy.STREAM_ONLY, 100));
            byte[] header = new byte[8];
            int n = io.zmux.internal.wire.Capsule.putDatagramHeader(header, 0, 0, 101);
            raw.write(header, 0, n); raw.write(new byte[101]);
            assertThrows(PacketErrorException.class, () -> receiver.receive(Duration.ofSeconds(3)));
            assertThrows(PacketErrorException.class, () -> receiver.receive(Duration.ofSeconds(3)), "the error stays");
            var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(raw));
            assertEquals(PacketStream.PACKET_ERROR, reset.code());
            var stopped = assertThrows(StreamStoppedException.class, () -> { for (int i = 0; i < 100; i++) { raw.write(new byte[1000]); raw.flush(); } });
            assertEquals(PacketStream.PACKET_ERROR, stopped.code());
            receiver.close(); raw.close();
        }
    }

    @Test void unknownCapsulesAreSkippedBetweenPackets() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false))) {
            ZmuxStream raw = pair.client().openStream(HELLO); raw.flush();
            var receiver = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            // Type 0x4040 (2-byte), 3 bytes; DATAGRAM context 0x1234 "x"; type 0x21, empty.
            raw.write(HexFormat.of().parseHex("40400361626300035234782100"));
            var p = receiver.receive(Duration.ofSeconds(3));
            assertEquals(0x1234, p.context()); assertArrayEquals(new byte[] {'x'}, p.payload());
            raw.finish();
            assertNull(receiver.receive(Duration.ofSeconds(3)));
            assertEquals(2, receiver.stats().capsulesSkipped());
            receiver.close(); raw.close();
        }
    }

    @Test void bothDirectionsAsyncTimeoutsAndClose() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, true))) {
            var a = PacketStream.open(pair.client(), HELLO, Options.defaults());
            var b = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)), new Options(SendPolicy.STREAM_ONLY, PacketStream.DEFAULT_MAX_PACKET));
            assertThrows(java.net.SocketTimeoutException.class, () -> a.receive(Duration.ofMillis(50)));
            var pending = a.receiveAsync();
            var futures = new ArrayList<CompletableFuture<Boolean>>();
            for (int i = 0; i < 50; i++) futures.add(b.sendAsync(i, bytes(3000, i)));
            assertEquals(0, pending.get(3, TimeUnit.SECONDS).context());
            for (var f : futures) assertTrue(f.get(3, TimeUnit.SECONDS));
            for (int i = 1; i < 50; i++) {
                var p = a.receive(Duration.ofSeconds(3));
                assertEquals(i, p.context(), "async capsules keep their order"); assertArrayEquals(array(bytes(3000, i)), p.payload());
            }
            assertTrue(a.send(77, ByteBuffer.wrap(new byte[] {5})));
            assertEquals(77, b.receiveAsync().get(3, TimeUnit.SECONDS).context());
            var cancelled = b.receiveAsync(); assertTrue(cancelled.cancel(true));
            a.send(78, ByteBuffer.wrap(new byte[] {6}));
            assertEquals(78, b.receive(Duration.ofSeconds(3)).context(), "a cancelled receive takes nothing");
            b.close();
            assertNull(a.receive(Duration.ofSeconds(3)));
            assertNull(a.receiveAsync().get(3, TimeUnit.SECONDS));
            a.close(); a.close();
            assertThrows(StreamClosedException.class, () -> a.send(ByteBuffer.allocate(1)));
            assertThrows(StreamClosedException.class, () -> a.receive());
        }
    }

    @Test void concurrentSendersFrameEveryCapsule() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false));
             var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var s = PacketStream.open(pair.client(), HELLO, Options.defaults());
            var peer = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
            var all = new ArrayList<Future<?>>();
            for (int t = 0; t < 8; t++) {
                int sender = t;
                all.add(tasks.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        if (i % 2 == 0) s.send(sender, bytes(500 + i, i));
                        else s.sendAsync(sender, bytes(500 + i, i)).get();
                    }
                    return null;
                }));
            }
            var seen = new int[8];
            for (int k = 0; k < 1600; k++) {
                var p = peer.receive(Duration.ofSeconds(5));
                int i = seen[(int) p.context()]++;
                assertArrayEquals(array(bytes(500 + i, i)), p.payload());
            }
            for (var f : all) f.get(5, TimeUnit.SECONDS);
            s.close();
            assertNull(peer.receive(Duration.ofSeconds(3)));
            peer.close();
        }
    }

    @Test void relayWritesADatagramTheNextHopCannotTakeAsACapsule() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, true)); var right = Pair.memory(config(loops, false))) {
            var endpoint = PacketStream.open(left.client(), HELLO, Options.defaults());
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var splice = Zmux.splicePackets(a, b).toCompletableFuture();
            var destination = PacketStream.of(right.server().acceptStream(Duration.ofSeconds(3)));
            var capsule = bytes(PacketStream.DEFAULT_MAX_PACKET, 9); // Above the datagram limit: a capsule.
            assertTrue(endpoint.send(1, ByteBuffer.wrap(new byte[] {1, 2, 3})));
            assertEquals(1, endpoint.stats().sentAsDatagrams());
            var received = destination.receive(Duration.ofSeconds(3));
            assertEquals(1, received.context()); assertArrayEquals(new byte[] {1, 2, 3}, received.payload());
            assertEquals(1, destination.stats().receivedAsCapsules());
            for (int i = 0; i < 20; i++) {
                assertTrue(endpoint.send(100 + i, capsule.duplicate()));
                assertTrue(endpoint.send(200 + i, ByteBuffer.wrap(new byte[] {(byte) i})));
            }
            int capsules = 0, small = 0;
            for (int k = 0; k < 40; k++) {
                var p = destination.receive(Duration.ofSeconds(5));
                if (p.context() >= 200) { assertArrayEquals(new byte[] {(byte) (p.context() - 200)}, p.payload()); small++; }
                else { assertArrayEquals(array(capsule), p.payload()); capsules++; }
            }
            assertEquals(20, capsules); assertEquals(20, small);
            endpoint.close();
            assertNull(destination.receive(Duration.ofSeconds(3)));
            destination.close();
            var result = splice.get(5, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.Finished(), result.aToB().end());
            assertEquals(3 + 20, result.aToB().datagramBytes(), "converted datagrams count as datagrams");
            assertEquals(21, result.aToB().datagramsAsCapsules(), "every datagram went as a capsule");
            assertEquals(0, result.aToB().datagramsDropped());
        }
    }

    @Test void relayResetsANextHopWhenTheSourceEndsInsideACapsule() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, false)); var right = Pair.memory(config(loops, false))) {
            ZmuxStream raw = left.client().openStream(HELLO);
            raw.write(HexFormat.of().parseHex("000300"));
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var splice = Zmux.splicePackets(a, b).toCompletableFuture();
            var destination = PacketStream.of(right.server().acceptStream(Duration.ofSeconds(3)));
            raw.finish();
            var reset = assertThrows(StreamResetException.class, () -> destination.receive(Duration.ofSeconds(3)));
            assertEquals(PacketStream.PACKET_ERROR, reset.code());
            destination.close();
            assertNull(raw.readChunk(1));
            var result = splice.get(5, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.Reset(PacketStream.PACKET_ERROR), result.aToB().end());
            assertEquals(new SpliceEnd.Finished(), result.bToA().end());
            raw.close();
        }
    }

    @Test void packetSpliceAcquiresEachWholeCapsuleAndPolicesDatagrams() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, true)); var right = Pair.memory(config(loops, true))) {
            ZmuxStream endpoint = left.client().openStream(HELLO); endpoint.flush();
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var calls = new java.util.concurrent.CopyOnWriteArrayList<long[]>();
            var charged = new java.util.concurrent.atomic.AtomicLong();
            Limiter limiter = new Limiter() {
                // A burst below the larger units: a whole unit still passes on its (n, n) grant.
                @Override public CompletionStage<Long> acquire(long min, long max) {
                    calls.add(new long[] {min, max}); long n = Math.min(max, 1000); charged.addAndGet(n); return CompletableFuture.completedFuture(n);
                }
                @Override public long tryAcquire(long min, long max) { calls.add(new long[] {min, max}); charged.addAndGet(max); return max; }
                @Override public void refund(long n) { charged.addAndGet(-n); }
            };
            var splice = Zmux.splicePackets(a, b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            var destination = PacketStream.of(right.server().acceptStream(Duration.ofSeconds(3)));
            int[] sizes = {100, 0, 3000};
            long expected = 0;
            for (int i = 0; i < sizes.length; i++) {
                ByteBuffer head = ByteBuffer.allocate(io.zmux.internal.wire.Capsule.MAX_DATAGRAM_HEADER);
                io.zmux.internal.wire.Capsule.putDatagramHeader(head, i, sizes[i]); head.flip();
                expected += head.remaining() + sizes[i];
                endpoint.write(head); endpoint.write(bytes(sizes[i], i));
            }
            for (int i = 0; i < sizes.length; i++) {
                var p = destination.receive(Duration.ofSeconds(3));
                assertEquals(i, p.context()); assertEquals(bytes(sizes[i], i), ByteBuffer.wrap(p.payload()));
            }
            endpoint.sendDatagram(9, bytes(10, 9));
            var d = destination.receive(Duration.ofSeconds(3));
            assertEquals(9, d.context()); assertEquals(10, d.payload().length);
            endpoint.finish(); destination.close();
            var result = splice.get(5, TimeUnit.SECONDS);
            assertEquals(expected, result.aToB().bytes());
            assertEquals(10, result.aToB().datagramBytes());
            assertEquals(List.of(104L, 3L, 3004L, 10L), calls.stream().map(c -> c[1]).toList(), "acquire per capsule, tryAcquire per datagram");
            for (long[] c : calls) assertEquals(c[0], c[1], "acquire(n, n)");
            assertEquals(104 + 3 + 1000 + 10, charged.get(), "grants stay with their forwarded units");
            endpoint.close();
        }
    }

    @Test void packetSpliceDropsADatagramWhoseLimiterFailsAndKeepsRelayingCapsules() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, true)); var right = Pair.memory(config(loops, true))) {
            ZmuxStream endpoint = left.client().openStream(HELLO); endpoint.flush();
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var tries = new AtomicInteger(); var refunded = new java.util.concurrent.atomic.AtomicLong();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
                // Datagram 1 throws, 2 grants more than asked, 3 grants a negative amount: each counts as nothing taken.
                @Override public long tryAcquire(long min, long max) {
                    return switch (tries.incrementAndGet()) {
                        case 1 -> throw new IllegalStateException("datagram limiter broke");
                        case 2 -> max + 1;
                        case 3 -> -7;
                        default -> max;
                    };
                }
                @Override public void refund(long n) { refunded.addAndGet(n); }
            };
            var splice = Zmux.splicePackets(a, b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            var destination = PacketStream.of(right.server().acceptStream(Duration.ofSeconds(3)));
            for (int i = 1; i <= 3; i++) endpoint.sendDatagram(i, bytes(10, i));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (tries.get() < 3 && System.nanoTime() < deadline) Thread.onSpinWait();
            assertEquals(3, tries.get());
            ByteBuffer head = ByteBuffer.allocate(io.zmux.internal.wire.Capsule.MAX_DATAGRAM_HEADER);
            io.zmux.internal.wire.Capsule.putDatagramHeader(head, 5, 20); head.flip();
            endpoint.write(head); endpoint.write(bytes(20, 5));
            var capsule = destination.receive(Duration.ofSeconds(3));
            assertEquals(5, capsule.context()); assertEquals(20, capsule.payload().length);
            endpoint.sendDatagram(6, bytes(10, 6));
            var d = destination.receive(Duration.ofSeconds(3));
            assertEquals(6, d.context());
            endpoint.finish(); destination.close();
            var result = splice.get(5, TimeUnit.SECONDS);
            assertEquals(3, result.aToB().datagramsLimited()); assertEquals(3, result.aToB().datagramsDropped());
            assertEquals(10, result.aToB().datagramBytes());
            assertEquals(11, refunded.get(), "only the over-large grant is given back");
            endpoint.close();
        }
    }

    @Test void spliceCountsDatagramDropsByCause() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, true)); var right = Pair.memory(config(loops, false))) {
            ZmuxStream endpoint = left.client().openStream(HELLO); endpoint.flush();
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var dropped = new AtomicInteger();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { return dropped.incrementAndGet() == 1 ? 0 : max; }
                @Override public void refund(long n) {}
            };
            var splice = Zmux.splice(a, b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            try (var peer = right.server().acceptStream(Duration.ofSeconds(3))) {
                endpoint.sendDatagram(1, ByteBuffer.wrap(new byte[] {1}));
                endpoint.sendDatagram(2, ByteBuffer.wrap(new byte[] {2}));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (dropped.get() < 2 && System.nanoTime() < deadline) Thread.onSpinWait();
                Thread.sleep(50);
                endpoint.finish(); peer.finish();
                assertNull(peer.readChunk(1));
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsLimited());
                assertEquals(1, result.aToB().datagramsTooLarge(), "no datagrams on the next hop");
                assertEquals(2, result.aToB().datagramsDropped());
            }
            endpoint.close();
        }
    }

    @Test void datagramsQueuedBeforeTheEndAreDeliveredFirst() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, true))) {
            for (int round = 0; round < 20; round++) {
                var sender = PacketStream.open(pair.client(), HELLO, new Options(SendPolicy.DATAGRAM_ONLY, 1000));
                var receiver = PacketStream.of(pair.server().acceptStream(Duration.ofSeconds(3)));
                for (int i = 0; i < 5; i++) assertTrue(sender.send(i, bytes(10, i)));
                sender.close(); // FIN at a capsule boundary, after the datagrams.
                Thread.sleep(30); // Both are in the receiver's queues before its first receive starts the readers.
                var seen = new java.util.TreeSet<Long>();
                for (StreamDatagram p; (p = receiver.receive(Duration.ofSeconds(3))) != null; ) seen.add(p.context());
                assertEquals(java.util.Set.of(0L, 1L, 2L, 3L, 4L), seen, "round " + round);
                assertNull(receiver.receiveAsync().get(3, TimeUnit.SECONDS));
                receiver.close();
            }
        }
    }

    @Test void aCancelledSendThatWroteNothingIsNeverSentLater() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var pair = Pair.memory(config(loops, false))) {
            var sender = PacketStream.open(pair.client(), HELLO, Options.defaults());
            var peer = pair.server().acceptStream(Duration.ofSeconds(3));
            // Fill the flow control: the peer reads nothing, so a capsule stays queued.
            var blockers = new ArrayList<CompletableFuture<Boolean>>();
            for (int i = 0; i < 400; i++) {
                var f = sender.sendAsync(1, bytes(PacketStream.DEFAULT_MAX_PACKET, i));
                blockers.add(f);
                Thread.sleep(2);
                if (!f.isDone()) break;
            }
            assertFalse(blockers.getLast().isDone(), "a capsule waits for flow control");
            var cancelled = sender.sendAsync(998, ByteBuffer.wrap(new byte[] {1}));
            assertTrue(cancelled.cancel(true));
            var interrupted = new CompletableFuture<Throwable>();
            Thread t = Thread.ofVirtual().start(() -> {
                try { sender.send(999, ByteBuffer.wrap(new byte[] {2})); interrupted.complete(null); }
                catch (Throwable e) { interrupted.complete(e); }
            });
            Thread.sleep(100);
            t.interrupt();
            assertInstanceOf(java.io.InterruptedIOException.class, interrupted.get(3, TimeUnit.SECONDS));
            var after = sender.sendAsync(997, ByteBuffer.wrap(new byte[] {3}));
            sender.finishAsync();
            var receiver = PacketStream.of(peer, new Options(SendPolicy.STREAM_ONLY, PacketStream.DEFAULT_MAX_PACKET));
            var contexts = new ArrayList<Long>();
            for (StreamDatagram p; (p = receiver.receive(Duration.ofSeconds(10))) != null; ) contexts.add(p.context());
            assertTrue(after.get(3, TimeUnit.SECONDS));
            assertFalse(contexts.contains(998L), "cancelled in the queue");
            assertFalse(contexts.contains(999L), "interrupted before its turn");
            assertEquals(997L, contexts.getLast());
            assertEquals(blockers.size() + 1, contexts.size());
            receiver.close(); sender.close();
        }
    }

    @Test void relayCountsDatagramsStillWaitingAtAnEndAsDropped() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var left = Pair.memory(config(loops, true)); var right = Pair.memory(config(loops, false))) {
            ZmuxStream raw = left.client().openStream(HELLO);
            raw.write(HexFormat.of().parseHex("000300")); // A capsule header, one of its three bytes: mid-capsule.
            raw.flush();
            ZmuxStream a = left.server().acceptStream(Duration.ofSeconds(3));
            ZmuxStream b = right.client().openStream(HELLO); b.flush();
            var splice = Zmux.splicePackets(a, b).toCompletableFuture();
            var destination = right.server().acceptStream(Duration.ofSeconds(3));
            Thread.sleep(100);
            assertTrue(raw.sendDatagram(1, ByteBuffer.wrap(new byte[] {1, 2})));
            Thread.sleep(200); // It waits for a capsule boundary that never comes.
            raw.finish(); destination.finish();
            var result = splice.get(5, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.Reset(PacketStream.PACKET_ERROR), result.aToB().end());
            assertEquals(0, result.aToB().datagramBytes(), "never written: not forwarded");
            assertEquals(0, result.aToB().datagramsTooLarge(), "dropped for the end, not for its size");
            assertEquals(1, result.aToB().datagramsDropped());
            destination.close(); raw.close();
        }
    }
}
