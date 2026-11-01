package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.PipeIo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class NestedSessionTest {
    static Pair tcp(ZmuxConfig config) throws Exception {
        try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config))) {
            ZmuxSession client = Zmux.connect(listener.localAddress(), config);
            return new Pair(client, listener.accept(Duration.ofSeconds(5)));
        }
    }
    @Test void nestedSessionsUseOwnedBuffersAndPreserveOuterOwnership() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            for (boolean tcp : new boolean[] {false, true}) try (var outer = tcp ? tcp(config) : Pair.memory(config)) {
                var accepting = workers.submit(() -> {
                    ZmuxStream stream = outer.server().acceptStream();
                    assertTrue(stream.preamble().pipeCarrier());
                    return Zmux.open(PipeIo.ofStream(stream), Role.SERVER, config);
                });
                var carrier = outer.client().openPipeStream();
                try (var client = Zmux.open(carrier, Role.CLIENT, config);
                     var server = accepting.get(5, TimeUnit.SECONDS)) {
                    assertSame(outer.client().driver.loop(), client.driver.loop());
                    assertSame(outer.server().driver.loop(), server.driver.loop());
                    byte[] payload = new byte[2 << 20]; new Random(12).nextBytes(payload);
                    var reader = workers.submit(() -> {
                        try (var stream = server.acceptStream()) {
                            for (Chunk chunk; (chunk = stream.readChunk(65536)) != null;) stream.writeChunk(chunk);
                            stream.finish();
                        }
                        return null;
                    });
                    try (var stream = client.openStream()) {
                        var writing = workers.submit(() -> { stream.writeChunk(ByteBuffer.wrap(payload)); stream.finish(); return null; });
                        ByteArrayOutputStream received = new ByteArrayOutputStream();
                        for (Chunk chunk; (chunk = stream.readChunk(65536)) != null;) {
                            try (Chunk owned = chunk) { ByteBuffer data = owned.buffer(); byte[] bytes = new byte[data.remaining()]; data.get(bytes); received.write(bytes); }
                        }
                        writing.get(10, TimeUnit.SECONDS); assertArrayEquals(payload, received.toByteArray());
                    }
                    reader.get(10, TimeUnit.SECONDS);
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
                // Closing the inner session does not close its outer session.
                try (var stream = outer.client().openStream()) {
                    stream.write(new byte[] {7}); stream.finish();
                    try (var peer = outer.server().acceptStream(Duration.ofSeconds(5)); Chunk chunk = peer.readChunk(1)) {
                        assertEquals(7, chunk.buffer().get());
                    }
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void nestedLanesShareAnOuterCreditLeaseAndDoNotWait() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.insecure())).limits(Limits.defaults().withIncomingStreams(8)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config));
                 var client = Zmux.connect(listener.localAddress(), config);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                PipeIo first = client.openPipeStream();
                assertThrows(ExecutionException.class, () -> first.capabilities().laneOpener().open()
                        .toCompletableFuture().get(2, TimeUnit.SECONDS)); // A single TCP path gains no independence.
                client.paths().add(listener.localAddress(), PipeOptions.defaults());
                {
                    PipeIo second = client.openPipeStream();
                    var cancelled = new CompletableFuture<Void>();
                    client.driver.executeCarrier(() -> {
                        for (int i = 0; i < 16; i++) first.capabilities().laneOpener().open().toCompletableFuture().cancel(true);
                        cancelled.complete(null);
                    });
                    cancelled.get(2, TimeUnit.SECONDS);
                    // Successfully opened but discarded before any frame also returns its lease.
                    for (int i = 0; i < 16; i++) {
                        var unused = first.capabilities().laneOpener().open().toCompletableFuture().get(2, TimeUnit.SECONDS);
                        unused.writer().abort(new IOException("discarded before attach"));
                    }
                    PipeIo lane1 = first.capabilities().laneOpener().open().toCompletableFuture().get(2, TimeUnit.SECONDS);
                    PipeIo lane2 = second.capabilities().laneOpener().open().toCompletableFuture().get(2, TimeUnit.SECONDS);
                    var lane3 = first.capabilities().laneOpener().open().toCompletableFuture().get(2, TimeUnit.SECONDS);
                    var lane4 = second.capabilities().laneOpener().open().toCompletableFuture().get(2, TimeUnit.SECONDS);
                    assertNull(lane1.reader()); assertNull(lane2.reader());
                    assertSame(client.driver, lane1.carrierConn());
                    assertThrows(ExecutionException.class, () -> first.capabilities().laneOpener().open()
                            .toCompletableFuture().get(2, TimeUnit.SECONDS));
                    lane1.writer().close(); lane2.writer().close(); lane3.writer().close(); lane4.writer().close();
                    first.reader().close(); first.writer().close(); second.reader().close(); second.writer().close();
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void nestedLanesCarryIndependentStreamsOverMultipathOuterSession() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var outerConfig = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.insecure())).build();
            var innerConfig = ZmuxConfig.builder().eventLoops(loops).limits(Limits.defaults().withLanes(4)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(outerConfig));
                 var outerClient = Zmux.connect(listener.localAddress(), outerConfig);
                 var outerServer = listener.accept(Duration.ofSeconds(5));
                 var innerListener = Zmux.listener(ListenerConfig.of(innerConfig))) {
                outerClient.paths().add(listener.localAddress(), PipeOptions.defaults());
                var routing = workers.submit(() -> {
                    try {
                        while (true) innerListener.handle(outerServer.acceptStream().asPipe(), JoinHint.inBand(TestAddr.peer("nested"))).closed();
                    } catch (IOException expected) { return null; }
                });
                try (var client = Zmux.open(outerClient.openPipeStream(), Role.CLIENT, innerConfig);
                     var server = innerListener.accept(Duration.ofSeconds(5))) {
                    client.ping();
                    long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (client.paths().stats().getFirst().bytesAcked().orElse(0) == 0 && System.nanoTime() < ackDeadline) Thread.sleep(10);
                    assertTrue(client.paths().stats().getFirst().bytesAcked().orElse(0) > 0, "lane 0 must be acknowledged before requesting lanes");
                    // Keep several directions live at once, to request lanes after lane 0 is acknowledged.
                    var streams = new java.util.ArrayList<ZmuxStream>();
                    var peers = new java.util.ArrayList<ZmuxStream>();
                    for (int i = 0; i < 8; i++) {
                        var stream = client.openStream();
                        try { stream.write(new byte[] {(byte)i}); }
                        catch (IOException e) { throw new AssertionError("outer=" + outerClient.error() + "/" + outerServer.error()
                                + " inner=" + client.error() + "/" + server.error(), e); }
                        streams.add(stream);
                        var peer = server.acceptStream(Duration.ofSeconds(5));
                        ByteBuffer b = ByteBuffer.allocate(1); assertEquals(1, peer.read(b)); peers.add(peer);
                    }
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (client.paths().list().getFirst().stats().orElseThrow().lanesOut() < 2 && System.nanoTime() < until) Thread.sleep(10);
                    assertTrue(client.paths().list().getFirst().stats().orElseThrow().lanesOut() > 1);
                    for (int i = 0; i < streams.size(); i++) {
                        streams.get(i).write(new byte[] {42}); streams.get(i).finish();
                        assertArrayEquals(new byte[] {42}, SessionTest.readAll(peers.get(i)));
                        streams.get(i).close(); peers.get(i).close();
                    }
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
                outerServer.close(); outerClient.close(); routing.get(5, TimeUnit.SECONDS);
                outerServer.awaitClosed(); outerClient.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void addingAPipeThroughTheSameSessionIsRejectedWithoutBreakingTheSession() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.psk(new byte[32]))).build();
            try (var outer = Pair.memory(config)) {
                var carrier = outer.client().openPipeStream();
                assertThrows(PipeRefusedException.class, () -> outer.client().paths().add(carrier, PipeOptions.defaults()));
                assertNotNull(outer.client().ping());
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
