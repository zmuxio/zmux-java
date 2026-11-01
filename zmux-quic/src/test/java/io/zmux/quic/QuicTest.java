package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class QuicTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }
    static InetSocketAddress any() { return new InetSocketAddress("127.0.0.1", 0); }
    static byte[] read(ZmuxReceiveStream stream) throws Exception {
        var output = new ByteArrayOutputStream(); ByteBuffer buffer = ByteBuffer.allocate(65536);
        while (stream.read(buffer.clear()) >= 0) { buffer.flip(); output.write(buffer.array(), 0, buffer.remaining()); }
        return output.toByteArray();
    }
    static void transfer(ExecutorService workers, ZmuxSession client, ZmuxSession server, int count, int size) throws Exception {
        var incoming = workers.submit(() -> {
            var jobs = new ArrayList<Future<?>>();
            var accepted = new ArrayList<ZmuxStream>();
            try {
                for (int i=0; i<count; i++) {
                    ZmuxStream stream = server.acceptStream(); accepted.add(stream);
                    jobs.add(workers.submit(() -> {
                        try (stream) {
                            byte[] bytes = read(stream);
                            stream.write(MessageDigest.getInstance("SHA-256").digest(bytes)); stream.finish();
                        }
                        return null;
                    }));
                }
                for (Future<?> job : jobs) job.get(20, TimeUnit.SECONDS);
            } finally {
                for (var job : jobs) if (!job.isDone()) job.cancel(true);
                // A task cancelled before it starts never enters its try-with-resources.
                for (var stream : accepted) stream.close();
            }
            return null;
        });
        var jobs = new ArrayList<Future<?>>();
        try {
            for (int i=0; i<count; i++) {
                int seed = i;
                jobs.add(workers.submit(() -> {
                    byte[] bytes = new byte[size]; new Random(seed).nextBytes(bytes);
                    try (ZmuxStream stream = client.openStream()) {
                        stream.write(bytes); stream.finish();
                        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(bytes), read(stream));
                    }
                    return null;
                }));
            }
            for (Future<?> job : jobs) job.get(20, TimeUnit.SECONDS);
            incoming.get(20, TimeUnit.SECONDS);
        } finally {
            for (var job : jobs) if (!job.isDone()) job.cancel(true);
            if (!incoming.isDone()) incoming.cancel(true);
        }
    }
    @Test void sessionsAndLanesUseRealQuicStreams() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10)); assertNotNull(accepted);
                assertEquals("qmux-02", client.info().applicationProtocol());
                assertTrue(listener.memoryUsed() >= options.connectionWindow());
                transfer(workers, client, accepted, 16, 256 << 10);
                assertTrue(client.paths().stats().getFirst().lanesOut() > 1, "outbound lanes were not opened");
                var laneStats = client.paths().stats().getFirst();
                assertEquals(laneStats.lanesOut(), laneStats.lanes().size());
                assertEquals(laneStats.bytesSent(), laneStats.lanes().stream().mapToLong(l -> l.positionSent().orElseThrow()).sum(),
                        "the pipe's position is the sum over its lanes");
                transfer(workers, accepted, client, 4, 64 << 10);
                client.close();
                SessionError error = accepted.awaitClosed();
                assertInstanceOf(SessionError.ApplicationClosed.class, error);
                client.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    /** A lane's records, its PIPE_LANE record included, are records of the pipe: its shaper plans and delays them. */
    @Test void laneRecordsGoThroughThePipesShaper() throws Exception {
        var lanes = java.util.concurrent.ConcurrentHashMap.<Long>newKeySet();
        io.zmux.shaping.ShaperFactory delayAll = p -> new io.zmux.shaping.Shaper() {
            @Override public java.util.Set<io.zmux.shaping.Capability> capabilities() { return java.util.EnumSet.of(io.zmux.shaping.Capability.PLAN); }
            @Override public void plan(io.zmux.shaping.RecordView view, io.zmux.shaping.RecordPlan plan) {
                lanes.add(view.lane());
                plan.delayNanos(200_000);
            }
            @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Reply reply) {}
        };
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var ccfg = ZmuxConfig.builder().eventLoops(loops).shaper(delayAll).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(scfg));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connect(server.address(), "localhost", ccfg).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10)); assertNotNull(accepted);
                transfer(workers, client, accepted, 16, 256 << 10);
                assertTrue(client.paths().stats().getFirst().lanesOut() > 1, "outbound lanes were not opened");
                var laneStats = client.paths().stats().getFirst();
                assertEquals(laneStats.lanesOut(), laneStats.lanes().size());
                assertEquals(laneStats.bytesSent(), laneStats.lanes().stream().mapToLong(l -> l.positionSent().orElseThrow()).sum(),
                        "the pipe's position is the sum over its lanes");
                assertTrue(lanes.stream().anyMatch(l -> l > 0), "records of lanes other than 0 were planned: " + lanes);
                client.close();
                accepted.awaitClosed();
                client.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    /** Both sides' shaper factories see their QUIC pipes as quic, from the configuration. */
    @Test void quicPipesAreShapedAsQuic() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var seenClient = new CopyOnWriteArrayList<io.zmux.shaping.ShapedPipe>();
            var seenServer = new CopyOnWriteArrayList<io.zmux.shaping.ShapedPipe>();
            var ccfg = ZmuxConfig.builder().eventLoops(loops).shaper(p -> { seenClient.add(p); return null; }).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).shaper(p -> { seenServer.add(p); return null; }).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(scfg));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connect(server.address(), "localhost", ccfg).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10)); assertNotNull(accepted);
                transfer(workers, client, accepted, 1, 1024);
                transfer(workers, accepted, client, 1, 1024);
                assertFalse(seenClient.isEmpty()); assertFalse(seenServer.isEmpty());
                for (var p : seenClient) assertEquals(CarrierKind.QUIC, p.carrier(), p.toString());
                for (var p : seenServer) assertEquals(CarrierKind.QUIC, p.carrier(), p.toString());
                assertEquals(CarrierKind.QUIC, client.paths().list().getFirst().stats().orElseThrow().carrier());
                client.close(); accepted.awaitClosed(); client.awaitClosed();
            }
        }
    }

    @Test void joinsOnAnotherConnectionAndSurvivesItsPeerConnectionLoss() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(3).withAuth(PipeAuth.psk(new byte[32])).withScheduler(Scheduler.SPREAD_ETA).withProbation(Duration.ZERO)).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var client = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                var accepted = listener.accept(Duration.ofSeconds(10)); assertNotNull(accepted);
                var original = endpoint.connections().getFirst();
                var pipe = endpoint.openPipe(server.address(), "localhost", client).thenApply(value -> {
                    assertFalse(original.eventLoop().inEventLoop(), "application callback ran on the native event loop");
                    return value;
                }).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxPipe joined = client.paths().add(pipe, PipeOptions.defaults());
                assertTrue(joined.id() > 0); assertEquals(2, client.paths().list().size());
                transfer(workers, client, accepted, 8, 256 << 10);
                original.close().sync();
                transfer(workers, client, accepted, 4, 256 << 10);
                client.close(); accepted.close(); client.awaitClosed(); accepted.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    @Test void invalidHostnameIsRejected() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build(); var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                assertThrows(ExecutionException.class, () -> endpoint.connect(server.address(), "wrong.invalid", config)
                        .toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
        }
    }
    @Test void endpointCloseCompletesPendingConnectAndRejectsNewOperations() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var blackhole = new java.net.DatagramSocket(any())) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults());
            try {
                var pending = endpoint.connect((InetSocketAddress) blackhole.getLocalSocketAddress(), "localhost", config).toCompletableFuture();
                endpoint.close();
                assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
                assertThrows(ExecutionException.class, () -> endpoint.connect((InetSocketAddress) blackhole.getLocalSocketAddress(), "localhost", config)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertTrue(endpoint.connections().isEmpty());
            } finally { endpoint.close(); }
        }
    }
    @Test void cancellingAConnectAndClosingFromACompletionDoNotBlock() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var blackhole = new java.net.DatagramSocket(any())) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var endpoint = new QuicClient(TestTls.client, options)) {
                var pending = endpoint.connect((InetSocketAddress) blackhole.getLocalSocketAddress(), "localhost", config).toCompletableFuture();
                assertTrue(pending.cancel(false)); assertTrue(pending.isCancelled());
            }
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                CompletableFuture<Void> callback = new CompletableFuture<>();
                endpoint.connect(server.address(), "localhost", config).whenComplete((session, error) -> {
                    try { if (error != null) throw new CompletionException(error); endpoint.close(); callback.complete(null); }
                    catch (Throwable failure) { callback.completeExceptionally(failure); }
                });
                callback.get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(5));
                if (accepted != null) { accepted.close(); accepted.awaitClosed(); }
            }
        }
    }

    @Test void readerStartingAfterNativeStreamFailureStillReceivesTheFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build(); var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var session = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                var accepted = listener.accept(Duration.ofSeconds(5)); assertNotNull(accepted);
                var connection = endpoint.connections().getFirst();
                var stream = connection.createStream(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL,
                        new io.netty.channel.ChannelInitializer<io.netty.handler.codec.quic.QuicStreamChannel>() {
                            @Override protected void initChannel(io.netty.handler.codec.quic.QuicStreamChannel channel) {
                                channel.pipeline().addLast(new StreamCarrier(channel, connection.pipeline().get(ConnectionCarrier.class).host(), true, true));
                            }
                        }).sync().getNow();
                var pipeIo = stream.pipeline().get(StreamCarrier.class).io(connection.pipeline().get(ConnectionCarrier.class));
                stream.close().sync();
                CompletableFuture<Throwable> failure = new CompletableFuture<>();
                pipeIo.reader().start(new io.zmux.carrier.PipeReader.Sink() {
                    @Override public void onData(ByteBuffer bytes, Runnable release) { release.run(); failure.completeExceptionally(new AssertionError("unexpected data")); }
                    @Override public void onEnd() { failure.completeExceptionally(new AssertionError("failure mistaken for FIN")); }
                    @Override public void onError(Throwable error) { failure.complete(error); }
                });
                assertInstanceOf(java.io.IOException.class, failure.get(5, TimeUnit.SECONDS));
                session.close(); accepted.close(); session.awaitClosed(); accepted.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

}
