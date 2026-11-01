package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class QuicEndpointTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }
    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(6).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO).withScheduler(Scheduler.SPREAD_ETA)).build();
    }
    static void closed(ZmuxSession a, ZmuxSession b) throws Exception {
        a.close(); b.close(); a.awaitClosed(); b.awaitClosed();
    }
    static io.zmux.carrier.PipeIo awaitPipe(CompletionStage<io.zmux.carrier.PipeIo> operation) throws java.io.IOException, InterruptedException {
        var future = operation.toCompletableFuture();
        try { return future.get(5,TimeUnit.SECONDS); }
        catch (ExecutionException | TimeoutException | InterruptedException error) {
            if (!future.cancel(false)) future.thenAccept(pipe -> {
                try { if (pipe.reader()!=null) pipe.reader().close(); }
                finally { if (pipe.writer()!=null) pipe.writer().abort(error); }
            });
            if (error instanceof InterruptedException interrupted) throw interrupted;
            throw new java.io.IOException("pipe opening failed",error);
        }
    }
    @Test void responderJoinsAnInitiatedSessionAndKeepsTheSharedConnectionAlive() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops); var options = QuicOptions.defaults();
            try (var local = Zmux.listener(ListenerConfig.of(cfg)); var remote = Zmux.listener(ListenerConfig.of(cfg));
                 var server = new QuicServer(QuicTest.any(),TestTls.server,options,remote);
                 var client = new QuicClient(TestTls.client,options,local)) {
                var a = client.connect(server.address(),"localhost").toCompletableFuture().get(10,TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5)); assertNotNull(b);
                var shared = server.connections().getFirst();
                var carrier = server.openPipe(shared).thenApply(pipe -> {
                    assertFalse(shared.eventLoop().inEventLoop(),"application callback ran on the native loop"); return pipe;
                }).toCompletableFuture().get(5,TimeUnit.SECONDS);
                var joined = b.paths().add(carrier,PipeOptions.defaults()); assertTrue(joined.id()>0);
                assertEquals(2,a.paths().list().size());
                a.paths().list().stream().filter(p -> p.id()==0).findFirst().orElseThrow().abort();
                QuicTest.transfer(workers,a,b,4,128<<10); QuicTest.transfer(workers,b,a,4,64<<10);
                assertTrue(shared.isActive(),"aborting pipe 0 must not close its shared QUIC connection");
                assertNull(local.accept(Duration.ofMillis(20)),"join created an application session");
                closed(a,b);
            }
            assertEquals(0,io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    @Test void freshJoinedConnectionsKeepTheSameListenerRoutingInBothDirections() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops); var options = QuicOptions.defaults();
            try (var local = Zmux.listener(ListenerConfig.of(cfg)); var remote = Zmux.listener(ListenerConfig.of(cfg));
                 var server = new QuicServer(QuicTest.any(),TestTls.server,options,remote);
                 var client = new QuicClient(TestTls.client,options,local)) {
                var a = client.connect(server.address(),"localhost").toCompletableFuture().get(10,TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5)); assertNotNull(b);
                var original = client.connections().getFirst(); var originalRemote = server.connections().getFirst();
                var joined = client.openPipe(server.address(),"localhost",a).toCompletableFuture().get(5,TimeUnit.SECONDS);
                a.paths().add(joined,PipeOptions.defaults());
                var fresh = server.connections().stream().filter(c -> c != originalRemote).findFirst().orElseThrow();
                b.paths().add(server.openPipe(fresh).toCompletableFuture().get(5,TimeUnit.SECONDS),PipeOptions.defaults());
                original.close().sync();
                QuicTest.transfer(workers,a,b,4,128<<10); QuicTest.transfer(workers,b,a,4,64<<10); closed(a,b);
            }
            assertEquals(0,io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    @Test void cancellingAReversePipeReturnsOnlyItsStreamAndRejectsForeignConnections() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops); var options = QuicOptions.defaults();
            try (var local = Zmux.listener(ListenerConfig.of(cfg)); var remote = Zmux.listener(ListenerConfig.of(cfg));
                 var server = new QuicServer(QuicTest.any(),TestTls.server,options,remote);
                 var client = new QuicClient(TestTls.client,options,local)) {
                var a = client.connect(server.address(),"localhost").toCompletableFuture().get(10,TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5)); assertNotNull(b);
                var shared = server.connections().getFirst();
                var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
                shared.eventLoop().execute(() -> { entered.countDown(); try { release.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); } });
                assertTrue(entered.await(2,TimeUnit.SECONDS));
                try {
                    var pending = server.openPipe(shared).toCompletableFuture(); assertTrue(pending.cancel(false));
                } finally { release.countDown(); }
                shared.eventLoop().submit(() -> {}).sync();
                assertTrue(shared.isActive()); QuicTest.transfer(workers,a,b,2,64<<10);
                var foreign = client.connections().getFirst();
                assertInstanceOf(IllegalArgumentException.class,assertThrows(ExecutionException.class,() -> server.openPipe(foreign)
                        .toCompletableFuture().get(3,TimeUnit.SECONDS)).getCause());
                closed(a,b); server.close();
                assertInstanceOf(java.io.IOException.class,assertThrows(ExecutionException.class,() -> server.openPipe(shared)
                        .toCompletableFuture().get(3,TimeUnit.SECONDS)).getCause());
            }
            assertEquals(0,io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    @Test void duplicateResolutionRejoinsThroughQuicAfterTheLosingConnectionCloses() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops); var options = QuicOptions.defaults();
            try (var left = Zmux.listener(ListenerConfig.of(cfg)); var right = Zmux.listener(ListenerConfig.of(cfg));
                 var leftServer = new QuicServer(QuicTest.any(),TestTls.server,options,left);
                 var rightServer = new QuicServer(QuicTest.any(),TestTls.server,options,right);
                 var leftClient = new QuicClient(TestTls.client,options,left);
                 var rightClient = new QuicClient(TestTls.client,options,right)) {
                var a1 = leftClient.connect(rightServer.address(),"localhost").toCompletableFuture().get(10,TimeUnit.SECONDS);
                var b1 = right.accept(Duration.ofSeconds(5)); assertNotNull(b1);
                var b2 = rightClient.connect(leftServer.address(),"localhost").toCompletableFuture().get(10,TimeUnit.SECONDS);
                var a2 = left.accept(Duration.ofSeconds(5)); assertNotNull(a2);
                var oldConnection = rightClient.connections().getFirst();
                try (var old = b2.openStream()) {
                    old.write(new byte[] {7});
                    try (var receiving = a2.acceptStream(Duration.ofSeconds(3))) {
                        var ar = Zmux.resolveDuplicate(a1,a2,DuplicateOptions.defaults().withIdentities(new byte[] {1},new byte[] {2}));
                        assertSame(a1,ar.survivor());
                        var br = Zmux.resolveDuplicate(b1,b2,DuplicateOptions.defaults().withIdentities(new byte[] {2},new byte[] {1})
                                .withRejoin(PipePool.of(attempt -> attempt.join(awaitPipe(rightClient.openPipe(leftServer.address(),"localhost",b1))))));
                        var result = br.rejoin().completion().toCompletableFuture().get(6,TimeUnit.SECONDS);
                        assertNull(result.cause()); assertEquals(1,result.joined());
                        old.finish(); assertArrayEquals(new byte[] {7},QuicTest.read(receiving));
                        receiving.finish(); assertArrayEquals(new byte[0],QuicTest.read(old));
                        oldConnection.close().sync(); leftClient.connections().getFirst().close().sync();
                        QuicTest.transfer(workers,a1,b1,4,128<<10); QuicTest.transfer(workers,b1,a1,2,64<<10);
                    }
                }
                closed(a1,b1); closed(a2,b2);
            }
            assertEquals(0,io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }
    @Test void listenerRemainsTheConfigurationAuthorityAndRejectsMismatchedAlpn() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops); var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(cfg)); var server = new QuicServer(QuicTest.any(),TestTls.server,options,listener);
                 var backed = new QuicClient(TestTls.client,options,listener); var plain = new QuicClient(TestTls.client,options);
                 var mismatched = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().eventLoops(loops).alpn(java.util.List.of("another-protocol")).build()));
                 var wrong = new QuicClient(TestTls.client,options,mismatched)) {
                assertThrows(ExecutionException.class,() -> backed.connect(server.address(),"localhost",cfg).toCompletableFuture().get(3,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> plain.connect(server.address(),"localhost").toCompletableFuture().get(3,TimeUnit.SECONDS));
                assertInstanceOf(javax.net.ssl.SSLHandshakeException.class,assertThrows(ExecutionException.class,() -> wrong.connect(server.address(),"localhost")
                        .toCompletableFuture().get(5,TimeUnit.SECONDS)).getCause());
                assertNull(listener.accept(Duration.ofMillis(100)),"mismatched ALPN must not start a zmux session");
            }
        }
    }
}
