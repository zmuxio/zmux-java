package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(45)
class ListenerEndpointTest {
    static ZmuxConfig config(EventLoops loops) { return PipePoolTest.config(loops,6); }
    static void exchange(ZmuxSession client, ZmuxSession server) throws Exception {
        try (var s = client.openStream()) {
            s.write(new byte[] {8,5,2}); s.finish();
            try (var t = server.acceptStream(Duration.ofSeconds(3))) {
                assertArrayEquals(new byte[] {8,5,2},SessionTest.readAll(t)); t.write(new byte[] {4}); t.finish();
            }
            assertArrayEquals(new byte[] {4},SessionTest.readAll(s));
        }
    }
    static void closed(ZmuxSession a, ZmuxSession b) throws Exception {
        a.close(); b.close(); a.awaitClosed(); b.awaitClosed();
    }
    static int registered(ZmuxListener listener, String name) throws Exception {
        var f = ZmuxListener.class.getDeclaredField("hub"); f.setAccessible(true); var hub = f.get(listener);
        var route = hub.getClass().getDeclaredField(name); route.setAccessible(true);
        synchronized (hub) {
            var value = route.get(hub);
            return value instanceof java.util.Map<?,?> map ? map.size() : ((java.util.Collection<?>)value).size();
        }
    }
    static void empty(ZmuxListener listener) throws Exception {
        PipePoolTest.await(() -> registered(listener,"registrations") == 0 && registered(listener,"sessions") == 0
                && registered(listener,"laneSessions") == 0 && registered(listener,"resources") == 0);
    }
    @Test void bothEndpointsJoinTheSameTcpSessionThroughTheirOwnListener() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = config(loops);
            try (var a = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var b = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = a.connect(b.localAddress()); var server = b.accept(Duration.ofSeconds(3))) {
                assertEquals(16,((byte[])client.run(op -> client.core.locator().clone())).length);
                var fromResponder = server.paths().add(a.localAddress(),PipeOptions.defaults());
                var fromInitiator = client.paths().add(b.localAddress(),PipeOptions.defaults());
                assertEquals(PipeState.Outbound.ACTIVE, fromResponder.state().orElseThrow().outbound()); assertEquals(PipeState.Outbound.ACTIVE, fromInitiator.state().orElseThrow().outbound());
                assertNull(a.accept(Duration.ofMillis(20)),"a join must not create a second application session");
                exchange(client,server); exchange(server,client); closed(client,server);
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void initiatedExternalSessionAcceptsOobJoinsAndRejectsBadMacAndReplay() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM); b.handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap"))).closed();
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(3))) {
                    for (Framing framing : Framing.values()) try (var reserved = server.paths().reserve(PipeOptions.defaults())) {
                        int before = client.paths().list().size();
                        byte[] body = Base64.getUrlDecoder().decode(reserved.header()); body[body.length-1] ^= 1;
                        var invalid = MemoryPipe.pair(framing);
                        a.handle(invalid[1],JoinHint.join(Base64.getUrlEncoder().withoutPadding().encodeToString(body), TestAddr.peer("bad"))).closed()
                                .toCompletableFuture().get(3,TimeUnit.SECONDS);
                        invalid[0].reader().close(); invalid[0].writer().close();
                        assertEquals(before,client.paths().list().size());
                        var pair = MemoryPipe.pair(framing);
                        var lifetime = a.handle(pair[1],JoinHint.join(reserved.header(), TestAddr.peer("joined"))).closed();
                        var joined = reserved.attach(pair[0]);
                        assertEquals(PipeState.Outbound.ACTIVE, joined.state().orElseThrow().outbound());
                        var replay = MemoryPipe.pair(framing);
                        a.handle(replay[1],JoinHint.join(reserved.header(), TestAddr.peer("replay"))).closed().toCompletableFuture().get(3,TimeUnit.SECONDS);
                        replay[0].reader().close(); replay[0].writer().close();
                        assertEquals(PipeState.Outbound.ACTIVE, joined.state().orElseThrow().outbound()); exchange(client,server);
                        joined.abort(); lifetime.toCompletableFuture().get(3,TimeUnit.SECONDS);
                    }
                    closed(client,server);
                }
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void duplicateResolutionRejoinsTheLosersRouteAndSurvivesBootstrapLoss() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = config(loops);
            try (var a = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var b = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var a1 = a.connect(b.localAddress()); var b1 = b.accept(Duration.ofSeconds(3));
                 var b2 = b.connect(a.localAddress()); var a2 = a.accept(Duration.ofSeconds(3))) {
                try (var old = b2.openStream()) {
                    old.write(new byte[] {7});
                    try (var accepted = a2.acceptStream(Duration.ofSeconds(3))) {
                        var ar = Zmux.resolveDuplicate(a1,a2,DuplicateTest.ids(1,2)); assertSame(a1,ar.survivor());
                        var cancel = new Cancellation();
                        var br = Zmux.resolveDuplicate(b1,b2,DuplicateTest.ids(2,1)
                                .withRejoin(PipePool.of(attempt -> attempt.join(a.localAddress()))).withCancellation(cancel));
                        assertSame(b1,br.survivor()); assertTrue(br.rejoinStarted());
                        var result = br.rejoin().completion().toCompletableFuture().get(4,TimeUnit.SECONDS);
                        assertNull(result.cause()); assertEquals(1,result.joined());
                        assertEquals(0,CancellationTest.registrations(cancel));
                        old.finish(); assertArrayEquals(new byte[] {7},SessionTest.readAll(accepted));
                        accepted.finish(); assertArrayEquals(new byte[0],SessionTest.readAll(old));
                        PipePoolTest.await(() -> a1.paths().list().size() > 1);
                        a1.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow().abort();
                        exchange(a1,b1); exchange(b1,a1);
                    }
                }
                closed(a1,b1); closed(a2,b2);
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void tlsEndpointRegistersBeforeParametersAndSupportsReverseJoin() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = config(loops);
            try (var a = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build());
                 var b = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build());
                 var client = a.connectTls(b.localAddress(), TlsOptions.client(TestTls.client(), "localhost")); var server = b.accept(Duration.ofSeconds(3))) {
                var joined = server.paths().addTls(a.localAddress(), TlsOptions.client(TestTls.client(), "localhost"), PipeOptions.defaults());
                assertEquals(PipeState.Outbound.ACTIVE, joined.state().orElseThrow().outbound()); exchange(client,server); closed(client,server);
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void listenerCloseStopsJoiningButKeepsHandedOutSessionsUsable() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM); b.handle(pair[1], JoinHint.inBand(TestAddr.peer("bootstrap"))).closed();
                try (var client = a.open(pair[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(3))) {
                    assertTrue((Integer)client.run(op -> io.zmux.internal.core.ObserverFixture.closedWaiters(client.core)) > 0);
                    a.close(); exchange(client,server); empty(a);
                    assertEquals(0,(Integer)client.run(op -> io.zmux.internal.core.ObserverFixture.closedWaiters(client.core)));
                    try (var reservation = server.paths().reserve(PipeOptions.defaults())) {
                        var refused = MemoryPipe.pair(Framing.BYTE_STREAM);
                        a.handle(refused[1],JoinHint.join(reservation.header(), TestAddr.peer("late"))).closed().toCompletableFuture().get(3,TimeUnit.SECONDS);
                        refused[0].reader().close(); refused[0].writer().close();
                    }
                    assertThrows(java.nio.channels.ClosedChannelException.class,() -> a.connect(NetworkTest.any()));
                    var unused = MemoryPipe.pair(Framing.BYTE_STREAM);
                    assertThrows(java.nio.channels.ClosedChannelException.class,() -> a.open(unused[0], Role.CLIENT));
                    unused[1].reader().close(); unused[1].writer().close();
                    closed(client,server);
                }
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void closeWakesBlockedAcceptAndEveryAcceptThenThrows() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var ex = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var listener = Zmux.listener(ListenerConfig.of(config(loops)));
            var blocked = ex.submit(() -> listener.accept());
            var timed = ex.submit(() -> listener.accept(Duration.ofSeconds(30)));
            Thread.sleep(20); listener.close();
            for (var waiter : java.util.List.of(blocked, timed)) {
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> waiter.get(1, TimeUnit.SECONDS));
                assertInstanceOf(java.nio.channels.ClosedChannelException.class, failure.getCause());
            }
            assertThrows(java.nio.channels.ClosedChannelException.class, () -> listener.accept(Duration.ZERO));
        }
    }
    @Test void failedExternalStartupRollsBackAllRoutingAndResources() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var endpoint = Zmux.listener(ListenerConfig.of(config(loops)))) {
            var readerClosed = new java.util.concurrent.atomic.AtomicInteger();
            var writerClosed = new java.util.concurrent.atomic.AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink sink) { throw new IllegalStateException("start failed"); }
                @Override public void close() { readerClosed.incrementAndGet(); }
            };
            PipeWriter writer = new PipeWriter() {
                @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { done.failed(new java.io.IOException("not started")); }
                @Override public void close() { writerClosed.incrementAndGet(); }
            };
            assertThrows(java.io.IOException.class,() -> endpoint.open(PipeIo.of(reader,writer), Role.CLIENT));
            empty(endpoint);
            assertEquals(1,readerClosed.get()); assertEquals(1,writerClosed.get()); assertEquals(0,endpoint.memoryUsed());
        }
    }
    @Test void repeatedEndpointSessionsUnregisterWithoutRetainingClosedDrivers() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                for (int i=0;i<20;i++) {
                    var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM); b.handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("repeat"))).closed();
                    try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(3))) {
                        assertEquals(1,registered(a,"sessions")); assertEquals(1,registered(b,"sessions"));
                        closed(client,server);
                    }
                    empty(a); empty(b);
                }
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void closedLoopGroupRejectsBeforeDialAndReturnsExternalTransportOwnership() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var target = new java.net.ServerSocket()) {
            var cfg = config(loops);
            try (var endpoint = Zmux.listener(ListenerConfig.of(cfg))) {
                target.bind(NetworkTest.any()); target.setSoTimeout(100);
                var address = (java.net.InetSocketAddress)target.getLocalSocketAddress(); loops.close();
                assertThrows(IllegalStateException.class,() -> endpoint.connect(address));
                assertThrows(IllegalStateException.class,() -> endpoint.connectTls(address, TlsOptions.client(TestTls.client(), "localhost")));
                assertThrows(IllegalStateException.class,() -> Zmux.connect(address,cfg));
                assertThrows(IllegalStateException.class,() -> Zmux.connectTls(address, TlsOptions.client(TestTls.client(), "localhost"), cfg));
                assertThrows(java.net.SocketTimeoutException.class,target::accept,"no socket should have been dialled");
                for (int mode=0;mode<3;mode++) {
                    var closes = new java.util.concurrent.atomic.AtomicInteger();
                    PipeReader reader = new PipeReader() { @Override public void start(Sink sink) { fail("reader started"); } @Override public void close() { closes.incrementAndGet(); } };
                    PipeWriter writer = new PipeWriter() {
                        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("writer started"); }
                        @Override public void close() { closes.incrementAndGet(); }
                    };
                    PipeIo carrier = PipeIo.of(reader,writer); int kind = mode;
                    assertThrows(IllegalStateException.class,() -> {
                        if (kind==0) endpoint.open(carrier, Role.CLIENT); else if (kind==1) endpoint.open(carrier, Role.PEER); else Zmux.open(carrier, Role.CLIENT, cfg);
                    });
                    assertEquals(2,closes.get());
                }
                empty(endpoint);
            }
        }
    }
    @Test void symmetricSessionsAdvertiseRoutesWithoutEnteringTheAcceptQueue() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM);
                var remote = CompletableFuture.supplyAsync(() -> {
                    try { return b.open(bootstrap[1], Role.PEER); } catch(java.io.IOException e) { throw new CompletionException(e); }
                }, r -> Thread.ofVirtual().start(r));
                try (var first = a.open(bootstrap[0], Role.PEER); var second = remote.get(3,TimeUnit.SECONDS)) {
                    first.designateDialer(true); second.designateDialer(false);
                    assertNull(a.accept(Duration.ofMillis(10))); assertNull(b.accept(Duration.ofMillis(10)));
                    var extra = MemoryPipe.pair(Framing.BYTE_STREAM); a.handle(extra[1], JoinHint.inBand(TestAddr.peer("joined"))).closed();
                    var joined = second.paths().add(extra[0],PipeOptions.defaults());
                    assertEquals(PipeState.Outbound.ACTIVE, joined.state().orElseThrow().outbound()); exchange(first,second); closed(first,second);
                }
                empty(a); empty(b);
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void inboundClosedLoopAndAlpnRejectionReturnBothHalvesEvenIfReaderCloseFails() throws Exception {
        for (boolean stopped : new boolean[] {false,true}) try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var endpoint = Zmux.listener(ListenerConfig.of(cfg))) {
                if (stopped) loops.close();
                var reads = new java.util.concurrent.atomic.AtomicInteger(); var writes = new java.util.concurrent.atomic.AtomicInteger();
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink sink) { fail("reader must not start"); }
                    @Override public void close() { reads.incrementAndGet(); throw new IllegalStateException("adapter close failed"); }
                };
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) { fail("writer must not start"); }
                    @Override public void close() { writes.incrementAndGet(); }
                };
                PipeIo carrier = PipeIo.of(reader,writer).withProtocol(stopped ? "qmux-02" : "another-protocol");
                var rejected = endpoint.handle(carrier, JoinHint.inBand(TestAddr.peer("rejected"))).closed().toCompletableFuture();
                if (stopped) assertThrows(ExecutionException.class,() -> rejected.get(2,TimeUnit.SECONDS));
                else rejected.get(2,TimeUnit.SECONDS);
                assertEquals(1,reads.get()); assertEquals(1,writes.get()); empty(endpoint);
            }
        }
    }
}

