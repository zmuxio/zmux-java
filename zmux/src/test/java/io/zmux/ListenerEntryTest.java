package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The listener's one entry (API.md Sections 4.4, 4.7, 7.2): the carrier's source and peer, the
 * source slot before the outer handshake, asynchronous decisions bounded by the peek timeout and
 * cancelled on close, the admission counts, the preamble bound, and the graceful shutdown.
 */
@Timeout(60)
class ListenerEntryTest {
    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops)
                .multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
    }

    static InetSocketAddress ip(String literal, int port) {
        return new InetSocketAddress(java.net.InetAddress.ofLiteral(literal), port);
    }

    /** Ends when the listener closes the carrier's other side. */
    static CompletableFuture<Void> watchEnd(PipeIo io) {
        var ended = new CompletableFuture<Void>();
        io.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { release.run(); }
            @Override public void onEnd() { ended.complete(null); }
            @Override public void onError(Throwable cause) { ended.complete(null); }
        });
        return ended;
    }

    static void await(java.util.function.BooleanSupplier c) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!c.getAsBoolean()) {
            if (System.nanoTime() > end) fail("condition not met");
            Thread.sleep(10);
        }
    }

    @Test void theHostsSourceAndThePeerReachTheDecisionAndKeyTheSlots() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var seen = new LinkedBlockingQueue<Admission.CarrierInfo>();
            var lc = ListenerConfig.builder().session(config(loops)).maxPendingPerSource(1)
                    .admission(Admission.of(0, Admission.Decider.immediate((info, prefix) -> { seen.add(info); return Admission.Decision.accept(0); })))
                    .build();
            try (var l = Zmux.listener(lc)) {
                var peer = ip("192.0.2.9", 4000);
                var source = ip("2001:db8:1:2::7", 5000);
                var one = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(one[1], JoinHint.inBand(peer).withSource(source));
                var info = seen.poll(5, TimeUnit.SECONDS);
                assertEquals(source, info.source());
                assertEquals(peer, info.peer());
                // Another address of the same /64, another port: the same client, whose one slot is held.
                var two = MemoryPipe.pair(Framing.BYTE_STREAM);
                var refused = l.handle(two[1], JoinHint.inBand(peer).withSource(ip("2001:db8:1:2::99", 1)));
                assertFalse(refused.taken().toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals(1, l.stats().refused());
                assertTrue(seen.isEmpty(), "refused at its slot, before a decision");
                // Without a source the peer is the source.
                var three = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(three[1], JoinHint.inBand(ip("198.51.100.1", 1)));
                info = seen.poll(5, TimeUnit.SECONDS);
                assertEquals(info.peer(), info.source());
                assertEquals(l.stallDuration(ip("2001:db8:1:2::1", 0)), l.stallDuration(ip("2001:db8:1:2::2", 9)));
            }
        }
    }

    @Test void aDecisionMayCompleteLaterAndIsCancelledAtThePeekDeadline() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            var later = new AtomicReference<CompletableFuture<Admission.Decision>>();
            var deadlines = new LinkedBlockingQueue<Admission.Deadline>();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(0, (info, prefix, deadline) -> {
                        var f = new CompletableFuture<Admission.Decision>();
                        later.set(f);
                        deadlines.add(deadline); // after the set: the test reads later once it sees the deadline
                        return f;
                    }).withPeekTimeout(Duration.ofMillis(800))).build();
            try (var l = Zmux.listener(lc)) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                var handled = l.handle(boot[1], JoinHint.inBand(TestAddr.peer("a")));
                Admission.Deadline d = deadlines.poll(5, TimeUnit.SECONDS);
                assertFalse(d.expired());
                assertTrue(d.remaining().compareTo(Duration.ofMillis(800)) <= 0);
                Thread.sleep(100);
                later.get().complete(Admission.Decision.accept(0)); // off the loop, later
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    assertEquals(Admission.Verdict.ACCEPT, handled.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                }
                // A decision past the deadline: cancelled, the carrier closed.
                var slow = MemoryPipe.pair(Framing.BYTE_STREAM);
                var ended = watchEnd(slow[0]);
                l.handle(slow[1], JoinHint.inBand(TestAddr.peer("b")));
                deadlines.poll(5, TimeUnit.SECONDS);
                var pending = later.get();
                ended.get(5, TimeUnit.SECONDS);
                await(pending::isCancelled); // the cancel and the close both happen, in no promised order
                var a = l.stats().admission();
                assertEquals(1, a.accepted());
                assertEquals(1, a.peekTimedOut());
            }
        }
    }

    @Test void closingTheListenerCancelsTheDecisionsUnderWay() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var pending = new LinkedBlockingQueue<CompletableFuture<Admission.Decision>>();
            var lc = ListenerConfig.builder().session(config(loops))
                    .admission(Admission.of(0, (info, prefix, deadline) -> {
                        var f = new CompletableFuture<Admission.Decision>();
                        pending.add(f);
                        return f;
                    }).withPeekTimeout(Duration.ofSeconds(30))).build();
            var l = Zmux.listener(lc);
            var p = MemoryPipe.pair(Framing.BYTE_STREAM);
            var ended = watchEnd(p[0]);
            l.handle(p[1], JoinHint.inBand(TestAddr.peer("a")));
            var f = pending.poll(5, TimeUnit.SECONDS);
            l.close();
            ended.get(5, TimeUnit.SECONDS);
            assertTrue(f.isCancelled());
            assertEquals(new AdmissionStats(0, 0, 0, 1, 0, 0, 0), l.stats().admission());
        }
    }

    @Test void aCloseWhileTheDeciderDecidesStillCancelsTheDecision() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var inDecider = new java.util.concurrent.CountDownLatch(1);
            var closeDone = new java.util.concurrent.CountDownLatch(1);
            var pending = new LinkedBlockingQueue<CompletableFuture<Admission.Decision>>();
            var lc = ListenerConfig.builder().session(config(loops))
                    .admission(Admission.of(0, (info, prefix, deadline) -> {
                        var f = new CompletableFuture<Admission.Decision>();
                        pending.add(f);
                        inDecider.countDown();
                        // The close runs while the decider has not returned its stage yet.
                        try { closeDone.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        return f;
                    }).withPeekTimeout(Duration.ofSeconds(30))).build();
            var l = Zmux.listener(lc);
            var p = MemoryPipe.pair(Framing.BYTE_STREAM);
            var ended = watchEnd(p[0]);
            l.handle(p[1], JoinHint.inBand(TestAddr.peer("a")));
            assertTrue(inDecider.await(5, TimeUnit.SECONDS));
            l.close();
            closeDone.countDown();
            ended.get(5, TimeUnit.SECONDS);
            await(() -> pending.peek().isCancelled());
            assertEquals(new AdmissionStats(0, 0, 0, 1, 0, 0, 0), l.stats().admission());
        }
    }

    @Test void everyVerdictCountsOnceInItsOwnFieldAndNotAsARejection() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var next = new AtomicReference<Admission.Decision>();
            var lc = ListenerConfig.builder().session(config(loops)).maxPendingPerSource(3)
                    .admission(Admission.of(0, Admission.Decider.immediate((info, prefix) -> {
                        var d = next.get();
                        if (d == null) throw new IllegalStateException("decider failure");
                        return d;
                    })).withStall(Duration.ofSeconds(20), Duration.ofSeconds(20))).build();
            try (var l = Zmux.listener(lc)) {
                java.util.function.Function<Admission.Decision, ZmuxListener.Handled> carrier = d -> {
                    next.set(d);
                    var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var h = l.handle(p[1], JoinHint.inBand(TestAddr.peer("s")),
                            (io, info) -> { io.reader().close(); io.writer().close(); });
                    try { h.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS); } catch (Exception e) { throw new AssertionError(e); }
                    return h;
                };
                assertEquals(Admission.Verdict.CLOSE, carrier.apply(Admission.Decision.CLOSE).verdict().toCompletableFuture().join());
                assertEquals(Admission.Verdict.CLOSE, carrier.apply(null).verdict().toCompletableFuture().join()); // a decider that throws closes
                assertEquals(Admission.Verdict.CLOSE, carrier.apply(Admission.Decision.accept(5)).verdict().toCompletableFuture().join()); // a skip outside the prefix closes
                assertEquals(Admission.Verdict.FALLBACK, carrier.apply(Admission.Decision.FALLBACK).verdict().toCompletableFuture().join());
                assertEquals(Admission.Verdict.STALL, carrier.apply(Admission.Decision.STALL).verdict().toCompletableFuture().join());
                assertEquals(Admission.Verdict.STALL, carrier.apply(Admission.Decision.STALL).verdict().toCompletableFuture().join());
                // Two stalls hold two of the source's three slots: a third stall is closed at once.
                assertEquals(Admission.Verdict.CLOSE, carrier.apply(Admission.Decision.STALL).verdict().toCompletableFuture().join());
                await(() -> l.stats().admission().stallOverflow() == 1);
                assertEquals(new AdmissionStats(0, 1, 2, 3, 1, 0, 0), l.stats().admission());
                assertEquals(0, l.stats().rejected(), "an admission's decision is not a rejection");
            }
        }
    }

    @Test void theSourceSlotComesBeforeTheTlsHandshakeAndAFailedHandshakeCountsOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).timeouts(Timeouts.defaults().withHandshake(Duration.ofMillis(800))).build();
            for (boolean admission : new boolean[] {true, false}) {
                var b = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).maxPendingPerSource(1);
                if (admission) b.admission(Admission.of(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.accept(0))));
                try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), b.build());
                     var silent = new Socket("127.0.0.1", l.localAddress().getPort())) {
                    Thread.sleep(200);
                    try (var second = new Socket("127.0.0.1", l.localAddress().getPort())) {
                        second.setSoTimeout(3000);
                        InputStream in = second.getInputStream();
                        assertEquals(-1, in.read(), "the source's one slot is taken before the handshake");
                    }
                    await(() -> l.stats().refused() == 1);
                    // The silent carrier's handshake times out.
                    await(() -> admission ? l.stats().admission().handshakeFailed() == 1 : l.stats().setupFailures() == 1);
                    assertEquals(admission ? 0 : 1, l.stats().setupFailures());
                    assertEquals(admission ? 1 : 0, l.stats().admission().handshakeFailed());
                }
            }
        }
    }

    @Test void anOversizedPreambleIsRefusedWhenOpened() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            assertThrows(PreambleTooLargeException.class, () -> p.client().openStream(Preamble.openInfo(new byte[5000])));
            try (var s = p.client().openStream(Preamble.openInfo(new byte[100]))) {
                s.write(new byte[] {1});
                s.finish();
                try (var in = p.server().acceptStream(Duration.ofSeconds(5))) {
                    assertEquals(0, in.id(), "the refused open took no stream");
                    assertEquals(100, in.preamble().openInfo().length);
                }
            }
        }
    }

    @Test void aShutdownTakesNoNewSessionButJoinsUntilItsSessionsEnd() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var l = Zmux.listener(ListenerConfig.of(cfg));
            var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
            l.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
            var client = Zmux.open(boot[0], Role.CLIENT, cfg);
            var server = l.accept(Duration.ofSeconds(5));
            var done = l.shutdown(Duration.ofSeconds(30)).toCompletableFuture();
            assertThrows(ClosedChannelException.class, () -> l.accept(Duration.ofMillis(10)));
            // A new session is refused.
            var fresh = MemoryPipe.pair(Framing.BYTE_STREAM);
            long refused = l.stats().refused();
            l.handle(fresh[1], JoinHint.inBand(TestAddr.peer("d")));
            var connecting = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(fresh[0], Role.CLIENT, cfg); } catch (Exception e) { return null; }
            }, r -> Thread.ofVirtual().start(r));
            await(() -> l.stats().refused() == refused + 1);
            // The live session still joins a pipe.
            try (var reservation = client.paths().reserve(PipeOptions.builder().probation(Duration.ZERO).build())) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(pair[1], JoinHint.join(reservation.header(), TestAddr.peer("c")));
                reservation.attach(pair[0]);
                await(() -> l.stats().joins() == 1);
            }
            assertFalse(done.isDone(), "open while its session lives");
            assertFalse(l.isClosed());
            client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            done.get(5, TimeUnit.SECONDS);
            assertTrue(l.isClosed());
            var late = MemoryPipe.pair(Framing.BYTE_STREAM);
            assertFalse(l.handle(late[1], JoinHint.inBand(TestAddr.peer("c"))).taken().toCompletableFuture().get(5, TimeUnit.SECONDS));
            var c = connecting.get(10, TimeUnit.SECONDS);
            if (c != null) c.close();
        }
    }
}
