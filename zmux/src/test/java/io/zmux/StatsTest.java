package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.CarrierProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The session and pipe statistics: each count is checked against what the test made happen. */
@Timeout(60)
class StatsTest {
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test
    void streamAndSessionCountsFollowTheirEvents() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig server = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).maxIncomingStreams(4).build();
            ZmuxConfig client = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
            PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
            PipeIo labelled = new PipeIo(p[0].reader(), p[0].writer(), p[0].framing(), "memory-a", p[0].carrierConn()).withCapabilities(p[0].capabilities());
            Future<ZmuxSession> sf = ex.submit(() -> Zmux.open(p[1], Role.SERVER, server));
            try (ZmuxSession c = Zmux.open(labelled, Role.CLIENT, client); ZmuxSession s = sf.get(5, TimeUnit.SECONDS)) {
                assertEquals(1, c.stats().pipesAdded(), "the bootstrap pair is an attachment");
                PipeStats bootstrap = c.pipeStats().getFirst();
                assertEquals("memory-a", bootstrap.label());
                assertTrue(bootstrap.local(), "the dialer joined the bootstrap pipe");
                assertEquals(PipeState.Outbound.ACTIVE, bootstrap.outbound());
                assertTrue(bootstrap.carrierSample().isEmpty(), "the memory carrier has no connection identity");
                assertTrue(c.pipes().getFirst().stats().orElseThrow().carrierSample().isEmpty());
                assertTrue(bootstrap.lanes().isEmpty(), "a pipe without lanes has no lane positions");
                assertEquals(Optional.empty(), new ZmuxPipe(c, 2).stats(), "a missing pipe has no snapshot");
                byte[] data = new byte[300_000];
                new Random(7).nextBytes(data);
                ZmuxStream st = c.openStream();
                st.write(data);
                Future<byte[]> got = ex.submit(() -> {
                    ZmuxStream in = s.acceptStream();
                    byte[] b = SessionTest.readAll(in);
                    StreamStats r = in.stats();
                    assertEquals(data.length, r.bytesRead());
                    assertEquals(data.length, r.bytesReceived(), "distinct payload bytes, the preamble excluded");
                    assertEquals(0, r.recvBuffered());
                    assertTrue(r.recvWindow() > 0);
                    assertEquals(0, r.bytesWritten(), "nothing written on the reverse direction");
                    in.finish();
                    in.close();
                    return b;
                });
                st.finish();
                assertEquals(data.length, got.get(10, TimeUnit.SECONDS).length);
                SessionTest.readAll(st);
                StreamStats w = st.stats();
                assertEquals(data.length, w.bytesWritten());
                assertEquals(data.length, w.bytesSent(), "one copy of each byte without multipath");
                assertEquals(data.length, w.bytesAcknowledged(), "passed to the carrier");
                assertEquals(0, w.sendBuffered());
                assertTrue(w.pipe().isEmpty(), "no affinity binding outside multipath");

                // Datagrams: session and stream counts and bytes, then a stream's full queue.
                c.datagrams().send(ByteBuffer.wrap(new byte[100]));
                assertEquals(100, s.datagrams().receive(Duration.ofSeconds(5)).length);
                ZmuxStream ds = c.openStream();
                ds.write(new byte[] {1});
                ZmuxStream dr = s.acceptStream(Duration.ofSeconds(5));
                ds.sendDatagram(5, ByteBuffer.wrap(new byte[40]));
                StreamDatagram d = dr.receiveDatagram(Duration.ofSeconds(5));
                assertEquals(5, d.context());
                assertEquals(40, d.payload().length);
                SessionTestSupport.await(c, () -> ds.stats().datagramsSent() == 1);
                assertEquals(40, ds.stats().datagramBytesSent());
                assertEquals(1, dr.stats().datagramsReceived());
                assertEquals(40, dr.stats().datagramBytesReceived());
                SessionTestSupport.await(c, () -> c.stats().datagramBytesSent() == 140);
                SessionTestSupport.await(s, () -> s.stats().datagramBytesReceived() == 140);
                assertEquals(2, c.stats().datagramsSent());

                // Once the peer's credit is used, an open waits: the server allows four streams.
                assertEquals(0, c.stats().streamCreditWaits());
                var held = new java.util.ArrayList<ZmuxStream>();
                while (true) {
                    try { held.add(c.tryOpenStream(Preamble.EMPTY)); } catch (IOException noCredit) { break; }
                }
                Future<ZmuxStream> waiting = ex.submit(() -> c.openStream());
                SessionTestSupport.await(c, () -> c.stats().streamCreditWaits() == 1);
                assertFalse(waiting.isDone());
                assertTrue(c.stats().streamsActive() >= held.size());
                ds.close();
                dr.close();
                ZmuxStream fifth = waiting.get(5, TimeUnit.SECONDS);
                assertEquals(1, c.stats().streamCreditWaits(), "one wait per open, however often it wakes");
                fifth.close();
                for (ZmuxStream h : held) h.close();

                // The final snapshot stays readable once the session released the stream.
                st.close();
                SessionTestSupport.await(c, () -> st.stats().bytesWritten() == data.length);
                c.close();
                c.awaitClosed();
                StreamStats last = st.stats();
                assertEquals(data.length, last.bytesWritten());
                assertEquals(0, last.bytesRead(), "the server wrote nothing back");
                assertEquals(data.length, last.bytesAcknowledged());
                assertEquals(data.length + 1, c.stats().bytesWritten(), "the sum over its streams");
                assertNotNull(c.stats().smoothedRtt(), "QX_PING or the carrier measured the RTT");
            }
            Leaks.none(loops);
        }
    }

    @Test void idlePipeStatsUseTheCurrentClock() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        try (var loops = EventLoops.create(1); var pair = Pair.memory(
                ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain()).build())) {
            var elapsed = Duration.ofMillis(150);
            var before = pair.client().pipeStats().getFirst().attachedFor();
            clock.advance(elapsed);
            assertEquals(before.plus(elapsed), pair.client().pipeStats().getFirst().attachedFor());
        }
    }

    @Test
    void nativeTcpIdentifiesConnectionsWithUnknownCounts() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var first = Zmux.connect(listener.localAddress(), cfg);
                 var firstPeer = listener.accept(Duration.ofSeconds(5));
                 var second = Zmux.connect(listener.localAddress(), cfg);
                 var secondPeer = listener.accept(Duration.ofSeconds(5))) {
                var firstSample = tcpSample(first);
                var secondSample = tcpSample(second);
                assertFalse(firstSample.carrierId().equals(secondSample.carrierId()));
                assertFalse(firstSample.carrierId().equals(tcpSample(firstPeer).carrierId()));
                assertFalse(secondSample.carrierId().equals(tcpSample(secondPeer).carrierId()));
            }
        }
    }

    @Test
    void receiveOnlyPipeHasNoSampleEvenWithAConnectionIdentity() throws Exception {
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 3))) {
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            f.listener.adapter().handle(pair[1].withCarrierConn(new Object()),
                    JoinHint.inBand(TestAddr.peer("receive-only-stats"))).closed();
            var sender = f.client.addPipe(pair[0].withCarrierConn(new Object()),
                    PipeOptions.builder().direction(PipeOptions.Direction.SEND_ONLY).build());
            sender.awaitAttached(Duration.ofSeconds(5));
            assertTrue(sender.stats().orElseThrow().carrierSample().isPresent());
            var receiver = f.server.pipes().stream().filter(p -> p.id().equals(sender.id())).findFirst().orElseThrow();
            var snapshot = receiver.stats().orElseThrow();
            assertNull(snapshot.outbound());
            assertTrue(snapshot.carrierSample().isEmpty());
            assertTrue(f.server.pipeStats().stream().filter(p -> p.pipe().equals(sender.id())).findFirst()
                    .orElseThrow().carrierSample().isEmpty());
        }
    }

    private static CarrierSample tcpSample(ZmuxSession session) throws Exception {
        SessionTestSupport.await(session, () -> session.pipeStats().getFirst().carrierSample().isPresent());
        CarrierSample sample = session.pipeStats().getFirst().carrierSample().orElseThrow();
        assertEquals(CarrierSource.UNKNOWN, sample.source());
        assertEquals(Direction.OUTBOUND, sample.direction());
        assertEquals(0, sample.generation());
        assertTrue(sample.counts().isEmpty());
        return sample;
    }

    @Test
    void carrierQueriesStartOnlyOnAnExplicitStatisticsRead() throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            var writer = new SampledCarrierWriter(pair[0].writer());
            var io = new PipeIo(pair[0].reader(), writer, pair[0].framing(), "sampled-memory", writer);
            var accepting = workers.submit(() -> Zmux.open(pair[1], Role.SERVER, cfg));
            try (var client = Zmux.open(io, Role.CLIENT, cfg); var _ = accepting.get(30, TimeUnit.SECONDS)) {
                var pipe = client.pipes().getFirst();
                assertEquals(0, writer.queries.get());
                assertTrue(pipe.stats().orElseThrow().carrierSample().isEmpty());
                assertTrue(writer.sampled.await(30, TimeUnit.SECONDS));
                SessionTestSupport.await(client, () -> pipe.stats().orElseThrow().carrierSample().isPresent());
                var single = pipe.stats().orElseThrow().carrierSample().orElseThrow();
                assertSame(single, client.pipeStats().getFirst().carrierSample().orElseThrow());
                assertEquals(new QuicCounters(1, 0), single.counts().orElseThrow());
                assertFalse(writer.sampledOnSessionLoop);
            }
        }
    }

    private static final class SampledCarrierWriter implements PipeWriter, CarrierProvider {
        private final PipeWriter delegate;
        private final AtomicInteger queries = new AtomicInteger();
        private final CountDownLatch sampled = new CountDownLatch(1);
        private volatile boolean sampledOnSessionLoop;

        private SampledCarrierWriter(PipeWriter delegate) { this.delegate = delegate; }

        @Override public Object carrierIdentity() { return this; }

        @Override public Query carrierQuery(Object identity) {
            var attachingThread = Thread.currentThread();
            return new Query(CarrierSource.NETTY_QUIC, complete -> {
                sampledOnSessionLoop = Thread.currentThread() == attachingThread;
                complete.accept(System.nanoTime(), new QuicCounters(queries.incrementAndGet(), 0));
                sampled.countDown();
            });
        }

        @Override public void write(OutboundBatch batch, PipeWriter.Completion done) { delegate.write(batch, done); }
        @Override public void close() { delegate.close(); }
        @Override public void abort(Throwable cause) { delegate.abort(cause); }
    }

    private static ZmuxConfig.Builder multipath(EventLoops loops, byte[] psk) {
        return ZmuxConfig.builder().eventLoops(loops)
                .sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(psk).withEndToEnd()).noProbation();
    }

    @Test
    void aJoinWithTheWrongSecretIsReportedAndCounted() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            byte[] other = new byte[32];
            other[0] = 1;
            var deadline = Duration.ofMinutes(1);
            var lc = ListenerConfig.builder().session(multipath(loops, new byte[32]).build()).firstRecordTimeout(deadline).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), lc)) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), multipath(loops, other).build());
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                assertEquals(1, l.stats().sessions());
                assertThrows(IOException.class, () -> c.addPipe(l.localAddress(), PipeOptions.defaults()));
                SessionEvent e;
                do e = s.nextEvent(Duration.ofSeconds(5)); while (!(e instanceof SessionEvent.JoinAuthFailed));
                assertEquals("auth", ((SessionEvent.JoinAuthFailed) e).reason());
                assertEquals(1, ((SessionEvent.JoinAuthFailed) e).count());
                assertEquals(1, s.stats().joinAuthFailures());
                SessionTestSupport.await(loops, () -> l.stats().rejected() == 1);
                assertEquals(0, l.stats().joins());

                // A carrier without a first record times out; one that is not zmux is rejected.
                var idle = GlobalBoundTest.handle(l, "idle");
                idle.sink.get(20, TimeUnit.SECONDS);
                ListenerStatsTest.expire(loops, deadline, idle.handled);
                assertEquals(1, l.stats().timedOut());
                try (Socket junk = new Socket(l.localAddress().getAddress(), l.localAddress().getPort())) {
                    OutputStream out = junk.getOutputStream();
                    out.write("GET / HTTP/1.1\r\n\r\n".getBytes(UTF_8));
                    out.flush();
                    SessionTestSupport.await(loops, () -> l.stats().rejected() == 2);
                }
                ListenerStats ls = l.stats();
                assertEquals(1 + 0 + 2 + 0 + 1, ls.sessions() + ls.joins() + ls.rejected() + ls.refused() + ls.timedOut());
                assertEquals(0, ls.setupFailures());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    @Test
    void redundantCopiesAreCountedAtTheSenderAndAsDuplicatesAtTheReceiver() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = multipath(loops, new byte[32]).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                ZmuxPipe added = c.addPipe(l.localAddress(), PipeOptions.defaults());
                SessionTestSupport.await(loops, () -> l.stats().joins() == 1);
                assertEquals(2, c.stats().pipesAdded());
                SessionTestSupport.await(c, () -> c.stats().pipesActive() == 2);
                PipeStats joined = added.stats().orElseThrow();
                assertTrue(joined.local());
                assertNull(joined.failure());
                assertTrue(joined.bytesAcked().isPresent(), "a multipath pipe has acknowledgements");
                Future<byte[]> in = ex.submit(() -> {
                    ZmuxStream r = s.acceptStream();
                    byte[] b = SessionTest.readAll(r);
                    r.close();
                    return b;
                });
                ZmuxStream w = c.openStream();
                w.setRedundant(true);
                byte[] data = new byte[1 << 20];
                w.write(data);
                w.finish();
                assertEquals(data.length, in.get(10, TimeUnit.SECONDS).length);
                SessionTestSupport.await(c, () -> c.stats().bytesRedundant() > 0);
                // The session total is the sum over its pipes, once one snapshot holds both.
                SessionTestSupport.await(c, () -> c.stats().bytesRedundant() == c.pipeStats().stream().mapToLong(PipeStats::bytesRedundant).sum());
                SessionTestSupport.await(s, () -> s.stats().bytesDuplicateReceived() > 0);
                assertEquals(s.stats().bytesDuplicateReceived(),
                        s.pipeStats().stream().mapToLong(PipeStats::bytesDuplicateReceived).sum());
                assertTrue(w.stats().bytesSent() > data.length, "copies are sent again");
                SessionTestSupport.await(c, () -> w.stats().bytesAcknowledged() == data.length);

                // Aborting a pipe is a failure, with its reason.
                added.abort();
                SessionTestSupport.await(c, () -> c.stats().pipeFailures() == 1 && c.stats().pipesActive() == 1);
                w.close();
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    @Test
    void aJoinRefusedAtThePeersPipeLimitIsCounted() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig server = ZmuxConfig.builder().eventLoops(loops)
                    .sessionPolicy(new SessionPolicy(new CapacityLimits(2))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(server))) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), ZmuxConfig.builder().eventLoops(loops)
                        .sessionPolicy(new SessionPolicy(new CapacityLimits(16))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build());
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                // The peer's limit is its pipe count and a headroom of 8: joins beyond it are refused.
                for (int i = 0; i < 9; i++) c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertThrows(JoinRefusedException.class, () -> c.addPipe(l.localAddress(), PipeOptions.defaults()));
                SessionTestSupport.await(c, () -> c.stats().joinRefusals() == 1);
                SessionTestSupport.await(loops, () -> l.stats().refused() == 1);
                assertEquals(9, l.stats().joins(), "with the bootstrap pipe, two pipes and the headroom");
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    @Test
    void pingsAreAnsweredByAcknowledgements() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var clock = new SpliceHooksTest.ManualClock();
            ZmuxConfig cfg = multipath(loops, new byte[32]).clockDomain(clock.domain())
                    .liveness(Liveness.builder().noKeepAlivePeriod().probeJitter(0).build()).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                ZmuxPipe added = c.addPipe(l.localAddress(), PipeOptions.builder().liveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(1)).build()).build());
                clock.advance(Duration.ofSeconds(1));
                SessionTestSupport.await(c, () -> added.stats().orElseThrow().pingsAnswered() >= 1);
                PipeStats st = added.stats().orElseThrow();
                assertTrue(st.pingsAnswered() <= st.pingsSent());
                assertNotNull(st.sinceLastAck());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            Leaks.none(loops);
        }
    }
}
