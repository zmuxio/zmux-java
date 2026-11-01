package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The statistics of API.md Section 7.2: each count is checked against what the test made happen. */
@Timeout(60)
class StatsTest {
    @Test
    void streamAndSessionCountsFollowTheirEvents() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig server = ZmuxConfig.builder().eventLoops(loops).datagrams(true).limits(Limits.defaults().withIncomingStreams(4)).build();
            ZmuxConfig client = ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
            PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
            PipeIo labelled = new PipeIo(p[0].reader(), p[0].writer(), p[0].framing(), "memory-a", p[0].carrierConn(), p[0].capabilities());
            Future<ZmuxSession> sf = ex.submit(() -> Zmux.open(p[1], Role.SERVER, server));
            try (ZmuxSession c = Zmux.open(labelled, Role.CLIENT, client); ZmuxSession s = sf.get(5, TimeUnit.SECONDS)) {
                assertEquals(1, c.stats().pipesAdded(), "the bootstrap pair is an attachment");
                PipeStats bootstrap = c.paths().stats().getFirst();
                assertEquals("memory-a", bootstrap.label());
                assertTrue(bootstrap.local(), "the dialer joined the bootstrap pipe");
                assertEquals(PipeState.Outbound.ACTIVE, bootstrap.outbound());
                assertTrue(bootstrap.lanes().isEmpty(), "a pipe without lanes has no lane positions");
                Thread.sleep(60);
                assertTrue(c.paths().stats().getFirst().attachedFor().minus(bootstrap.attachedFor()).toMillis() >= 50,
                        "an idle session's pipe statistics are of the current time");

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
                assertTrue(w.pipeId().isEmpty(), "no affinity binding outside multipath");

                // Datagrams: session and stream counts and bytes, then a stream's full queue.
                assertTrue(c.datagrams().send(ByteBuffer.wrap(new byte[100])));
                assertEquals(100, s.datagrams().receive(Duration.ofSeconds(5)).length);
                ZmuxStream ds = c.openStream();
                ds.write(new byte[] {1});
                ZmuxStream dr = s.acceptStream(Duration.ofSeconds(5));
                assertTrue(ds.sendDatagram(5, ByteBuffer.wrap(new byte[40])));
                StreamDatagram d = dr.receiveDatagram(Duration.ofSeconds(5));
                assertEquals(5, d.context());
                assertEquals(40, d.payload().length);
                PipePoolTest.await(() -> ds.stats().datagramsSent() == 1);
                assertEquals(40, ds.stats().datagramBytesSent());
                assertEquals(1, dr.stats().datagramsReceived());
                assertEquals(40, dr.stats().datagramBytesReceived());
                PipePoolTest.await(() -> c.stats().datagramBytesSent() == 140 && s.stats().datagramBytesReceived() == 140);
                assertEquals(2, c.stats().datagramsSent());

                // Once the peer's credit is used, an open waits: the server allows four streams.
                assertEquals(0, c.stats().streamCreditWaits());
                var held = new java.util.ArrayList<ZmuxStream>();
                while (true) {
                    try { held.add(c.tryOpenStream(Preamble.EMPTY)); } catch (IOException noCredit) { break; }
                }
                Future<ZmuxStream> waiting = ex.submit(() -> c.openStream());
                PipePoolTest.await(() -> c.stats().streamCreditWaits() == 1);
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
                PipePoolTest.await(() -> st.stats().bytesWritten() == data.length);
                c.close();
                c.awaitClosed();
                StreamStats last = st.stats();
                assertEquals(data.length, last.bytesWritten());
                assertEquals(0, last.bytesRead(), "the server wrote nothing back");
                assertEquals(data.length, last.bytesAcknowledged());
                assertEquals(data.length + 1, c.stats().bytesWritten(), "the sum over its streams");
                assertNotNull(c.stats().smoothedRtt(), "QX_PING or the carrier measured the RTT");
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    private static ZmuxConfig.Builder multipath(EventLoops loops, byte[] psk) {
        return ZmuxConfig.builder().eventLoops(loops)
                .multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(psk)).withProbation(Duration.ZERO));
    }

    @Test
    void aJoinWithTheWrongSecretIsReportedAndCounted() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            byte[] other = new byte[32];
            other[0] = 1;
            var lc = ListenerConfig.builder().session(multipath(loops, new byte[32]).build()).firstRecordTimeout(Duration.ofMillis(200)).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), lc)) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), multipath(loops, other).build());
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                assertEquals(1, l.stats().sessions());
                assertThrows(IOException.class, () -> c.paths().add(l.localAddress(), PipeOptions.defaults()));
                SessionEvent e;
                do e = s.nextEvent(Duration.ofSeconds(5)); while (!(e instanceof SessionEvent.JoinAuthFailed));
                assertEquals("mac", ((SessionEvent.JoinAuthFailed) e).reason());
                assertEquals(1, s.stats().joinAuthFailures());
                PipePoolTest.await(() -> l.stats().rejected() == 1);
                assertEquals(0, l.stats().joins());

                // A carrier without a first record times out; one that is not zmux is rejected.
                try (Socket idle = new Socket(l.localAddress().getAddress(), l.localAddress().getPort())) {
                    PipePoolTest.await(() -> l.stats().timedOut() == 1);
                }
                try (Socket junk = new Socket(l.localAddress().getAddress(), l.localAddress().getPort())) {
                    OutputStream out = junk.getOutputStream();
                    out.write("GET / HTTP/1.1\r\n\r\n".getBytes());
                    out.flush();
                    PipePoolTest.await(() -> l.stats().rejected() == 2);
                }
                ListenerStats ls = l.stats();
                assertEquals(1 + 0 + 2 + 0 + 1, ls.sessions() + ls.joins() + ls.rejected() + ls.refused() + ls.timedOut());
                assertEquals(0, ls.setupFailures());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
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
                ZmuxPipe added = c.paths().add(l.localAddress(), PipeOptions.defaults());
                PipePoolTest.await(() -> l.stats().joins() == 1);
                assertEquals(2, c.stats().pipesAdded());
                PipePoolTest.await(() -> c.stats().pipesActive() == 2);
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
                PipePoolTest.await(() -> c.stats().bytesRedundant() > 0);
                long perPipe = c.paths().stats().stream().mapToLong(PipeStats::bytesRedundant).sum();
                assertEquals(c.stats().bytesRedundant(), perPipe, "the session total is the sum over its pipes");
                PipePoolTest.await(() -> s.stats().bytesDuplicateReceived() > 0);
                assertEquals(s.stats().bytesDuplicateReceived(),
                        s.paths().stats().stream().mapToLong(PipeStats::bytesDuplicateReceived).sum());
                assertTrue(w.stats().bytesSent() > data.length, "copies are sent again");
                PipePoolTest.await(() -> w.stats().bytesAcknowledged() == data.length);

                // Aborting a pipe is a failure, with its reason.
                added.abort();
                PipePoolTest.await(() -> c.stats().pipeFailures() == 1 && c.stats().pipesActive() == 1);
                w.close();
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test
    void aJoinRefusedAtThePeersPipeLimitIsCounted() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig server = ZmuxConfig.builder().eventLoops(loops)
                    .multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.psk(new byte[32]))).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(server))) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), ZmuxConfig.builder().eventLoops(loops)
                        .multipath(MultipathConfig.defaults().withMaxPipes(16).withAuth(PipeAuth.psk(new byte[32]))).build());
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                // The peer's limit is its pipe count and a headroom of 8: joins beyond it are refused.
                int added = 0;
                for (int i = 0; i < 14 && c.stats().joinRefusals() == 0; i++) {
                    try { c.paths().add(l.localAddress(), PipeOptions.defaults()); added++; } catch (IOException refused) { /* Counted below. */ }
                    Thread.sleep(5);
                }
                PipePoolTest.await(() -> c.stats().joinRefusals() == 1);
                assertEquals(9, l.stats().joins(), "with the bootstrap pipe, two pipes and the headroom");
                int joined = added;
                PipePoolTest.await(() -> l.stats().refused() >= 1 && l.stats().joins() + l.stats().refused() >= joined + 1);
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test
    void pingsAreAnsweredByAcknowledgements() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = multipath(loops, new byte[32]).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                Future<ZmuxSession> sf = ex.submit((java.util.concurrent.Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), cfg);
                ZmuxSession s = sf.get(5, TimeUnit.SECONDS);
                ZmuxPipe added = c.paths().add(l.localAddress(), PipeOptions.builder().probeInterval(Duration.ofSeconds(1)).build());
                PipePoolTest.await(() -> added.stats().orElseThrow().pingsAnswered() >= 1);
                PipeStats st = added.stats().orElseThrow();
                assertTrue(st.pingsAnswered() <= st.pingsSent());
                assertNotNull(st.sinceLastAck());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
