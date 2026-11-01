package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.wire.FrameType;
import io.zmux.carrier.VarInt;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Pipe liveness (MULTIPATH.md 5.2, 6.5, 7.1): answers to probes, deadlines made due in a timer pass, delayed first records. */
@Timeout(60)
class LivenessTest {
    static MultipathConfig multipath() {
        return MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO);
    }

    /**
     * A last pipe whose T_dead equals its T_suspect (DeadTimeout below T_suspect) fails in the timer pass that makes
     * it suspect: the session without resume ends with its pipes lost at once, not at its idle timeout.
     */
    @Test void aDeadTimeoutBelowTheSuspicionFailsTheLastPipeInTheSamePass() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).timeouts(Timeouts.defaults().disableKeepAlive())
                    .multipath(multipath().withProbeInterval(Duration.ofSeconds(1)).withProbeJitter(0).withDeadTimeout(Duration.ofSeconds(1))
                            .withSuspectFloor(Duration.ofSeconds(2))).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(relay.address(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                relay.blackhole = true;
                long start = System.nanoTime();
                SessionError e = client.awaitClosed();
                assertInstanceOf(SessionError.PipesLost.class, e);
                // k = 1 s, then T_suspect = T_dead = 2 s, and a margin; the idle timeout is 30 s.
                assertTrue(System.nanoTime() - start < 8_000_000_000L, "ended after " + (System.nanoTime() - start) / 1_000_000 + " ms");
                server.close();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    /**
     * An end with its idle probes turned off still answers the peer's PIPE_PING on the probed pipe pair, and adds a
     * PIPE_PING of its own once that pipe has been idle for max(1 s, T_suspect): one exchange checks both directions.
     */
    @Test void anEndWithProbesOffAnswersWithAProbeOfItsOwn() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var ccfg = ZmuxConfig.builder().eventLoops(loops).timeouts(Timeouts.defaults().disableKeepAlive()).multipath(multipath().withProbeJitter(0)).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).timeouts(Timeouts.defaults().disableKeepAlive()).multipath(multipath().disableIdleProbes()).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(scfg));
                 var client = Zmux.connect(listener.localAddress(), ccfg); var server = listener.accept(Duration.ofSeconds(5))) {
                var pipe = client.paths().add(listener.localAddress(), PipeOptions.builder().probeInterval(Duration.ofSeconds(2)).build());
                Thread.sleep(1000);
                ZmuxPipe peer = server.paths().list().stream().filter(p -> p.id() == pipe.id()).findFirst().orElseThrow();
                long before = peer.stats().orElseThrow().pingsSent();
                Thread.sleep(7000);
                long answers = peer.stats().orElseThrow().pingsSent() - before;
                assertTrue(answers >= 2, answers + " PIPE_PINGs in the answers to a probe every 2 s");
                assertEquals(PipeState.Outbound.ACTIVE, peer.stats().orElseThrow().outbound());
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    /** Records what a session writes to one carrier. */
    static final class Recorder implements PipeWriter {
        final PipeWriter inner;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            synchronized (out) {
                for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) {
                    var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); out.writeBytes(a);
                }
            }
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }
        byte[] firstRecord() {
            byte[] b; synchronized (out) { b = out.toByteArray(); }
            if (b.length == 0) return null;
            ByteBuffer bb = ByteBuffer.wrap(b);
            long n = VarInt.get(bb, 0, b.length);
            int start = VarInt.lengthOf(b[0]);
            return start + n > b.length ? null : Arrays.copyOfRange(b, start, (int) (start + n));
        }
    }

    static byte[] type(long t) {
        ByteBuffer b = ByteBuffer.allocate(8);
        VarInt.put(b, t);
        return Arrays.copyOf(b.array(), b.position());
    }

    static boolean contains(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int k = 0; k < needle.length; k++) if (hay[i + k] != needle[k]) continue outer;
            return true;
        }
        return false;
    }

    /**
     * The acceptor of a join whose records are delayed delays the pipe's first record whole: once the delay ends it
     * still leads with the PIPE_ACK snapshot and carries the PIPE_PING (MULTIPATH.md 5.2).
     */
    @Test void aDelayedFirstRecordOfAnAcceptedJoinKeepsItsSnapshotAndPing() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var ccfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath()).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath()).shaper(ShapingTest.firstDelay(30_000_000, false)).build();
            try (var l = Zmux.listener(ListenerConfig.of(scfg))) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(boot[1], JoinHint.inBand(TestAddr.peer("peer"))).closed();
                try (var c = Zmux.open(boot[0], Role.CLIENT, ccfg); var s = l.accept(Duration.ofSeconds(5))) {
                    ShapingTest.transfer(c, s, 100);
                    var m = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var rec = new Recorder(m[1].writer());
                    l.handle(new PipeIo(m[1].reader(), rec, Framing.BYTE_STREAM, "memory", null), JoinHint.inBand(TestAddr.peer("peer"))).closed();
                    c.paths().add(m[0], PipeOptions.defaults());
                    long until = System.nanoTime() + 5_000_000_000L;
                    byte[] first;
                    while ((first = rec.firstRecord()) == null) {
                        assertTrue(System.nanoTime() < until, "no record on the joined pipe");
                        Thread.sleep(5);
                    }
                    byte[] ack = type(FrameType.PIPE_ACK);
                    assertArrayEquals(ack, Arrays.copyOf(first, ack.length), "the first record leads with the snapshot");
                    assertTrue(contains(first, type(FrameType.PIPE_PING)), "and holds the PIPE_PING");
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
