package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.VarInt;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Pipe liveness: answers to probes, deadlines made due in a timer pass, delayed first records. */
@Timeout(60)
class LivenessTest {
    static ZmuxConfig.Builder multipath(ZmuxConfig.Builder b) {
        return b.maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation();
    }

    /**
     * A last pipe whose T_dead equals its T_suspect (DeadTimeout below T_suspect) fails in the timer pass that makes
     * it suspect: the session without resume ends with its pipes lost at once, not at its idle timeout.
     */
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void aDeadTimeoutBelowTheSuspicionFailsTheLastPipeInTheSamePass() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).liveness(Liveness.builder().noKeepAlivePeriod().probeInterval(Duration.ofSeconds(1)).probeJitter(0).deadTimeout(Duration.ofSeconds(1)).suspectFloor(Duration.ofSeconds(2)).build())
                    .maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                            .build();
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
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void anEndWithProbesOffAnswersWithAProbeOfItsOwn() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var ccfg = multipath(ZmuxConfig.builder().eventLoops(loops).liveness(Liveness.builder().probeJitter(0).noKeepAlivePeriod().build())).build();
            var scfg = multipath(ZmuxConfig.builder().eventLoops(loops).liveness(Liveness.builder().noProbeInterval().noKeepAlivePeriod().build())).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(scfg));
                 var client = Zmux.connect(listener.localAddress(), ccfg); var server = listener.accept(Duration.ofSeconds(5))) {
                var pipe = client.addPipe(listener.localAddress(), PipeOptions.builder().liveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(2)).build()).build());
                Thread.sleep(1000);
                ZmuxPipe peer = server.pipes().stream().filter(p -> p.id().value() == pipe.id().value()).findFirst().orElseThrow();
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
        final java.util.List<byte[]> writes = new java.util.ArrayList<>();
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            ByteArrayOutputStream one = new ByteArrayOutputStream();
            for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) {
                var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); one.writeBytes(a);
            }
            synchronized (writes) { writes.add(one.toByteArray()); }
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }
        /** The first record after the Join's Reply, which is one write. */
        byte[] firstRecord() {
            byte[] b;
            synchronized (writes) { if (writes.size() < 2) return null; b = writes.get(1); }
            long n = VarInt.get(ByteBuffer.wrap(b), 0, b.length);
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
     * still leads with the PIPE_ACK snapshot and carries the PIPE_PING.
     */
    @Test void aDelayedFirstRecordOfAnAcceptedJoinKeepsItsSnapshotAndPing() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var ccfg = multipath(ZmuxConfig.builder().eventLoops(loops)).recordPolicy(c -> RecordMode.TRUSTED).build();
            var scfg = multipath(ZmuxConfig.builder().eventLoops(loops)).allowRecordMode((c, m, p) -> true).shaper(ShapingTest.firstDelay(30_000_000, false)).build();
            try (var l = Zmux.listener(ListenerConfig.of(scfg))) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(boot[1], JoinHint.inBand(TestAddr.peer("peer"))).closed();
                try (var c = Zmux.open(boot[0], Role.CLIENT, ccfg); var s = l.accept(Duration.ofSeconds(5))) {
                    ShapingTest.transfer(c, s, 100);
                    var m = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var rec = new Recorder(m[1].writer());
                    l.adapter().handle(new PipeIo(m[1].reader(), rec, Framing.BYTE_STREAM, "memory", null), JoinHint.inBand(TestAddr.peer("peer"))).closed();
                    c.addPipe(m[0], PipeOptions.defaults());
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
