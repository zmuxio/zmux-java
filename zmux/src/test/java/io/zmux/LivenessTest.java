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

/** The first liveness record of a newly joined carrier. */
@Timeout(60)
class LivenessTest {
    static ZmuxConfig.Builder multipath(ZmuxConfig.Builder b) {
        return b.sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation();
    }

    /** Records what a session writes to one carrier. */
    static final class Recorder implements PipeWriter {
        final PipeWriter inner;
        final java.util.concurrent.LinkedBlockingQueue<byte[]> writes = new java.util.concurrent.LinkedBlockingQueue<>();
        Recorder(PipeWriter inner) { this.inner = inner; }
        @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
            ByteArrayOutputStream one = new ByteArrayOutputStream();
            for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) {
                var d = b.duplicate(); byte[] a = new byte[d.remaining()]; d.get(a); one.writeBytes(a);
            }
            writes.add(one.toByteArray());
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }
        /** The first record after the Join's Reply, which is one write. */
        byte[] firstRecord() throws InterruptedException {
            writes.take(); // Join Reply.
            byte[] b = writes.take();
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
                    byte[] first = rec.firstRecord();
                    assertNotNull(first, "first join record is complete");
                    byte[] ack = type(FrameType.PIPE_ACK);
                    assertArrayEquals(ack, Arrays.copyOf(first, ack.length), "the first record leads with the snapshot");
                    assertTrue(contains(first, type(FrameType.PIPE_PING)), "and holds the PIPE_PING");
                }
            }
            Leaks.none(loops);
        }
    }
}
