package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The listener's classification: every pipe input handed to it is counted once, in exactly
 * one of sessions, joins, rejected, refused and timedOut, and setupFailures stays out of that sum.
 */
@Timeout(60)
class ListenerStatsTest {
    static long sum(ListenerStats s) {
        return s.sessions() + s.joins() + s.rejected() + s.refused() + s.timedOut();
    }

    static PipeIo bytes(byte[] b) {
        PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
        p[0].writer().write(OutboundBatch.of(ByteBuffer.wrap(b)), new PipeWriter.Completion() {
            @Override public void completed() {}
            @Override public void failed(Throwable cause) {}
        });
        return p[1];
    }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void everyPipeInputCountsOnceInOneOutcome() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).handshakeTimeout(Duration.ofMillis(800)).build();
            var lc = ListenerConfig.builder().session(cfg).firstRecordTimeout(Duration.ofMillis(100)).maxPendingPerSource(1).build();
            int inputs = 0;
            try (var l = ZmuxListener.of(lc)) {
                // A split bootstrap: two carriers, one input, one session.
                var up = MemoryPipe.pair(Framing.BYTE_STREAM);
                var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                String token = SplitBootstrapTest.token(1);
                l.adapter().handle(PipeIo.of(up[1].reader(), null), JoinHint.pair(token, true, TestAddr.peer("a"))).closed();
                // Its halves arrive together: each holds a slot of its source until it waits for the other.
                l.adapter().handle(PipeIo.of(null, down[1].writer()), JoinHint.pair(token, false, TestAddr.peer("a2"))).closed();
                inputs++;
                var connecting = ex.submit(() -> Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()), PipeIo.of(down[0].reader(), null)),
                        Role.CLIENT, cfg));
                try (var client = connecting.get(5, TimeUnit.SECONDS); var server = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(server);
                    PipePoolTest.await(() -> l.stats().sessions() == 1);
                    // The token again: a replayed half, rejected.
                    l.adapter().handle(PipeIo.of(null, MemoryPipe.pair(Framing.BYTE_STREAM)[1].writer()), JoinHint.pair(token, false, TestAddr.peer("b"))).closed();
                    inputs++;
                    PipePoolTest.await(() -> l.stats().rejected() == 1);
                    // An uplink half without a first record: ended by the handshake timeout, on its own.
                    l.adapter().handle(PipeIo.of(MemoryPipe.pair(Framing.BYTE_STREAM)[1].reader(), null), JoinHint.pair(SplitBootstrapTest.token(2), true, TestAddr.peer("c"))).closed();
                    inputs++;
                    PipePoolTest.await(() -> l.stats().timedOut() == 1);
                    // Not zmux at all: rejected.
                    l.adapter().handle(bytes("GET / HTTP/1.1\r\n\r\n".getBytes(UTF_8)), JoinHint.inBand(TestAddr.peer("d"))).closed();
                    inputs++;
                    PipePoolTest.await(() -> l.stats().rejected() == 2);
                    // A pending carrier holds its source's only slot; the next one of that source is refused.
                    var idle = MemoryPipe.pair(Framing.BYTE_STREAM);
                    assertNotNull(l.adapter().reserve(TestAddr.peer("e"), null).handle(idle[1], JoinHint.inBand(TestAddr.peer("e")), null));
                    inputs++;
                    assertNull(l.adapter().reserve(TestAddr.peer("e"), null));
                    inputs++;
                    assertEquals(1, l.stats().refused());
                    PipePoolTest.await(() -> l.stats().timedOut() == 2);
                    // An adapter's connection that never became an input.
                    l.adapter().countSetupFailure();
                    ListenerStats s = l.stats();
                    assertEquals(new ListenerStats(1, 0, 1, 2, 1, 2, new AdmissionStats(0, 0, 0, 0, 0, 0, 0)), s);
                    assertEquals(inputs, sum(s), "every input once");
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void aStalledCarrierCountsInTheAdmissionWhenDecided() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.STALL))
                            .withStall(Duration.ofMillis(300), Duration.ofMillis(300))).build();
            try (var l = ZmuxListener.of(lc)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var admitted = l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("s")), null);
                assertEquals(AdmissionVerdict.STALL, admitted.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                PipePoolTest.await(() -> l.stats().admission().stalled() == 1);
                assertEquals(0, sum(l.stats()), "an admission's decision is no pipe input");
                Thread.sleep(500); // the stall ends
                assertEquals(0, l.stats().rejected());
                p[0].reader().close(); p[0].writer().close();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    /**
     * A first record that is not a well-formed Hello (empty, a truncated or trailing-garbage
     * body, another frame type) is closed and counted rejected, and leaves the established session working.
     */
    @Test void aFirstRecordThatIsNoHelloIsRejectedAndLeavesTheSessionAlone() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops)
                    .maxPipes(8).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            var lc = ListenerConfig.builder().session(cfg).firstRecordTimeout(Duration.ofSeconds(10)).build();
            try (var l = ZmuxListener.of(lc)) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(boot[1], JoinHint.inBand(TestAddr.peer("peer"))).closed();
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    byte[] tag = {(byte) 0x9a, 0x24, (byte) 0xde, 0x36};
                    byte[][] records = {
                            {0x00}, // an empty record
                            record(tag, new byte[] {0x3f}, new byte[21]), // the join tag, its body truncated
                            record(tag, new byte[22]), // the join tag over a zero body
                            record(tag, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, new byte[18]), // a garbled join, a second frame after it
                            record(new byte[] {0x01, 0x00}, new byte[24]), // two frames of another type
                    };
                    for (int i = 0; i < records.length; i++) {
                        long before = l.stats().rejected();
                        AdmissionTest.silentCarrier(l, "peer", records[i]).get(5, TimeUnit.SECONDS); // closed
                        int n = i;
                        PipePoolTest.await(() -> l.stats().rejected() == before + 1);
                        assertEquals(0, l.stats().joins(), "record " + n);
                    }
                    assertFalse(s.closed().isDone() || c.closed().isDone(), "the session is not affected");
                    try (var st = c.openStream()) {
                        st.write(new byte[] {7, 8, 9});
                        st.finish();
                        try (var in = s.acceptStream(Duration.ofSeconds(5))) { assertArrayEquals(new byte[] {7, 8, 9}, in.readAll(16)); }
                    }
                }
            }
        }
    }

    /** A record of the concatenated body parts, prefixed with its one-byte Size. */
    static byte[] record(byte[]... parts) {
        var o = new ByteArrayOutputStream();
        int n = 0;
        for (byte[] p : parts) n += p.length;
        o.write(n);
        for (byte[] p : parts) o.writeBytes(p);
        return o.toByteArray();
    }
}
