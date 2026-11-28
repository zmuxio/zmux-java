package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.CoreProbe;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The stream datagram-bytes limit and the stream credit maximum. */
@Timeout(30)
class StreamLimitsTest {
    static void await(java.util.function.BooleanSupplier done) throws InterruptedException {
        for (long end = System.nanoTime() + 5_000_000_000L; !done.getAsBoolean(); Thread.sleep(5))
            assertTrue(System.nanoTime() < end, "timed out");
    }

    private static Pair pair(EventLoops loops, UnaryOperator<ZmuxConfig.Builder> server) throws Exception {
        var c = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
        return Pair.memory(c, server.apply(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true)).build(), io.zmux.carrier.Framing.BYTE_STREAM);
    }

    /** Under a receive limiter that admits nothing, the datagrams awaiting it keep the stream's bounds. */
    @Test void datagramsAwaitingTheLimiterKeepTheStreamsBounds() throws Exception {
        // The receive hook takes one datagram at a time; holding its answer keeps the rest awaiting it.
        var answer = new java.util.concurrent.CountDownLatch(1);
        io.zmux.helper.Limiter never = new io.zmux.helper.Limiter() {
            @Override public java.util.concurrent.CompletionStage<Long> acquire(long min, long max) { return new java.util.concurrent.CompletableFuture<>(); }
            @Override public long tryAcquire(long min, long max) {
                try { answer.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return 0;
            }
            @Override public void refund(long bytes) { }
        };
        try (EventLoops loops = EventLoops.create(2);
             var p = pair(loops, b -> b.datagramStreamQueue(32).datagramStreamBytes(16 << 10).datagramLimiter(new io.zmux.helper.DatagramLimiter(null, never)))) {
            var st = p.client().openStream();
            st.write(new byte[] {0});
            var in = p.server().acceptStream(Duration.ofSeconds(5));
            for (int i = 0; i < 10; i++) st.sendDatagram(0, ByteBuffer.wrap(new byte[4 << 10]));
            await(() -> {
                try { return in.stats().datagramsDroppedRecv() >= 6; } catch (java.io.IOException e) { throw new AssertionError(e); }
            });
            // The ten have arrived once the session counted them all as received.
            await(() -> p.server().stats().datagramsReceived() >= 10);
            assertEquals(6, in.stats().datagramsDroppedRecv(), "four of 4 KiB stay within 16 KiB");
            answer.countDown();
        }
    }

    @Test void theStreamDatagramBoundsAreValidated() {
        var b = ZmuxConfig.builder();
        assertThrows(IllegalArgumentException.class, () -> b.datagramStreamBytes(0));
        assertEquals(java.util.OptionalLong.of(512 << 10), b.build().datagramStreamBytes());
        assertEquals(java.util.OptionalLong.empty(), b.noDatagramStreamBytes().build().datagramStreamBytes());
    }

    @Test void theStreamCreditMaximumIsAdvertisedAndRaised() throws Exception {
        long max = Zmux.MAX_STREAM_CREDIT;
        assertEquals(1L << 60, max);
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().maxIncomingStreams(max + 1));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().maxIncomingUniStreams(max + 1));
        try (EventLoops loops = EventLoops.create(2)) {
            try (var p = pair(loops, b -> b.noMemoryBudget().maxIncomingStreams(max).maxIncomingUniStreams(max))) {
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, true));
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, false));
            }
            try (var p = pair(loops, b -> b.noMemoryBudget().maxIncomingUniStreams(16))) {
                assertThrows(IllegalArgumentException.class, () -> p.server().raiseIncomingStreams(max + 1, 0));
                assertThrows(IllegalArgumentException.class, () -> p.server().raiseIncomingStreams(0, max + 1));
                p.server().raiseIncomingStreams(max, max);
                // A stream record ordered after the queued MAX_STREAMS frames signals delivery.
                try (var outgoing = p.server().openStream()) {
                    outgoing.write(ByteBuffer.wrap(new byte[] {7}));
                    try (var incoming = p.client().acceptStream()) {
                        assertEquals(1, incoming.read(ByteBuffer.allocate(1)));
                    }
                }
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, true));
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, false));
            }
        }
    }
}
