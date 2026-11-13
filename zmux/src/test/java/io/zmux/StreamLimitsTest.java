package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.CoreProbe;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** N18 (stream datagram bytes) and N19 (stream credit maximum). */
@Timeout(30)
class StreamLimitsTest {
    static void await(java.util.function.BooleanSupplier done) throws InterruptedException {
        for (long end = System.nanoTime() + 5_000_000_000L; !done.getAsBoolean(); Thread.sleep(5))
            assertTrue(System.nanoTime() < end, "timed out");
    }

    /** Sends n datagrams of size bytes on a fresh stream to the server, which accepts it and reads none; returns the server's stream once all arrived or dropped. */
    private static ZmuxStream unread(Pair p, int n, int size) throws Exception {
        var st = p.client().openStream();
        st.write(new byte[] {0});
        var in = p.server().acceptStream(Duration.ofSeconds(5));
        assertNotNull(in);
        for (int i = 0; i < n; i++) {
            byte[] b = new byte[size];
            b[0] = (byte) i;
            assertTrue(st.sendDatagram(0, ByteBuffer.wrap(b)));
        }
        await(() -> {
            try { var s = in.stats(); return s.datagramsReceived() >= n; } catch (java.io.IOException e) { throw new AssertionError(e); }
        });
        return in;
    }

    private static List<Integer> drain(ZmuxStream in) throws Exception {
        var got = new ArrayList<Integer>();
        try {
            while (true) got.add((int) in.receiveDatagram(Duration.ofMillis(100)).payload()[0]);
        } catch (java.net.SocketTimeoutException drained) {
            return got;
        }
    }

    private static Pair pair(EventLoops loops, UnaryOperator<ZmuxConfig.Builder> server) throws Exception {
        var c = ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
        return Pair.memory(c, server.apply(ZmuxConfig.builder().eventLoops(loops).datagrams(true)).build(), io.zmux.carrier.Framing.BYTE_STREAM);
    }

    @Test void aStreamsDatagramQueueIsBoundedByBytes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            // A small session byte bound: stream queues do not count against it.
            try (var p = pair(loops, b -> b.datagramStreamQueue(32).datagramStreamBytes(16 << 10).datagramReceiveBytes(8 << 10))) {
                var in = unread(p, 10, 4 << 10);
                assertEquals(6, in.stats().datagramsDroppedRecv());
                assertEquals(List.of(6, 7, 8, 9), drain(in), "the newest four stay");
            }
            try (var p = pair(loops, b -> b.datagramStreamQueue(32).noDatagramStreamBytes().datagramReceiveBytes(8 << 10))) {
                var in = unread(p, 10, 4 << 10);
                assertEquals(0, in.stats().datagramsDroppedRecv());
                assertEquals(0, p.server().stats().datagramsDroppedRecv(), "the session's receive bytes leave stream queues alone");
                assertEquals(10, drain(in).size());
            }
            try (var p = pair(loops, b -> b)) {
                var in = unread(p, 32, 9000);
                assertEquals(0, in.stats().datagramsDroppedRecv(), "the defaults keep 32 datagrams of 9000 bytes");
                assertEquals(32, drain(in).size());
            }
            // A single datagram above the bound is kept alone.
            try (var p = pair(loops, b -> b.datagramStreamBytes(1000))) {
                var in = unread(p, 3, 2000);
                assertEquals(2, in.stats().datagramsDroppedRecv());
                assertEquals(List.of(2), drain(in));
            }
        }
    }

    /** Under a receive limiter that admits nothing, the datagrams awaiting it keep the stream's bounds. */
    @Test void datagramsAwaitingTheLimiterKeepTheStreamsBounds() throws Exception {
        // The receive hook takes one datagram at a time; holding its answer keeps the rest awaiting it.
        var answer = new java.util.concurrent.CountDownLatch(1);
        io.zmux.helper.Limiter never = new io.zmux.helper.Limiter() {
            @Override public java.util.concurrent.CompletionStage<Integer> ready(int min, int max) { return new java.util.concurrent.CompletableFuture<>(); }
            @Override public int tryReady(int max) {
                try { answer.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return 0;
            }
            @Override public void charge(int bytes) { }
        };
        try (EventLoops loops = EventLoops.create(2);
             var p = pair(loops, b -> b.datagramStreamQueue(32).datagramStreamBytes(16 << 10).datagramLimiters(null, never))) {
            var st = p.client().openStream();
            st.write(new byte[] {0});
            var in = p.server().acceptStream(Duration.ofSeconds(5));
            for (int i = 0; i < 10; i++) assertTrue(st.sendDatagram(0, ByteBuffer.wrap(new byte[4 << 10])));
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
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().incomingStreams(max + 1));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().incomingUniStreams(max + 1));
        try (EventLoops loops = EventLoops.create(2)) {
            try (var p = pair(loops, b -> b.incomingStreams(max).incomingUniStreams(max))) {
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, true));
                assertEquals(max, CoreProbe.peerStreamCredit(p.client().core, false));
            }
            try (var p = pair(loops, b -> b.incomingUniStreams(16))) {
                assertThrows(IllegalArgumentException.class, () -> p.server().raiseIncomingStreams(max + 1, 0));
                assertThrows(IllegalArgumentException.class, () -> p.server().raiseIncomingStreams(0, max + 1));
                p.server().raiseIncomingStreams(max, max);
                await(() -> CoreProbe.peerStreamCredit(p.client().core, true) == max && CoreProbe.peerStreamCredit(p.client().core, false) == max);
            }
        }
    }
}
