package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** shutdownAsync waits for no peer: the sessions nobody accepted are aborted, not closed one by one. */
@Timeout(60)
class ListenerShutdownStuckTest {
    static final int PEERS = 128;

    @Test void shutdownAsyncReturnsAtOnceWithStuckPeers() throws Exception {
        try (EventLoops serverLoops = EventLoops.create(1); EventLoops clientLoops = EventLoops.create(1)) {
            var l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(serverLoops).build())
                            .backlog(PEERS).noMaxPending().maxPendingPerSource(PEERS).build());
            List<ZmuxSession> peers = new ArrayList<>();
            var stuck = new CountDownLatch(1);
            try {
                ZmuxConfig cc = ZmuxConfig.builder().eventLoops(clientLoops).build();
                for (int i = 0; i < PEERS; i++) peers.add(Zmux.connect(l.localAddress(), cc));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (l.queued.get() < PEERS) {
                    assertTrue(System.nanoTime() - until < 0, "queued " + l.queued.get());
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                // Every peer's loop stops answering.
                assertTrue(peers.getFirst().driver.executeCarrier(() -> {
                    try { stuck.await(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                }));
                long t0 = System.nanoTime();
                var done = l.shutdownAsync(0, "test");
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(took < 5_000, "shutdownAsync took " + took + " ms");
                assertFalse(l.shutdownAsync(0, "again").isDone()); // a second shutdown joins the first
                done.toCompletableFuture().get(10, TimeUnit.SECONDS);
            } finally {
                stuck.countDown();
                for (ZmuxSession p : peers) p.abort(0, "");
                l.close();
            }
        }
    }

    @Test void aLaterBoundedShutdownBoundsTheRunningOne() throws Exception {
        try (EventLoops serverLoops = EventLoops.create(1); EventLoops clientLoops = EventLoops.create(1)) {
            var l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(serverLoops).build()).build());
            var stuck = new CountDownLatch(1);
            ZmuxSession peer = Zmux.connect(l.localAddress(), ZmuxConfig.builder().eventLoops(clientLoops).build());
            try {
                ZmuxSession accepted = l.accept();
                assertNotNull(accepted);
                // The peer's loop stops answering: the accepted session's graceful shutdown never ends.
                assertTrue(peer.driver.executeCarrier(() -> {
                    try { stuck.await(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                }));
                var first = l.shutdownAsync(0, "unbounded");
                assertFalse(first.isDone());
                long t0 = System.nanoTime();
                l.shutdown(0, "bounded", Duration.ofMillis(200));
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(l.isClosed(), "the later call's bound closed the listener");
                assertTrue(took < 5_000, "shutdown took " + took + " ms");
                first.toCompletableFuture().get(5, TimeUnit.SECONDS);
            } finally {
                stuck.countDown();
                peer.abort(0, "");
                l.close();
            }
        }
    }
}
