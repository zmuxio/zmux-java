package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** shutdownAsync waits for no peer: the sessions nobody accepted are aborted, not closed one by one. */
@Timeout(60)
class ListenerShutdownStuckTest {
    static final int PEERS = 128;

    @Test void shutdownAsyncReturnsAtOnceWithStuckPeers() throws Exception {
        try (EventLoops serverLoops = EventLoops.create(1); EventLoops clientLoops = EventLoops.create(1)) {
            var established = new CountDownLatch(PEERS);
            var config = ZmuxConfig.builder().eventLoops(serverLoops).maxIncomingStreams(0).maxIncomingUniStreams(0)
                    .tracer(new Tracer() {
                        @Override public void established(SessionId session) { established.countDown(); }
                    }).build();
            var l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    ListenerConfig.builder().session(config).backlog(PEERS).noMaxPending().maxPendingPerSource(PEERS).build());
            List<ZmuxSession> peers = new ArrayList<>();
            var stuck = new CountDownLatch(1);
            try {
                ZmuxConfig cc = ZmuxConfig.builder().eventLoops(clientLoops).build();
                for (int i = 0; i < PEERS; i++) peers.add(Zmux.connect(l.localAddress(), cc));
                established.await();
                var queued = new CompletableFuture<Integer>();
                assertTrue(serverLoops.next().tryExecute(() -> queued.complete(l.queued.get())));
                assertEquals(PEERS, queued.get(30, TimeUnit.SECONDS));
                holdPeerLoop(peers.getFirst(), stuck);
                var done = l.shutdownAsync(0, "test");
                var joined = l.shutdownAsync(0, "again");
                done.get(30, TimeUnit.SECONDS);
                joined.get(30, TimeUnit.SECONDS);
            } finally {
                stuck.countDown();
                for (ZmuxSession p : peers) p.abort(0, "");
                l.close();
            }
        }
    }

    private static void holdPeerLoop(ZmuxSession peer, CountDownLatch release) throws InterruptedException {
        var entered = new CountDownLatch(1);
        assertTrue(peer.driver.executeCarrier(() -> {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }));
        entered.await();
    }

    @Test void cancellingALaterShutdownBoundsTheRunningOne() throws Exception {
        try (EventLoops serverLoops = EventLoops.create(1); EventLoops clientLoops = EventLoops.create(1)) {
            var l = Zmux.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(serverLoops).build()).build());
            var stuck = new CountDownLatch(1);
            ZmuxSession peer = Zmux.connect(l.localAddress(), ZmuxConfig.builder().eventLoops(clientLoops).build());
            try {
                ZmuxSession accepted = l.accept();
                assertNotNull(accepted);
                try (var sent = peer.openStream()) {
                    sent.write(new byte[] {7});
                    try (var received = accepted.acceptStream()) {
                        assertEquals(1, received.read(new byte[1], 0, 1));
                        holdPeerLoop(peer, stuck);
                        try {
                            var first = l.shutdownAsync(0, "unbounded");
                            var bounded = l.shutdownAsync(0, "bounded");
                            assertFalse(first.isDone());
                            assertTrue(bounded.cancel(false));
                            first.get(30, TimeUnit.SECONDS);
                            assertTrue(l.isClosed());
                        } finally {
                            stuck.countDown();
                        }
                    }
                }
            } finally {
                stuck.countDown();
                peer.abort(0, "");
                l.close();
            }
        }
    }
}
