package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.net.*;
import java.io.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Timeout;

/** Real TCP carriers: an upstream black hole stays open and accepts writes, but stops delivering them. */
@Timeout(40)
class PathLossTest {
    private static final class Blackhole implements AutoCloseable {
        final ServerSocket acceptor = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        volatile Socket client, upstream;
        volatile boolean armed;
        Blackhole(InetSocketAddress target) throws IOException {
            workers.submit(() -> {
                try {
                    client = acceptor.accept(); upstream = new Socket(); upstream.connect(target);
                    workers.submit(() -> { try { upstream.getInputStream().transferTo(client.getOutputStream()); } catch (IOException ignored) {} });
                    byte[] b = new byte[8192]; long bytes = 0;
                    for (int n; (n = client.getInputStream().read(b)) >= 0;) {
                        if (!armed || bytes < 65536) { upstream.getOutputStream().write(b, 0, n); if (armed) bytes += n; }
                    }
                } catch (IOException ignored) { }
            });
        }
        InetSocketAddress address() { return new InetSocketAddress(InetAddress.getLoopbackAddress(), acceptor.getLocalPort()); }
        @Override public void close() throws IOException {
            acceptor.close(); if (client != null) client.close(); if (upstream != null) upstream.close();
            workers.close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Scheduler.Builtin.class, names = {"AFFINITY", "SPREAD_ETA", "SPREAD_ROUND_ROBIN", "SPREAD_RATE_WEIGHTED"})
    void aSilentFailedPreferredPipeReinjectsOnTheRemainingCarrier(Scheduler policy) throws Exception {
        try (EventLoops loops = EventLoops.create(3); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(3).withAuth(PipeAuth.psk(new byte[32])).withScheduler(policy).withProbation(Duration.ZERO).withSuspectFloor(Duration.ofMillis(50)).withDeadTimeout(Duration.ofSeconds(1))).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var proxy = new Blackhole(listener.localAddress())) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(proxy.address(), cfg); var s = accepted.get();
                var backup = c.paths().add(listener.localAddress(), PipeOptions.builder().sendPriority(1).receivePriority(1).build());
                var incoming = ex.submit(() -> {
                    var stream = s.acceptStream(); byte[] bytes = SessionTest.readAll(stream);
                    stream.write(MessageDigest.getInstance("SHA-256").digest(bytes)); stream.finish();
                    return bytes;
                });
                proxy.armed = true;
                byte[] data = new byte[4 << 20]; new java.util.Random(11).nextBytes(data);
                var stream = c.openStream(); stream.setRedundant(false);
                stream.write(data); stream.finish();
                assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(data), SessionTest.readAll(stream));
                assertArrayEquals(data, incoming.get());
                assertTrue(backup.stats().orElseThrow().bytesSent() > 65536);
                boolean suspect = false;
                while (!suspect) {
                    SessionEvent event = c.nextEvent();
                    suspect = event instanceof SessionEvent.OutboundPipeStateChanged p && p.pipe() == 0
                            && (p.state() == PipeState.Outbound.SUSPECT || p.state() == PipeState.Outbound.FAILED);
                }
                PipePoolTest.await(() -> c.stats().reinjections() >= 1);
                var st = c.stats();
                assertTrue(st.bytesReinjected() > 0 && st.bytesReinjected() >= st.bytesOpportunistic(), st.toString());
                stream.close(); c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
