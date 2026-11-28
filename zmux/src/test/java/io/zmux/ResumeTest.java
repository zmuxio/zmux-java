package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ResumeTest {
    static <T extends SessionEvent> void event(ZmuxSession session, Class<T> kind) throws Exception {
        while (true) if (kind.isInstance(session.nextEvent())) return;
    }
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void tcpSessionResumesTheSameLogicalStreamAfterLosingEveryCarrier() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var clock = new SpliceHooksTest.ManualClock();
            var domain = new ClockDomain(new ClockDomain.Source() {
                @Override public long nanoTime() { return clock.now(); }
                @Override public Instant wallTime() { return Instant.EPOCH.plusNanos(clock.now()); }
                @Override public Runnable schedule(long delay, Runnable task) {
                    return clock.schedule(Math.addExact(clock.now(), delay), task);
                }
            });
            var config = ZmuxConfig.builder().eventLoops(loops).clockDomain(domain).sessionPolicy(new SessionPolicy(new CapacityLimits(2))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).resumeTimeout(Duration.ofSeconds(10)).noProbation().build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config));
                 var client = Zmux.connect(listener.localAddress(), config);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                assertEquals(1, client.stats().pipesAdded());
                assertEquals(0, client.stats().suspensions());
                assertEquals(Duration.ZERO, client.stats().suspendedTime());
                byte[] bytes = new byte[8 << 10]; new Random(93).nextBytes(bytes);
                int beforeSuspension = 2 << 10;
                var sender = client.openStream(); sender.write(bytes, 0, beforeSuspension);
                var receiver = server.acceptStream();
                ByteArrayOutputStream received = new ByteArrayOutputStream();
                ByteBuffer first = ByteBuffer.allocate(16384); assertTrue(receiver.read(first) > 0);
                received.write(first.array(), 0, first.position());
                var clientSuspended = workers.submit(() -> { event(client, SessionEvent.SessionSuspended.class); return null; });
                var serverSuspended = workers.submit(() -> { event(server, SessionEvent.SessionSuspended.class); return null; });
                client.pipes().getFirst().abort();
                clientSuspended.get(5, TimeUnit.SECONDS); serverSuspended.get(5, TimeUnit.SECONDS);
                assertEquals(1, client.stats().suspensions());
                assertEquals(0, client.stats().pipesActive());
                var suspended = Duration.ofMillis(60);
                clock.advance(suspended);
                assertEquals(suspended, client.stats().suspendedTime(), "the current suspension counts");
                sender.write(bytes, beforeSuspension, bytes.length - beforeSuspension); sender.finish();
                var resumed = workers.submit(() -> { event(server, SessionEvent.SessionResumed.class); return null; });
                var pipe = client.addPipe(listener.localAddress(), PipeOptions.builder().noProbation().build());
                assertTrue(pipe.id().value() > 0); resumed.get(5, TimeUnit.SECONDS);
                event(client, SessionEvent.SessionResumed.class);
                assertEquals(2, client.stats().pipesAdded());
                assertEquals(1, client.stats().pipesActive());
                clock.advance(Duration.ofMillis(20));
                assertEquals(suspended, client.stats().suspendedTime(), "a resumed session's suspended time stays");
                assertEquals(1, listener.stats().sessions());
                assertEquals(1, listener.stats().joins());
                for (Chunk chunk; (chunk = receiver.readChunk(65536)) != null;) {
                    try (Chunk owned = chunk) { received.writeBytes(owned.copy()); }
                }
                assertArrayEquals(bytes, received.toByteArray());
                receiver.write(new byte[] {1}); receiver.finish();
                ByteBuffer reply = ByteBuffer.allocate(1); assertEquals(1, sender.read(reply)); assertEquals(1, reply.get(0));
                sender.close(); receiver.close();
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            Leaks.none(loops);
        }
    }
}
