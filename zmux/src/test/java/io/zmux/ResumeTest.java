package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ResumeTest {
    static <T extends SessionEvent> void event(ZmuxSession session, Class<T> kind) throws Exception {
        while (true) if (kind.isInstance(session.nextEvent())) return;
    }
    @Test void tcpSessionResumesTheSameLogicalStreamAfterLosingEveryCarrier() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.psk(new byte[32])).withResumeTimeout(Duration.ofSeconds(10)).withProbation(Duration.ZERO)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config));
                 var client = Zmux.connect(listener.localAddress(), config);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                assertEquals(1, client.stats().pipesAdded());
                assertEquals(0, client.stats().suspensions());
                assertEquals(Duration.ZERO, client.stats().suspendedTime());
                byte[] bytes = new byte[512 << 10]; new Random(93).nextBytes(bytes);
                var sender = client.openStream(); sender.write(bytes, 0, 128 << 10);
                var receiver = server.acceptStream();
                ByteArrayOutputStream received = new ByteArrayOutputStream();
                ByteBuffer first = ByteBuffer.allocate(16384); assertTrue(receiver.read(first) > 0);
                received.write(first.array(), 0, first.position());
                var clientSuspended = workers.submit(() -> { event(client, SessionEvent.Suspended.class); return null; });
                var serverSuspended = workers.submit(() -> { event(server, SessionEvent.Suspended.class); return null; });
                client.paths().list().getFirst().abort();
                clientSuspended.get(5, TimeUnit.SECONDS); serverSuspended.get(5, TimeUnit.SECONDS);
                PipePoolTest.await(() -> client.stats().suspensions() == 1 && client.stats().pipesActive() == 0);
                Duration early = client.stats().suspendedTime();
                Thread.sleep(20);
                assertTrue(client.stats().suspendedTime().compareTo(early) > 0, "the current suspension counts");
                sender.write(bytes, 128 << 10, bytes.length - (128 << 10)); sender.finish();
                var resumed = workers.submit(() -> { event(server, SessionEvent.Resumed.class); return null; });
                var pipe = client.paths().add(listener.localAddress(), PipeOptions.builder().probation(Duration.ZERO).build());
                assertTrue(pipe.id() > 0); resumed.get(5, TimeUnit.SECONDS);
                PipePoolTest.await(() -> client.stats().pipesAdded() == 2 && client.stats().pipesActive() == 1);
                Duration total = client.stats().suspendedTime();
                assertTrue(total.toMillis() >= 20);
                Thread.sleep(20);
                assertEquals(total, client.stats().suspendedTime(), "a resumed session's suspended time stays");
                assertEquals(1, listener.stats().sessions());
                PipePoolTest.await(() -> listener.stats().joins() == 1);
                for (Chunk chunk; (chunk = receiver.readChunk(65536)) != null;) {
                    try (Chunk owned = chunk) { ByteBuffer data = owned.buffer(); byte[] part = new byte[data.remaining()]; data.get(part); received.write(part); }
                }
                assertArrayEquals(bytes, received.toByteArray());
                receiver.write(new byte[] {1}); receiver.finish();
                ByteBuffer reply = ByteBuffer.allocate(1); assertEquals(1, sender.read(reply)); assertEquals(1, reply.get(0));
                sender.close(); receiver.close();
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
