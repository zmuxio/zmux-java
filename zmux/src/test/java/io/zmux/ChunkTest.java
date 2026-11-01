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

@Timeout(30)
class ChunkTest {
    @Test void receivedChunksSurviveTheSourceAndTransferOwnershipToAnotherSession() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            try (var source = Pair.memory(config); var target = Pair.memory(config)) {
                byte[] payload = new byte[1 << 20]; new Random(7).nextBytes(payload);
                var send = source.client().openStream();
                var sending = workers.submit(() -> { send.write(payload); send.finish(); return null; });
                var input = source.server().acceptStream();
                var output = target.client().openStream();
                var relay = workers.submit(() -> {
                    for (Chunk chunk; (chunk = input.readChunk(65536)) != null;) {
                        output.writeChunk(chunk);
                        assertThrows(IllegalStateException.class, chunk::buffer);
                    }
                    output.finish(); return null;
                });
                var received = target.server().acceptStream();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                for (Chunk chunk; (chunk = received.readChunk(65536)) != null;) {
                    try (Chunk owned = chunk) { ByteBuffer data = owned.buffer(); byte[] part = new byte[data.remaining()]; data.get(part); bytes.write(part); }
                }
                assertArrayEquals(payload, bytes.toByteArray());
                sending.get(5, TimeUnit.SECONDS); relay.get(5, TimeUnit.SECONDS);
                send.close(); input.close(); output.close(); received.close();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void heldChunkRemainsValidAfterSessionCloseAndFailedWriteConsumesIt() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            Chunk held;
            ZmuxStream sender;
            byte[] payload = new byte[16384]; Arrays.fill(payload, (byte) 42);
            try (var pair = Pair.memory(config)) {
                sender = pair.client().openStream(); sender.write(payload);
                var receiver = pair.server().acceptStream();
                held = receiver.readChunk(payload.length); receiver.close();
            }
            assertEquals(42, held.buffer().get());
            assertThrows(java.io.IOException.class, () -> sender.writeChunk(held));
            assertThrows(IllegalStateException.class, held::buffer);
            held.close();
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void timedOutReadDoesNotKeepTheOldWakeupSlot() throws Exception {
        try (var pair = Pair.memory(ZmuxConfig.defaults())) {
            var send = pair.client().openStream(); send.write(new byte[] {1});
            var receive = pair.server().acceptStream();
            try (Chunk first = receive.readChunk(1)) { assertEquals(1, first.buffer().get()); }
            receive.setReadTimeout(Duration.ofMillis(5));
            assertThrows(java.net.SocketTimeoutException.class, () -> receive.readChunk(1));
            send.write(new byte[] {2}); send.finish(); receive.setReadTimeout(Duration.ofSeconds(5));
            try (Chunk next = receive.readChunk(1)) { assertEquals(2, next.buffer().get()); }
            assertNull(receive.readChunk(1)); send.close(); receive.close();
        }
    }
}
