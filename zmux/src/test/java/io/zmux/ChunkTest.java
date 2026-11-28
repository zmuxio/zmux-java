package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.diag.Diagnostics;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class ChunkTest {
    @Test void receivedChunksSurviveTheSourceAndTransferOwnershipToAnotherSession() throws Exception {
        EventLoops loops = EventLoops.create(2);
        try (loops; var workers = Executors.newVirtualThreadPerTaskExecutor()) {
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
                        assertThrows(IllegalStateException.class, chunk::remaining);
                    }
                    output.finish(); return null;
                });
                var received = target.server().acceptStream();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                for (Chunk chunk; (chunk = received.readChunk(65536)) != null;) {
                    try (Chunk owned = chunk) { bytes.writeBytes(owned.copy()); }
                }
                assertArrayEquals(payload, bytes.toByteArray());
                sending.get(20, TimeUnit.SECONDS); relay.get(20, TimeUnit.SECONDS);
                send.close(); input.close(); output.close(); received.close();
            }
        }
        assertEquals(0, loops.outstandingBuffers());
    }
    @Test void heldChunkRemainsValidAfterSessionCloseAndFailedWriteConsumesIt() throws Exception {
        EventLoops loops = EventLoops.create(1);
        try (loops) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            Chunk held;
            ZmuxStream sender;
            byte[] payload = new byte[16384]; Arrays.fill(payload, (byte) 42);
            try (var pair = Pair.memory(config)) {
                sender = pair.client().openStream(); sender.write(payload);
                var receiver = pair.server().acceptStream();
                held = receiver.readChunk(payload.length); receiver.close();
            }
            held.withBuffer(data -> assertEquals(42, data.get()));
            assertThrows(java.io.IOException.class, () -> sender.writeChunk(held));
            assertThrows(IllegalStateException.class, held::remaining);
            held.close();
        }
        assertEquals(0, loops.outstandingBuffers());
    }
    @Test void timedOutReadDoesNotKeepTheOldWakeupSlot() throws Exception {
        try (EventLoops loops = EventLoops.create(1);
                var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).noMaxIdleTimeout().build())) {
            var send = pair.client().openStream();
            send.write(new byte[] {1});
            var receive = pair.server().acceptStream();
            try (Chunk first = receive.readChunk(1)) { first.withBuffer(data -> assertEquals(1, data.get())); }
            Duration timeout = Duration.ofMinutes(5);
            receive.setReadTimeout(timeout);
            var waiting = receive.readChunkAsync(1);
            Diagnostics.advanceTimersForTests(loops, timeout.toNanos()).get(20, TimeUnit.SECONDS);
            var failure = assertThrows(ExecutionException.class, () -> waiting.get(20, TimeUnit.SECONDS));
            assertInstanceOf(java.net.SocketTimeoutException.class, failure.getCause());
            send.write(new byte[] {2});
            send.finish();
            try (Chunk next = receive.readChunk(1)) { next.withBuffer(data -> assertEquals(2, data.get())); }
            assertNull(receive.readChunk(1));
            send.close();
            receive.close();
        }
    }
}
