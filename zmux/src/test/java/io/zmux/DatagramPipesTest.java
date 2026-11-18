package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Datagrams over pipes whose records differ: the maximum is what the usable pipes carry, and it follows them. */
@Timeout(60)
class DatagramPipesTest {
    static final int CAP = PipeIo.MIN_RECORD;
    static final int SEAL = 16;

    /** A big bootstrap pipe and a joined pipe capped at CAP: datagrams go on whichever pipe holds them; the cap leaves with the pipe. */
    @Test void theMaximumFollowsTheUsablePipes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = MaxRecordTest.config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    int big = client.datagrams().maxPayload();
                    assertTrue(big > CAP, "the bootstrap pipe takes large records: " + big);
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("joined")));
                    var joined = client.addPipe(pair[0].withMaxRecord(CAP + SEAL), PipeOptions.defaults());
                    awaitMax(client, m -> m < CAP);
                    int small = client.datagrams().maxPayload();
                    assertTrue(small > 0 && small <= CAP - 3, "the capped pipe sets the maximum: " + small);
                    assertTrue(client.datagrams().send(ByteBuffer.allocate(small)));
                    assertEquals(small, server.datagrams().receive(Duration.ofSeconds(5)).length);
                    assertThrows(DatagramTooLargeException.class, () -> client.datagrams().send(ByteBuffer.allocate(small + 1)));
                    // The capped pipe goes: the maximum is what the pipe left carries.
                    joined.abort();
                    awaitMax(client, m -> m > CAP);
                    int after = client.datagrams().maxPayload();
                    assertEquals(big, after);
                    assertTrue(client.datagrams().send(ByteBuffer.allocate(after)));
                    assertEquals(after, server.datagrams().receive(Duration.ofSeconds(5)).length);
                    assertTrue(client.stats().datagramsDroppedSend() == 0, "nothing was dropped");
                }
            }
        }
    }

    private static void awaitMax(ZmuxSession s, java.util.function.IntPredicate ok) throws Exception {
        for (long end = System.nanoTime() + 10_000_000_000L; !ok.test(s.datagrams().maxPayload()); ) {
            if (System.nanoTime() - end > 0) fail("maximum " + s.datagrams().maxPayload());
            Thread.sleep(10);
        }
    }
}
