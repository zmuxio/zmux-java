package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The eligible maximum follows the largest path in service. */
@Timeout(60)
class DatagramPipesTest {
    static final int CAP = PipeIo.MIN_RECORD;
    static final int SEAL = 16;

    @Test void aNarrowPipeDoesNotReduceTheLargestPath() throws Exception {
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
                    client.addPipe(pair[0].withMaxRecord(CAP + SEAL), PipeOptions.defaults());
                    assertEquals(big, client.datagrams().pathPayload());
                    client.datagrams().send(ByteBuffer.allocate(big));
                    assertEquals(big, server.datagrams().receive(Duration.ofSeconds(30)).length);
                    client.pipes().stream().filter(p -> p.rawId() == 0).findFirst().orElseThrow().abort();
                    int small = client.datagrams().pathPayload();
                    assertTrue(small > 0 && small <= CAP - 3, "only the capped pipe remains");
                    client.datagrams().send(ByteBuffer.allocate(small));
                    assertEquals(small, server.datagrams().receive(Duration.ofSeconds(30)).length);
                    assertTrue(client.stats().datagramsDroppedSend() == 0, "nothing was dropped");
                }
            }
        }
    }
}
