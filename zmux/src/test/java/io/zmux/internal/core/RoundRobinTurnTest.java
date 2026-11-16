package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** A round-robin turn covers one whole spread chunk, however the writes that fill it are cut. */
class RoundRobinTurnTest {
    @Test void aTurnCutShortByCreditGoesOnUntilItCarriedAChunk() throws Exception {
        Session s = CounterSemanticsTest.established();
        Pipe a = s.pipes.get(0);
        Pipe c = PipeTargetTest.appendOut(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        s.setPolicy(CoreConfig.Policy.SPREAD_ROUND_ROBIN);
        Stream st = s.streams.openStream(true);
        s.streams.pinId(st);
        int chunk = CoreConfig.DEFAULT_SPREAD_CHUNK;
        s.send.write(st, ByteBuffer.allocate(64 * chunk), false);
        long[] carried = new long[2];
        for (int round = 0; round < 16; round++) {
            // The credit alternates: a whole chunk, then 1 KiB.
            long grant = round % 2 == 0 ? chunk : 1024;
            Poke.set(st.snd, "maxData", st.snd.sent() + grant);
            for (int i = 0; i < 2; i++) {
                Pipe p = i == 0 ? a : c;
                long before = st.snd.sent();
                CustomPickTest.write(s, p);
                carried[i] += st.snd.sent() - before;
            }
            assertTrue(Math.abs(carried[0] - carried[1]) <= chunk, "round " + round + ": " + carried[0] + " / " + carried[1]);
        }
        assertTrue(carried[0] > 0 && carried[1] > 0);
    }
}
