package io.zmux.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FrameEnvelopeGatherScratchTest {
    @Test
    void gatherScratchDropsOversizedRetainedBuffersOnReset() {
        FrameEnvelopeCodec.GatherScratch scratch = new FrameEnvelopeCodec.GatherScratch();

        scratch.ensureCapacity(512, 512);
        assertTrue(scratch.headerArena().length > 128,
                "test requires an oversized retained header arena before reset");
        assertTrue(scratch.buffers().length > 128,
                "test requires oversized retained gather buffers before reset");

        scratch.reset(1, 1);

        assertEquals(0, scratch.headerArena().length,
                "smaller gather reset should drop an oversized retained header arena");
        assertEquals(0, scratch.buffers().length,
                "smaller gather reset should drop oversized retained buffer slots");
        assertEquals(0, scratch.bufferCount(),
                "reset should leave gather scratch empty and ready for reuse");
    }
}
