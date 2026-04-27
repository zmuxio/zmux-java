package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.ZmuxErrors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void gatherScratchDropsOversizedRetainedBuffersOnClear() {
        FrameEnvelopeCodec.GatherScratch scratch = new FrameEnvelopeCodec.GatherScratch();

        scratch.ensureCapacity(0, 2049);
        scratch.addBuffer(ByteBuffer.wrap(new byte[]{1}));
        assertTrue(scratch.buffers().length > 2048,
                "test requires an oversized retained gather buffer array before clear");

        scratch.clear();

        assertEquals(0, scratch.buffers().length,
                "clear should drop oversized retained gather buffer slots");
        assertEquals(0, scratch.bufferCount(),
                "clear should leave gather scratch empty and ready for reuse");
    }

    @Test
    void writeGatheredBuffersRejectsInvalidGatheringProgress() {
        long[] writes = {-1L, 4L};
        for (long write : writes) {
            FrameEnvelopeCodec.GatherScratch scratch = new FrameEnvelopeCodec.GatherScratch();
            scratch.ensureCapacity(0, 1);
            scratch.addBuffer(ByteBuffer.wrap(new byte[]{1, 2, 3}));

            IOException error = assertThrows(
                    IOException.class,
                    () -> FrameEnvelopeCodec.writeGatheredBuffers(
                            new InvalidProgressGatheringChannel(write),
                            scratch
                    )
            );

            assertEquals(ErrorCode.INTERNAL, ZmuxErrors.code(error));
            assertEquals(0, scratch.bufferCount(),
                    "failed gather write should clear retained scratch buffers");
        }
    }

    private static final class InvalidProgressGatheringChannel implements GatheringByteChannel {
        private final long write;
        private boolean open = true;

        private InvalidProgressGatheringChannel(long write) {
            this.write = write;
        }

        @Override
        public int write(ByteBuffer src) {
            return (int) write;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            return write;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
