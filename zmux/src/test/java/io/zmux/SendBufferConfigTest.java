package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.CoreConfig;
import org.junit.jupiter.api.Test;

class SendBufferConfigTest {
    @Test void aMaximumBelowTheBufferIsRejectedNotRaised() {
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().sendBufferMax(-1));
        assertEquals(0, ZmuxConfig.builder().build().toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax(), "unset: automatic");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().sendBufferMax(2L << 20).build(), "below the 4 MiB buffer");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().sendBufferMax(8L << 20).sendBuffer(16L << 20).build());
        var equal = ZmuxConfig.builder().sendBufferMax(4L << 20).build();
        assertEquals(4L << 20, equal.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax());
        var larger = ZmuxConfig.builder().sendBufferMax(128L << 20).build();
        assertEquals(128L << 20, larger.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax());
    }
}
