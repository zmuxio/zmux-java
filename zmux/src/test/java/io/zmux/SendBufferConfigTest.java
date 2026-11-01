package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.CoreConfig;
import org.junit.jupiter.api.Test;

class SendBufferConfigTest {
    @Test void maximumIsMappedOnceAndNeverBelowInitial() {
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().flowControl(FlowControl.defaults().withMaxSendBuffer(-1)));
        assertEquals(0, ZmuxConfig.builder().build().toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax());
        var fixed = ZmuxConfig.builder().flowControl(FlowControl.defaults().withMaxSendBuffer(2L << 20).withSendBuffer(8L << 20)).build();
        assertEquals(8L << 20, fixed.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax());
        var larger = ZmuxConfig.builder().flowControl(FlowControl.defaults().withMaxSendBuffer(128L << 20)).build();
        assertEquals(128L << 20, larger.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).sendBufferMax());
    }
}
