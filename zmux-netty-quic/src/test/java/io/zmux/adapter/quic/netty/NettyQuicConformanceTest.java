package io.zmux.adapter.quic.netty;

import io.zmux.ZmuxClaim;
import io.zmux.ZmuxConformanceSuite;
import org.junit.jupiter.api.Test;

import static io.zmux.adapter.quic.netty.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NettyQuicConformanceTest {
    @Test
    void adapterPublishesStreamAdapterClaimOnly() {
        assertEquals(
                listOf(ZmuxClaim.STREAM_ADAPTER_PROFILE_V1),
                NettyQuicConformance.targetClaims()
        );
        assertTrue(NettyQuicConformance.targetImplementationProfiles().isEmpty());
        assertEquals(
                listOf(ZmuxConformanceSuite.STREAM_ADAPTER_PROFILE),
                NettyQuicConformance.targetSuites()
        );
    }
}
