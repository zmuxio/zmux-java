package io.zmux.adapter.quic.netty;

import io.zmux.ZmuxClaim;
import io.zmux.ZmuxConformanceSuite;
import io.zmux.ZmuxImplementationProfile;

import java.util.Collections;
import java.util.List;

public final class NettyQuicConformance {
    private static final List<ZmuxClaim> TARGET_CLAIMS =
            Collections.singletonList(ZmuxClaim.STREAM_ADAPTER_PROFILE_V1);
    private static final List<ZmuxImplementationProfile> TARGET_IMPLEMENTATION_PROFILES = Collections.emptyList();
    private static final List<ZmuxConformanceSuite> TARGET_SUITES =
            Collections.singletonList(ZmuxConformanceSuite.STREAM_ADAPTER_PROFILE);

    private NettyQuicConformance() {
    }

    public static List<ZmuxClaim> targetClaims() {
        return TARGET_CLAIMS;
    }

    public static List<ZmuxImplementationProfile> targetImplementationProfiles() {
        return TARGET_IMPLEMENTATION_PROFILES;
    }

    public static List<ZmuxConformanceSuite> targetSuites() {
        return TARGET_SUITES;
    }
}
