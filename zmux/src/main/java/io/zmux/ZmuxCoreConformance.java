package io.zmux;

import java.util.List;

public final class ZmuxCoreConformance {
    private ZmuxCoreConformance() {
    }

    public static List<ZmuxClaim> targetClaims() {
        return ZmuxConformance.coreModuleTargetClaims();
    }

    public static List<ZmuxImplementationProfile> targetImplementationProfiles() {
        return ZmuxConformance.coreModuleTargetImplementationProfiles();
    }

    public static List<ZmuxConformanceSuite> targetSuites() {
        return ZmuxConformance.coreModuleTargetSuites();
    }
}
