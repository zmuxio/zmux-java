package io.zmux;

import java.util.List;
import java.util.Optional;

public enum ZmuxClaim {
    WIRE_V1("zmux-wire-v1"),
    API_SEMANTICS_PROFILE_V1("zmux-api-semantics-profile-v1"),
    STREAM_ADAPTER_PROFILE_V1("zmux-stream-adapter-profile-v1"),
    OPEN_METADATA("zmux-open_metadata"),
    PRIORITY_UPDATE("zmux-priority_update");

    private final String claimName;

    ZmuxClaim(String claimName) {
        this.claimName = claimName;
    }

    public static Optional<ZmuxClaim> fromClaimName(String claimName) {
        return ZmuxConformance.claimByName(claimName);
    }

    public String claimName() {
        return claimName;
    }

    public List<String> acceptanceChecklist() {
        return ZmuxConformance.acceptanceChecklist(this);
    }

    public List<ZmuxConformanceSuite> requiredConformanceSuites() {
        return ZmuxConformance.requiredConformanceSuites(this);
    }

    @Override
    public String toString() {
        return claimName;
    }
}
