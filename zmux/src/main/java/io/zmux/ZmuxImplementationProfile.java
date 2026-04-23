package io.zmux;

import java.util.List;
import java.util.Optional;

public enum ZmuxImplementationProfile {
    V1("zmux-v1"),
    REFERENCE_PROFILE_V1("zmux-reference-profile-v1");

    private final String profileName;

    ZmuxImplementationProfile(String profileName) {
        this.profileName = profileName;
    }

    public static Optional<ZmuxImplementationProfile> fromProfileName(String profileName) {
        return ZmuxConformance.implementationProfileByName(profileName);
    }

    public String profileName() {
        return profileName;
    }

    public List<ZmuxClaim> claims() {
        return ZmuxConformance.claims(this);
    }

    public List<String> acceptanceChecklist() {
        return ZmuxConformance.acceptanceChecklist(this);
    }

    public List<ZmuxConformanceSuite> requiredConformanceSuites() {
        return ZmuxConformance.requiredConformanceSuites(this);
    }

    public List<ZmuxConformanceSuite> releaseCertificationGate() {
        return ZmuxConformance.releaseCertificationGate(this);
    }

    @Override
    public String toString() {
        return profileName;
    }
}
