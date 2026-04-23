package io.zmux;

import java.util.Optional;

public enum ZmuxConformanceSuite {
    CORE_WIRE_INTEROPERABILITY("core-wire-interoperability"),
    INVALID_INPUT_HANDLING("invalid-input-handling"),
    EXTENSION_TOLERANCE("extension-tolerance"),
    CORE_STREAM_LIFECYCLE("core-stream-lifecycle"),
    CORE_FLOW_CONTROL("core-flow-control"),
    CORE_SESSION_LIFECYCLE("core-session-lifecycle"),
    OPEN_METADATA("open_metadata"),
    PRIORITY_UPDATE("priority_update"),
    PRIORITY_HINTS_AND_STREAM_GROUPS("priority-hints-and-stream-groups"),
    V1_PROFILE_COMPATIBILITY("v1-profile-compatibility"),
    API_SEMANTICS_PROFILE("api-semantics-profile"),
    STREAM_ADAPTER_PROFILE("stream-adapter-profile"),
    REFERENCE_PROFILE_CLAIM_GATE("reference-profile-claim-gate"),
    REFERENCE_QUALITY_BEHAVIORS("reference-quality-behaviors");

    private final String suiteName;

    ZmuxConformanceSuite(String suiteName) {
        this.suiteName = suiteName;
    }

    public static Optional<ZmuxConformanceSuite> fromSuiteName(String suiteName) {
        return ZmuxConformance.conformanceSuiteByName(suiteName);
    }

    public String suiteName() {
        return suiteName;
    }

    @Override
    public String toString() {
        return suiteName;
    }
}
