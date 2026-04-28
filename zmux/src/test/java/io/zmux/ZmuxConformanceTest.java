package io.zmux;

import org.junit.jupiter.api.Test;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ZmuxConformanceTest {
    @Test
    void knownRepositoryNamesMatchSpecAndGoRegistry() {
        assertEquals(
                listOf(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.API_SEMANTICS_PROFILE_V1,
                        ZmuxClaim.STREAM_ADAPTER_PROFILE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                ),
                ZmuxConformance.knownClaims()
        );
        assertEquals(
                listOf(
                        ZmuxImplementationProfile.V1,
                        ZmuxImplementationProfile.REFERENCE_PROFILE_V1
                ),
                ZmuxConformance.knownImplementationProfiles()
        );
        assertEquals(
                listOf(
                        ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                        ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                        ZmuxConformanceSuite.EXTENSION_TOLERANCE,
                        ZmuxConformanceSuite.CORE_STREAM_LIFECYCLE,
                        ZmuxConformanceSuite.CORE_FLOW_CONTROL,
                        ZmuxConformanceSuite.CORE_SESSION_LIFECYCLE,
                        ZmuxConformanceSuite.OPEN_METADATA,
                        ZmuxConformanceSuite.PRIORITY_UPDATE,
                        ZmuxConformanceSuite.PRIORITY_HINTS_AND_STREAM_GROUPS,
                        ZmuxConformanceSuite.V1_PROFILE_COMPATIBILITY,
                        ZmuxConformanceSuite.API_SEMANTICS_PROFILE,
                        ZmuxConformanceSuite.STREAM_ADAPTER_PROFILE,
                        ZmuxConformanceSuite.REFERENCE_PROFILE_CLAIM_GATE,
                        ZmuxConformanceSuite.REFERENCE_QUALITY_BEHAVIORS
                ),
                ZmuxConformance.knownConformanceSuites()
        );
    }

    @Test
    void implementationProfilesExposeRepositoryClaimBundlesAndSuites() {
        assertEquals(
                listOf(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                ),
                ZmuxImplementationProfile.V1.claims()
        );
        assertEquals(
                listOf(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.API_SEMANTICS_PROFILE_V1,
                        ZmuxClaim.STREAM_ADAPTER_PROFILE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                ),
                ZmuxImplementationProfile.REFERENCE_PROFILE_V1.claims()
        );
        assertEquals(
                ZmuxImplementationProfile.REFERENCE_PROFILE_V1.requiredConformanceSuites(),
                ZmuxImplementationProfile.REFERENCE_PROFILE_V1.releaseCertificationGate()
        );
    }

    @Test
    void coreModulePublishesDocumentedClaimTargetsAndV1ImplementationProfile() {
        assertEquals(
                listOf(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.API_SEMANTICS_PROFILE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                ),
                ZmuxCoreConformance.targetClaims()
        );
        assertEquals(
                listOf(ZmuxImplementationProfile.V1),
                ZmuxCoreConformance.targetImplementationProfiles()
        );
        assertEquals(
                listOf(
                        ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                        ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                        ZmuxConformanceSuite.EXTENSION_TOLERANCE,
                        ZmuxConformanceSuite.CORE_STREAM_LIFECYCLE,
                        ZmuxConformanceSuite.CORE_FLOW_CONTROL,
                        ZmuxConformanceSuite.CORE_SESSION_LIFECYCLE,
                        ZmuxConformanceSuite.OPEN_METADATA,
                        ZmuxConformanceSuite.PRIORITY_UPDATE,
                        ZmuxConformanceSuite.PRIORITY_HINTS_AND_STREAM_GROUPS,
                        ZmuxConformanceSuite.V1_PROFILE_COMPATIBILITY,
                        ZmuxConformanceSuite.API_SEMANTICS_PROFILE
                ),
                ZmuxCoreConformance.targetSuites()
        );
    }

    @Test
    void conformanceNamesRoundTripThroughLookupApis() {
        assertEquals(
                ZmuxClaim.PRIORITY_UPDATE,
                ZmuxClaim.fromClaimName("zmux-priority_update")
                        .orElseThrow(() -> new AssertionError("priority update claim lookup failed"))
        );
        assertEquals(
                ZmuxImplementationProfile.V1,
                ZmuxImplementationProfile.fromProfileName("zmux-v1")
                        .orElseThrow(() -> new AssertionError("v1 profile lookup failed"))
        );
        assertEquals(
                ZmuxImplementationProfile.REFERENCE_PROFILE_V1,
                ZmuxImplementationProfile.fromProfileName("zmux-reference-profile-v1")
                        .orElseThrow(() -> new AssertionError("reference profile lookup failed"))
        );
        assertEquals(
                ZmuxConformanceSuite.V1_PROFILE_COMPATIBILITY,
                ZmuxConformanceSuite.fromSuiteName("v1-profile-compatibility")
                        .orElseThrow(() -> new AssertionError("v1 compatibility suite lookup failed"))
        );
        assertEquals(
                listOf(
                        "repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT",
                        "repository-default Close() acts as a full local close helper",
                        "repository-default Close() on a unidirectional stream silently ignores the locally absent direction rather than failing solely because that half does not exist",
                        "each exposed API surface keeps one documented primary spelling per operation family, with any extra convenience spellings documented as wrappers over the same semantic action rather than as distinct lifecycle operations",
                        "before session-ready, repository-default sender behavior emits only the local preface and a fatal establishment CLOSE, and emits none of new-stream DATA, stream-scoped control, ordinary session-scoped control, or EXT",
                        "repository-default sender and receiver memory rules enforce the documented hidden-state, provisional-open, and late-tail bounds",
                        "repository-default liveness rules keep at most one outstanding protocol PING and does not treat weak local signals as strong progress"
                ),
                ZmuxConformance.referenceProfileClaimGate()
        );
    }

    @Test
    void conformanceChecklistTextMatchesSpec() {
        assertEquals(
                "document and implement the repository-default semantic operation families from API_SEMANTICS.md, including full local close helper, graceful send-half completion, read-side stop, send-side reset, whole-stream abort, structured error surfacing, open/cancel behavior, and accept visibility rules",
                ZmuxClaim.API_SEMANTICS_PROFILE_V1.acceptanceChecklist().get(0)
        );
        assertEquals(
                "provide one consistent convenience mapping or fuller documented control layer or both",
                ZmuxClaim.STREAM_ADAPTER_PROFILE_V1.acceptanceChecklist().get(1)
        );
        assertEquals(
                "repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT",
                ZmuxConformance.referenceProfileClaimGate().get(0)
        );
        assertEquals(
                "repository-default liveness rules keep at most one outstanding protocol PING and does not treat weak local signals as strong progress",
                ZmuxConformance.referenceProfileClaimGate().get(6)
        );
    }
}
