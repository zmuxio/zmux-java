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
                ZmuxClaim.fromClaimName("zmux-priority_update").get()
        );
        assertEquals(
                ZmuxImplementationProfile.V1,
                ZmuxImplementationProfile.fromProfileName("zmux-v1").get()
        );
        assertEquals(
                ZmuxImplementationProfile.REFERENCE_PROFILE_V1,
                ZmuxImplementationProfile.fromProfileName("zmux-reference-profile-v1").get()
        );
        assertEquals(
                ZmuxConformanceSuite.V1_PROFILE_COMPATIBILITY,
                ZmuxConformanceSuite.fromSuiteName("v1-profile-compatibility").get()
        );
        assertTrue(
                ZmuxConformance.referenceProfileClaimGate().contains("Close acts as a full local close helper"),
                "reference-profile gate should expose the documented release checklist"
        );
    }
}
