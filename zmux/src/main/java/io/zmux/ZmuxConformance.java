package io.zmux;

import java.util.*;

public final class ZmuxConformance {
    private static final List<ZmuxClaim> KNOWN_CLAIMS = list(
            ZmuxClaim.WIRE_V1,
            ZmuxClaim.API_SEMANTICS_PROFILE_V1,
            ZmuxClaim.STREAM_ADAPTER_PROFILE_V1,
            ZmuxClaim.OPEN_METADATA,
            ZmuxClaim.PRIORITY_UPDATE
    );
    private static final List<ZmuxImplementationProfile> KNOWN_IMPLEMENTATION_PROFILES = list(
            ZmuxImplementationProfile.V1,
            ZmuxImplementationProfile.REFERENCE_PROFILE_V1
    );
    private static final List<ZmuxConformanceSuite> KNOWN_CONFORMANCE_SUITES = list(
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
    );

    private static final Map<ZmuxClaim, List<String>> CLAIM_ACCEPTANCE_CHECKLIST = claimAcceptanceChecklist();
    private static final Map<ZmuxImplementationProfile, List<String>> PROFILE_ACCEPTANCE_CHECKLIST = profileAcceptanceChecklist();
    private static final List<String> REFERENCE_PROFILE_CLAIM_GATE = list(
            "repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT",
            "repository-default Close() acts as a full local close helper",
            "repository-default Close() on a unidirectional stream silently ignores the locally absent direction rather than failing solely because that half does not exist",
            "each exposed API surface keeps one documented primary spelling per operation family, with any extra convenience spellings documented as wrappers over the same semantic action rather than as distinct lifecycle operations",
            "before session-ready, repository-default sender behavior emits only the local preface and a fatal establishment CLOSE, and emits none of new-stream DATA, stream-scoped control, ordinary session-scoped control, or EXT",
            "repository-default sender and receiver memory rules enforce the documented hidden-state, provisional-open, and late-tail bounds",
            "repository-default liveness rules keep at most one outstanding protocol PING and does not treat weak local signals as strong progress"
    );
    private static final Map<ZmuxClaim, List<ZmuxConformanceSuite>> CLAIM_REQUIRED_SUITES = claimRequiredSuites();
    private static final Map<ZmuxImplementationProfile, List<ZmuxConformanceSuite>> PROFILE_REQUIRED_SUITES = profileRequiredSuites();

    private static final Map<String, ZmuxClaim> CLAIMS_BY_NAME = indexClaims();
    private static final Map<String, ZmuxImplementationProfile> IMPLEMENTATION_PROFILES_BY_NAME = indexImplementationProfiles();
    private static final Map<String, ZmuxConformanceSuite> CONFORMANCE_SUITES_BY_NAME = indexConformanceSuites();

    private static final List<ZmuxClaim> CORE_MODULE_TARGET_CLAIMS = list(
            ZmuxClaim.WIRE_V1,
            ZmuxClaim.API_SEMANTICS_PROFILE_V1,
            ZmuxClaim.OPEN_METADATA,
            ZmuxClaim.PRIORITY_UPDATE
    );
    private static final List<ZmuxImplementationProfile> CORE_MODULE_TARGET_IMPLEMENTATION_PROFILES = list(
            ZmuxImplementationProfile.V1
    );
    private static final List<ZmuxConformanceSuite> CORE_MODULE_TARGET_SUITES = mergeRequiredSuites(
            CORE_MODULE_TARGET_CLAIMS,
            CORE_MODULE_TARGET_IMPLEMENTATION_PROFILES
    );

    private ZmuxConformance() {
    }

    public static List<ZmuxClaim> knownClaims() {
        return KNOWN_CLAIMS;
    }

    public static List<ZmuxImplementationProfile> knownImplementationProfiles() {
        return KNOWN_IMPLEMENTATION_PROFILES;
    }

    public static List<ZmuxConformanceSuite> knownConformanceSuites() {
        return KNOWN_CONFORMANCE_SUITES;
    }

    public static List<ZmuxClaim> claims(ZmuxImplementationProfile profile) {
        Objects.requireNonNull(profile, "profile");
        switch (profile) {
            case V1:
                return list(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                );
            case REFERENCE_PROFILE_V1:
                return list(
                        ZmuxClaim.WIRE_V1,
                        ZmuxClaim.API_SEMANTICS_PROFILE_V1,
                        ZmuxClaim.STREAM_ADAPTER_PROFILE_V1,
                        ZmuxClaim.OPEN_METADATA,
                        ZmuxClaim.PRIORITY_UPDATE
                );
            default:
                throw new IllegalStateException("unexpected implementation profile: " + profile);
        }
    }

    public static List<String> acceptanceChecklist(ZmuxClaim claim) {
        Objects.requireNonNull(claim, "claim");
        List<String> checklist = CLAIM_ACCEPTANCE_CHECKLIST.get(claim);
        return checklist == null ? Collections.emptyList() : checklist;
    }

    public static List<String> acceptanceChecklist(ZmuxImplementationProfile profile) {
        Objects.requireNonNull(profile, "profile");
        List<String> checklist = PROFILE_ACCEPTANCE_CHECKLIST.get(profile);
        return checklist == null ? Collections.emptyList() : checklist;
    }

    public static List<String> referenceProfileClaimGate() {
        return REFERENCE_PROFILE_CLAIM_GATE;
    }

    public static List<ZmuxConformanceSuite> requiredConformanceSuites(ZmuxClaim claim) {
        Objects.requireNonNull(claim, "claim");
        List<ZmuxConformanceSuite> suites = CLAIM_REQUIRED_SUITES.get(claim);
        return suites == null ? Collections.emptyList() : suites;
    }

    public static List<ZmuxConformanceSuite> requiredConformanceSuites(ZmuxImplementationProfile profile) {
        Objects.requireNonNull(profile, "profile");
        List<ZmuxConformanceSuite> suites = PROFILE_REQUIRED_SUITES.get(profile);
        return suites == null ? Collections.emptyList() : suites;
    }

    public static List<ZmuxConformanceSuite> releaseCertificationGate(ZmuxImplementationProfile profile) {
        return requiredConformanceSuites(profile);
    }

    public static List<ZmuxClaim> coreModuleTargetClaims() {
        return CORE_MODULE_TARGET_CLAIMS;
    }

    public static List<ZmuxImplementationProfile> coreModuleTargetImplementationProfiles() {
        return CORE_MODULE_TARGET_IMPLEMENTATION_PROFILES;
    }

    public static List<ZmuxConformanceSuite> coreModuleTargetSuites() {
        return CORE_MODULE_TARGET_SUITES;
    }

    static Optional<ZmuxClaim> claimByName(String claimName) {
        return Optional.ofNullable(CLAIMS_BY_NAME.get(claimName));
    }

    static Optional<ZmuxImplementationProfile> implementationProfileByName(String profileName) {
        return Optional.ofNullable(IMPLEMENTATION_PROFILES_BY_NAME.get(profileName));
    }

    static Optional<ZmuxConformanceSuite> conformanceSuiteByName(String suiteName) {
        return Optional.ofNullable(CONFORMANCE_SUITES_BY_NAME.get(suiteName));
    }

    private static Map<ZmuxClaim, List<String>> claimAcceptanceChecklist() {
        EnumMap<ZmuxClaim, List<String>> checklist = new EnumMap<>(ZmuxClaim.class);
        checklist.put(ZmuxClaim.WIRE_V1, list(
                "pass core wire interoperability",
                "pass invalid-input handling",
                "pass extension-tolerance behavior"
        ));
        checklist.put(ZmuxClaim.OPEN_METADATA, list(
                "satisfy zmux-wire-v1",
                "negotiate open_metadata",
                "accept valid DATA|OPEN_METADATA on first opening DATA",
                "reject unnegotiated or misplaced OPEN_METADATA",
                "ignore unknown metadata TLVs",
                "drop duplicate singleton metadata while preserving the enclosing DATA"
        ));
        checklist.put(ZmuxClaim.PRIORITY_UPDATE, list(
                "satisfy zmux-wire-v1",
                "negotiate priority_update",
                "process stream_priority and stream_group",
                "ignore open_info inside PRIORITY_UPDATE",
                "ignore unknown advisory TLVs",
                "ignore duplicate singleton advisory updates as one dropped update"
        ));
        checklist.put(ZmuxClaim.API_SEMANTICS_PROFILE_V1, list(
                "document and implement the repository-default semantic operation families from API_SEMANTICS.md, including full local close helper, graceful send-half completion, read-side stop, send-side reset, whole-stream abort, structured error surfacing, open/cancel behavior, and accept visibility rules",
                "document whether the binding exposes a stream-style convenience profile, a full-control protocol surface, or both",
                "exact API spellings are not required"
        ));
        checklist.put(ZmuxClaim.STREAM_ADAPTER_PROFILE_V1, list(
                "satisfy the stream-adapter subset from API_SEMANTICS.md, including bidirectional/unidirectional open and accept mapping",
                "provide one consistent convenience mapping or fuller documented control layer or both",
                "document limits/non-goals"
        ));
        return immutableMap(checklist);
    }

    private static Map<ZmuxImplementationProfile, List<String>> profileAcceptanceChecklist() {
        EnumMap<ZmuxImplementationProfile, List<String>> checklist = new EnumMap<>(ZmuxImplementationProfile.class);
        checklist.put(ZmuxImplementationProfile.V1, list(
                "satisfy zmux-wire-v1",
                "interoperate on explicit-role and role=auto establishment",
                "pass core stream-lifecycle scenarios",
                "pass core flow-control scenarios",
                "pass core session-lifecycle scenarios",
                "satisfy every currently active same-version optional surface in this repository",
                "negotiate and handle open_metadata, priority_update, priority_hints, and stream_groups correctly"
        ));
        checklist.put(ZmuxImplementationProfile.REFERENCE_PROFILE_V1, list(
                "satisfy zmux-v1",
                "satisfy the repository-defined reference-profile claim gate",
                "preserve the documented repository-default sender, memory, liveness, API, and scheduling behavior closely enough for release claims"
        ));
        return immutableMap(checklist);
    }

    private static Map<ZmuxClaim, List<ZmuxConformanceSuite>> claimRequiredSuites() {
        EnumMap<ZmuxClaim, List<ZmuxConformanceSuite>> suites = new EnumMap<>(ZmuxClaim.class);
        suites.put(ZmuxClaim.WIRE_V1, list(
                ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                ZmuxConformanceSuite.EXTENSION_TOLERANCE
        ));
        suites.put(ZmuxClaim.OPEN_METADATA, list(
                ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                ZmuxConformanceSuite.EXTENSION_TOLERANCE,
                ZmuxConformanceSuite.OPEN_METADATA
        ));
        suites.put(ZmuxClaim.PRIORITY_UPDATE, list(
                ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                ZmuxConformanceSuite.EXTENSION_TOLERANCE,
                ZmuxConformanceSuite.PRIORITY_UPDATE
        ));
        suites.put(ZmuxClaim.API_SEMANTICS_PROFILE_V1, list(
                ZmuxConformanceSuite.API_SEMANTICS_PROFILE
        ));
        suites.put(ZmuxClaim.STREAM_ADAPTER_PROFILE_V1, list(
                ZmuxConformanceSuite.STREAM_ADAPTER_PROFILE
        ));
        return immutableMap(suites);
    }

    private static Map<ZmuxImplementationProfile, List<ZmuxConformanceSuite>> profileRequiredSuites() {
        EnumMap<ZmuxImplementationProfile, List<ZmuxConformanceSuite>> suites = new EnumMap<>(ZmuxImplementationProfile.class);
        suites.put(ZmuxImplementationProfile.V1, list(
                ZmuxConformanceSuite.CORE_WIRE_INTEROPERABILITY,
                ZmuxConformanceSuite.INVALID_INPUT_HANDLING,
                ZmuxConformanceSuite.EXTENSION_TOLERANCE,
                ZmuxConformanceSuite.CORE_STREAM_LIFECYCLE,
                ZmuxConformanceSuite.CORE_FLOW_CONTROL,
                ZmuxConformanceSuite.CORE_SESSION_LIFECYCLE,
                ZmuxConformanceSuite.OPEN_METADATA,
                ZmuxConformanceSuite.PRIORITY_UPDATE,
                ZmuxConformanceSuite.PRIORITY_HINTS_AND_STREAM_GROUPS,
                ZmuxConformanceSuite.V1_PROFILE_COMPATIBILITY
        ));
        suites.put(ZmuxImplementationProfile.REFERENCE_PROFILE_V1, list(
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
        ));
        return immutableMap(suites);
    }

    private static Map<String, ZmuxClaim> indexClaims() {
        LinkedHashMap<String, ZmuxClaim> index = new LinkedHashMap<>();
        for (ZmuxClaim claim : KNOWN_CLAIMS) {
            index.put(claim.claimName(), claim);
        }
        return immutableMap(index);
    }

    private static Map<String, ZmuxImplementationProfile> indexImplementationProfiles() {
        LinkedHashMap<String, ZmuxImplementationProfile> index = new LinkedHashMap<>();
        for (ZmuxImplementationProfile profile : KNOWN_IMPLEMENTATION_PROFILES) {
            index.put(profile.profileName(), profile);
        }
        return immutableMap(index);
    }

    private static Map<String, ZmuxConformanceSuite> indexConformanceSuites() {
        LinkedHashMap<String, ZmuxConformanceSuite> index = new LinkedHashMap<>();
        for (ZmuxConformanceSuite suite : KNOWN_CONFORMANCE_SUITES) {
            index.put(suite.suiteName(), suite);
        }
        return immutableMap(index);
    }

    private static List<ZmuxConformanceSuite> mergeRequiredSuites(List<ZmuxClaim> claims,
                                                                  List<ZmuxImplementationProfile> profiles) {
        LinkedHashSet<ZmuxConformanceSuite> merged = new LinkedHashSet<>();
        if (claims != null) {
            for (ZmuxClaim claim : claims) {
                merged.addAll(requiredConformanceSuites(claim));
            }
        }
        if (profiles != null) {
            for (ZmuxImplementationProfile profile : profiles) {
                merged.addAll(requiredConformanceSuites(profile));
            }
        }
        if (merged.isEmpty()) {
            return Collections.emptyList();
        }
        ArrayList<ZmuxConformanceSuite> ordered = new ArrayList<>(merged.size());
        for (ZmuxConformanceSuite suite : KNOWN_CONFORMANCE_SUITES) {
            if (merged.contains(suite)) {
                ordered.add(suite);
            }
        }
        return Collections.unmodifiableList(ordered);
    }

    @SafeVarargs
    private static <T> List<T> list(T... values) {
        return Collections.unmodifiableList(Arrays.asList(values));
    }

    private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
