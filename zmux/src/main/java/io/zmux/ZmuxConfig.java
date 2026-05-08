package io.zmux;

import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class ZmuxConfig {
    public static final long DEFAULT_PREFACE_PADDING_MIN_BYTES = 16L;
    public static final long DEFAULT_PREFACE_PADDING_MAX_BYTES = 256L;
    public static final long DEFAULT_PING_PADDING_MIN_BYTES = 16L;
    public static final long DEFAULT_PING_PADDING_MAX_BYTES = 64L;
    private static final Duration DEFAULT_IDLE_KEEPALIVE_INTERVAL = Duration.ofMinutes(1);
    private static final Duration DEFAULT_KEEPALIVE_MAX_PING_INTERVAL = Duration.ofMinutes(5);
    private static final SecureRandom NONCE_RANDOM = new SecureRandom();
    private static final Object DEFAULT_LOCK = new Object();
    private static final ZmuxConfig BUILTIN_DEFAULTS = new Builder().build();
    private static ZmuxConfig defaultTemplate = BUILTIN_DEFAULTS;

    private final Role role;
    private final long tieBreakerNonce;
    private final long minProto;
    private final long maxProto;
    private final long capabilities;
    private final Settings settings;
    private final boolean prefacePadding;
    private final long prefacePaddingMinBytes;
    private final long prefacePaddingMaxBytes;
    private final Duration keepaliveInterval;
    private final Duration keepaliveMaxPingInterval;
    private final Duration keepaliveTimeout;
    private final boolean pingPadding;
    private final long pingPaddingMinBytes;
    private final long pingPaddingMaxBytes;
    private final long sessionMemoryCap;
    private final long perStreamQueuedDataHwm;
    private final long sessionQueuedDataHwm;
    private final long urgentQueuedBytesCap;
    private final long pendingControlBytesBudget;
    private final long pendingPriorityBytesBudget;
    private final Duration abuseWindow;
    private final Duration gracefulCloseDrainTimeout;
    private final Duration stopSendingGracefulDrainWindow;
    private final long stopSendingGracefulTailCap;
    private final Duration hiddenAbortChurnWindow;
    private final int hiddenAbortChurnThreshold;
    private final Duration visibleTerminalChurnWindow;
    private final int visibleTerminalChurnThreshold;
    private final int inboundControlFrameBudget;
    private final long inboundControlBytesBudget;
    private final int inboundExtFrameBudget;
    private final long inboundExtBytesBudget;
    private final int inboundMixedFrameBudget;
    private final long inboundMixedBytesBudget;
    private final int noOpControlFloodThreshold;
    private final int noOpMaxDataFloodThreshold;
    private final int noOpBlockedFloodThreshold;
    private final int noOpZeroDataFloodThreshold;
    private final int noOpPriorityUpdateFloodThreshold;
    private final int groupRebucketChurnThreshold;
    private final int inboundPingFloodThreshold;
    private final int acceptBacklogLimit;
    private final long acceptBacklogBytesLimit;
    private final int tombstoneLimit;
    private final int markerOnlyUsedStreamLimit;
    private final long retainedOpenInfoBytesBudget;
    private final long retainedPeerReasonBytesBudget;
    private final long aggregateLateDataCap;
    private final ZmuxEventHandler eventHandler;

    public ZmuxConfig(Role role,
                      long tieBreakerNonce,
                      long minProto,
                      long maxProto,
                      long capabilities,
                      Settings settings,
                      Duration keepaliveInterval,
                      Duration keepaliveMaxPingInterval,
                      Duration keepaliveTimeout,
                      long sessionMemoryCap,
                      long perStreamQueuedDataHwm,
                      long sessionQueuedDataHwm,
                      long urgentQueuedBytesCap,
                      long pendingControlBytesBudget,
                      long pendingPriorityBytesBudget,
                      Duration abuseWindow,
                      Duration gracefulCloseDrainTimeout,
                      Duration stopSendingGracefulDrainWindow,
                      long stopSendingGracefulTailCap,
                      Duration hiddenAbortChurnWindow,
                      int hiddenAbortChurnThreshold,
                      Duration visibleTerminalChurnWindow,
                      int visibleTerminalChurnThreshold,
                      int inboundControlFrameBudget,
                      long inboundControlBytesBudget,
                      int inboundExtFrameBudget,
                      long inboundExtBytesBudget,
                      int inboundMixedFrameBudget,
                      long inboundMixedBytesBudget,
                      int noOpControlFloodThreshold,
                      int noOpMaxDataFloodThreshold,
                      int noOpBlockedFloodThreshold,
                      int noOpZeroDataFloodThreshold,
                      int noOpPriorityUpdateFloodThreshold,
                      int groupRebucketChurnThreshold,
                      int inboundPingFloodThreshold,
                      int acceptBacklogLimit,
                      long acceptBacklogBytesLimit,
                      int tombstoneLimit,
                      int markerOnlyUsedStreamLimit,
                      long retainedOpenInfoBytesBudget,
                      long retainedPeerReasonBytesBudget,
                      long aggregateLateDataCap,
                      ZmuxEventHandler eventHandler) {
        this(
                role,
                tieBreakerNonce,
                minProto,
                maxProto,
                capabilities,
                settings,
                false,
                0L,
                0L,
                keepaliveInterval,
                keepaliveMaxPingInterval,
                keepaliveTimeout,
                false,
                0L,
                0L,
                sessionMemoryCap,
                perStreamQueuedDataHwm,
                sessionQueuedDataHwm,
                urgentQueuedBytesCap,
                pendingControlBytesBudget,
                pendingPriorityBytesBudget,
                abuseWindow,
                gracefulCloseDrainTimeout,
                stopSendingGracefulDrainWindow,
                stopSendingGracefulTailCap,
                hiddenAbortChurnWindow,
                hiddenAbortChurnThreshold,
                visibleTerminalChurnWindow,
                visibleTerminalChurnThreshold,
                inboundControlFrameBudget,
                inboundControlBytesBudget,
                inboundExtFrameBudget,
                inboundExtBytesBudget,
                inboundMixedFrameBudget,
                inboundMixedBytesBudget,
                noOpControlFloodThreshold,
                noOpMaxDataFloodThreshold,
                noOpBlockedFloodThreshold,
                noOpZeroDataFloodThreshold,
                noOpPriorityUpdateFloodThreshold,
                groupRebucketChurnThreshold,
                inboundPingFloodThreshold,
                acceptBacklogLimit,
                acceptBacklogBytesLimit,
                tombstoneLimit,
                markerOnlyUsedStreamLimit,
                retainedOpenInfoBytesBudget,
                retainedPeerReasonBytesBudget,
                aggregateLateDataCap,
                eventHandler
        );
    }

    public ZmuxConfig(Role role,
                      long tieBreakerNonce,
                      long minProto,
                      long maxProto,
                      long capabilities,
                      Settings settings,
                      boolean prefacePadding,
                      long prefacePaddingMinBytes,
                      long prefacePaddingMaxBytes,
                      Duration keepaliveInterval,
                      Duration keepaliveMaxPingInterval,
                      Duration keepaliveTimeout,
                      boolean pingPadding,
                      long pingPaddingMinBytes,
                      long pingPaddingMaxBytes,
                      long sessionMemoryCap,
                      long perStreamQueuedDataHwm,
                      long sessionQueuedDataHwm,
                      long urgentQueuedBytesCap,
                      long pendingControlBytesBudget,
                      long pendingPriorityBytesBudget,
                      Duration abuseWindow,
                      Duration gracefulCloseDrainTimeout,
                      Duration stopSendingGracefulDrainWindow,
                      long stopSendingGracefulTailCap,
                      Duration hiddenAbortChurnWindow,
                      int hiddenAbortChurnThreshold,
                      Duration visibleTerminalChurnWindow,
                      int visibleTerminalChurnThreshold,
                      int inboundControlFrameBudget,
                      long inboundControlBytesBudget,
                      int inboundExtFrameBudget,
                      long inboundExtBytesBudget,
                      int inboundMixedFrameBudget,
                      long inboundMixedBytesBudget,
                      int noOpControlFloodThreshold,
                      int noOpMaxDataFloodThreshold,
                      int noOpBlockedFloodThreshold,
                      int noOpZeroDataFloodThreshold,
                      int noOpPriorityUpdateFloodThreshold,
                      int groupRebucketChurnThreshold,
                      int inboundPingFloodThreshold,
                      int acceptBacklogLimit,
                      long acceptBacklogBytesLimit,
                      int tombstoneLimit,
                      int markerOnlyUsedStreamLimit,
                      long retainedOpenInfoBytesBudget,
                      long retainedPeerReasonBytesBudget,
                      long aggregateLateDataCap,
                      ZmuxEventHandler eventHandler) {
        role = role == null ? Role.AUTO : role;
        requireVarint62(tieBreakerNonce, "tieBreakerNonce");
        minProto = normalizeProtocolVersion(minProto, "minProto");
        maxProto = normalizeProtocolVersion(maxProto, "maxProto");
        if (minProto > maxProto) {
            throw new IllegalArgumentException("zmux config minProto must be <= maxProto");
        }
        requireVarint62(capabilities, "capabilities");
        settings = normalizeConfigSettings(settings);
        requireNonNegative(prefacePaddingMinBytes, "prefacePaddingMinBytes");
        requireNonNegative(prefacePaddingMaxBytes, "prefacePaddingMaxBytes");
        keepaliveInterval = normalizeOptionalDuration(keepaliveInterval, "keepaliveInterval");
        keepaliveMaxPingInterval = normalizeOptionalDuration(keepaliveMaxPingInterval, "keepaliveMaxPingInterval");
        keepaliveTimeout = normalizeOptionalDuration(keepaliveTimeout, "keepaliveTimeout");
        requireNonNegative(pingPaddingMinBytes, "pingPaddingMinBytes");
        requireNonNegative(pingPaddingMaxBytes, "pingPaddingMaxBytes");
        requireNonNegative(sessionMemoryCap, "sessionMemoryCap");
        requireNonNegative(perStreamQueuedDataHwm, "perStreamQueuedDataHwm");
        requireNonNegative(sessionQueuedDataHwm, "sessionQueuedDataHwm");
        requireNonNegative(urgentQueuedBytesCap, "urgentQueuedBytesCap");
        requireNonNegative(pendingControlBytesBudget, "pendingControlBytesBudget");
        requireNonNegative(pendingPriorityBytesBudget, "pendingPriorityBytesBudget");
        abuseWindow = normalizeOptionalDuration(abuseWindow, "abuseWindow");
        gracefulCloseDrainTimeout = normalizeOptionalDuration(gracefulCloseDrainTimeout, "gracefulCloseDrainTimeout");
        stopSendingGracefulDrainWindow = normalizeOptionalDuration(stopSendingGracefulDrainWindow, "stopSendingGracefulDrainWindow");
        requireNonNegative(stopSendingGracefulTailCap, "stopSendingGracefulTailCap");
        hiddenAbortChurnWindow = normalizeOptionalDuration(hiddenAbortChurnWindow, "hiddenAbortChurnWindow");
        requireNonNegative(hiddenAbortChurnThreshold, "hiddenAbortChurnThreshold");
        visibleTerminalChurnWindow = normalizeOptionalDuration(visibleTerminalChurnWindow, "visibleTerminalChurnWindow");
        requireNonNegative(visibleTerminalChurnThreshold, "visibleTerminalChurnThreshold");
        requireNonNegative(inboundControlFrameBudget, "inboundControlFrameBudget");
        requireNonNegative(inboundControlBytesBudget, "inboundControlBytesBudget");
        requireNonNegative(inboundExtFrameBudget, "inboundExtFrameBudget");
        requireNonNegative(inboundExtBytesBudget, "inboundExtBytesBudget");
        requireNonNegative(inboundMixedFrameBudget, "inboundMixedFrameBudget");
        requireNonNegative(inboundMixedBytesBudget, "inboundMixedBytesBudget");
        requireNonNegative(noOpControlFloodThreshold, "noOpControlFloodThreshold");
        requireNonNegative(noOpMaxDataFloodThreshold, "noOpMaxDataFloodThreshold");
        requireNonNegative(noOpBlockedFloodThreshold, "noOpBlockedFloodThreshold");
        requireNonNegative(noOpZeroDataFloodThreshold, "noOpZeroDataFloodThreshold");
        requireNonNegative(noOpPriorityUpdateFloodThreshold, "noOpPriorityUpdateFloodThreshold");
        requireNonNegative(groupRebucketChurnThreshold, "groupRebucketChurnThreshold");
        requireNonNegative(inboundPingFloodThreshold, "inboundPingFloodThreshold");
        requireNonNegative(acceptBacklogLimit, "acceptBacklogLimit");
        requireNonNegative(acceptBacklogBytesLimit, "acceptBacklogBytesLimit");
        requireNonNegative(tombstoneLimit, "tombstoneLimit");
        requireNonNegative(markerOnlyUsedStreamLimit, "markerOnlyUsedStreamLimit");
        requireNonNegative(retainedOpenInfoBytesBudget, "retainedOpenInfoBytesBudget");
        requireNonNegative(retainedPeerReasonBytesBudget, "retainedPeerReasonBytesBudget");
        requireNonNegative(aggregateLateDataCap, "aggregateLateDataCap");
        this.role = role;
        this.tieBreakerNonce = tieBreakerNonce;
        this.minProto = minProto;
        this.maxProto = maxProto;
        this.capabilities = capabilities;
        this.settings = settings;
        this.prefacePadding = prefacePadding;
        this.prefacePaddingMinBytes = prefacePaddingMinBytes;
        this.prefacePaddingMaxBytes = prefacePaddingMaxBytes;
        this.keepaliveInterval = keepaliveInterval;
        this.keepaliveMaxPingInterval = keepaliveMaxPingInterval;
        this.keepaliveTimeout = keepaliveTimeout;
        this.pingPadding = pingPadding;
        this.pingPaddingMinBytes = pingPaddingMinBytes;
        this.pingPaddingMaxBytes = pingPaddingMaxBytes;
        this.sessionMemoryCap = sessionMemoryCap;
        this.perStreamQueuedDataHwm = perStreamQueuedDataHwm;
        this.sessionQueuedDataHwm = sessionQueuedDataHwm;
        this.urgentQueuedBytesCap = urgentQueuedBytesCap;
        this.pendingControlBytesBudget = pendingControlBytesBudget;
        this.pendingPriorityBytesBudget = pendingPriorityBytesBudget;
        this.abuseWindow = abuseWindow;
        this.gracefulCloseDrainTimeout = gracefulCloseDrainTimeout;
        this.stopSendingGracefulDrainWindow = stopSendingGracefulDrainWindow;
        this.stopSendingGracefulTailCap = stopSendingGracefulTailCap;
        this.hiddenAbortChurnWindow = hiddenAbortChurnWindow;
        this.hiddenAbortChurnThreshold = hiddenAbortChurnThreshold;
        this.visibleTerminalChurnWindow = visibleTerminalChurnWindow;
        this.visibleTerminalChurnThreshold = visibleTerminalChurnThreshold;
        this.inboundControlFrameBudget = inboundControlFrameBudget;
        this.inboundControlBytesBudget = inboundControlBytesBudget;
        this.inboundExtFrameBudget = inboundExtFrameBudget;
        this.inboundExtBytesBudget = inboundExtBytesBudget;
        this.inboundMixedFrameBudget = inboundMixedFrameBudget;
        this.inboundMixedBytesBudget = inboundMixedBytesBudget;
        this.noOpControlFloodThreshold = noOpControlFloodThreshold;
        this.noOpMaxDataFloodThreshold = noOpMaxDataFloodThreshold;
        this.noOpBlockedFloodThreshold = noOpBlockedFloodThreshold;
        this.noOpZeroDataFloodThreshold = noOpZeroDataFloodThreshold;
        this.noOpPriorityUpdateFloodThreshold = noOpPriorityUpdateFloodThreshold;
        this.groupRebucketChurnThreshold = groupRebucketChurnThreshold;
        this.inboundPingFloodThreshold = inboundPingFloodThreshold;
        this.acceptBacklogLimit = acceptBacklogLimit;
        this.acceptBacklogBytesLimit = acceptBacklogBytesLimit;
        this.tombstoneLimit = tombstoneLimit;
        this.markerOnlyUsedStreamLimit = markerOnlyUsedStreamLimit;
        this.retainedOpenInfoBytesBudget = retainedOpenInfoBytesBudget;
        this.retainedPeerReasonBytesBudget = retainedPeerReasonBytesBudget;
        this.aggregateLateDataCap = aggregateLateDataCap;
        this.eventHandler = eventHandler;
    }

    public static ZmuxConfig defaults() {
        synchronized (DEFAULT_LOCK) {
            return defaultTemplate.toBuilder().build();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void configureDefaultConfig(Consumer<Builder> customizer) {
        if (customizer == null) {
            return;
        }
        ZmuxConfig base;
        synchronized (DEFAULT_LOCK) {
            base = defaultTemplate;
        }
        Builder builder = base.toBuilder();
        customizer.accept(builder);
        ZmuxConfig next = sanitizeDefaultTemplate(builder.build());
        synchronized (DEFAULT_LOCK) {
            defaultTemplate = next;
        }
    }

    public static void resetDefaultConfig() {
        synchronized (DEFAULT_LOCK) {
            defaultTemplate = BUILTIN_DEFAULTS;
        }
    }

    private static long randomVarint62() {
        long value;
        do {
            value = NONCE_RANDOM.nextLong() & Protocol.MAX_VARINT62;
        } while (value == 0L);
        return value;
    }

    private static ZmuxConfig sanitizeDefaultTemplate(ZmuxConfig config) {
        Settings settings = config.settings();
        if (settings.pingPaddingKey() != 0L) {
            settings = settings.toBuilder().pingPaddingKey(0L).build();
        }
        return config.toBuilder()
                .tieBreakerNonce(0L)
                .settings(settings)
                .build();
    }

    private static Settings normalizeConfigSettings(Settings value) {
        if (value == null) {
            return Settings.defaults();
        }
        Settings defaults = Settings.defaults();
        if (value.maxFramePayload() != 0L
                && value.maxControlPayloadBytes() != 0L
                && value.maxExtensionPayloadBytes() != 0L) {
            return value;
        }
        return value.toBuilder()
                .maxFramePayload(value.maxFramePayload() == 0L ? defaults.maxFramePayload() : value.maxFramePayload())
                .maxControlPayloadBytes(value.maxControlPayloadBytes() == 0L
                        ? defaults.maxControlPayloadBytes()
                        : value.maxControlPayloadBytes())
                .maxExtensionPayloadBytes(value.maxExtensionPayloadBytes() == 0L
                        ? defaults.maxExtensionPayloadBytes()
                        : value.maxExtensionPayloadBytes())
                .build();
    }

    private static Duration normalizeOptionalDuration(Duration value, String field) {
        if (value == null || value.isZero()) {
            return Duration.ZERO;
        }
        if (value.isNegative()) {
            throw new IllegalArgumentException("zmux config " + field + " must be >= 0");
        }
        return value;
    }

    private static void requireVarint62(long value, String field) {
        if (value < 0L || value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux config " + field + " must be within varint62 range");
        }
    }

    private static long normalizeProtocolVersion(long value, String field) {
        if (value == 0L) {
            return Protocol.PROTO_VERSION;
        }
        if (value < 0L) {
            throw new IllegalArgumentException("zmux config " + field + " must be > 0");
        }
        if (value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux config " + field + " must be within varint62 range");
        }
        return value;
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0L) {
            throw new IllegalArgumentException("zmux config " + field + " must be >= 0");
        }
    }

    private static void requireNonNegative(int value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException("zmux config " + field + " must be >= 0");
        }
    }

    public Builder toBuilder() {
        return new Builder()
                .role(role)
                .tieBreakerNonce(tieBreakerNonce)
                .minProto(minProto)
                .maxProto(maxProto)
                .capabilities(capabilities)
                .settings(settings)
                .prefacePadding(prefacePadding)
                .prefacePaddingMinBytes(prefacePaddingMinBytes)
                .prefacePaddingMaxBytes(prefacePaddingMaxBytes)
                .keepaliveInterval(keepaliveInterval)
                .keepaliveMaxPingInterval(keepaliveMaxPingInterval)
                .keepaliveTimeout(keepaliveTimeout)
                .pingPadding(pingPadding)
                .pingPaddingMinBytes(pingPaddingMinBytes)
                .pingPaddingMaxBytes(pingPaddingMaxBytes)
                .sessionMemoryCap(sessionMemoryCap)
                .perStreamQueuedDataHwm(perStreamQueuedDataHwm)
                .sessionQueuedDataHwm(sessionQueuedDataHwm)
                .urgentQueuedBytesCap(urgentQueuedBytesCap)
                .pendingControlBytesBudget(pendingControlBytesBudget)
                .pendingPriorityBytesBudget(pendingPriorityBytesBudget)
                .abuseWindow(abuseWindow)
                .gracefulCloseDrainTimeout(gracefulCloseDrainTimeout)
                .stopSendingGracefulDrainWindow(stopSendingGracefulDrainWindow)
                .stopSendingGracefulTailCap(stopSendingGracefulTailCap)
                .hiddenAbortChurnWindow(hiddenAbortChurnWindow)
                .hiddenAbortChurnThreshold(hiddenAbortChurnThreshold)
                .visibleTerminalChurnWindow(visibleTerminalChurnWindow)
                .visibleTerminalChurnThreshold(visibleTerminalChurnThreshold)
                .inboundControlFrameBudget(inboundControlFrameBudget)
                .inboundControlBytesBudget(inboundControlBytesBudget)
                .inboundExtFrameBudget(inboundExtFrameBudget)
                .inboundExtBytesBudget(inboundExtBytesBudget)
                .inboundMixedFrameBudget(inboundMixedFrameBudget)
                .inboundMixedBytesBudget(inboundMixedBytesBudget)
                .noOpControlFloodThreshold(noOpControlFloodThreshold)
                .noOpMaxDataFloodThreshold(noOpMaxDataFloodThreshold)
                .noOpBlockedFloodThreshold(noOpBlockedFloodThreshold)
                .noOpZeroDataFloodThreshold(noOpZeroDataFloodThreshold)
                .noOpPriorityUpdateFloodThreshold(noOpPriorityUpdateFloodThreshold)
                .groupRebucketChurnThreshold(groupRebucketChurnThreshold)
                .inboundPingFloodThreshold(inboundPingFloodThreshold)
                .acceptBacklogLimit(acceptBacklogLimit)
                .acceptBacklogBytesLimit(acceptBacklogBytesLimit)
                .tombstoneLimit(tombstoneLimit)
                .markerOnlyUsedStreamLimit(markerOnlyUsedStreamLimit)
                .retainedOpenInfoBytesBudget(retainedOpenInfoBytesBudget)
                .retainedPeerReasonBytesBudget(retainedPeerReasonBytesBudget)
                .aggregateLateDataCap(aggregateLateDataCap)
                .eventHandler(eventHandler);
    }

    public ZmuxConfig withRole(Role role) {
        Role effectiveRole = role == null ? Role.AUTO : role;
        long effectiveNonce = effectiveRole == Role.AUTO ? tieBreakerNonce : 0L;
        return toBuilder()
                .role(effectiveRole)
                .tieBreakerNonce(effectiveNonce)
                .build();
    }

    public Preface localPreface() {
        long nonce = tieBreakerNonce;
        if (role == Role.INITIATOR || role == Role.RESPONDER) {
            nonce = 0L;
        } else if (nonce == 0L) {
            nonce = randomVarint62();
        }
        Settings localSettings = settings;
        if (pingPadding) {
            if (localSettings.pingPaddingKey() == 0L) {
                localSettings = localSettings.toBuilder()
                        .pingPaddingKey(randomVarint62())
                        .build();
            }
        } else if (localSettings.pingPaddingKey() != 0L) {
            localSettings = localSettings.toBuilder()
                    .pingPaddingKey(0L)
                    .build();
        }
        return new Preface(
                Protocol.PREFACE_VERSION,
                role,
                nonce,
                minProto,
                maxProto,
                capabilities,
                localSettings
        );
    }

    public Role role() {
        return role;
    }

    public long tieBreakerNonce() {
        return tieBreakerNonce;
    }

    public long minProto() {
        return minProto;
    }

    public long maxProto() {
        return maxProto;
    }

    public long capabilities() {
        return capabilities;
    }

    public Settings settings() {
        return settings;
    }

    public boolean prefacePadding() {
        return prefacePadding;
    }

    public long prefacePaddingMinBytes() {
        return prefacePaddingMinBytes;
    }

    public long prefacePaddingMaxBytes() {
        return prefacePaddingMaxBytes;
    }

    public Duration keepaliveInterval() {
        return keepaliveInterval;
    }

    public Duration keepaliveMaxPingInterval() {
        return keepaliveMaxPingInterval;
    }

    public Duration keepaliveTimeout() {
        return keepaliveTimeout;
    }

    public boolean pingPadding() {
        return pingPadding;
    }

    public long pingPaddingMinBytes() {
        return pingPaddingMinBytes;
    }

    public long pingPaddingMaxBytes() {
        return pingPaddingMaxBytes;
    }

    public long sessionMemoryCap() {
        return sessionMemoryCap;
    }

    public long perStreamQueuedDataHwm() {
        return perStreamQueuedDataHwm;
    }

    public long sessionQueuedDataHwm() {
        return sessionQueuedDataHwm;
    }

    public long urgentQueuedBytesCap() {
        return urgentQueuedBytesCap;
    }

    public long pendingControlBytesBudget() {
        return pendingControlBytesBudget;
    }

    public long pendingPriorityBytesBudget() {
        return pendingPriorityBytesBudget;
    }

    public Duration abuseWindow() {
        return abuseWindow;
    }

    public Duration gracefulCloseDrainTimeout() {
        return gracefulCloseDrainTimeout;
    }

    public Duration stopSendingGracefulDrainWindow() {
        return stopSendingGracefulDrainWindow;
    }

    public long stopSendingGracefulTailCap() {
        return stopSendingGracefulTailCap;
    }

    public Duration hiddenAbortChurnWindow() {
        return hiddenAbortChurnWindow;
    }

    public int hiddenAbortChurnThreshold() {
        return hiddenAbortChurnThreshold;
    }

    public Duration visibleTerminalChurnWindow() {
        return visibleTerminalChurnWindow;
    }

    public int visibleTerminalChurnThreshold() {
        return visibleTerminalChurnThreshold;
    }

    public int inboundControlFrameBudget() {
        return inboundControlFrameBudget;
    }

    public long inboundControlBytesBudget() {
        return inboundControlBytesBudget;
    }

    public int inboundExtFrameBudget() {
        return inboundExtFrameBudget;
    }

    public long inboundExtBytesBudget() {
        return inboundExtBytesBudget;
    }

    public int inboundMixedFrameBudget() {
        return inboundMixedFrameBudget;
    }

    public long inboundMixedBytesBudget() {
        return inboundMixedBytesBudget;
    }

    public int noOpControlFloodThreshold() {
        return noOpControlFloodThreshold;
    }

    public int noOpMaxDataFloodThreshold() {
        return noOpMaxDataFloodThreshold;
    }

    public int noOpBlockedFloodThreshold() {
        return noOpBlockedFloodThreshold;
    }

    public int noOpZeroDataFloodThreshold() {
        return noOpZeroDataFloodThreshold;
    }

    public int noOpPriorityUpdateFloodThreshold() {
        return noOpPriorityUpdateFloodThreshold;
    }

    public int groupRebucketChurnThreshold() {
        return groupRebucketChurnThreshold;
    }

    public int inboundPingFloodThreshold() {
        return inboundPingFloodThreshold;
    }

    public int acceptBacklogLimit() {
        return acceptBacklogLimit;
    }

    public long acceptBacklogBytesLimit() {
        return acceptBacklogBytesLimit;
    }

    public int tombstoneLimit() {
        return tombstoneLimit;
    }

    public int markerOnlyUsedStreamLimit() {
        return markerOnlyUsedStreamLimit;
    }

    public long retainedOpenInfoBytesBudget() {
        return retainedOpenInfoBytesBudget;
    }

    public long retainedPeerReasonBytesBudget() {
        return retainedPeerReasonBytesBudget;
    }

    public long aggregateLateDataCap() {
        return aggregateLateDataCap;
    }

    public ZmuxEventHandler eventHandler() {
        return eventHandler;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ZmuxConfig)) {
            return false;
        }
        ZmuxConfig that = (ZmuxConfig) other;
        return tieBreakerNonce == that.tieBreakerNonce
                && minProto == that.minProto
                && maxProto == that.maxProto
                && capabilities == that.capabilities
                && prefacePadding == that.prefacePadding
                && prefacePaddingMinBytes == that.prefacePaddingMinBytes
                && prefacePaddingMaxBytes == that.prefacePaddingMaxBytes
                && pingPadding == that.pingPadding
                && pingPaddingMinBytes == that.pingPaddingMinBytes
                && pingPaddingMaxBytes == that.pingPaddingMaxBytes
                && sessionMemoryCap == that.sessionMemoryCap
                && perStreamQueuedDataHwm == that.perStreamQueuedDataHwm
                && sessionQueuedDataHwm == that.sessionQueuedDataHwm
                && urgentQueuedBytesCap == that.urgentQueuedBytesCap
                && pendingControlBytesBudget == that.pendingControlBytesBudget
                && pendingPriorityBytesBudget == that.pendingPriorityBytesBudget
                && stopSendingGracefulTailCap == that.stopSendingGracefulTailCap
                && hiddenAbortChurnThreshold == that.hiddenAbortChurnThreshold
                && visibleTerminalChurnThreshold == that.visibleTerminalChurnThreshold
                && inboundControlFrameBudget == that.inboundControlFrameBudget
                && inboundControlBytesBudget == that.inboundControlBytesBudget
                && inboundExtFrameBudget == that.inboundExtFrameBudget
                && inboundExtBytesBudget == that.inboundExtBytesBudget
                && inboundMixedFrameBudget == that.inboundMixedFrameBudget
                && inboundMixedBytesBudget == that.inboundMixedBytesBudget
                && noOpControlFloodThreshold == that.noOpControlFloodThreshold
                && noOpMaxDataFloodThreshold == that.noOpMaxDataFloodThreshold
                && noOpBlockedFloodThreshold == that.noOpBlockedFloodThreshold
                && noOpZeroDataFloodThreshold == that.noOpZeroDataFloodThreshold
                && noOpPriorityUpdateFloodThreshold == that.noOpPriorityUpdateFloodThreshold
                && groupRebucketChurnThreshold == that.groupRebucketChurnThreshold
                && inboundPingFloodThreshold == that.inboundPingFloodThreshold
                && acceptBacklogLimit == that.acceptBacklogLimit
                && acceptBacklogBytesLimit == that.acceptBacklogBytesLimit
                && tombstoneLimit == that.tombstoneLimit
                && markerOnlyUsedStreamLimit == that.markerOnlyUsedStreamLimit
                && retainedOpenInfoBytesBudget == that.retainedOpenInfoBytesBudget
                && retainedPeerReasonBytesBudget == that.retainedPeerReasonBytesBudget
                && aggregateLateDataCap == that.aggregateLateDataCap
                && role == that.role
                && Objects.equals(settings, that.settings)
                && Objects.equals(keepaliveInterval, that.keepaliveInterval)
                && Objects.equals(keepaliveMaxPingInterval, that.keepaliveMaxPingInterval)
                && Objects.equals(keepaliveTimeout, that.keepaliveTimeout)
                && Objects.equals(abuseWindow, that.abuseWindow)
                && Objects.equals(gracefulCloseDrainTimeout, that.gracefulCloseDrainTimeout)
                && Objects.equals(stopSendingGracefulDrainWindow, that.stopSendingGracefulDrainWindow)
                && Objects.equals(hiddenAbortChurnWindow, that.hiddenAbortChurnWindow)
                && Objects.equals(visibleTerminalChurnWindow, that.visibleTerminalChurnWindow)
                && Objects.equals(eventHandler, that.eventHandler);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                role,
                tieBreakerNonce,
                minProto,
                maxProto,
                capabilities,
                settings,
                prefacePadding,
                prefacePaddingMinBytes,
                prefacePaddingMaxBytes,
                keepaliveInterval,
                keepaliveMaxPingInterval,
                keepaliveTimeout,
                pingPadding,
                pingPaddingMinBytes,
                pingPaddingMaxBytes,
                sessionMemoryCap,
                perStreamQueuedDataHwm,
                sessionQueuedDataHwm,
                urgentQueuedBytesCap,
                pendingControlBytesBudget,
                pendingPriorityBytesBudget,
                abuseWindow,
                gracefulCloseDrainTimeout,
                stopSendingGracefulDrainWindow,
                stopSendingGracefulTailCap,
                hiddenAbortChurnWindow,
                hiddenAbortChurnThreshold,
                visibleTerminalChurnWindow,
                visibleTerminalChurnThreshold,
                inboundControlFrameBudget,
                inboundControlBytesBudget,
                inboundExtFrameBudget,
                inboundExtBytesBudget,
                inboundMixedFrameBudget,
                inboundMixedBytesBudget,
                noOpControlFloodThreshold,
                noOpMaxDataFloodThreshold,
                noOpBlockedFloodThreshold,
                noOpZeroDataFloodThreshold,
                noOpPriorityUpdateFloodThreshold,
                groupRebucketChurnThreshold,
                inboundPingFloodThreshold,
                acceptBacklogLimit,
                acceptBacklogBytesLimit,
                tombstoneLimit,
                markerOnlyUsedStreamLimit,
                retainedOpenInfoBytesBudget,
                retainedPeerReasonBytesBudget,
                aggregateLateDataCap,
                eventHandler
        );
    }

    public static final class Builder {
        private Role role = Role.AUTO;
        private long tieBreakerNonce;
        private long minProto = Protocol.PROTO_VERSION;
        private long maxProto = Protocol.PROTO_VERSION;
        private long capabilities;
        private Settings settings = Settings.defaults();
        private boolean prefacePadding = true;
        private long prefacePaddingMinBytes;
        private long prefacePaddingMaxBytes;
        private Duration keepaliveInterval = DEFAULT_IDLE_KEEPALIVE_INTERVAL;
        private Duration keepaliveMaxPingInterval = DEFAULT_KEEPALIVE_MAX_PING_INTERVAL;
        private Duration keepaliveTimeout = Duration.ZERO;
        private boolean pingPadding = true;
        private long pingPaddingMinBytes;
        private long pingPaddingMaxBytes;
        private long sessionMemoryCap;
        private long perStreamQueuedDataHwm;
        private long sessionQueuedDataHwm;
        private long urgentQueuedBytesCap;
        private long pendingControlBytesBudget;
        private long pendingPriorityBytesBudget;
        private Duration abuseWindow = Duration.ZERO;
        private Duration gracefulCloseDrainTimeout = Duration.ZERO;
        private Duration stopSendingGracefulDrainWindow = Duration.ZERO;
        private long stopSendingGracefulTailCap;
        private Duration hiddenAbortChurnWindow = Duration.ZERO;
        private int hiddenAbortChurnThreshold;
        private Duration visibleTerminalChurnWindow = Duration.ZERO;
        private int visibleTerminalChurnThreshold;
        private int inboundControlFrameBudget;
        private long inboundControlBytesBudget;
        private int inboundExtFrameBudget;
        private long inboundExtBytesBudget;
        private int inboundMixedFrameBudget;
        private long inboundMixedBytesBudget;
        private int noOpControlFloodThreshold;
        private int noOpMaxDataFloodThreshold;
        private int noOpBlockedFloodThreshold;
        private int noOpZeroDataFloodThreshold;
        private int noOpPriorityUpdateFloodThreshold;
        private int groupRebucketChurnThreshold;
        private int inboundPingFloodThreshold;
        private int acceptBacklogLimit;
        private long acceptBacklogBytesLimit;
        private int tombstoneLimit;
        private int markerOnlyUsedStreamLimit;
        private long retainedOpenInfoBytesBudget;
        private long retainedPeerReasonBytesBudget;
        private long aggregateLateDataCap;
        private ZmuxEventHandler eventHandler;

        public Builder role(Role value) {
            this.role = value;
            return this;
        }

        public Builder tieBreakerNonce(long value) {
            this.tieBreakerNonce = value;
            return this;
        }

        public Builder minProto(long value) {
            this.minProto = value;
            return this;
        }

        public Builder maxProto(long value) {
            this.maxProto = value;
            return this;
        }

        public Builder capabilities(long value) {
            this.capabilities = value;
            return this;
        }

        public Builder settings(Settings value) {
            this.settings = value;
            return this;
        }

        public Builder prefacePadding(boolean value) {
            this.prefacePadding = value;
            return this;
        }

        public Builder prefacePaddingMinBytes(long value) {
            this.prefacePaddingMinBytes = value;
            return this;
        }

        public Builder prefacePaddingMaxBytes(long value) {
            this.prefacePaddingMaxBytes = value;
            return this;
        }

        public Builder keepaliveInterval(Duration value) {
            this.keepaliveInterval = value;
            return this;
        }

        public Builder keepaliveMaxPingInterval(Duration value) {
            this.keepaliveMaxPingInterval = value;
            return this;
        }

        public Builder keepaliveTimeout(Duration value) {
            this.keepaliveTimeout = value;
            return this;
        }

        public Builder pingPadding(boolean value) {
            this.pingPadding = value;
            return this;
        }

        public Builder pingPaddingMinBytes(long value) {
            this.pingPaddingMinBytes = value;
            return this;
        }

        public Builder pingPaddingMaxBytes(long value) {
            this.pingPaddingMaxBytes = value;
            return this;
        }

        public Builder sessionMemoryCap(long value) {
            this.sessionMemoryCap = value;
            return this;
        }

        public Builder perStreamQueuedDataHwm(long value) {
            this.perStreamQueuedDataHwm = value;
            return this;
        }

        public Builder sessionQueuedDataHwm(long value) {
            this.sessionQueuedDataHwm = value;
            return this;
        }

        public Builder urgentQueuedBytesCap(long value) {
            this.urgentQueuedBytesCap = value;
            return this;
        }

        public Builder pendingControlBytesBudget(long value) {
            this.pendingControlBytesBudget = value;
            return this;
        }

        public Builder pendingPriorityBytesBudget(long value) {
            this.pendingPriorityBytesBudget = value;
            return this;
        }

        public Builder abuseWindow(Duration value) {
            this.abuseWindow = value;
            return this;
        }

        public Builder gracefulCloseDrainTimeout(Duration value) {
            this.gracefulCloseDrainTimeout = value;
            return this;
        }

        public Builder stopSendingGracefulDrainWindow(Duration value) {
            this.stopSendingGracefulDrainWindow = value;
            return this;
        }

        public Builder stopSendingGracefulTailCap(long value) {
            this.stopSendingGracefulTailCap = value;
            return this;
        }

        public Builder hiddenAbortChurnWindow(Duration value) {
            this.hiddenAbortChurnWindow = value;
            return this;
        }

        public Builder hiddenAbortChurnThreshold(int value) {
            this.hiddenAbortChurnThreshold = value;
            return this;
        }

        public Builder visibleTerminalChurnWindow(Duration value) {
            this.visibleTerminalChurnWindow = value;
            return this;
        }

        public Builder visibleTerminalChurnThreshold(int value) {
            this.visibleTerminalChurnThreshold = value;
            return this;
        }

        public Builder inboundControlFrameBudget(int value) {
            this.inboundControlFrameBudget = value;
            return this;
        }

        public Builder inboundControlBytesBudget(long value) {
            this.inboundControlBytesBudget = value;
            return this;
        }

        public Builder inboundExtFrameBudget(int value) {
            this.inboundExtFrameBudget = value;
            return this;
        }

        public Builder inboundExtBytesBudget(long value) {
            this.inboundExtBytesBudget = value;
            return this;
        }

        public Builder inboundMixedFrameBudget(int value) {
            this.inboundMixedFrameBudget = value;
            return this;
        }

        public Builder inboundMixedBytesBudget(long value) {
            this.inboundMixedBytesBudget = value;
            return this;
        }

        public Builder noOpControlFloodThreshold(int value) {
            this.noOpControlFloodThreshold = value;
            return this;
        }

        public Builder noOpMaxDataFloodThreshold(int value) {
            this.noOpMaxDataFloodThreshold = value;
            return this;
        }

        public Builder noOpBlockedFloodThreshold(int value) {
            this.noOpBlockedFloodThreshold = value;
            return this;
        }

        public Builder noOpZeroDataFloodThreshold(int value) {
            this.noOpZeroDataFloodThreshold = value;
            return this;
        }

        public Builder noOpPriorityUpdateFloodThreshold(int value) {
            this.noOpPriorityUpdateFloodThreshold = value;
            return this;
        }

        public Builder groupRebucketChurnThreshold(int value) {
            this.groupRebucketChurnThreshold = value;
            return this;
        }

        public Builder inboundPingFloodThreshold(int value) {
            this.inboundPingFloodThreshold = value;
            return this;
        }

        public Builder acceptBacklogLimit(int value) {
            this.acceptBacklogLimit = value;
            return this;
        }

        public Builder acceptBacklogBytesLimit(long value) {
            this.acceptBacklogBytesLimit = value;
            return this;
        }

        public Builder tombstoneLimit(int value) {
            this.tombstoneLimit = value;
            return this;
        }

        public Builder markerOnlyUsedStreamLimit(int value) {
            this.markerOnlyUsedStreamLimit = value;
            return this;
        }

        public Builder retainedOpenInfoBytesBudget(long value) {
            this.retainedOpenInfoBytesBudget = value;
            return this;
        }

        public Builder retainedPeerReasonBytesBudget(long value) {
            this.retainedPeerReasonBytesBudget = value;
            return this;
        }

        public Builder aggregateLateDataCap(long value) {
            this.aggregateLateDataCap = value;
            return this;
        }

        public Builder eventHandler(ZmuxEventHandler value) {
            this.eventHandler = value;
            return this;
        }

        public ZmuxConfig build() {
            return new ZmuxConfig(
                    role,
                    tieBreakerNonce,
                    minProto,
                    maxProto,
                    capabilities,
                    settings,
                    prefacePadding,
                    prefacePaddingMinBytes,
                    prefacePaddingMaxBytes,
                    keepaliveInterval,
                    keepaliveMaxPingInterval,
                    keepaliveTimeout,
                    pingPadding,
                    pingPaddingMinBytes,
                    pingPaddingMaxBytes,
                    sessionMemoryCap,
                    perStreamQueuedDataHwm,
                    sessionQueuedDataHwm,
                    urgentQueuedBytesCap,
                    pendingControlBytesBudget,
                    pendingPriorityBytesBudget,
                    abuseWindow,
                    gracefulCloseDrainTimeout,
                    stopSendingGracefulDrainWindow,
                    stopSendingGracefulTailCap,
                    hiddenAbortChurnWindow,
                    hiddenAbortChurnThreshold,
                    visibleTerminalChurnWindow,
                    visibleTerminalChurnThreshold,
                    inboundControlFrameBudget,
                    inboundControlBytesBudget,
                    inboundExtFrameBudget,
                    inboundExtBytesBudget,
                    inboundMixedFrameBudget,
                    inboundMixedBytesBudget,
                    noOpControlFloodThreshold,
                    noOpMaxDataFloodThreshold,
                    noOpBlockedFloodThreshold,
                    noOpZeroDataFloodThreshold,
                    noOpPriorityUpdateFloodThreshold,
                    groupRebucketChurnThreshold,
                    inboundPingFloodThreshold,
                    acceptBacklogLimit,
                    acceptBacklogBytesLimit,
                    tombstoneLimit,
                    markerOnlyUsedStreamLimit,
                    retainedOpenInfoBytesBudget,
                    retainedPeerReasonBytesBudget,
                    aggregateLateDataCap,
                    eventHandler
            );
        }
    }
}
