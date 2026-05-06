package io.zmux;

import java.util.Objects;

public final class Settings {
    private static final long DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED = 65_536L;
    private static final long DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED = 65_536L;
    private static final long DEFAULT_INITIAL_MAX_STREAM_DATA_UNI = 65_536L;
    private static final long DEFAULT_INITIAL_MAX_DATA = 262_144L;
    private static final long DEFAULT_MAX_INCOMING_STREAMS_BIDI = 256L;
    private static final long DEFAULT_MAX_INCOMING_STREAMS_UNI = 256L;
    private static final long DEFAULT_MAX_FRAME_PAYLOAD = 16_384L;
    private static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 0L;
    private static final long DEFAULT_KEEPALIVE_HINT_MILLIS = 0L;
    private static final long DEFAULT_MAX_CONTROL_PAYLOAD_BYTES = 4_096L;
    private static final long DEFAULT_MAX_EXTENSION_PAYLOAD_BYTES = 4_096L;
    private static final SchedulerHint DEFAULT_SCHEDULER_HINTS = SchedulerHint.UNSPECIFIED_OR_BALANCED;
    private static final long DEFAULT_PING_PADDING_KEY = 0L;
    private static final Settings DEFAULTS = new Settings(
            DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED,
            DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED,
            DEFAULT_INITIAL_MAX_STREAM_DATA_UNI,
            DEFAULT_INITIAL_MAX_DATA,
            DEFAULT_MAX_INCOMING_STREAMS_BIDI,
            DEFAULT_MAX_INCOMING_STREAMS_UNI,
            DEFAULT_MAX_FRAME_PAYLOAD,
            DEFAULT_IDLE_TIMEOUT_MILLIS,
            DEFAULT_KEEPALIVE_HINT_MILLIS,
            DEFAULT_MAX_CONTROL_PAYLOAD_BYTES,
            DEFAULT_MAX_EXTENSION_PAYLOAD_BYTES,
            DEFAULT_SCHEDULER_HINTS,
            DEFAULT_PING_PADDING_KEY
    );

    private final long initialMaxStreamDataBidiLocallyOpened;
    private final long initialMaxStreamDataBidiPeerOpened;
    private final long initialMaxStreamDataUni;
    private final long initialMaxData;
    private final long maxIncomingStreamsBidi;
    private final long maxIncomingStreamsUni;
    private final long maxFramePayload;
    private final long idleTimeoutMillis;
    private final long keepaliveHintMillis;
    private final long maxControlPayloadBytes;
    private final long maxExtensionPayloadBytes;
    private final SchedulerHint schedulerHints;
    private final long pingPaddingKey;
    private final Limits limits;

    public Settings(long initialMaxStreamDataBidiLocallyOpened,
                    long initialMaxStreamDataBidiPeerOpened,
                    long initialMaxStreamDataUni,
                    long initialMaxData,
                    long maxIncomingStreamsBidi,
                    long maxIncomingStreamsUni,
                    long maxFramePayload,
                    long idleTimeoutMillis,
                    long keepaliveHintMillis,
                    long maxControlPayloadBytes,
                    long maxExtensionPayloadBytes,
                    SchedulerHint schedulerHints) {
        this(
                initialMaxStreamDataBidiLocallyOpened,
                initialMaxStreamDataBidiPeerOpened,
                initialMaxStreamDataUni,
                initialMaxData,
                maxIncomingStreamsBidi,
                maxIncomingStreamsUni,
                maxFramePayload,
                idleTimeoutMillis,
                keepaliveHintMillis,
                maxControlPayloadBytes,
                maxExtensionPayloadBytes,
                schedulerHints,
                0L
        );
    }

    public Settings(long initialMaxStreamDataBidiLocallyOpened,
                    long initialMaxStreamDataBidiPeerOpened,
                    long initialMaxStreamDataUni,
                    long initialMaxData,
                    long maxIncomingStreamsBidi,
                    long maxIncomingStreamsUni,
                    long maxFramePayload,
                    long idleTimeoutMillis,
                    long keepaliveHintMillis,
                    long maxControlPayloadBytes,
                    long maxExtensionPayloadBytes,
                    SchedulerHint schedulerHints,
                    long pingPaddingKey) {
        requireVarint62(initialMaxStreamDataBidiLocallyOpened, "initialMaxStreamDataBidiLocallyOpened");
        requireVarint62(initialMaxStreamDataBidiPeerOpened, "initialMaxStreamDataBidiPeerOpened");
        requireVarint62(initialMaxStreamDataUni, "initialMaxStreamDataUni");
        requireVarint62(initialMaxData, "initialMaxData");
        requireVarint62(maxIncomingStreamsBidi, "maxIncomingStreamsBidi");
        requireVarint62(maxIncomingStreamsUni, "maxIncomingStreamsUni");
        requireVarint62(maxFramePayload, "maxFramePayload");
        requireVarint62(idleTimeoutMillis, "idleTimeoutMillis");
        requireVarint62(keepaliveHintMillis, "keepaliveHintMillis");
        requireVarint62(maxControlPayloadBytes, "maxControlPayloadBytes");
        requireVarint62(maxExtensionPayloadBytes, "maxExtensionPayloadBytes");
        requireVarint62(pingPaddingKey, "pingPaddingKey");
        this.initialMaxStreamDataBidiLocallyOpened = initialMaxStreamDataBidiLocallyOpened;
        this.initialMaxStreamDataBidiPeerOpened = initialMaxStreamDataBidiPeerOpened;
        this.initialMaxStreamDataUni = initialMaxStreamDataUni;
        this.initialMaxData = initialMaxData;
        this.maxIncomingStreamsBidi = maxIncomingStreamsBidi;
        this.maxIncomingStreamsUni = maxIncomingStreamsUni;
        this.maxFramePayload = maxFramePayload;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.keepaliveHintMillis = keepaliveHintMillis;
        this.maxControlPayloadBytes = maxControlPayloadBytes;
        this.maxExtensionPayloadBytes = maxExtensionPayloadBytes;
        this.schedulerHints = schedulerHints == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : schedulerHints;
        this.pingPaddingKey = pingPaddingKey;
        this.limits = new Limits(maxFramePayload, maxControlPayloadBytes, maxExtensionPayloadBytes);
    }

    public static Settings defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static void requireVarint62(long value, String field) {
        if (value < 0L) {
            throw new IllegalArgumentException("zmux settings " + field + " must be >= 0");
        }
        if (value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux settings " + field + " must be within varint62 range");
        }
    }

    public Builder toBuilder() {
        return new Builder()
                .initialMaxStreamDataBidiLocallyOpened(initialMaxStreamDataBidiLocallyOpened)
                .initialMaxStreamDataBidiPeerOpened(initialMaxStreamDataBidiPeerOpened)
                .initialMaxStreamDataUni(initialMaxStreamDataUni)
                .initialMaxData(initialMaxData)
                .maxIncomingStreamsBidi(maxIncomingStreamsBidi)
                .maxIncomingStreamsUni(maxIncomingStreamsUni)
                .maxFramePayload(maxFramePayload)
                .idleTimeoutMillis(idleTimeoutMillis)
                .keepaliveHintMillis(keepaliveHintMillis)
                .maxControlPayloadBytes(maxControlPayloadBytes)
                .maxExtensionPayloadBytes(maxExtensionPayloadBytes)
                .schedulerHints(schedulerHints)
                .pingPaddingKey(pingPaddingKey);
    }

    public Limits limits() {
        return limits;
    }

    public long initialMaxStreamDataBidiLocallyOpened() {
        return initialMaxStreamDataBidiLocallyOpened;
    }

    public long initialMaxStreamDataBidiPeerOpened() {
        return initialMaxStreamDataBidiPeerOpened;
    }

    public long initialMaxStreamDataUni() {
        return initialMaxStreamDataUni;
    }

    public long initialMaxData() {
        return initialMaxData;
    }

    public long maxIncomingStreamsBidi() {
        return maxIncomingStreamsBidi;
    }

    public long maxIncomingStreamsUni() {
        return maxIncomingStreamsUni;
    }

    public long maxFramePayload() {
        return maxFramePayload;
    }

    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    public long keepaliveHintMillis() {
        return keepaliveHintMillis;
    }

    public long maxControlPayloadBytes() {
        return maxControlPayloadBytes;
    }

    public long maxExtensionPayloadBytes() {
        return maxExtensionPayloadBytes;
    }

    public SchedulerHint schedulerHints() {
        return schedulerHints;
    }

    public long pingPaddingKey() {
        return pingPaddingKey;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Settings)) {
            return false;
        }
        Settings that = (Settings) other;
        return initialMaxStreamDataBidiLocallyOpened == that.initialMaxStreamDataBidiLocallyOpened
                && initialMaxStreamDataBidiPeerOpened == that.initialMaxStreamDataBidiPeerOpened
                && initialMaxStreamDataUni == that.initialMaxStreamDataUni
                && initialMaxData == that.initialMaxData
                && maxIncomingStreamsBidi == that.maxIncomingStreamsBidi
                && maxIncomingStreamsUni == that.maxIncomingStreamsUni
                && maxFramePayload == that.maxFramePayload
                && idleTimeoutMillis == that.idleTimeoutMillis
                && keepaliveHintMillis == that.keepaliveHintMillis
                && maxControlPayloadBytes == that.maxControlPayloadBytes
                && maxExtensionPayloadBytes == that.maxExtensionPayloadBytes
                && pingPaddingKey == that.pingPaddingKey
                && schedulerHints == that.schedulerHints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                initialMaxStreamDataBidiLocallyOpened,
                initialMaxStreamDataBidiPeerOpened,
                initialMaxStreamDataUni,
                initialMaxData,
                maxIncomingStreamsBidi,
                maxIncomingStreamsUni,
                maxFramePayload,
                idleTimeoutMillis,
                keepaliveHintMillis,
                maxControlPayloadBytes,
                maxExtensionPayloadBytes,
                schedulerHints,
                pingPaddingKey
        );
    }

    @Override
    public String toString() {
        return "Settings[initialMaxStreamDataBidiLocallyOpened=" + initialMaxStreamDataBidiLocallyOpened
                + ", initialMaxStreamDataBidiPeerOpened=" + initialMaxStreamDataBidiPeerOpened
                + ", initialMaxStreamDataUni=" + initialMaxStreamDataUni
                + ", initialMaxData=" + initialMaxData
                + ", maxIncomingStreamsBidi=" + maxIncomingStreamsBidi
                + ", maxIncomingStreamsUni=" + maxIncomingStreamsUni
                + ", maxFramePayload=" + maxFramePayload
                + ", idleTimeoutMillis=" + idleTimeoutMillis
                + ", keepaliveHintMillis=" + keepaliveHintMillis
                + ", maxControlPayloadBytes=" + maxControlPayloadBytes
                + ", maxExtensionPayloadBytes=" + maxExtensionPayloadBytes
                + ", schedulerHints=" + schedulerHints
                + ", pingPaddingKey=" + pingPaddingKey
                + "]";
    }

    public static final class Builder {
        private long initialMaxStreamDataBidiLocallyOpened = DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED;
        private long initialMaxStreamDataBidiPeerOpened = DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED;
        private long initialMaxStreamDataUni = DEFAULT_INITIAL_MAX_STREAM_DATA_UNI;
        private long initialMaxData = DEFAULT_INITIAL_MAX_DATA;
        private long maxIncomingStreamsBidi = DEFAULT_MAX_INCOMING_STREAMS_BIDI;
        private long maxIncomingStreamsUni = DEFAULT_MAX_INCOMING_STREAMS_UNI;
        private long maxFramePayload = DEFAULT_MAX_FRAME_PAYLOAD;
        private long idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MILLIS;
        private long keepaliveHintMillis = DEFAULT_KEEPALIVE_HINT_MILLIS;
        private long maxControlPayloadBytes = DEFAULT_MAX_CONTROL_PAYLOAD_BYTES;
        private long maxExtensionPayloadBytes = DEFAULT_MAX_EXTENSION_PAYLOAD_BYTES;
        private SchedulerHint schedulerHints = DEFAULT_SCHEDULER_HINTS;
        private long pingPaddingKey = DEFAULT_PING_PADDING_KEY;

        public Builder initialMaxStreamDataBidiLocallyOpened(long value) {
            this.initialMaxStreamDataBidiLocallyOpened = value;
            return this;
        }

        public Builder initialMaxStreamDataBidiPeerOpened(long value) {
            this.initialMaxStreamDataBidiPeerOpened = value;
            return this;
        }

        public Builder initialMaxStreamDataUni(long value) {
            this.initialMaxStreamDataUni = value;
            return this;
        }

        public Builder initialMaxData(long value) {
            this.initialMaxData = value;
            return this;
        }

        public Builder maxIncomingStreamsBidi(long value) {
            this.maxIncomingStreamsBidi = value;
            return this;
        }

        public Builder maxIncomingStreamsUni(long value) {
            this.maxIncomingStreamsUni = value;
            return this;
        }

        public Builder maxFramePayload(long value) {
            this.maxFramePayload = value;
            return this;
        }

        public Builder idleTimeoutMillis(long value) {
            this.idleTimeoutMillis = value;
            return this;
        }

        public Builder keepaliveHintMillis(long value) {
            this.keepaliveHintMillis = value;
            return this;
        }

        public Builder maxControlPayloadBytes(long value) {
            this.maxControlPayloadBytes = value;
            return this;
        }

        public Builder maxExtensionPayloadBytes(long value) {
            this.maxExtensionPayloadBytes = value;
            return this;
        }

        public Builder schedulerHints(SchedulerHint value) {
            this.schedulerHints = value == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : value;
            return this;
        }

        public Builder pingPaddingKey(long value) {
            this.pingPaddingKey = value;
            return this;
        }

        public Settings build() {
            return new Settings(
                    initialMaxStreamDataBidiLocallyOpened,
                    initialMaxStreamDataBidiPeerOpened,
                    initialMaxStreamDataUni,
                    initialMaxData,
                    maxIncomingStreamsBidi,
                    maxIncomingStreamsUni,
                    maxFramePayload,
                    idleTimeoutMillis,
                    keepaliveHintMillis,
                    maxControlPayloadBytes,
                    maxExtensionPayloadBytes,
                    schedulerHints,
                    pingPaddingKey
            );
        }
    }
}
